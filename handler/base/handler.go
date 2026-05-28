// Package base keeps the generic Handler.
// It's not intended to be used independently.
// Other handlers should be defined based on this handler
package base

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/frontend"
	"github.com/noPerfection/protocol/handler/handler_manager"
	"github.com/noPerfection/protocol/handler/instance_manager"
	"github.com/noPerfection/protocol/handler/route"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// The Handler is the socket wrapper for the zeromq socket.
type Handler struct {
	config                 *config.Handler
	socket                 *zmq.Socket
	logger                 *log.Logger
	Routes                 datatype.KeyValue
	Frontend               *frontend.Frontend
	InstanceManager        *instance_manager.Parent
	instanceManagerStarted bool
	Manager                *handler_manager.HandlerManager
	status                 string
}

// New handler
func New() *Handler {
	return &Handler{
		logger:                 nil,
		Routes:                 datatype.New(),
		Frontend:               frontend.New(),
		InstanceManager:        nil,
		instanceManagerStarted: false,
		Manager:                nil,
		status:                 "",
	}
}

// IsRouteExist returns true if the given route exists
func (c *Handler) IsRouteExist(command string) bool {
	return c.Routes.Exist(command)
}

// RouteCommands returns list of all route commands
func (c *Handler) RouteCommands() []string {
	commands := make([]string, len(c.Routes))

	i := 0
	for command := range c.Routes {
		commands[i] = command
		i++
	}

	return commands
}

func (c *Handler) Config() *config.Handler {
	return c.config
}

// SetConfig adds the parameters of the handler from the config.
//
// Sets Frontend configuration as well.
func (c *Handler) SetConfig(handler *config.Handler) {
	c.config = handler
	c.Frontend.SetConfig(handler)
}

// SetLogger sets the logger (depends on context).
//
// Creates instance Manager.
//
// Creates handler Manager.
func (c *Handler) SetLogger(parent *log.Logger) error {
	if c.config == nil {
		return fmt.Errorf("missing configuration")
	}
	logger := parent.Child(c.config.Id)
	c.logger = logger

	c.InstanceManager = instance_manager.New(c.config.Id, c.logger)
	c.Frontend.SetInstanceManager(c.InstanceManager)

	c.Manager = handler_manager.New(parent, c.Frontend, c.InstanceManager, c.StartInstanceManager)
	c.Manager.SetConfig(c.config)

	return nil
}

// A reply sends to the caller the message.
//
// If a handler doesn't support replying (for example, PULL handler),
// then it returns success.
func (c *Handler) reply(socket *zmq.Socket, message message.ReplyInterface) error {
	if !config.CanReply(c.config.Type) {
		return nil
	}

	reply, err := message.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("message.ZmqEnvelope: %w", err)
	}
	if _, err := socket.SendMessage(reply); err != nil {
		return fmt.Errorf("recv error replying error %w" + err.Error())
	}

	return nil
}

// Route adds a route along with its handler to this handler
func (c *Handler) Route(cmd string, handle any) error {
	if !route.IsHandleFunc(handle) {
		return fmt.Errorf("handle is not a valid handle function")
	}

	if c.Routes.Exist(cmd) {
		return nil
	}

	c.Routes.Set(cmd, handle)

	return nil
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Handler) Type() config.HandlerType {
	if c.config == nil {
		return config.UnknownType
	}
	return c.config.Type
}

// StartInstanceManager starts the instance Manager and listens to its events
func (c *Handler) StartInstanceManager() error {
	ready := make(chan error)

	go func(ready chan error) {
		socket, err := zmq.NewSocket(zmq.SUB)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket('sub'): %w", err)
			return
		}

		if err := socket.SetSubscribe(""); err != nil {
			ready <- fmt.Errorf("socket.SetSubscriber(''): %w", err)
			return
		}

		url := config.InstanceManagerEventUrl(c.config.Id)
		err = socket.Connect(url)
		if err != nil {
			ready <- fmt.Errorf("socket.Connect('%s'): %w", url, err)
			return
		}
		c.instanceManagerStarted = true

		err = c.InstanceManager.Start()
		if err != nil {
			ready <- fmt.Errorf("c.InstanceManager.Start: %w", err)
			return
		}

		// The first Instance created by handler when the instance Manager is ready.
		firstInstance := false
		// Verify that the first instance was added.
		instanceId := ""

		// Notify that instance manager, and it's subscriber are ready.
		// StartInstanceManager will return back to the caller.
		//
		// The errors thereafter are logged on std error.
		ready <- nil

		for {
			raw, err := socket.RecvMessage(0)
			if err != nil {
				c.logger.Error("eventSocket.RecvMessage", "id", c.config.Id, "error", err)
				break
			}

			req, err := c.InstanceManager.MessageOps.NewReq(raw)
			if err != nil {
				c.logger.Error("eventSocket: convert raw to message", "id", c.config.Id, "message", raw, "error", err)
				continue
			}

			if req.CommandName() == instance_manager.EventReady {
				if !firstInstance {
					instanceId, err = c.InstanceManager.AddInstance(c.config.Type, &c.Routes)
					if err != nil {
						c.logger.Error("InstanceManager.AddInstance", "id", c.config.Id, "event", req.CommandName(), "type", c.config.Type, "error", err)
						continue
					}
					firstInstance = true
				}
			} else if req.CommandName() == instance_manager.EventInstanceAdded {
				if firstInstance && len(instanceId) > 0 {
					addedInstanceId, err := req.RouteParameters().StringValue("id")
					if err != nil {
						c.logger.Error("req.Parameters.GetString('id')", "id", c.config.Id, "event", req.CommandName(), "instanceId", instanceId, "error", err)
						continue
					}
					if addedInstanceId != instanceId {
						continue
					} else {
						instanceId = ""
					}
				}
			} else if req.CommandName() == instance_manager.EventError {
				_, err := req.RouteParameters().StringValue("message")
				if err != nil {
					c.logger.Error("req.Parameters.GetString('message')", "id", c.config.Id, "event", req.CommandName(), "error", err)
					continue
				}

				break
			} else if req.CommandName() == instance_manager.EventIdle {
				closeSignal, _ := req.RouteParameters().BoolValue("close")
				if closeSignal {
					break
				}
			} else {
				c.logger.Warn("unhandled instance_manager event", "event", req.CommandName(), "parameters", req.RouteParameters())
			}
		}

		err = socket.Close()
		if err != nil {
			c.logger.Error("failed to close instance Manager sub", "id", c.config.Id, "error", err)
		}

		c.instanceManagerStarted = false
	}(ready)

	return <-ready
}

func (c *Handler) Status() string {
	return c.status
}

// Start the handler directly, not by goroutine.
// Will call the start function of each part.
func (c *Handler) Start() error {
	if c.config == nil {
		return fmt.Errorf("configuration not set")
	}
	if c.InstanceManager == nil {
		return fmt.Errorf("instance Manager not set")
	}
	if c.Frontend == nil {
		return fmt.Errorf("frontend not set")
	}

	// Adding the first instance Manager
	if err := c.Frontend.Start(); err != nil {
		return fmt.Errorf("c.Frontend.Start: %w", err)
	}

	if err := c.StartInstanceManager(); err != nil {
		return fmt.Errorf("c.StartInstanceManager: %w", err)
	}
	if err := c.Manager.Start(); err != nil {
		return fmt.Errorf("c.Manager.Start: %w", err)
	}

	return nil
}

// Does nothing, simply returns the data
var anyHandler = func(request message.RequestInterface) message.ReplyInterface {
	replyParameters := datatype.New().Set("route", request.CommandName())

	reply := request.Ok(replyParameters)
	return reply
}

func AnyRoute(handler Interface) error {
	if err := handler.Route(route.Any, anyHandler); err != nil {
		return fmt.Errorf("failed to '%s' route into the handler: %w", route.Any, err)
	}
	return nil
}

func requiredMetadata() []string {
	return []string{"Identity", "pub_key"}
}
