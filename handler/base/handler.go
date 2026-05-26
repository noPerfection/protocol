// Package base keeps the generic Handler.
// It's not intended to be used independently.
// Other handlers should be defined based on this handler
package base

import (
	"fmt"
	"slices"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/client"
	clientConfig "github.com/noPerfection/protocol/client/config"
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
	RouteDeps              datatype.KeyValue
	depIds                 []string
	depConfigs             datatype.KeyValue
	DepClients             datatype.KeyValue
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
		RouteDeps:              datatype.New(),
		depIds:                 make([]string, 0),
		depConfigs:             datatype.New(),
		DepClients:             datatype.New(),
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

// AddDepByService adds the config of the dependency. Intended to be called by Service not by developer
func (c *Handler) AddDepByService(dep *clientConfig.Client) error {
	if c.AddedDepByService(dep.Id) {
		return fmt.Errorf("dependency configuration already added")
	}

	if !slices.Contains(c.depIds, dep.Id) {
		return fmt.Errorf("no handler depends on '%s'", dep.Id)
	}

	c.depConfigs.Set(dep.Id, dep)
	return nil
}

// AddedDepByService returns true if the configuration exists
func (c *Handler) AddedDepByService(id string) bool {
	return c.depConfigs.Exist(id)
}

// addDep adds the dependency id required by one of the Routes.
// Already added dependency id skipped.
func (c *Handler) addDep(id string) {
	if !slices.Contains(c.depIds, id) {
		c.depIds = append(c.depIds, id)
	}
}

// DepIds return the list of extension names required by this handler.
func (c *Handler) DepIds() []string {
	return c.depIds
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

// Calls handler.reply() with the error message.
func (c *Handler) replyError(socket *zmq.Socket, err error) error {
	return c.reply(socket, c.InstanceManager.MessageOps.EmptyReq().Fail(err.Error()))
}

// Route adds a route along with its handler to this handler
func (c *Handler) Route(cmd string, handle any, depIds ...string) error {
	if !route.IsHandleFunc(handle) {
		return fmt.Errorf("handle is not a valid handle function")
	}
	depAmount := route.DepAmount(handle)
	if !route.IsHandleFuncWithDeps(handle, len(depIds)) {
		return fmt.Errorf("the '%s' command handler requires %d dependencies, but route has %d dependencies", cmd, depAmount, len(depIds))
	}

	if c.Routes.Exist(cmd) {
		return nil
	}

	for _, dep := range depIds {
		c.addDep(dep)
	}

	c.Routes.Set(cmd, handle)
	if len(depIds) > 0 {
		c.RouteDeps.Set(cmd, depIds)
	}

	return nil
}

// depConfigsAdded checks that the required DepClients are added into the handler.
// If no DepClients are added by calling handler.addDep(), then it will return nil.
func (c *Handler) depConfigsAdded() error {
	if len(c.depIds) != len(c.depConfigs) {
		return fmt.Errorf("required dependencies and configurations are not matching")
	}
	for _, id := range c.depIds {
		if !c.depConfigs.Exist(id) {
			return fmt.Errorf("'%s' dependency configuration not added", id)
		}
	}

	return nil
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Handler) Type() config.HandlerType {
	if c.config == nil {
		return config.UnknownType
	}
	return c.config.Type
}

// initDepClients will set up the extension clients for this handler.
// It will be called by c.Start(), automatically.
//
// The reason for why we call it by c.Start() is due to the thread-safety.
//
// The handler is intended to be called as the goroutine.
// And if the sockets are not initiated within the same goroutine,
// then zeromq doesn't guarantee the socket work as it's intended.
func (c *Handler) initDepClients() error {
	for _, depInterface := range c.depConfigs {
		depConfig := depInterface.(*clientConfig.Client)

		if c.DepClients.Exist(depConfig.Id) {
			return fmt.Errorf("DepClients.Exist('%s')", depConfig.Id)
		}

		depClient, err := client.New(depConfig)
		if err != nil {
			return fmt.Errorf("client.NewReq('%s', '%d'): %w", depConfig.Id, depConfig.Port, err)
		}
		c.DepClients.Set(depConfig.Id, depClient)
	}

	return nil
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
					instanceId, err = c.InstanceManager.AddInstance(c.config.Type, &c.Routes, &c.RouteDeps, &c.DepClients)
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
	if err := c.depConfigsAdded(); err != nil {
		return fmt.Errorf("depConfigsAdded: %w", err)
	}
	if c.InstanceManager == nil {
		return fmt.Errorf("instance Manager not set")
	}
	if c.Frontend == nil {
		return fmt.Errorf("frontend not set")
	}
	if err := c.initDepClients(); err != nil {
		return fmt.Errorf("initDepClients: %w", err)
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
