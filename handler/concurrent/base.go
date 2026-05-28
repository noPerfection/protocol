package concurrent

import (
	"fmt"

	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/control"
	zmq "github.com/pebbe/zmq4"
)

// Concurrent extends Handler with the frontend and instance-manager lifecycle.
type Concurrent struct {
	*base.Handler
	config                 *Config
	logger                 *log.Logger
	Frontend               *Frontend
	InstanceManager        *Parent
	Manager                base.Interface
	instanceManagerStarted bool
}

// NewConcurrent creates a handler that can run instances concurrently.
func NewConcurrent() *Concurrent {
	return &Concurrent{
		Handler:                base.New(),
		Frontend:               NewFrontend(),
		InstanceManager:        nil,
		instanceManagerStarted: false,
	}
}

func (c *Concurrent) Config() *Config {
	return c.config
}

// SetConfig adds the parameters of the concurrent handler from the config.
func (c *Concurrent) SetConfig(handler *Config) {
	c.config = handler
	c.Handler.SetConfig(handler.Handler)
	c.Frontend.SetConfig(handler)
}

// SetLogger sets the logger and creates the concurrent handler parts.
func (c *Concurrent) SetLogger(parent *log.Logger) error {
	if c.config == nil {
		return fmt.Errorf("missing configuration")
	}
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	c.logger = parent.Child(c.config.Id)

	c.InstanceManager = NewInstanceManager(c.config.Id, c.logger)
	c.Frontend.SetInstanceManager(c.InstanceManager)

	c.Manager = control.New(parent)
	c.Manager.SetConfig(c.config.Handler.ManagerHandler())
	if err := c.SetControlRoutes(); err != nil {
		return err
	}

	return nil
}

// StartInstanceManager starts the instance Manager and listens to its events.
func (c *Concurrent) StartInstanceManager() error {
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

		url := InstanceManagerEventUrl(c.config.Id)
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

			if req.CommandName() == EventReady {
				if !firstInstance {
					instanceId, err = c.InstanceManager.AddInstance(c.config.Type, &c.Routes)
					if err != nil {
						c.logger.Error("InstanceManager.AddInstance", "id", c.config.Id, "event", req.CommandName(), "type", c.config.Type, "error", err)
						continue
					}
					firstInstance = true
				}
			} else if req.CommandName() == EventInstanceAdded {
				if firstInstance && len(instanceId) > 0 {
					addedInstanceId, err := req.RouteParameters().StringValue("id")
					if err != nil {
						c.logger.Error("req.Parameters.GetString('id')", "id", c.config.Id, "event", req.CommandName(), "instanceId", instanceId, "error", err)
						continue
					}
					if addedInstanceId != instanceId {
						continue
					}
					instanceId = ""
				}
			} else if req.CommandName() == EventError {
				_, err := req.RouteParameters().StringValue("message")
				if err != nil {
					c.logger.Error("req.Parameters.GetString('message')", "id", c.config.Id, "event", req.CommandName(), "error", err)
					continue
				}

				break
			} else if req.CommandName() == EventIdle {
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

// Start the concurrent handler directly, not by goroutine.
// Will call the start function of each part.
func (c *Concurrent) Start() error {
	if c.config == nil {
		return fmt.Errorf("configuration not set")
	}
	if c.InstanceManager == nil {
		return fmt.Errorf("instance Manager not set")
	}
	if c.Frontend == nil {
		return fmt.Errorf("frontend not set")
	}

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
