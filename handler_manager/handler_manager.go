// Package handler_manager creates a socket that manages the handler
package handler_manager

import (
	"fmt"
	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/datatype-lib/message"
	"github.com/sds-framework/handler-lib/config"
	"github.com/sds-framework/handler-lib/frontend"
	instances "github.com/sds-framework/handler-lib/instance_manager"
	"github.com/sds-framework/handler-lib/route"
	"github.com/sds-framework/log-lib"
	zmq "github.com/pebbe/zmq4"
	"time"
)

const (
	Incomplete  = "incomplete"
	Ready       = "ready"
	SocketIdle  = "idle"
	SocketReady = "ready"
)

type HandlerManager struct {
	logger               *log.Logger
	frontend             *frontend.Frontend
	instanceManager      *instances.Parent
	startInstanceManager func() error
	config               *config.Handler
	routes               key_value.KeyValue
	routeDeps            key_value.KeyValue
	depClients           key_value.KeyValue
	status               string // It's the socket status, not the handler status
	close                bool
}

// New creates a new HandlerManager
func New(parent *log.Logger, frontend *frontend.Frontend, instanceManager *instances.Parent, startInstanceManager func() error) *HandlerManager {
	logger := parent.Child("handler_manager")

	m := &HandlerManager{
		frontend:             frontend,
		instanceManager:      instanceManager,
		startInstanceManager: startInstanceManager,
		routes:               key_value.New(),
		routeDeps:            key_value.New(),
		depClients:           key_value.New(),
		status:               SocketIdle,
		logger:               logger,
	}

	// Add the default routes
	m.setRoutes()

	return m
}

// SetConfig sets the link to the configuration of the handler
func (m *HandlerManager) SetConfig(config *config.Handler) {
	m.config = config
}

// Status returns the socket status of the HandlerManager.
func (m *HandlerManager) Status() string {
	return m.status
}

// PartStatuses returns statuses of the base handler parts.
//
// Intended to be used by the extending handlers.
func (m *HandlerManager) PartStatuses() key_value.KeyValue {
	frontendStatus := m.frontend.Status()
	instanceStatus := m.instanceManager.Status()

	parts := key_value.New().
		Set("frontend", frontendStatus).
		Set("instance_manager", instanceStatus)

	return parts
}

// SetClose adds a close signal to the queue.
func (m *HandlerManager) SetClose(req message.RequestInterface) message.ReplyInterface {
	m.close = true

	return req.Ok(key_value.New())
}

// setRoutes sets the default command handlers
func (m *HandlerManager) setRoutes() {
	// Requesting status which is calculated from statuses of the handler parts
	onStatus := func(req message.RequestInterface) message.ReplyInterface {
		frontendStatus := m.frontend.Status()
		instanceStatus := m.instanceManager.Status()

		params := key_value.New()

		if frontendStatus == frontend.RUNNING && instanceStatus == instances.Running {
			params.Set("status", Ready)
		} else {
			parts := key_value.New().
				Set("frontend", frontendStatus).
				Set("instance_manager", instanceStatus)

			params.Set("status", Incomplete).
				Set("parts", parts)
		}

		return req.Ok(params)
	}

	// Stop one of the parts.
	// For example, frontend or instance_manager
	onClosePart := func(req message.RequestInterface) message.ReplyInterface {
		part, err := req.RouteParameters().StringValue("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}

		if part == "frontend" {
			if m.frontend.Status() != frontend.RUNNING {
				return req.Fail("frontend not running")
			} else {
				if err := m.frontend.Close(); err != nil {
					return req.Fail(fmt.Sprintf("failed to close the frontend: %v", err))
				}
				return req.Ok(key_value.New())
			}
		} else if part == "instance_manager" {
			if m.instanceManager.Status() != instances.Running {
				return req.Fail("instance_manager not running")
			} else {
				m.instanceManager.Close()
				return req.Ok(key_value.New())
			}
		} else {
			return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
		}
	}

	onRunPart := func(req message.RequestInterface) message.ReplyInterface {
		part, err := req.RouteParameters().StringValue("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}

		if part == "frontend" {
			if m.frontend.Status() == frontend.RUNNING {
				return req.Fail("frontend running")
			} else {
				err := m.frontend.Start()
				if err != nil {
					return req.Fail(fmt.Sprintf("frontend.Start: %v", err))
				}
				return req.Ok(key_value.New())
			}
		} else if part == "instance_manager" {
			if m.instanceManager.Status() == instances.Running {
				return req.Fail("instance_manager running")
			} else {
				err := m.startInstanceManager()
				if err != nil {
					return req.Fail(fmt.Sprintf("m.startInstanceManager: %v", err))
				}
				return req.Ok(key_value.New())
			}
		} else {
			return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
		}
	}

	onInstanceAmount := func(req message.RequestInterface) message.ReplyInterface {
		instanceAmount := len(m.instanceManager.Instances())
		return req.Ok(key_value.New().Set("instance_amount", instanceAmount))
	}

	// Returns queue amount and currently processed images amount
	onMessageAmount := func(req message.RequestInterface) message.ReplyInterface {
		params := key_value.New().
			Set("queue_length", m.frontend.QueueLen()).
			Set("processing_length", m.frontend.ProcessingLen())
		return req.Ok(params)
	}

	// Add a new instance, but it doesn't check that instance was added
	onAddInstance := func(req message.RequestInterface) message.ReplyInterface {
		instanceId, err := m.instanceManager.AddInstance(m.config.Type, &m.routes, &m.routeDeps, &m.depClients)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", m.config.Type, err))
		}

		params := key_value.New().Set("instance_id", instanceId)
		return req.Ok(params)
	}

	// Delete the instance
	onDeleteInstance := func(req message.RequestInterface) message.ReplyInterface {
		instanceId, err := req.RouteParameters().StringValue("instance_id")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('instance_id'): %v", err))
		}

		err = m.instanceManager.DeleteInstance(instanceId, false)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.DeleteInstance('%s'): %v", instanceId, err))
		}

		return req.Ok(key_value.New())
	}

	onParts := func(req message.RequestInterface) message.ReplyInterface {
		parts := []string{
			"frontend",
			"instance_manager",
		}
		messageTypes := []string{
			"queue_length",
			"processing_length",
		}

		params := key_value.New().
			Set("parts", parts).
			Set("message_types", messageTypes)

		return req.Ok(params)
	}

	onConfig := func(req message.RequestInterface) message.ReplyInterface {
		params := key_value.New().Set("config", m.config)
		return req.Ok(params)
	}

	m.routes.Set(config.HandlerStatus, onStatus)
	m.routes.Set(config.ClosePart, onClosePart)
	m.routes.Set(config.RunPart, onRunPart)
	m.routes.Set(config.InstanceAmount, onInstanceAmount)
	m.routes.Set(config.MessageAmount, onMessageAmount)
	m.routes.Set(config.AddInstance, onAddInstance)
	m.routes.Set(config.DeleteInstance, onDeleteInstance)
	m.routes.Set(config.Parts, onParts)
	m.routes.Set(config.HandlerClose, m.SetClose)
	m.routes.Set(config.HandlerConfig, onConfig)
}

// Route overrides the default route with the given handle.
// Returns an error if the command is not supported.
// Returns an error if HandlerManager is running.
func (m *HandlerManager) Route(cmd string, handle route.HandleFunc0) error {
	if m.status == SocketReady {
		return fmt.Errorf("can not overwrite handler when HandlerManager is running")
	}
	if !m.routes.Exist(cmd) {
		return fmt.Errorf("'%s' command not found", cmd)
	}
	m.routes.Set(cmd, handle)

	return nil
}

// Start the HandlerManager
func (m *HandlerManager) Start() error {
	if m.config == nil {
		return fmt.Errorf("no config")
	}

	ready := make(chan error)

	go func(ready chan error) {
		socket, err := zmq.NewSocket(zmq.ROUTER)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket: %w", err)
			return
		}

		url := config.ManagerUrl(m.config.Id)
		err = socket.Bind(url)
		if err != nil {
			ready <- fmt.Errorf("socket.Bind('%s'): %w", url, err)
			return
		}

		poller := zmq.NewPoller()
		poller.Add(socket, zmq.POLLIN)

		m.status = SocketReady

		// Exit from Start function
		ready <- nil

		for {
			if m.close {
				err = poller.RemoveBySocket(socket)
				if err != nil {
					m.logger.Error("poller.RemoveBySocket", "error", err)
				}
				break
			}

			sockets, err := poller.Poll(time.Millisecond)
			if err != nil {
				m.logger.Error("poller.Poll", "error", err)
				break
			}

			if len(sockets) == 0 {
				continue
			}

			raw, err := socket.RecvMessage(0)
			if err != nil {
				m.logger.Error("socket.RecvMessage", "error", err)
				break
			}

			req, err := message.NewReq(raw)
			if err != nil {
				m.logger.Error("message.NewReq", "messages", raw, "error", err)
				continue
			}

			handleInterface, depNames, err := route.Route(req.CommandName(), m.routes, m.routeDeps)
			if err != nil {
				reply := req.Fail(fmt.Sprintf("route.Route(%s): %v", req.CommandName(), err))
				replyStr, err := reply.ZmqEnvelope()
				if err != nil {
					reply := req.Fail(fmt.Sprintf("failed to convert reply [%v] to string", reply))
					replyStr, err := reply.ZmqEnvelope()
					if err != nil {
						m.logger.Error("req.Fail.String", "request", req, "reply", reply, "error", err)
						continue
					}
					_, err = socket.SendMessage(replyStr)
					if err != nil {
						m.logger.Error("socket.SendMessage", "reply", reply, "error", err)
					}
				} else {
					_, err = socket.SendMessage(replyStr)
					if err != nil {
						m.logger.Error("socket.SendMessage", "reply", reply, "error", err)
					}
				}
				continue
			}

			depClients := route.FilterExtensionClients(depNames, m.depClients)

			reply := route.Handle(req, handleInterface, depClients)
			replyStr, err := reply.ZmqEnvelope()
			if err != nil {
				reply := req.Fail(fmt.Sprintf("failed to convert handle reply [%v] to string", reply))
				replyStr, err := reply.ZmqEnvelope()
				if err != nil {
					m.logger.Error("req.Fail.String", "request", req, "reply", reply, "error", err)
					continue
				}
				_, err = socket.SendMessage(replyStr)
				if err != nil {
					m.logger.Error("socket.SendMessage", "reply", reply, "error", err)
					continue
				}
			} else {
				_, err = socket.SendMessage(replyStr)
				if err != nil {
					m.logger.Error("socket.SendMessage", "reply", reply, "error", err)
					continue
				}
			}
		}

		m.status = SocketIdle
		m.close = false

		//
		// Close the parts
		//

		// Since routes are over-writeable, as extending handlers might add new parts.
		// We don't call frontend or instanceManager directly.
		partsHandle := m.routes[config.Parts].(func(message.RequestInterface) message.ReplyInterface)
		closeHandle := m.routes[config.ClosePart].(func(message.RequestInterface) message.ReplyInterface)

		defReq := message.Request{Command: config.Parts, Parameters: key_value.New()}
		var req message.RequestInterface = &defReq

		partsReply := partsHandle(req)
		if !partsReply.IsOK() {
			m.logger.Error("failed to handle", "command", config.Parts, "request", req, "reply.Message", partsReply.ErrorMessage())
		} else {
			parts, err := partsReply.ReplyParameters().StringsValue("parts")
			if err != nil {
				m.logger.Error("reply.Parameters.GetStringList", "argument", "parts", "command", config.Parts, "request", req, "error", err)
			} else {
				defReq.Command = config.ClosePart
				req = &defReq
				for _, part := range parts {
					req.RouteParameters().Set("part", part)

					// if it's failed, it might be because part already closed
					_ = closeHandle(req)
				}
			}
		}

		closeErr := socket.Close()
		if closeErr != nil {
			m.logger.Error("socket.Close", "error", err)
		}
	}(ready)

	return <-ready
}
