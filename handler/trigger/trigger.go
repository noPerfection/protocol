package trigger

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	clientConfig "github.com/noPerfection/protocol/client/config"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/frontend"
	"github.com/noPerfection/protocol/handler/handler_manager"
	instances "github.com/noPerfection/protocol/handler/instance_manager"
	"github.com/noPerfection/protocol/handler/route"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	triggerType        = config.ReplierType
	BroadcasterRunning = "running"
	BroadcasterIdle    = "idle"
)

type Trigger struct {
	*base.Handler
	socket       *zmq.Socket
	closePub     bool
	port         uint64
	id           string
	logger       *log.Logger
	handlerType  config.HandlerType
	broadcasting *datatype.Queue
}

// New trigger-able handler
func New() *Trigger {
	handler := &Trigger{
		Handler:      base.New(),
		closePub:     false,
		socket:       nil,
		broadcasting: datatype.NewQueue(),
	}
	return handler
}

// TriggerClient is the client parameters to trigger this handler
func (handler *Trigger) TriggerClient() *clientConfig.Client {
	handlerConfig := handler.Handler.Config()
	client := &clientConfig.Client{
		ServiceUrl: "",
		Id:         handlerConfig.Id,
		Port:       handlerConfig.Port,
		TargetType: config.SocketType(triggerType),
	}
	return client.UrlFunc(clientConfig.Url)
}

// Route adds a route along with its handler to this handler
func (handler *Trigger) Route(_ string, _ interface{}) error {
	return fmt.Errorf("trigger doesn't support routing")
}

func (handler *Trigger) Config() *config.Trigger {
	baseConfig := handler.Handler.Config()
	trigger := config.Trigger{
		Handler:       baseConfig,
		BroadcastId:   handler.id,
		BroadcastPort: handler.port,
		BroadcastType: handler.handlerType,
	}
	return &trigger
}

// SetConfig adds the parameters of the handler from the config.
//
// Sets Frontend configuration as well.
func (handler *Trigger) SetConfig(trigger *config.Trigger) {
	// The broadcaster
	handler.port = trigger.BroadcastPort
	handler.id = trigger.BroadcastId
	handler.handlerType = trigger.BroadcastType

	// Todo change to the puller
	trigger.Handler.Type = triggerType

	handler.Handler.SetConfig(trigger.Handler)
}

func (handler *Trigger) SetLogger(logger *log.Logger) error {
	handler.logger = logger

	return handler.Handler.SetLogger(logger)
}

// startBroadcaster creates a socket that will be linked by the user
func (handler *Trigger) startBroadcaster() error {
	ready := make(chan error)

	go func(ready chan error) {
		socket, err := zmq.NewSocket(config.SocketType(handler.handlerType))
		if err != nil {
			ready <- fmt.Errorf("new_socket('%s'): %v", handler.handlerType, err)
			return
		}

		pubUrl := config.ExternalUrl(handler.id, handler.port)
		err = socket.Bind(pubUrl)
		if err != nil {
			ready <- fmt.Errorf("socket.Bind('%s'): %v", pubUrl, err)
			return
		}

		handler.socket = socket

		// Socket preparation finished without any error, return back from startBroadcaster
		ready <- nil

		for {
			if handler.closePub {
				break
			}
			if handler.broadcasting.IsEmpty() {
				continue
			}

			req := handler.broadcasting.Pop().(message.RequestInterface)
			reqStr, err := req.ZmqEnvelope()
			if err != nil {
				handler.logger.Error("handler.broadcasting.Pop", "type", "message.Request", "error", err)
				break
			}
			_, err = socket.SendMessageDontwait(reqStr)
			if err != nil {
				handler.logger.Error("socket.SendMessageDontWait", "request", reqStr, "error", err)
				break
			}
		}

		handler.closePub = false
		err = socket.Close()
		if err != nil {
			handler.logger.Error("socket.Close", "error", err)
			return
		}

		handler.socket = nil
	}(ready)

	return <-ready
}

func (handler *Trigger) onTrigger(req message.RequestInterface) message.ReplyInterface {
	if handler.broadcasting.IsFull() {
		return req.Fail("broadcasting queue full")
	}

	handler.broadcasting.Push(req)

	return req.Ok(datatype.New())
}

func (handler *Trigger) broadcasterStatus() string {
	if handler.socket == nil {
		return BroadcasterIdle
	}
	return BroadcasterRunning
}

// Start the trigger directly, not by goroutine.
//
// The Trigger-able handlers can have only one instance
func (handler *Trigger) Start() error {
	m := handler.Handler

	if m.Config() == nil {
		return fmt.Errorf("configuration not set")
	}
	if !config.CanTrigger(handler.handlerType) {
		return fmt.Errorf("the '%s' handler type in configuration is not triggerable", handler.handlerType)
	}
	if m.Manager == nil {
		return fmt.Errorf("handler manager not set. call SetConfig and SetLogger first")
	}

	if err := m.Route(route.Any, handler.onTrigger); err != nil {
		return fmt.Errorf("handler.Route: %w", err)
	}

	onStatus := func(req message.RequestInterface) message.ReplyInterface {
		partStatuses := m.Manager.PartStatuses()
		frontendStatus, err := partStatuses.StringValue("frontend")
		if err != nil {
			return req.Fail(fmt.Sprintf("partStatuses.GetString('frontend'): %v", err))
		}
		instanceStatus, err := partStatuses.StringValue("instance_manager")
		if err != nil {
			return req.Fail(fmt.Sprintf("partStatuses.GetString('instance_manager'): %v", err))
		}
		broadcasterStatus := handler.broadcasterStatus()

		params := datatype.New()

		if frontendStatus == frontend.RUNNING &&
			instanceStatus == instances.Running &&
			broadcasterStatus == BroadcasterRunning {
			params.Set("status", handler_manager.Ready)
		} else {
			partStatuses.Set("broadcaster", broadcasterStatus)
			params.Set("status", handler_manager.Incomplete).
				Set("parts", partStatuses)
		}

		return req.Ok(params)
	}

	// add a routing that redirects the messages to the trigger
	onAddInstance := func(req message.RequestInterface) message.ReplyInterface {
		if len(m.InstanceManager.Instances()) != 0 {
			return req.Fail("only one instance allowed in sync replier")
		}

		instanceId, err := m.InstanceManager.AddInstance(m.Config().Type, &m.Routes)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", m.Config().Type, err))
		}

		params := datatype.New().Set("instance_id", instanceId)
		return req.Ok(params)
	}
	onClose := func(req message.RequestInterface) message.ReplyInterface {
		part, err := req.RouteParameters().StringValue("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}

		switch part {
		case "frontend":
			if m.Frontend.Status() != frontend.RUNNING {
				return req.Fail("frontend not running")
			} else {
				if err := m.Frontend.Close(); err != nil {
					return req.Fail(fmt.Sprintf("failed to close the frontend: %v", err))
				}
				return req.Ok(datatype.New())
			}
		case "instance_manager":
			if m.InstanceManager.Status() != instances.Running {
				return req.Fail("instance manager not running")
			} else {
				m.InstanceManager.Close()
				return req.Ok(datatype.New())
			}
		case "broadcaster":
			handler.closePub = true
			return req.Ok(datatype.New())
		default:
			return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
		}
	}
	onRunPart := func(req message.RequestInterface) message.ReplyInterface {
		part, err := req.RouteParameters().StringValue("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}

		if part == "frontend" {
			if m.Frontend.Status() == frontend.RUNNING {
				return req.Fail("frontend running")
			} else {
				err := m.Frontend.Start()
				if err != nil {
					return req.Fail(fmt.Sprintf("m.Frontend.Start: %v", err))
				}
				return req.Ok(datatype.New())
			}
		} else if part == "instance_manager" {
			if m.InstanceManager.Status() == instances.Running {
				return req.Fail("instance manager running")
			} else {
				err := m.StartInstanceManager()
				if err != nil {
					return req.Fail(fmt.Sprintf("base.StartInstanceManager: %v", err))
				}
				return req.Ok(datatype.New())
			}
		} else if part == "broadcaster" {
			err := handler.startBroadcaster()
			if err != nil {
				return req.Fail(fmt.Sprintf("trigger.startBroadcaster: %v", err))
			}
			return req.Ok(datatype.New())
		} else {
			return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
		}
	}
	onMessageAmount := func(req message.RequestInterface) message.ReplyInterface {
		params := datatype.New().
			Set("queue_length", m.Frontend.QueueLen()).
			Set("processing_length", m.Frontend.ProcessingLen()).
			Set("broadcasting_length", handler.broadcasting.Len())
		return req.Ok(params)
	}

	onParts := func(req message.RequestInterface) message.ReplyInterface {
		parts := []string{
			"frontend",
			"instance_manager",
			"broadcaster",
		}
		messageTypes := []string{
			"queue_length",
			"processing_length",
			"broadcasting_length",
		}

		params := datatype.New().
			Set("parts", parts).
			Set("message_types", messageTypes)

		return req.Ok(params)
	}

	if err := m.Manager.Route(config.AddInstance, onAddInstance); err != nil {
		return fmt.Errorf("overwriting handler manager 'add_instance' failed: %w", err)
	}
	if err := m.Manager.Route(config.ClosePart, onClose); err != nil {
		return fmt.Errorf("overwriting handler manager 'close' failed: %w", err)
	}
	if err := m.Manager.Route(config.RunPart, onRunPart); err != nil {
		return fmt.Errorf("overwriting handler manager 'run' failed: %w", err)
	}
	if err := m.Manager.Route(config.MessageAmount, onMessageAmount); err != nil {
		return fmt.Errorf("overwriting handler manager 'message_amount' failed: %w", err)
	}
	if err := m.Manager.Route(config.Parts, onParts); err != nil {
		return fmt.Errorf("overwriting handler manager 'parts' failed: %w", err)
	}
	if err := m.Manager.Route(config.HandlerStatus, onStatus); err != nil {
		return fmt.Errorf("overwriting handler manager 'parts' failed: %w", err)
	}

	if err := handler.startBroadcaster(); err != nil {
		return fmt.Errorf("trigger.startBroadcaster: %w", err)
	}

	if err := m.Start(); err != nil {
		return fmt.Errorf("base.Start: %w", err)
	}

	return nil
}
