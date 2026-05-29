package trigger

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	clientConfig "github.com/noPerfection/protocol/client/config"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/concurrent"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
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
	instance     *concurrent.Instance
	Manager      base.Interface
	messageOps   *message.Operations
	status       string
}

// New trigger-able handler
func New() *Trigger {
	handler := &Trigger{
		Handler:      base.New(),
		closePub:     false,
		socket:       nil,
		broadcasting: datatype.NewQueue(),
		messageOps:   message.DefaultMessage(),
	}
	return handler
}

// TriggerClient is the client parameters to trigger this handler
func (handler *Trigger) TriggerClient() *clientConfig.Client {
	handlerConfig := handler.Handler.Config()
	client := clientConfig.New("", handlerConfig.Id, handlerConfig.Port, config.SocketType(triggerType))
	return client.UrlFunc(clientConfig.Url)
}

// Route adds a route along with its handler to this handler
func (handler *Trigger) Route(_ string, _ base.HandleFunc) error {
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
	if err := handler.Handler.SetLogger(logger); err != nil {
		return err
	}
	handler.logger = logger.Child(handler.Config().Id)
	handler.Manager = control.New(logger)
	handler.Manager.SetConfig(control.CreateInternalConfig(handler.Handler.Config()))

	return nil
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

		pubUrl := message.NewEndpoint(handler.id, handler.port).HandlerUrl()
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

func (handler *Trigger) Status() string {
	return handler.status
}

// Start the trigger directly, not by goroutine.
//
// The Trigger-able handlers can have only one instance
func (handler *Trigger) Start() error {
	if handler.Config() == nil {
		return fmt.Errorf("configuration not set")
	}
	if !config.CanTrigger(handler.handlerType) {
		return fmt.Errorf("the '%s' handler type in configuration is not triggerable", handler.handlerType)
	}
	if handler.logger == nil {
		return fmt.Errorf("logger not set")
	}

	if err := handler.Handler.Route(base.Any, handler.onTrigger); err != nil {
		return fmt.Errorf("handler.Route: %w", err)
	}

	parent, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket('PULL'): %w", err)
	}
	parentUrl := concurrent.ParentUrl(handler.Config().Id)
	if err := parent.Bind(parentUrl); err != nil {
		_ = parent.Close()
		return fmt.Errorf("parent.Bind('%s'): %w", parentUrl, err)
	}

	instanceId := handler.Config().Id + "_instance"
	handler.instance = concurrent.NewInstance(triggerType, instanceId, handler.Config().Id, handler.logger)
	handler.instance.SetRouter(handler.Handler)
	handler.instance.SetMessageOps(handler.messageOps)
	if err := handler.instance.Start(); err != nil {
		_ = parent.Close()
		return fmt.Errorf("instance.Start: %w", err)
	}

	instanceClient, err := zmq.NewSocket(clientConfig.TargetToClient(config.SocketType(triggerType)))
	if err != nil {
		_ = parent.Close()
		return fmt.Errorf("zmq.NewSocket('instanceClient'): %w", err)
	}
	instanceUrl := concurrent.InstanceHandleUrl(handler.Config().Id, instanceId)
	if err := instanceClient.Connect(instanceUrl); err != nil {
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("instanceClient.Connect('%s'): %w", instanceUrl, err)
	}

	triggerSocket, err := zmq.NewSocket(config.SocketType(triggerType))
	if err != nil {
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("zmq.NewSocket('%s'): %w", triggerType, err)
	}
	triggerUrl := handler.Config().HandlerUrl()
	if err := triggerSocket.Bind(triggerUrl); err != nil {
		_ = triggerSocket.Close()
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("triggerSocket.Bind('%s'): %w", triggerUrl, err)
	}

	manager, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		_ = triggerSocket.Close()
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("zmq.NewSocket('manager'): %w", err)
	}
	managerConfig := control.CreateInternalConfig(handler.Config().Handler)
	managerUrl := managerConfig.HandlerUrl()
	if err := manager.Bind(managerUrl); err != nil {
		_ = manager.Close()
		_ = triggerSocket.Close()
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("manager.Bind('%s'): %w", managerUrl, err)
	}

	if err := handler.startBroadcaster(); err != nil {
		_ = manager.Close()
		_ = triggerSocket.Close()
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("trigger.startBroadcaster: %w", err)
	}

	handler.Handler.SetClose(false)
	handler.status = base.SocketReady
	go handler.run(parent, triggerSocket, instanceClient, manager)

	return nil
}

func (handler *Trigger) run(parent *zmq.Socket, triggerSocket *zmq.Socket, instanceClient *zmq.Socket, manager *zmq.Socket) {
	poller := zmq.NewPoller()
	poller.Add(parent, zmq.POLLIN)
	poller.Add(triggerSocket, zmq.POLLIN)
	poller.Add(manager, zmq.POLLIN)

	for !handler.Handler.Closed() {
		sockets, err := poller.Poll(time.Millisecond)
		if err != nil {
			handler.status = err.Error()
			break
		}

		for _, polled := range sockets {
			switch polled.Socket {
			case parent:
				handler.handleInstanceStatus(parent)
			case triggerSocket:
				if err := handler.forwardToInstance(triggerSocket, instanceClient); err != nil {
					handler.logger.Error("trigger.forwardToInstance", "error", err)
					handler.status = err.Error()
				}
			case manager:
				if err := handler.handleManager(manager); err != nil {
					handler.logger.Error("trigger.handleManager", "error", err)
					handler.status = err.Error()
				}
			}
		}
	}

	handler.closePub = true
	handler.closeInstance(false)
	_ = poller.RemoveBySocket(parent)
	_ = poller.RemoveBySocket(triggerSocket)
	_ = poller.RemoveBySocket(manager)
	_ = manager.Close()
	_ = triggerSocket.Close()
	_ = instanceClient.Close()
	_ = parent.Close()
	handler.status = ""
}

func (handler *Trigger) handleInstanceStatus(parent *zmq.Socket) {
	raw, err := parent.RecvMessage(0)
	if err != nil {
		handler.logger.Error("parent.RecvMessage", "error", err)
		return
	}
	if _, err := handler.messageOps.NewReq(raw); err != nil {
		handler.logger.Error("messageOps.NewReq", "messages", raw, "error", err)
	}
}

func (handler *Trigger) forwardToInstance(triggerSocket *zmq.Socket, instanceClient *zmq.Socket) error {
	raw, err := triggerSocket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("triggerSocket.RecvMessage: %w", err)
	}
	if len(raw) < 3 {
		return handler.replyTriggerError(triggerSocket, raw, "trigger request missing identity or separator")
	}
	if _, err := instanceClient.SendMessage("", raw[2:]); err != nil {
		return fmt.Errorf("instanceClient.SendMessage: %w", err)
	}
	reply, err := instanceClient.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("instanceClient.RecvMessage: %w", err)
	}
	if _, err := triggerSocket.SendMessage(raw[0], raw[1], reply); err != nil {
		return fmt.Errorf("triggerSocket.SendMessage: %w", err)
	}
	return nil
}

func (handler *Trigger) replyTriggerError(triggerSocket *zmq.Socket, raw []string, text string) error {
	reply, err := (&message.Request{}).Fail(text).ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("request.Fail.ZmqEnvelope: %w", err)
	}
	if len(raw) >= 2 {
		if _, err := triggerSocket.SendMessage(raw[0], raw[1], reply); err != nil {
			return fmt.Errorf("triggerSocket.SendMessage: %w", err)
		}
	}
	return nil
}

func (handler *Trigger) handleManager(manager *zmq.Socket) error {
	raw, err := manager.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("manager.RecvMessage: %w", err)
	}
	req, err := handler.messageOps.NewReq(raw)
	if err != nil {
		return handler.replyManager(manager, (&message.Request{}).Fail(fmt.Sprintf("messageOps.NewReq: %v", err)))
	}

	switch req.CommandName() {
	case control.HandlerStatus:
		return handler.replyManager(manager, req.Ok(datatype.New().Set("status", handler.managerStatus())))
	case control.HandlerConfig:
		return handler.replyManager(manager, req.Ok(datatype.New().Set("config", handler.Config().Handler)))
	case control.HandlerClose:
		if err := handler.replyManager(manager, req.Ok(datatype.New())); err != nil {
			return err
		}
		handler.Handler.SetClose(true)
		return nil
	case concurrent.InstanceAmount:
		amount := uint64(0)
		if handler.instance != nil && handler.instance.Status() != concurrent.CLOSED {
			amount = 1
		}
		return handler.replyManager(manager, req.Ok(datatype.New().Set("instance_amount", amount)))
	case concurrent.MessageAmount:
		return handler.replyManager(manager, req.Ok(datatype.New().
			Set("queue_length", 0).
			Set("processing_length", 0).
			Set("broadcasting_length", handler.broadcasting.Len())))
	case concurrent.Parts:
		return handler.replyManager(manager, req.Ok(datatype.New().
			Set("parts", []string{"instance", "broadcaster"}).
			Set("message_types", []string{"queue_length", "processing_length", "broadcasting_length"})))
	case concurrent.AddInstance:
		return handler.replyManager(manager, req.Fail("only one private instance allowed in trigger"))
	case concurrent.DeleteInstance:
		return handler.replyManager(manager, req.Fail("trigger private instance can not be deleted"))
	case concurrent.ClosePart, concurrent.RunPart:
		return handler.replyManager(manager, req.Fail("trigger parts are managed as a single handler"))
	default:
		return handler.replyManager(manager, req.Fail(fmt.Sprintf("unknown command '%s'", req.CommandName())))
	}
}

func (handler *Trigger) managerStatus() string {
	if handler.instance != nil &&
		handler.instance.Status() == concurrent.READY &&
		handler.broadcasterStatus() == BroadcasterRunning {
		return base.SocketReady
	}
	return base.Incomplete
}

func (handler *Trigger) replyManager(manager *zmq.Socket, reply message.ReplyInterface) error {
	envelope, err := reply.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("reply.ZmqEnvelope: %w", err)
	}
	if _, err := manager.SendMessage(envelope); err != nil {
		return fmt.Errorf("manager.SendMessage: %w", err)
	}
	return nil
}

func (handler *Trigger) closeInstance(instant bool) {
	if handler.instance == nil || handler.instance.Status() == concurrent.CLOSED {
		return
	}
	socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		handler.logger.Error("zmq.NewSocket('instanceManager')", "error", err)
		return
	}
	defer socket.Close()

	url := concurrent.InstanceUrl(handler.Config().Id, handler.instance.Id)
	if err := socket.Connect(url); err != nil {
		handler.logger.Error("instanceManager.Connect", "url", url, "error", err)
		return
	}
	req := message.Request{
		Command:    control.HandlerClose,
		Parameters: datatype.New().Set("instant", instant),
	}
	envelope, err := req.ZmqEnvelope()
	if err != nil {
		handler.logger.Error("request.ZmqEnvelope", "error", err)
		return
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		handler.logger.Error("instanceManager.SendMessage", "error", err)
		return
	}
	if !instant {
		if _, err := socket.RecvMessage(0); err != nil {
			handler.logger.Error("instanceManager.RecvMessage", "error", err)
		}
	}
}
