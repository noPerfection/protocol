package sync_replier

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/concurrent"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type SyncReplier struct {
	*base.Handler
	handlerType config.HandlerType
	logger      *log.Logger
	instance    *concurrent.Instance
	messageOps  *message.Operations
	close       bool
	status      string
}

// New SyncReplier returned
func New() *SyncReplier {
	handler := base.New()
	return &SyncReplier{
		Handler:     handler,
		handlerType: config.SyncReplierType,
		messageOps:  message.DefaultMessage(),
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *SyncReplier) SetConfig(handler *config.Handler) {
	handler.Type = config.SyncReplierType
	c.Handler.SetConfig(handler)
}

func (c *SyncReplier) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	c.logger = parent.Child(c.Config().Id)
	return nil
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *SyncReplier) Type() config.HandlerType {
	return config.SyncReplierType
}

func (c *SyncReplier) Status() string {
	return c.status
}

// Start the handler directly, not by goroutine
func (c *SyncReplier) Start() error {
	if c.Config() == nil {
		return fmt.Errorf("configuration not set")
	}
	if c.logger == nil {
		return fmt.Errorf("logger not set")
	}

	parent, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket('PULL'): %w", err)
	}
	parentUrl := concurrent.ParentUrl(c.Config().Id)
	if err := parent.Bind(parentUrl); err != nil {
		_ = parent.Close()
		return fmt.Errorf("parent.Bind('%s'): %w", parentUrl, err)
	}

	instanceId := c.Config().Id + "_instance"
	c.instance = concurrent.NewInstance(config.SyncReplierType, instanceId, c.Config().Id, c.logger)
	c.instance.SetRoutes(&c.Routes)
	c.instance.SetMessageOps(c.messageOps)
	if err := c.instance.Start(); err != nil {
		_ = parent.Close()
		return fmt.Errorf("instance.Start: %w", err)
	}

	instanceClient, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		_ = parent.Close()
		return fmt.Errorf("zmq.NewSocket('instanceClient'): %w", err)
	}
	instanceUrl := concurrent.InstanceHandleUrl(c.Config().Id, instanceId)
	if err := instanceClient.Connect(instanceUrl); err != nil {
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("instanceClient.Connect('%s'): %w", instanceUrl, err)
	}

	external, err := zmq.NewSocket(config.SocketType(c.Type()))
	if err != nil {
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("zmq.NewSocket('%s'): %w", c.Type(), err)
	}
	externalUrl := config.ExternalUrl(c.Config().Id, c.Config().Port)
	if err := external.Bind(externalUrl); err != nil {
		_ = external.Close()
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("external.Bind('%s'): %w", externalUrl, err)
	}

	manager, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		_ = external.Close()
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("zmq.NewSocket('manager'): %w", err)
	}
	managerUrl := c.Config().ManagerExternalUrl()
	if err := manager.Bind(managerUrl); err != nil {
		_ = manager.Close()
		_ = external.Close()
		_ = instanceClient.Close()
		_ = parent.Close()
		return fmt.Errorf("manager.Bind('%s'): %w", managerUrl, err)
	}

	c.close = false
	c.status = control.Ready
	go c.run(parent, external, instanceClient, manager)

	return nil
}

func (c *SyncReplier) run(parent *zmq.Socket, external *zmq.Socket, instanceClient *zmq.Socket, manager *zmq.Socket) {
	poller := zmq.NewPoller()
	poller.Add(parent, zmq.POLLIN)
	poller.Add(external, zmq.POLLIN)
	poller.Add(manager, zmq.POLLIN)

	for !c.close {
		sockets, err := poller.Poll(time.Millisecond)
		if err != nil {
			c.status = err.Error()
			break
		}

		for _, polled := range sockets {
			switch polled.Socket {
			case parent:
				c.handleInstanceStatus(parent)
			case external:
				if err := c.forwardToInstance(external, instanceClient); err != nil {
					c.logger.Error("sync_replier.forwardToInstance", "error", err)
					c.status = err.Error()
				}
			case manager:
				if err := c.handleManager(manager); err != nil {
					c.logger.Error("sync_replier.handleManager", "error", err)
					c.status = err.Error()
				}
			}
		}
	}

	c.closeInstance(false)
	_ = poller.RemoveBySocket(parent)
	_ = poller.RemoveBySocket(external)
	_ = poller.RemoveBySocket(manager)
	_ = manager.Close()
	_ = external.Close()
	_ = instanceClient.Close()
	_ = parent.Close()
	c.status = ""
}

func (c *SyncReplier) handleInstanceStatus(parent *zmq.Socket) {
	raw, err := parent.RecvMessage(0)
	if err != nil {
		c.logger.Error("parent.RecvMessage", "error", err)
		return
	}
	if _, err := c.messageOps.NewReq(raw); err != nil {
		c.logger.Error("messageOps.NewReq", "messages", raw, "error", err)
	}
}

func (c *SyncReplier) forwardToInstance(external *zmq.Socket, instanceClient *zmq.Socket) error {
	raw, err := external.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("external.RecvMessage: %w", err)
	}
	if _, err := instanceClient.SendMessage(raw); err != nil {
		return fmt.Errorf("instanceClient.SendMessage: %w", err)
	}
	reply, err := instanceClient.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("instanceClient.RecvMessage: %w", err)
	}
	if _, err := external.SendMessage(reply); err != nil {
		return fmt.Errorf("external.SendMessage: %w", err)
	}
	return nil
}

func (c *SyncReplier) handleManager(manager *zmq.Socket) error {
	raw, err := manager.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("manager.RecvMessage: %w", err)
	}
	req, err := c.messageOps.NewReq(raw)
	if err != nil {
		return c.replyManager(manager, (&message.Request{}).Fail(fmt.Sprintf("messageOps.NewReq: %v", err)))
	}

	switch req.CommandName() {
	case config.HandlerStatus:
		return c.replyManager(manager, req.Ok(datatype.New().Set("status", c.managerStatus())))
	case config.HandlerConfig:
		return c.replyManager(manager, req.Ok(datatype.New().Set("config", c.Config())))
	case config.HandlerClose:
		if err := c.replyManager(manager, req.Ok(datatype.New())); err != nil {
			return err
		}
		c.close = true
		return nil
	case config.InstanceAmount:
		amount := uint64(0)
		if c.instance != nil && c.instance.Status() != concurrent.CLOSED {
			amount = 1
		}
		return c.replyManager(manager, req.Ok(datatype.New().Set("instance_amount", amount)))
	case config.AddInstance:
		return c.replyManager(manager, req.Fail("only one private instance allowed in sync replier"))
	case config.DeleteInstance:
		return c.replyManager(manager, req.Fail("sync replier private instance can not be deleted"))
	case config.MessageAmount:
		return c.replyManager(manager, req.Ok(datatype.New().Set("queue_length", 0).Set("processing_length", 0)))
	case config.Parts:
		return c.replyManager(manager, req.Ok(datatype.New().Set("parts", []string{"handler", "instance"}).Set("message_types", []string{"queue_length", "processing_length"})))
	case config.ClosePart, config.RunPart:
		return c.replyManager(manager, req.Fail("sync replier parts are managed as a single handler"))
	default:
		return c.replyManager(manager, req.Fail(fmt.Sprintf("unknown command '%s'", req.CommandName())))
	}
}

func (c *SyncReplier) managerStatus() string {
	if c.instance != nil && c.instance.Status() == concurrent.READY {
		return control.Ready
	}
	return control.Incomplete
}

func (c *SyncReplier) replyManager(manager *zmq.Socket, reply message.ReplyInterface) error {
	envelope, err := reply.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("reply.ZmqEnvelope: %w", err)
	}
	if _, err := manager.SendMessage(envelope); err != nil {
		return fmt.Errorf("manager.SendMessage: %w", err)
	}
	return nil
}

func (c *SyncReplier) closeInstance(instant bool) {
	if c.instance == nil || c.instance.Status() == concurrent.CLOSED {
		return
	}
	socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		c.logger.Error("zmq.NewSocket('instanceManager')", "error", err)
		return
	}
	defer socket.Close()

	url := concurrent.InstanceUrl(c.Config().Id, c.instance.Id)
	if err := socket.Connect(url); err != nil {
		c.logger.Error("instanceManager.Connect", "url", url, "error", err)
		return
	}
	req := message.Request{
		Command:    config.HandlerClose,
		Parameters: datatype.New().Set("instant", instant),
	}
	envelope, err := req.ZmqEnvelope()
	if err != nil {
		c.logger.Error("request.ZmqEnvelope", "error", err)
		return
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		c.logger.Error("instanceManager.SendMessage", "error", err)
		return
	}
	if !instant {
		if _, err := socket.RecvMessage(0); err != nil {
			c.logger.Error("instanceManager.RecvMessage", "error", err)
		}
	}
}
