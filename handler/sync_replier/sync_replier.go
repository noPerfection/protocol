package sync_replier

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type SyncReplier struct {
	*base.Handler
	handlerType config.HandlerType
	logger      *log.Logger
	Manager     base.Interface
	messageOps  *message.Operations
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
	c.Manager = control.New(parent)
	c.Manager.SetConfig(control.CreateInternalConfig(c.Config()))
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

	if err := c.setControlRoutes(); err != nil {
		return err
	}

	if err := c.bindExternal(); err != nil {
		return err
	}

	c.Handler.SetClose(false)
	if err := c.Manager.Start(); err != nil {
		c.cleanup()
		return fmt.Errorf("manager.Start: %w", err)
	}

	c.status = base.Ready
	go c.run()

	return nil
}

func (c *SyncReplier) setControlRoutes() error {
	if c.Manager == nil {
		return fmt.Errorf("control manager not initiated. call SetConfig and SetLogger")
	}

	routes := map[string]base.HandleFunc{
		control.HandlerStatus: c.onControlStatus,
		control.HandlerClose:  c.onControlClose,
		control.HandlerConfig: c.onControlConfig,
	}

	for cmd, handle := range routes {
		if err := c.Manager.Route(cmd, handle); err != nil {
			return fmt.Errorf("manager.Route('%s'): %w", cmd, err)
		}
	}

	return nil
}

func (c *SyncReplier) bindExternal() error {
	socket, err := zmq.NewSocket(config.SocketType(c.Type()))
	if err != nil {
		return fmt.Errorf("zmq.NewSocket('%s'): %w", c.Type(), err)
	}

	externalUrl := c.Config().HandlerUrl()
	if err := socket.Bind(externalUrl); err != nil {
		_ = socket.Close()
		return fmt.Errorf("external.Bind('%s'): %w", externalUrl, err)
	}

	c.Handler.SetSocket(socket)
	c.Handler.SetSocketReady()
	return nil
}

func (c *SyncReplier) run() {
	socket := c.Socket()
	if socket == nil {
		c.status = "external socket not set"
		return
	}

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	for !c.Handler.Closed() {
		sockets, err := poller.Poll(time.Millisecond)
		if err != nil {
			c.status = err.Error()
			break
		}

		for _, polled := range sockets {
			if polled.Socket != socket {
				continue
			}
			if err := c.handleRequest(socket); err != nil {
				c.logger.Error("sync_replier.handleRequest", "error", err)
				c.status = err.Error()
			}
		}
	}

	_ = poller.RemoveBySocket(socket)
	c.cleanup()
}

func (c *SyncReplier) handleRequest(socket *zmq.Socket) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, err := c.messageOps.NewReq(raw)
	if err != nil {
		reply := c.messageOps.EmptyReq().Fail(fmt.Sprintf("messageOps.NewReq: %v", err))
		return c.sendReply(socket, reply)
	}

	handleFunc, err := base.FindRoute(req.CommandName(), c.Routes)
	if err != nil {
		return c.sendReply(socket, req.Fail(fmt.Sprintf("base.FindRoute(%s): %v", req.CommandName(), err)))
	}

	reply := base.Handle(req, handleFunc)
	return c.sendReply(socket, reply)
}

func (c *SyncReplier) sendReply(socket *zmq.Socket, reply message.ReplyInterface) error {
	envelope, err := reply.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("reply.ZmqEnvelope: %w", err)
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		return fmt.Errorf("socket.SendMessage: %w", err)
	}
	return nil
}

func (c *SyncReplier) cleanup() {
	if socket := c.Socket(); socket != nil {
		_ = socket.Close()
	}
	c.Handler.SetSocketNil()
	c.status = ""
}

func (c *SyncReplier) onControlStatus(req message.RequestInterface) message.ReplyInterface {
	status := base.Incomplete
	if c.Handler.Status() == base.SocketReady && !c.Handler.Closed() {
		status = base.Ready
	}
	return req.Ok(datatype.New().Set("status", status))
}

func (c *SyncReplier) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Config()))
}

func (c *SyncReplier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	c.Handler.SetClose(true)
	c.Manager.SetClose(true)
	return req.Ok(datatype.New())
}
