package sync_replier

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/handler/route"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type SyncReplier struct {
	*base.Handler
	handlerType config.HandlerType
	logger      *log.Logger
	Manager     *control.Manager
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
	c.Manager = control.New(parent)
	c.Manager.SetConfig(c.Config())
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

	c.close = false
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

	routes := map[string]route.HandleFunc{
		control.HandlerStatus: c.onControlStatus,
		control.HandlerClose:  c.onControlClose,
	}

	if err := c.Manager.SetRoutes(routes); err != nil {
		return fmt.Errorf("manager.SetRoutes: %w", err)
	}

	return nil
}

func (c *SyncReplier) bindExternal() error {
	socket, err := zmq.NewSocket(config.SocketType(c.Type()))
	if err != nil {
		return fmt.Errorf("zmq.NewSocket('%s'): %w", c.Type(), err)
	}

	externalUrl := config.ExternalUrl(c.Config().Id, c.Config().Port)
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

	for !c.close {
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

	handleInterface, err := route.Route(req.CommandName(), c.Routes)
	if err != nil {
		return c.sendReply(socket, req.Fail(fmt.Sprintf("route.Route(%s): %v", req.CommandName(), err)))
	}

	reply := route.Handle(req, handleInterface)
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
	if c.Handler.Status() == base.SocketReady && !c.close {
		status = base.Ready
	}
	return req.Ok(datatype.New().Set("status", status))
}

func (c *SyncReplier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	c.close = true
	return c.Manager.SetClose(req)
}
