package replier

// Asynchronous replier

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

// Replier is the socket wrapper for the service.
type Replier struct {
	*base.Handler
	Control    base.Interface
	messageOps *message.Operations
}

type pendingReply struct {
	reply message.ReplyInterface
}

var _ base.Interface = (*Replier)(nil)

// New asynchronous replying handler.
func New() *Replier {
	return &Replier{
		Handler:    base.New(),
		Control:    control.New(),
		messageOps: message.DefaultMessage(),
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *Replier) SetConfig(handler *config.Handler) {
	handler.Type = config.ReplierType
	c.Handler.SetConfig(handler)
	c.Control.SetConfig(control.CreateInternalConfig(c.Config()))
}

func (c *Replier) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(control.ControlCategory))
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Replier) Type() config.HandlerType {
	return config.ReplierType
}

// Start the handler directly, not by goroutine
func (c *Replier) Start() error {
	if c.Config() == nil {
		return fmt.Errorf("configuration not set")
	}
	if c.Config().Type != config.ReplierType {
		return fmt.Errorf("I cant start a handler in a %s type, It must be %s", c.Config().Type, config.ReplierType)
	}
	if c.Control == nil {
		return fmt.Errorf("control not set")
	}

	c.setControlRoutes()

	if err := c.bindExternal(); err != nil {
		return err
	}

	c.Handler.SetClose(false)
	if c.Control.Status() != base.SocketReady {
		if err := c.Control.Start(); err != nil {
			c.cleanup()
			return fmt.Errorf("control.Start: %w", err)
		}
	}

	go c.run()

	return nil
}

func (c *Replier) setControlRoutes() {
	c.Control.Route(control.HandlerStatus, c.onControlStatus)
	c.Control.Route(control.HandlerStart, c.onControlStart)
	c.Control.Route(control.HandlerClose, c.onControlClose)
	c.Control.Route(control.HandlerConfig, c.onControlConfig)
}

func (c *Replier) bindExternal() error {
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

func (c *Replier) run() {
	socket := c.Socket()
	if socket == nil {
		return
	}

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)
	replies := make(chan pendingReply, 65536)

	for !c.Handler.Closed() {
		c.flushReplies(socket, replies)

		sockets, err := poller.Poll(time.Millisecond)
		if err != nil {
			break
		}

		for _, polled := range sockets {
			if polled.Socket != socket {
				continue
			}
			if err := c.handleRequest(socket, replies); err != nil {
				c.LogError("replier.handleRequest", "error", err)
			}
		}
	}

	_ = poller.RemoveBySocket(socket)
	c.cleanup()
}

func (c *Replier) flushReplies(socket *zmq.Socket, replies <-chan pendingReply) {
	for {
		select {
		case pending := <-replies:
			if err := c.sendReply(socket, pending.reply); err != nil {
				c.LogError("replier.sendReply", "error", err)
			}
		default:
			return
		}
	}
}

func (c *Replier) handleRequest(socket *zmq.Socket, replies chan<- pendingReply) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, err := c.messageOps.NewReq(raw)
	if err != nil {
		reply := c.messageOps.EmptyReq().Fail(fmt.Sprintf("messageOps.NewReq: %v", err))
		replies <- pendingReply{reply: reply}
		return nil
	}

	handleFunc, err := c.FindRoute(req.CommandName())
	if err != nil {
		replies <- pendingReply{reply: req.Fail(fmt.Sprintf("base.FindRoute(%s): %v", req.CommandName(), err))}
		return nil
	}

	go func() {
		reply := base.Handle(req, handleFunc)
		replies <- pendingReply{reply: reply}
	}()

	return nil
}

func (c *Replier) sendReply(socket *zmq.Socket, reply message.ReplyInterface) error {
	envelope, err := reply.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("reply.ZmqEnvelope: %w", err)
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		return fmt.Errorf("socket.SendMessage: %w", err)
	}
	return nil
}

func (c *Replier) cleanup() {
	if socket := c.Socket(); socket != nil {
		_ = socket.Close()
	}
	c.Handler.SetSocket(nil)
}

func (c *Replier) onControlStatus(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("status", c.Handler.Status()))
}

func (c *Replier) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Config()))
}

func (c *Replier) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Handler.Status() == base.SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Handler.Status()))
	}
	if err := c.Start(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Handler.Status()))
}

func (c *Replier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Handler.Status() == base.SocketReady {
		c.Handler.SetClose(true)
	}
	return req.Ok(datatype.New())
}
