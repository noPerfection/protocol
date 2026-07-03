package sync_replier

import (
	"errors"
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type SyncReplier struct {
	*base.Handler
	socket  *zmq.Socket
	Control *control.Manager
}

var _ base.Interface = (*SyncReplier)(nil)

// New SyncReplier returned
func New() *SyncReplier {
	return &SyncReplier{
		Handler: base.New(),
		Control: control.New(),
	}
}

// SetEndpoint adds the parameters of the handler from the config.
func (c *SyncReplier) SetEndpoint(endpoint message.Endpoint) {
	c.Handler.SetEndpoint(endpoint)
	c.Control.SetEndpoint(endpoint)
}

func (c *SyncReplier) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(control.ControlCategory))
}

// Type returns the handler type. If the configuration is not set, returns base.UnknownType.
func (c *SyncReplier) Type() base.HandlerType {
	return base.SyncReplierType
}

// Start the handler directly, not by goroutine
func (c *SyncReplier) Start() error {
	if c.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("configuration not set")
	}
	if c.Control == nil {
		return fmt.Errorf("control not set")
	}

	c.setControlRoutes()

	if c.Control.Status() != base.SocketReady {
		if err := c.Control.Start(); err != nil {
			return fmt.Errorf("control.Start: %w", err)
		}
	}

	if err := c.bindExternal(); err != nil {
		c.cleanup()
		return err
	}

	go c.run()

	return nil
}

func (c *SyncReplier) setControlRoutes() {
	c.Control.Route(control.HandlerConfig, c.onControlConfig)
	c.Control.Route(control.HandlerStart, c.onControlStart)
	c.Control.Route(control.HandlerClose, c.onControlClose)
}

func (c *SyncReplier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		c.Control.SetSocketNil()
	}
	return req.Ok(datatype.New())
}

func (c *SyncReplier) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Endpoint()))
}

func (c *SyncReplier) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Status() == base.SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Control.Status()))
	}
	if err := c.restartWork(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Control.Status()))
}

func (c *SyncReplier) restartWork() error {
	if err := c.bindExternal(); err != nil {
		return err
	}
	go c.run()
	return nil
}

func (c *SyncReplier) bindExternal() error {
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket(REP): %w", err)
	}

	externalUrl := c.Endpoint().HandlerUrl()
	if err := socket.Bind(externalUrl); err != nil {
		_ = socket.Close()
		return fmt.Errorf("external.Bind('%s'): %w", externalUrl, err)
	}

	c.socket = socket
	c.Control.SetSocketReady()
	return nil
}

func (c *SyncReplier) run() {
	socket := c.socket
	if socket == nil {
		return
	}

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	for c.Control.Running() {
		sockets, err := poller.Poll(time.Millisecond)
		if err != nil {
			break
		}

		for _, polled := range sockets {
			if polled.Socket != socket {
				continue
			}
			if err := c.handleRequest(socket); err != nil {
				c.LogError("sync_replier.handleRequest", "error", err)
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

	req, err := c.Packer().DeserializeRequest(raw)
	if err != nil {
		if errors.Is(err, message.ErrAccessDenied) {
			return c.sendReply(socket, c.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()))
		}
		reply := c.Packer().EmptyRequest().Fail(fmt.Sprintf("messageOps.DeserializeRequest: %v", err))
		return c.sendReply(socket, reply)
	}

	handleFunc, err := c.GetHandleFunc(req.CommandName())
	if err != nil {
		return c.sendReply(socket, req.Fail(fmt.Sprintf("base.GetHandleFunc(%s): %v", req.CommandName(), err)))
	}

	reply := handleFunc(req)
	return c.sendReply(socket, reply)
}

func (c *SyncReplier) sendReply(socket *zmq.Socket, reply message.ReplyInterface) error {
	envelope, err := c.Packer().SerializeReply(reply)
	if err != nil {
		return fmt.Errorf("messageOps.SerializeReply: %w", err)
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		return fmt.Errorf("socket.SendMessage: %w", err)
	}
	return nil
}

func (c *SyncReplier) cleanup() {
	if socket := c.socket; socket != nil {
		_ = socket.Close()
	}
	c.socket = nil
	c.Control.SetSocketNil()
}
