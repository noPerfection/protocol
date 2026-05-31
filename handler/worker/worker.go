package worker

// Asynchronous worker

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

// Worker is the socket wrapper for the service.
type Worker struct {
	*base.Handler
	Control    base.Interface
	messageOps message.Packer
}

var _ base.Interface = (*Worker)(nil)

// New asynchronous replying handler.
func New() *Worker {
	return &Worker{
		Handler:    base.New(),
		Control:    control.New(),
		messageOps: message.DefaultMessage(),
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *Worker) SetConfig(handler *config.Handler) {
	handler.Type = config.WorkerType
	c.Handler.SetConfig(handler)
	c.Control.SetConfig(control.CreateInternalConfig(c.Config()))
}

func (c *Worker) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(control.ControlCategory))
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Worker) Type() config.HandlerType {
	return config.WorkerType
}

// Start the handler directly, not by goroutine.
func (c *Worker) Start() error {
	if c.Config() == nil {
		return fmt.Errorf("configuration not set")
	}
	if c.Config().Type != config.WorkerType {
		return fmt.Errorf("I cant start a handler in a %s type, It must be %s", c.Config().Type, config.WorkerType)
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

func (c *Worker) setControlRoutes() {
	c.Control.Route(control.HandlerStatus, c.onControlStatus)
	c.Control.Route(control.HandlerStart, c.onControlStart)
	c.Control.Route(control.HandlerClose, c.onControlClose)
	c.Control.Route(control.HandlerConfig, c.onControlConfig)
}

func (c *Worker) bindExternal() error {
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

func (c *Worker) run() {
	socket := c.Socket()
	if socket == nil {
		return
	}

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	for !c.Handler.Closed() {
		sockets, err := poller.Poll(time.Millisecond)
		if err != nil {
			break
		}

		for _, polled := range sockets {
			if polled.Socket != socket {
				continue
			}
			if err := c.handleRequest(socket); err != nil {
				c.LogError("worker.handleRequest", "error", err)
			}
		}
	}

	_ = poller.RemoveBySocket(socket)
	c.cleanup()
}

func (c *Worker) handleRequest(socket *zmq.Socket) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, err := c.messageOps.DeserializeRequest(raw)
	if err != nil {
		return fmt.Errorf("messageOps.DeserializeRequest: %w", err)
	}

	handleFunc, err := c.FindRoute(req.CommandName())
	if err != nil {
		return fmt.Errorf("base.FindRoute(%s): %w", req.CommandName(), err)
	}

	go base.Handle(req, handleFunc)
	return nil
}

func (c *Worker) cleanup() {
	if socket := c.Socket(); socket != nil {
		_ = socket.Close()
	}
	c.Handler.SetSocket(nil)
}

func (c *Worker) onControlStatus(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("status", c.Handler.Status()))
}

func (c *Worker) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Config()))
}

func (c *Worker) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Handler.Status() == base.SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Handler.Status()))
	}
	if err := c.Start(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Handler.Status()))
}

func (c *Worker) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Handler.Status() == base.SocketReady {
		c.Handler.SetClose(true)
	}
	return req.Ok(datatype.New())
}
