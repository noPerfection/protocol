package publisher

import (
	"fmt"
	"sync"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const Broadcast = "broadcast"
const MessageAmount = "message-amount"

type Publisher struct {
	*base.Handler
	broadcasterW sync.WaitGroup
	broadcasting *datatype.Queue
	Control      base.Interface
}

// New Publisher returned
func New() *Publisher {
	return &Publisher{
		Handler:      base.New(),
		broadcasting: datatype.NewQueue(),
		Control:      control.New(),
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *Publisher) SetConfig(handler *config.Handler) {
	c.Handler.SetConfig(handler)
	c.Control.SetConfig(control.CreateInternalConfig(c.Handler.Config()))
}

func (c *Publisher) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(control.ControlCategory))
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Publisher) Type() config.HandlerType {
	return config.PublisherType
}

// Route adds a route along with its handler to this handler.
func (c *Publisher) Route(_ string, _ base.HandleFunc) error {
	return fmt.Errorf("publisher doesn't support routing")
}

// Start the publisher directly, not by goroutine.
func (c *Publisher) Start() error {
	if c.Config() == nil {
		return fmt.Errorf("configuration not set")
	}
	if c.Config().Type != config.PublisherType {
		return fmt.Errorf("I cant start a handler in a %s type, It must be %s", c.Config().Type, config.PublisherType)
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

	if err := c.startBroadcaster(); err != nil {
		return fmt.Errorf("publisher.startBroadcaster: %w", err)
	}

	return nil
}

func (c *Publisher) setControlRoutes() {
	c.Control.Route(Broadcast, c.onBroadcast)
	c.Control.Route(control.HandlerStart, c.onControlStart)
	c.Control.Route(control.HandlerClose, c.onControlStopBroadcaster)
	c.Control.Route(control.HandlerStatus, c.onControlStatus)
	c.Control.Route(control.HandlerConfig, c.onControlConfig)
	c.Control.Route(MessageAmount, c.onMessageAmount)
}

func (c *Publisher) startBroadcaster() error {
	if c.Handler.Status() == base.SocketReady {
		return fmt.Errorf("broadcaster already running")
	}

	c.Handler.SetClose(false)
	c.broadcasterW.Add(1)

	ready := make(chan error)

	go func(ready chan error) {
		defer c.broadcasterW.Done()

		socket, err := zmq.NewSocket(config.SocketType(c.Type()))
		if err != nil {
			ready <- fmt.Errorf("new_socket('%s'): %v", c.Type(), err)
			return
		}

		pubUrl := c.Config().HandlerUrl()
		if err := socket.Bind(pubUrl); err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("socket.Bind('%s'): %v", pubUrl, err)
			return
		}

		c.Handler.SetSocket(socket)
		c.Handler.SetSocketReady()
		ready <- nil

		for !c.Handler.Closed() {
			if c.broadcasting.IsEmpty() {
				continue
			}

			req := c.broadcasting.Pop().(message.RequestInterface)
			reqStr, err := c.Packer().SerializeRequest(req)
			if err != nil {
				c.LogError("publisher.broadcasting.Pop", "type", "message.Request", "error", err)
				break
			}
			if _, err = socket.SendMessageDontwait(reqStr); err != nil {
				c.LogError("socket.SendMessageDontWait", "request", reqStr, "error", err)
				break
			}
		}

		if err := socket.Close(); err != nil {
			c.LogError("socket.Close", "error", err)
		}
		c.Handler.SetClose(false)
		c.Handler.SetSocketNil()
	}(ready)

	return <-ready
}

func (c *Publisher) stopBroadcaster() {
	if c.Handler.Socket() == nil && c.Handler.Status() != base.SocketReady {
		return
	}

	c.Handler.SetClose(true)
	c.broadcasterW.Wait()
}

func (c *Publisher) onBroadcast(req message.RequestInterface) message.ReplyInterface {
	if c.broadcasting.IsFull() {
		return req.Fail("broadcasting queue full")
	}

	c.broadcasting.Push(req)

	return req.Ok(datatype.New())
}

func (c *Publisher) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Handler.Status() == base.SocketReady {
		return req.Fail(fmt.Sprintf("broadcaster already running with status %s", c.Handler.Status()))
	}
	if err := c.startBroadcaster(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Handler.Status()))
}

func (c *Publisher) onControlStopBroadcaster(req message.RequestInterface) message.ReplyInterface {
	c.stopBroadcaster()
	return req.Ok(datatype.New())
}

func (c *Publisher) onControlStatus(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("status", c.Handler.Status()))
}

func (c *Publisher) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Config()))
}

func (c *Publisher) onMessageAmount(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("broadcasting_length", c.broadcasting.Len()))
}
