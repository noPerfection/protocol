package handler

import (
	"fmt"
	"sync"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const Broadcast = "broadcast"
const MessageAmount = "message-amount"
const BroadcastParameter = "reply"

type Publisher struct {
	*Handler
	*Autocontext
	*Security
	socket       *zmq.Socket
	broadcasterW sync.WaitGroup
	broadcasting *datatype.Queue
	Control      *Control
}

// NewPublisher Publisher returned
func NewPublisher() *Publisher {
	return &Publisher{
		Handler:      New(),
		broadcasting: datatype.NewQueue(),
		Control:      NewControl(),
		Autocontext:  NewAutocontext(),
		Security:     NewSecurity(),
	}
}

// SetEndpoint adds the parameters of the handler from the config.
func (c *Publisher) SetEndpoint(endpoint message.Endpoint) {
	c.Handler.SetEndpoint(endpoint)
	c.Control.SetEndpoint(endpoint)
}

func (c *Publisher) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(ControlCategory))
}

// Type returns the handler type. If the configuration is not set, returns handler.UnknownType.
func (c *Publisher) Type() HandlerType {
	return PublisherType
}

// Route adds a route along with its handler to this handler.
func (c *Publisher) Route(_ string, _ HandleFunc) error {
	return fmt.Errorf("publisher doesn't support routing")
}

// Start the publisher directly, not by goroutine.
func (c *Publisher) Start() error {
	if c.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("configuration not set")
	}
	if c.Control == nil {
		return fmt.Errorf("control not set")
	}

	if c.mushroomURL == "" {
		return fmt.Errorf("mushroom URL not set, call SetMushroomURL first")
	}

	c.setControlRoutes()

	if c.Control.Status() != SocketReady {
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
	c.Control.Route(HandlerConfig, c.onControlConfig)
	c.Control.Route(HandlerStart, c.onControlStart)
	c.Control.Route(HandlerClose, c.onControlClose)
	c.Control.Route(Broadcast, c.onBroadcast)
	c.Control.Route(MessageAmount, c.onMessageAmount)
}

func (c *Publisher) onControlClose(req message.RequestInterface) message.ReplyInterface {
	c.stopBroadcaster()
	_ = c.npacRemoveHandler()
	return req.Ok(datatype.New())
}

func (c *Publisher) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Endpoint()))
}

func (c *Publisher) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Status() == SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Control.Status()))
	}
	if err := c.startBroadcaster(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Control.Status()))
}

func (c *Publisher) startBroadcaster() error {
	if c.socket != nil {
		return fmt.Errorf("broadcaster already running")
	}

	c.broadcasterW.Add(1)

	ready := make(chan error)

	go func(ready chan error) {
		defer c.broadcasterW.Done()

		socket, err := zmq.NewSocket(zmq.PUB)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket(PUB): %v", err)
			return
		}

		err = c.register(socket, c.Endpoint())
		if err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("register: %w", err)
			return
		}

		pubUrl := c.Endpoint().HandlerUrl()
		if err := socket.Bind(pubUrl); err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("socket.Bind('%s'): %v", pubUrl, err)
			return
		}

		c.socket = socket
		c.Control.SetSocketReady()

		_ = c.npacRegisterHandler(c.Control.Endpoint())

		ready <- nil

		for c.Control.Running() {
			if c.broadcasting.IsEmpty() {
				continue
			}

			reply := c.broadcasting.Pop().(message.ReplyInterface)
			replyStr, err := c.Packer().SerializeReply(reply)
			if err != nil {
				c.LogError("publisher.broadcasting.Pop", "type", "message.Reply", "error", err)
				break
			}
			if _, err = socket.SendMessageDontwait(replyStr); err != nil {
				c.LogError("socket.SendMessageDontWait", "reply", replyStr, "error", err)
				break
			}
		}

		if err := socket.Close(); err != nil {
			c.LogError("socket.Close", "error", err)
		}
		c.socket = nil
		c.Control.SetSocketNil()
	}(ready)

	return <-ready
}

func (c *Publisher) stopBroadcaster() {
	if c.socket == nil && !c.Control.Running() {
		return
	}

	c.Control.SetSocketNil()
	c.broadcasterW.Wait()
}

func (c *Publisher) onBroadcast(req message.RequestInterface) message.ReplyInterface {
	if c.broadcasting.IsFull() {
		return req.Fail("broadcasting queue full")
	}

	replyKV, err := req.RouteParameters().NestedValue(BroadcastParameter)
	if err != nil {
		return req.Fail(fmt.Sprintf("req.RouteParameters().NestedValue('%s'): %v", BroadcastParameter, err))
	}

	var broadcastReply message.Reply
	if err := replyKV.Interface(&broadcastReply); err != nil {
		return req.Fail(fmt.Sprintf("replyKV.Interface('message.Reply'): %v", err))
	}

	c.broadcasting.Push(&broadcastReply)

	return req.Ok(datatype.New())
}

func (c *Publisher) onMessageAmount(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("broadcasting_length", c.broadcasting.Len()))
}
