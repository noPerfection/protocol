// Package pair adds a layer that forwards incoming messages through an in-process pair socket.
package pair

import (
	"fmt"
	"sync"
	"time"

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
const BroadcastParameter = "reply"

type Pair struct {
	*base.Handler
	socket       *zmq.Socket
	pairW        sync.WaitGroup
	broadcasting *datatype.Queue
	Control      *control.Manager
}

var _ base.Interface = (*Pair)(nil)

// New Pair returned.
func New() *Pair {
	return &Pair{
		Handler:      base.New(),
		broadcasting: datatype.NewQueue(),
		Control:      control.New(),
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *Pair) SetConfig(handler *config.Handler) {
	c.Handler.SetConfig(handler)
	c.Control.SetConfig(control.CreateInternalConfig(c.Handler.Config()))
}

func (c *Pair) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(control.ControlCategory))
}

// Type returns the handler type.
func (c *Pair) Type() config.HandlerType {
	return config.PairType
}

// Start the pair directly, not by goroutine.
func (c *Pair) Start() error {
	if c.Config() == nil {
		return fmt.Errorf("configuration not set")
	}
	if c.Config().Type != config.PairType {
		return fmt.Errorf("I cant start a handler in a %s type, It must be %s", c.Config().Type, config.PairType)
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

	if err := c.startPair(); err != nil {
		return fmt.Errorf("pair.startPair: %w", err)
	}

	return nil
}

func (c *Pair) setControlRoutes() {
	_ = c.Control.Route(control.HandlerConfig, c.onControlConfig)
	_ = c.Control.Route(control.HandlerStart, c.onControlStart)
	_ = c.Control.Route(control.HandlerClose, c.onControlClose)
	_ = c.Control.Route(Broadcast, c.onBroadcast)
	_ = c.Control.Route(MessageAmount, c.onMessageAmount)
}

func (c *Pair) onControlClose(req message.RequestInterface) message.ReplyInterface {
	c.stopPair()
	return req.Ok(datatype.New())
}

func (c *Pair) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Config()))
}

func (c *Pair) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Status() == base.SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Control.Status()))
	}
	if err := c.startPair(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Control.Status()))
}

func (c *Pair) startPair() error {
	if c.socket != nil {
		return fmt.Errorf("pair already running")
	}

	c.pairW.Add(1)

	ready := make(chan error)

	go func(ready chan error) {
		defer c.pairW.Done()

		socket, err := zmq.NewSocket(config.SocketType(c.Type()))
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket('%s'): %w", c.Type(), err)
			return
		}

		pairUrl := c.Config().HandlerUrl()
		if err := socket.Bind(pairUrl); err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("socket.Bind('%s'): %w", pairUrl, err)
			return
		}

		c.socket = socket
		c.Control.SetSocketReady()
		ready <- nil

		poller := zmq.NewPoller()
		poller.Add(socket, zmq.POLLIN)

		for c.Control.Running() {
			c.flushBroadcast(socket)

			polled, err := poller.Poll(time.Millisecond)
			if err != nil {
				c.LogError("poller.Poll", "error", err)
				break
			}

			if len(polled) == 0 {
				continue
			}

			if err := c.handleRequest(socket); err != nil {
				c.LogError("pair.handleRequest", "error", err)
				break
			}
		}

		if err := poller.RemoveBySocket(socket); err != nil {
			c.LogError("poller.RemoveBySocket", "error", err)
		}
		if err := socket.Close(); err != nil {
			c.LogError("socket.Close", "error", err)
		}
		c.socket = nil
		c.Control.SetSocketNil()
	}(ready)

	return <-ready
}

func (c *Pair) flushBroadcast(socket *zmq.Socket) {
	for !c.broadcasting.IsEmpty() {
		reply := c.broadcasting.Pop().(message.ReplyInterface)
		envelope, err := c.Packer().SerializeReply(reply)
		if err != nil {
			c.LogError("messageOps.SerializeReply", "error", err)
			continue
		}
		if _, err := socket.SendMessageDontwait(envelope); err != nil {
			c.LogError("socket.SendMessageDontwait", "error", err)
			return
		}
	}
}

func (c *Pair) handleRequest(socket *zmq.Socket) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, err := c.Packer().DeserializeRequest(raw)
	if err != nil {
		reply := c.Packer().EmptyRequest().Fail(fmt.Sprintf("messageOps.DeserializeRequest: %v", err))
		return c.sendReply(socket, reply)
	}

	handleFunc, err := c.FindRoute(req.CommandName())
	if err != nil {
		return c.sendReply(socket, req.Fail(fmt.Sprintf("base.FindRoute(%s): %v", req.CommandName(), err)))
	}

	reply := base.Handle(req, handleFunc)
	return c.sendReply(socket, reply)
}

func (c *Pair) sendReply(socket *zmq.Socket, reply message.ReplyInterface) error {
	envelope, err := c.Packer().SerializeReply(reply)
	if err != nil {
		return fmt.Errorf("messageOps.SerializeReply: %w", err)
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		return fmt.Errorf("socket.SendMessage: %w", err)
	}
	return nil
}

func (c *Pair) stopPair() {
	if c.socket == nil && !c.Control.Running() {
		return
	}

	c.Control.SetSocketNil()
	c.pairW.Wait()
}

func (c *Pair) onBroadcast(req message.RequestInterface) message.ReplyInterface {
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

func (c *Pair) onMessageAmount(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("broadcasting_length", c.broadcasting.Len()))
}
