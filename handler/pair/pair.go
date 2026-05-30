// Package pair adds a layer that forwards incoming messages through an in-process pair socket.
package pair

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/client"
	clientConfig "github.com/noPerfection/protocol/client/config"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const Broadcast = "broadcast"
const MessageAmount = "message-amount"

type Pair struct {
	*base.Handler
	pairW        sync.WaitGroup
	broadcasting *datatype.Queue
	Manager      base.Interface
	messageOps   *message.Operations
}

var _ base.Interface = (*Pair)(nil)

// New Pair returned.
func New() *Pair {
	return &Pair{
		Handler:      base.New(),
		broadcasting: datatype.NewQueue(),
		Manager:      control.New(),
		messageOps:   message.DefaultMessage(),
	}
}

// Config converts the external handler config into the paired one
func Config(originalConfig *config.Handler) *config.Handler {
	pairConfig := &config.Handler{
		Type:     config.PairType,
		Category: originalConfig.Category + "_pair",
		Endpoint: message.NewEndpoint(originalConfig.Id+"_pair", 0),
	}
	return pairConfig
}

// NewClient returns a client that's connected to the external socket.
// Requires the original handler configuration.
// The Client will create a pair socket parameters using Config.
func NewClient(originalConfig *config.Handler) (*client.Socket, error) {
	externalConfig := Config(originalConfig)
	pairConfig := clientConfig.New("", externalConfig.Id, externalConfig.Port, config.SocketType(externalConfig.Type))
	pairConfig.UrlFunc(clientConfig.Url)

	pairClient, err := client.New(pairConfig)
	if err != nil {
		return nil, fmt.Errorf("client.New: %w", err)
	}
	return pairClient, nil
}

// SetConfig adds the parameters of the handler from the config.
func (c *Pair) SetConfig(handler *config.Handler) {
	c.Handler.SetConfig(handler)
	c.Manager.SetConfig(control.CreateInternalConfig(c.Handler.Config()))
}

func (c *Pair) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	return c.Manager.SetLogger(parent.Child(control.ControlCategory))
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
	if c.Logger() == nil {
		return fmt.Errorf("logger not set")
	}
	if c.Manager == nil {
		return fmt.Errorf("manager not set")
	}

	c.setControlRoutes()

	if c.Manager.Status() != base.SocketReady {
		if err := c.Manager.Start(); err != nil {
			return fmt.Errorf("manager.Start: %w", err)
		}
	}

	if err := c.startPair(); err != nil {
		return fmt.Errorf("pair.startPair: %w", err)
	}

	return nil
}

func (c *Pair) setControlRoutes() {
	c.Manager.Route(Broadcast, c.onBroadcast)
	c.Manager.Route(control.HandlerStart, c.onControlStart)
	c.Manager.Route(control.HandlerClose, c.onControlClose)
	c.Manager.Route(control.HandlerStatus, c.onControlStatus)
	c.Manager.Route(control.HandlerConfig, c.onControlConfig)
	c.Manager.Route(MessageAmount, c.onMessageAmount)
}

func (c *Pair) startPair() error {
	if c.Handler.Status() == base.SocketReady {
		return fmt.Errorf("pair already running")
	}

	c.Handler.SetClose(false)
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

		c.Handler.SetSocket(socket)
		c.Handler.SetSocketReady()
		ready <- nil

		poller := zmq.NewPoller()
		poller.Add(socket, zmq.POLLIN)

		for !c.Handler.Closed() {
			c.flushBroadcast(socket)

			polled, err := poller.Poll(time.Millisecond)
			if err != nil {
				c.Logger().Error("poller.Poll", "error", err)
				break
			}

			if len(polled) == 0 {
				continue
			}

			if err := c.handleRequest(socket); err != nil {
				c.Logger().Error("pair.handleRequest", "error", err)
				break
			}
		}

		if err := poller.RemoveBySocket(socket); err != nil {
			c.Logger().Error("poller.RemoveBySocket", "error", err)
		}
		if err := socket.Close(); err != nil {
			c.Logger().Error("socket.Close", "error", err)
		}
		c.Handler.SetClose(false)
		c.Handler.SetSocketNil()
	}(ready)

	return <-ready
}

func (c *Pair) flushBroadcast(socket *zmq.Socket) {
	for !c.broadcasting.IsEmpty() {
		req := c.broadcasting.Pop().(message.RequestInterface)
		envelope, err := req.ZmqEnvelope()
		if err != nil {
			c.Logger().Error("request.ZmqEnvelope", "error", err)
			continue
		}
		if _, err := socket.SendMessageDontwait(envelope); err != nil {
			c.Logger().Error("socket.SendMessageDontwait", "error", err)
			return
		}
	}
}

func (c *Pair) handleRequest(socket *zmq.Socket) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, err := c.messageOps.NewReq(raw)
	if err != nil {
		reply := c.messageOps.EmptyReq().Fail(fmt.Sprintf("messageOps.NewReq: %v", err))
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
	envelope, err := reply.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("reply.ZmqEnvelope: %w", err)
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		return fmt.Errorf("socket.SendMessage: %w", err)
	}
	return nil
}

func (c *Pair) stopPair() {
	if c.Handler.Socket() == nil && c.Handler.Status() != base.SocketReady {
		return
	}

	c.Handler.SetClose(true)
	c.pairW.Wait()
}

func (c *Pair) onBroadcast(req message.RequestInterface) message.ReplyInterface {
	if c.broadcasting.IsFull() {
		return req.Fail("broadcasting queue full")
	}

	c.broadcasting.Push(req)

	return req.Ok(datatype.New())
}

func (c *Pair) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Handler.Status() == base.SocketReady {
		return req.Fail(fmt.Sprintf("pair already running with status %s", c.Handler.Status()))
	}
	if err := c.startPair(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Handler.Status()))
}

func (c *Pair) onControlClose(req message.RequestInterface) message.ReplyInterface {
	c.stopPair()
	return req.Ok(datatype.New())
}

func (c *Pair) onControlStatus(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("status", c.Handler.Status()))
}

func (c *Pair) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Config()))
}

func (c *Pair) onMessageAmount(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("broadcasting_length", c.broadcasting.Len()))
}
