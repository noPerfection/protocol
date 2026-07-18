package handler

// Asynchronous replier

import (
	"fmt"
	"sync"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// Basic Replier is the Replier without Autocontext.
// Better name would be a manual replier or direct replier without autocontext.
type BasicReplier struct {
	*Handler
	*Security
	socket  *zmq.Socket
	wake    *wakePipe
	Control *Control
	workW   sync.WaitGroup
}

var _ Interface = (*BasicReplier)(nil)

// NewBasicReplier asynchronous replying handler.
func NewBasicReplier() *BasicReplier {
	return &BasicReplier{
		Handler:  New(),
		Control:  NewControl(),
		Security: NewSecurity(),
	}
}

func (c *BasicReplier) SetMushroomURL(_ string) {
}

// SetEndpoint adds the parameters of the handler from the config.
func (c *BasicReplier) SetEndpoint(endpoint message.Endpoint) {
	c.Handler.SetEndpoint(endpoint)
	c.Control.SetEndpoint(endpoint)
}

func (c *BasicReplier) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(ControlCategory))
}

// Type returns the handler type. If the configuration is not set, returns handler.UnknownType.
func (c *BasicReplier) Type() HandlerType {
	return ReplierType
}

// Start the handler directly, not by goroutine
func (c *BasicReplier) Start() error {
	if c.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("configuration not set")
	}
	if c.Control == nil {
		return fmt.Errorf("control not set")
	}

	c.setControlRoutes()

	if c.Control.Status() != SocketReady {
		if err := c.Control.Start(); err != nil {
			return fmt.Errorf("control.Start: %w", err)
		}
	}

	if err := c.bindExternal(); err != nil {
		c.cleanup()
		return err
	}

	c.workW.Add(1)
	go c.run()

	return nil
}

func (c *BasicReplier) setControlRoutes() {
	c.Control.Route(HandlerConfig, c.onControlConfig)
	c.Control.Route(HandlerStart, c.onControlStart)
	c.Control.Route(HandlerClose, c.onControlClose)
}

func (c *BasicReplier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		c.Control.SetSocketNil()
		if wake := c.wake; wake != nil {
			wake.signal()
		}
		c.workW.Wait()
	}
	return req.Ok(datatype.New())
}

func (c *BasicReplier) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Endpoint()))
}

func (c *BasicReplier) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Status() == SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Control.Status()))
	}
	if err := c.restartWork(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Control.Status()))
}

func (c *BasicReplier) restartWork() error {
	if err := c.bindExternal(); err != nil {
		return err
	}
	c.workW.Add(1)
	go c.run()
	return nil
}

func (c *BasicReplier) bindExternal() error {
	socket, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket('%s'): %w", c.Type(), err)
	}

	err = c.register(socket, c.Endpoint())
	if err != nil {
		_ = socket.Close()
		return fmt.Errorf("register: %w", err)
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

func (c *BasicReplier) run() {
	defer c.workW.Done()

	socket := c.socket
	if socket == nil {
		return
	}

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	wake, err := newWakePipe()
	if err != nil {
		c.LogError("basic_replier.newWakePipe", "error", err)
		c.cleanup()
		return
	}
	c.wake = wake
	defer wake.close()
	wake.addToPoller(poller)
	replies := make(chan pendingReply, 65536)

	for c.Control.Running() {
		c.flushReplies(socket, replies)

		sockets, err := poller.Poll(blockForever)
		if err != nil {
			break
		}

		for _, polled := range sockets {
			if isWakePoll(wake, polled) {
				wake.drain()
				continue
			}
			if polled.Socket != socket {
				continue
			}
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

func (c *BasicReplier) flushReplies(socket *zmq.Socket, replies <-chan pendingReply) {
	for {
		select {
		case pending := <-replies:
			if err := c.sendReply(socket, pending.reply, pending.cmd, pending.matchedSecret); err != nil {
				c.LogError("replier.sendReply", "error", err)
			}
		default:
			return
		}
	}
}

func (c *BasicReplier) failReply(raw []string, errMsg string) message.ReplyInterface {
	conId, _, _ := message.EnvelopeToMessage(raw)
	fail := c.Packer().EmptyRequest()
	fail.SetConId(conId)
	return fail.Fail(errMsg)
}

func (c *BasicReplier) handleRequest(socket *zmq.Socket, replies chan<- pendingReply) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, hmacHash, err := c.Packer().DeserializeRequest(raw)
	if err != nil {
		replies <- pendingReply{reply: c.failReply(raw, fmt.Sprintf("messageOps.DeserializeRequest: %v", err))}
		return nil
	}

	cmd := req.CommandName()
	matchedSecret := ""
	if c.IsWhitelistExist(cmd) {
		var ok bool
		matchedSecret, ok = c.getRequestSecret(req, hmacHash)
		if !ok {
			replies <- pendingReply{reply: req.Fail(message.ErrAccessDenied.Error())}
			return nil
		}
	} else if c.IsWhitelistRequired(cmd) {
		replies <- pendingReply{reply: req.Fail(message.ErrAccessDenied.Error() + ", whitelist required")}
		return nil
	}

	handleFunc, err := c.GetHandleFunc(cmd)
	if err != nil {
		replies <- pendingReply{reply: req.Fail(fmt.Sprintf("handler.GetHandleFunc(%s): %v", cmd, err))}
		return nil
	}

	reply := handleFunc(req)
	replies <- pendingReply{reply: reply, cmd: cmd, matchedSecret: matchedSecret}

	return nil
}

func (c *BasicReplier) sendReply(socket *zmq.Socket, reply message.ReplyInterface, cmd, matchedSecret string) error {
	var hmac string
	if c.IsWhitelistExist(cmd) && matchedSecret != "" {
		hmac = message.ComputeHMAC(reply.String(), matchedSecret)
	}
	envelope, err := c.Packer().SerializeReply(reply, hmac)
	if err != nil {
		return fmt.Errorf("messageOps.SerializeReply: %w", err)
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		return fmt.Errorf("socket.SendMessage: %w", err)
	}
	return nil
}

func (c *BasicReplier) cleanup() {
	takeAndCloseSocket(&c.socket)
	c.wake = nil
	c.Control.SetSocketNil()
}
