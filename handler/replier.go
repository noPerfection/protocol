package handler

// Asynchronous replier

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// Replier is the socket wrapper for the service.
type Replier struct {
	*Handler
	*Autocontext
	*Security
	socket   *zmq.Socket
	Control  *Control
	workW    sync.WaitGroup
	handlerW sync.WaitGroup
}

type pendingReply struct {
	reply         message.ReplyInterface
	cmd           string
	matchedSecret string
}

var _ Interface = (*Replier)(nil)

// NewReplier asynchronous replying handler.
func NewReplier() *Replier {
	return &Replier{
		Handler:     New(),
		Control:     NewControl(),
		Autocontext: NewAutocontext(),
		Security:    NewSecurity(),
	}
}

func (replier *Replier) Secure(secretKey string) {
	replier.Security.Secure(secretKey)
	replier.Control.setSecretKey(secretKey)
}

func (replier *Replier) wireControlAutocontext() {
	if replier.Autocontext != nil {
		replier.Control.setNpacSecureEdgeCase(replier.NpacSecureEdgeCase)
	}
}

// SetEndpoint adds the parameters of the handler from the config.
func (c *Replier) SetEndpoint(endpoint message.Endpoint) {
	c.Handler.SetEndpoint(endpoint)
	c.Control.SetEndpoint(endpoint)
}

func (c *Replier) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(ControlCategory))
}

// Type returns the handler type. If the configuration is not set, returns handler.UnknownType.
func (c *Replier) Type() HandlerType {
	return ReplierType
}

// Start the handler directly, not by goroutine
func (c *Replier) Start() error {
	if c.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("configuration not set")
	}
	if c.Control == nil {
		return fmt.Errorf("control not set")
	}

	if c.mushroomURL == "" {
		return fmt.Errorf("mushroom URL not set, call SetMushroomURL first")
	}

	c.wireControlAutocontext()
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

func (c *Replier) setControlRoutes() {
	c.Control.Route(HandlerConfig, c.onControlConfig)
	c.Control.Route(HandlerStart, c.onControlStart)
	c.Control.Route(HandlerClose, c.onControlClose)
	c.Control.Route(HandlerCommands, func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("commands", c.Commands()))
	})
	c.Control.Route(HandlerRequireWhitelist, c.onControlRequireWhitelist)
	c.Control.Route(HandlerRequireSecure, c.onControlRequireSecure)
}

func (c *Replier) MushroomURL() string {
	return c.mushroomURL
}

func (c *Replier) onControlRequireWhitelist(req message.RequestInterface) message.ReplyInterface {
	cmd, err := req.RouteParameters().StringValue("command")
	if err != nil {
		return req.Fail(fmt.Sprintf("req.RouteParameters().StringValue('command'): %v", err))
	}
	if cmd == "" {
		return req.Fail("command is required")
	}

	secret, err := req.RouteParameters().StringValue("secret")
	if err == nil && secret != "" {
		if c.IsWhitelistExist(cmd, true) {
			return req.Ok(datatype.New().Set("whitelisted", true))
		}
		if err := c.Whitelist(cmd, secret); err != nil {
			return req.Fail(fmt.Sprintf(`Whitelist("%s"): %v`, cmd, err))
		}
	} else {
		if c.IsWhitelistRequired(cmd, true) {
			return req.Ok(datatype.New())
		}
		c.RequireWhitelist(cmd)
	}

	return req.Ok(datatype.New())
}

func (c *Replier) onControlRequireSecure(req message.RequestInterface) message.ReplyInterface {
	if !c.IsSecure() {
		_, secret, err := message.GenerateCurveKey()
		if err != nil {
			return req.Fail(fmt.Sprintf("message.GenerateCurveKey: %v", err))
		}
		wasRunning := c.Control.Running()
		if wasRunning {
			c.stopWork()
		}
		c.Secure(secret)
		if wasRunning {
			if err := c.restartWork(); err != nil {
				return req.Fail(err.Error())
			}
		}
	}

	pubKey, err := c.PublicKey()
	if err != nil {
		return req.Fail(fmt.Sprintf("PublicKey: %v", err))
	}

	return req.Ok(datatype.New().Set("public-key", pubKey))
}

func (c *Replier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		c.stopWork()
	}
	_ = c.npacRemoveHandler()
	return req.Ok(datatype.New())
}

func (c *Replier) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Endpoint()))
}

func (c *Replier) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Status() == SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Control.Status()))
	}
	if err := c.restartWork(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Control.Status()))
}

func (c *Replier) stopWork() {
	if !c.Control.Running() {
		return
	}
	c.Control.SetSocketNil()
	takeAndCloseBoundSocket(&c.socket, c.Endpoint().HandlerUrl())
	c.workW.Wait()
}

func (c *Replier) restartWork() error {
	if err := c.bindExternal(); err != nil {
		return err
	}
	c.workW.Add(1)
	go c.run()
	return nil
}

func (c *Replier) bindExternal() error {
	socket, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket('%s'): %w", c.Type(), err)
	}

	err = c.auth(socket, c.mushroomURL)
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

	_ = c.npacRegisterHandler(c.Control.Endpoint())

	return nil
}

// pollInterval is how long Replier.run waits when no socket events are ready.
// Replies are flushed at the start of each loop iteration, so this bounds
// reply latency without a per-handler wake pipe (which can abort libzmq under load).
const pollInterval = 50 * time.Millisecond

func (c *Replier) run() {
	defer c.workW.Done()

	socket := c.socket
	if socket == nil {
		return
	}

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	replies := make(chan pendingReply, 65536)

	for c.Control.Running() {
		c.flushReplies(socket, replies)

		sockets, err := poller.Poll(pollInterval)
		if err != nil {
			break
		}

		for _, polled := range sockets {
			if polled.Socket != socket {
				continue
			}
			for {
				if err := c.handleRequest(socket, replies); err != nil {
					if isZmqWouldBlock(err) {
						break
					}
					c.LogError("replier.handleRequest", "error", err)
					break
				}
			}
		}
	}

	c.handlerW.Wait()
	c.flushReplies(socket, replies)

	_ = poller.RemoveBySocket(socket)
	c.cleanup()
}

func (c *Replier) flushReplies(socket *zmq.Socket, replies <-chan pendingReply) {
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

func (c *Replier) failReply(raw []string, errMsg string) message.ReplyInterface {
	conId, _, _ := message.EnvelopeToMessage(raw)
	fail := c.Packer().EmptyRequest()
	fail.SetConId(conId)
	return fail.Fail(errMsg)
}

func (c *Replier) enqueueReply(replies chan<- pendingReply, pending pendingReply) {
	replies <- pending
}

func (c *Replier) handleRequest(socket *zmq.Socket, replies chan<- pendingReply) error {
	raw, err := socket.RecvMessage(zmq.DONTWAIT)
	if err != nil {
		return err
	}

	req, hmacHash, err := c.Packer().DeserializeRequest(raw)
	if err != nil {
		reply := c.failReply(raw, fmt.Sprintf("messageOps.DeserializeRequest: %v", err))
		c.enqueueReply(replies, pendingReply{reply: reply})
		return nil
	}

	cmd := req.CommandName()
	matchedSecret := ""
	if c.IsWhitelistExist(cmd) {
		var ok bool
		matchedSecret, ok = c.getRequestSecret(req, hmacHash)
		if !ok {
			c.enqueueReply(replies, pendingReply{reply: req.Fail(message.ErrAccessDenied.Error())})
			return nil
		}
	} else if c.IsWhitelistRequired(cmd) {
		c.enqueueReply(replies, pendingReply{reply: req.Fail(message.ErrAccessDenied.Error() + ", whitelist required")})
		return nil
	}

	handleFunc, err := c.GetHandleFunc(cmd)
	if err != nil {
		c.enqueueReply(replies, pendingReply{reply: req.Fail(fmt.Sprintf("handler.GetHandleFunc(%s): %v", cmd, err))})
		return nil
	}

	c.handlerW.Add(1)
	go func(cmd, matchedSecret string) {
		defer c.handlerW.Done()

		if err := c.npacPushHandleContext(cmd, handleFunc); err != nil {
			c.LogError("npacPushHandleContext", "error", err)
		}

		reply := handleFunc(req)
		if err := c.npacPopHandleContext(cmd, handleFunc); err != nil {
			c.LogError("npacPopHandleContext", "error", err)
		}
		c.enqueueReply(replies, pendingReply{reply: reply, cmd: cmd, matchedSecret: matchedSecret})
	}(cmd, matchedSecret)

	return nil
}

func (c *Replier) sendReply(socket *zmq.Socket, reply message.ReplyInterface, cmd, matchedSecret string) error {
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

func (c *Replier) cleanup() {
	takeAndCloseBoundSocket(&c.socket, c.Endpoint().HandlerUrl())
	c.Control.SetSocketNil()
}
