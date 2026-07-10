package replier

// Asynchronous replier

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/autocontext"
	base "github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// Replier is the socket wrapper for the service.
type Replier struct {
	*base.Handler
	socket               *zmq.Socket
	Control              *control.Manager
	workW                sync.WaitGroup
	curveSecretKey       string
	allowedClientPubKeys []string
	npacSecret           string
}

type pendingReply struct {
	reply         message.ReplyInterface
	cmd           string
	matchedSecret string
}

var _ base.Interface = (*Replier)(nil)

// New asynchronous replying handler.
func New() *Replier {
	return &Replier{
		Handler:    base.New(),
		Control:    control.New(),
		npacSecret: base.GenerateSecret(),
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
	return c.Control.SetLogger(parent.Child(control.ControlCategory))
}

// Secure stores the CURVE server secret key. An empty key keeps the handler non-secure.
func (c *Replier) Secure(secretKey string) {
	c.curveSecretKey = secretKey
}

// Allow registers a client CURVE public key permitted to connect when ZAP is active (zmq.AuthStart).
func (c *Replier) Allow(clientPubKey string) {
	if clientPubKey == "" {
		return
	}
	for _, key := range c.allowedClientPubKeys {
		if key == clientPubKey {
			return
		}
	}
	c.allowedClientPubKeys = append(c.allowedClientPubKeys, clientPubKey)
}

// Type returns the handler type. If the configuration is not set, returns base.UnknownType.
func (c *Replier) Type() base.HandlerType {
	return base.ReplierType
}

// Start the handler directly, not by goroutine
func (c *Replier) Start() error {
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

	c.workW.Add(1)
	go c.run()

	return nil
}

func (c *Replier) setControlRoutes() {
	c.Control.Route(control.HandlerConfig, c.onControlConfig)
	c.Control.Route(control.HandlerStart, c.onControlStart)
	c.Control.Route(control.HandlerClose, c.onControlClose)
}

func (c *Replier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		c.Control.SetSocketNil()
		c.workW.Wait()
	}
	_ = autocontext.RemoveHandler(c.Endpoint().HandlerUrl(), c.npacSecret)
	return req.Ok(datatype.New())
}

func (c *Replier) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Endpoint()))
}

func (c *Replier) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Status() == base.SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Control.Status()))
	}
	if err := c.restartWork(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Control.Status()))
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

	pubKey := ""
	if c.curveSecretKey != "" {
		domain := c.Endpoint().ZapDomain()
		if err := socket.ServerAuthCurve(domain, c.curveSecretKey); err != nil {
			_ = socket.Close()
			return fmt.Errorf("socket.ServerAuthCurve: %w", err)
		}
		if len(c.allowedClientPubKeys) > 0 {
			zmq.AuthCurveAdd(domain, c.allowedClientPubKeys...)
		}
		if derivedKey, deriveErr := zmq.AuthCurvePublic(c.curveSecretKey); deriveErr == nil {
			pubKey = derivedKey
		}
	}

	externalUrl := c.Endpoint().HandlerUrl()
	if err := socket.Bind(externalUrl); err != nil {
		_ = socket.Close()
		return fmt.Errorf("external.Bind('%s'): %w", externalUrl, err)
	}

	c.socket = socket
	c.Control.SetSocketReady()

	_ = autocontext.AddHandler(externalUrl, pubKey, c.npacSecret)
	for cmd, secrets := range c.WhitelistSnapshot() {
		for _, secret := range secrets {
			_ = autocontext.AddRoute(externalUrl, cmd, secret, c.npacSecret)
		}
	}

	return nil
}

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
			if err := c.sendReply(socket, pending.reply, pending.cmd, pending.matchedSecret); err != nil {
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

	req, hmacHash, err := c.Packer().DeserializeRequest(raw)
	if err != nil {
		reply := c.Packer().EmptyRequest().Fail(fmt.Sprintf("messageOps.DeserializeRequest: %v", err))
		replies <- pendingReply{reply: reply}
		return nil
	}

	cmd := req.CommandName()
	matchedSecret := ""
	if c.RequiresWhitelist(cmd) {
		var ok bool
		matchedSecret, ok = c.MatchRequestSecret(req, hmacHash)
		if !ok {
			replies <- pendingReply{reply: c.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error())}
			return nil
		}
	}

	handleFunc, err := c.GetHandleFunc(cmd)
	if err != nil {
		replies <- pendingReply{reply: req.Fail(fmt.Sprintf("base.GetHandleFunc(%s): %v", cmd, err))}
		return nil
	}

	handlerUrl := c.Endpoint().HandlerUrl()
	if matchedSecret != "" {
		if err := autocontext.AddRoute(handlerUrl, cmd, matchedSecret, c.npacSecret); err != nil {
			c.LogError("autocontext.AddRoute", "error", err)
		}
	}

	go func(cmd, matchedSecret, handlerUrl string) {
		reply := handleFunc(req)
		if matchedSecret != "" {
			if err := autocontext.RemoveRoute(handlerUrl, cmd, c.npacSecret); err != nil {
				c.LogError("autocontext.RemoveRoute", "error", err)
			}
		}
		replies <- pendingReply{reply: reply, cmd: cmd, matchedSecret: matchedSecret}
	}(cmd, matchedSecret, handlerUrl)

	return nil
}

func (c *Replier) sendReply(socket *zmq.Socket, reply message.ReplyInterface, cmd, matchedSecret string) error {
	var hmac string
	if c.RequiresWhitelist(cmd) && matchedSecret != "" {
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
	if socket := c.socket; socket != nil {
		_ = socket.Close()
	}
	c.socket = nil
	c.Control.SetSocketNil()
}
