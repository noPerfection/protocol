package sync_replier

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/autocontext"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type SyncReplier struct {
	*handler.Handler
	socket               *zmq.Socket
	Control              *control.Manager
	workW                sync.WaitGroup
	curveSecretKey       string
	allowedClientPubKeys []string
	npacSecret           string
}

var _ handler.Interface = (*SyncReplier)(nil)

// New SyncReplier returned
func New() *SyncReplier {
	return &SyncReplier{
		Handler:    handler.New(),
		Control:    control.New(),
		npacSecret: handler.GenerateSecret(),
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

// Secure stores the CURVE server secret key. An empty key keeps the handler non-secure.
func (c *SyncReplier) Secure(secretKey string) {
	c.curveSecretKey = secretKey
}

// Allow registers a client CURVE public key permitted to access this handler (zmq.AuthStart).
func (c *SyncReplier) Allow(clientPubKey string) {
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

// Type returns the handler type. If the configuration is not set, returns handler.UnknownType.
func (c *SyncReplier) Type() handler.HandlerType {
	return handler.SyncReplierType
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

	if c.Control.Status() != handler.SocketReady {
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

func (c *SyncReplier) setControlRoutes() {
	c.Control.Route(control.HandlerConfig, c.onControlConfig)
	c.Control.Route(control.HandlerStart, c.onControlStart)
	c.Control.Route(control.HandlerClose, c.onControlClose)
}

func (c *SyncReplier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		c.Control.SetSocketNil()
		c.workW.Wait()
	}
	if c.Endpoint().Id != autocontext.NpacEndpointId {
		_ = autocontext.RemoveHandler(c.Endpoint().HandlerUrl(), c.npacSecret)
	}
	return req.Ok(datatype.New())
}

func (c *SyncReplier) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Endpoint()))
}

func (c *SyncReplier) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Status() == handler.SocketReady {
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
	c.workW.Add(1)
	go c.run()
	return nil
}

func (c *SyncReplier) bindExternal() error {
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket(REP): %w", err)
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

	/*
		ai extension adds its public key and all hmac secrets to npac,
		now any client can call it.

		then user requests the main handler.

		main handler adds the hello command to npac and removes after handling.

		main handler requests the ai extension.
		ai extension checks the current app.
		the client gets the ai extension's public key and hmac secrets from npac.

	*/

	// Register with npac (best-effort; silently ignored if npac is not running).
	// Always register so HMAC-whitelisted commands are discoverable by clients
	// even when no CURVE security is configured.
	if c.Endpoint().Id != autocontext.NpacEndpointId {
		_ = autocontext.AddHandler(externalUrl, pubKey, c.npacSecret)
		for cmd, secrets := range c.WhitelistSnapshot() {
			for _, secret := range secrets {
				_ = autocontext.AddRoute(externalUrl, cmd, secret, c.npacSecret)
			}
		}
	}

	return nil
}

func (c *SyncReplier) run() {
	defer c.workW.Done()

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

	req, hmacHash, err := c.Packer().DeserializeRequest(raw)
	if err != nil {
		reply := c.Packer().EmptyRequest().Fail(fmt.Sprintf("messageOps.DeserializeRequest: %v", err))
		return c.sendReply(socket, reply, "", "")
	}

	cmd := req.CommandName()
	matchedSecret := ""
	if c.RequiresWhitelist(cmd) {
		var ok bool
		matchedSecret, ok = c.MatchRequestSecret(req, hmacHash)
		if !ok {
			return c.sendReply(socket, c.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()), cmd, matchedSecret)
		}
	}

	handleFunc, err := c.GetHandleFunc(cmd)
	if err != nil {
		return c.sendReply(socket, req.Fail(fmt.Sprintf("handler.GetHandleFunc(%s): %v", cmd, err)), cmd, matchedSecret)
	}

	// Register the current route's HMAC secret with npac so clients can look it up.
	// Skip if this handler IS npac to avoid a self-referential inproc deadlock.
	handlerUrl := c.Endpoint().HandlerUrl()
	isNpac := c.Endpoint().Id == autocontext.NpacEndpointId
	if matchedSecret != "" && !isNpac {
		if err := autocontext.AddRoute(handlerUrl, cmd, matchedSecret, c.npacSecret); err != nil {
			c.LogError("autocontext.AddRoute", "error", err)
		}
	}

	reply := handleFunc(req)

	// Remove the route registration after handling.
	if matchedSecret != "" && !isNpac {
		if err := autocontext.RemoveRoute(handlerUrl, cmd, c.npacSecret); err != nil {
			c.LogError("autocontext.RemoveRoute", "error", err)
		}
	}

	return c.sendReply(socket, reply, cmd, matchedSecret)
}

func (c *SyncReplier) sendReply(socket *zmq.Socket, reply message.ReplyInterface, cmd, matchedSecret string) error {
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

func (c *SyncReplier) cleanup() {
	if socket := c.socket; socket != nil {
		_ = socket.Close()
	}
	c.socket = nil
	c.Control.SetSocketNil()
}
