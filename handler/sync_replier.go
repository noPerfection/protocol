package handler

import (
	"fmt"
	"sync"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type SyncReplier struct {
	*Handler
	*Autocontext
	*Security
	socket  *zmq.Socket
	wake    *wakePipe
	Control *Control
	workW   sync.WaitGroup
}

var _ Interface = (*SyncReplier)(nil)

// NewSyncReplier returns a new SyncReplier.
func NewSyncReplier() *SyncReplier {
	return &SyncReplier{
		Handler:     New(),
		Control:     NewControl(),
		Autocontext: NewAutocontext(),
		Security:    NewSecurity(),
	}
}

func (replier *SyncReplier) Secure(secretKey string) {
	replier.Security.Secure(secretKey)
	replier.Control.setSecretKey(secretKey)
}

func (replier *SyncReplier) wireControlAutocontext() {
	if replier.Autocontext != nil {
		replier.Control.setNpacSecureEdgeCase(replier.NpacSecureEdgeCase)
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
	return c.Control.SetLogger(parent.Child(ControlCategory))
}

// Type returns the handler type.
func (c *SyncReplier) Type() HandlerType {
	return SyncReplierType
}

// Start the handler directly, not by goroutine.
func (c *SyncReplier) Start() error {
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

func (c *SyncReplier) setControlRoutes() {
	c.Control.Route(HandlerConfig, c.onControlConfig)
	c.Control.Route(HandlerStart, c.onControlStart)
	c.Control.Route(HandlerClose, c.onControlClose)
	c.Control.Route(HandlerCommands, func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("commands", c.Commands()))
	})
	c.Control.Route(HandlerRequireWhitelist, c.onControlRequireWhitelist)
	c.Control.Route(HandlerRequireSecure, c.onControlRequireSecure)
}

func (c *SyncReplier) MushroomURL() string {
	return c.mushroomURL
}

func (c *SyncReplier) onControlRequireWhitelist(req message.RequestInterface) message.ReplyInterface {
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

func (c *SyncReplier) onControlRequireSecure(req message.RequestInterface) message.ReplyInterface {
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

func (c *SyncReplier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		c.stopWork()
	}
	_ = c.npacRemoveHandler()
	return req.Ok(datatype.New())
}

func (c *SyncReplier) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Endpoint()))
}

func (c *SyncReplier) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Status() == SocketReady {
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
	_ = socket.SetLinger(0)

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

	err = c.npacRegisterHandler(c.Control.Endpoint())
	if err != nil {
		_ = socket.Close()
		return fmt.Errorf("npacRegisterHandler: %w", err)
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

	wake, err := newWakePipe()
	if err != nil {
		c.LogError("sync_replier.newWakePipe", "error", err)
		c.cleanup()
		return
	}
	c.wake = wake
	defer wake.close()
	wake.addToPoller(poller)

	for c.Control.Running() {
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
			if err := c.handleRequest(socket); err != nil {
				c.LogError("sync_replier.handleRequest", "error", err)
			}
		}
	}

	_ = poller.RemoveBySocket(socket)
	c.finishRun(socket)
}

func (c *SyncReplier) stopWork() {
	if !c.Control.Running() {
		return
	}
	c.Control.SetSocketNil()
	if wake := c.wake; wake != nil {
		wake.signal()
	}
	c.workW.Wait()
}

func (c *SyncReplier) handleRequest(socket *zmq.Socket) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, hmacHash, err := c.Packer().DeserializeRequest(raw)
	if err != nil {
		reply := c.Packer().EmptyRequest().Fail(fmt.Sprintf("messageOps.DeserializeRequest: %v", err))
		return c.sendSyncReply(socket, reply, "")
	}

	cmd := req.CommandName()
	matchedSecret := ""
	if c.IsWhitelistExist(cmd) {
		var ok bool
		matchedSecret, ok = c.getRequestSecret(req, hmacHash)
		if !ok {
			return c.sendSyncReply(socket, c.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()), "")
		}
	} else if c.IsWhitelistRequired(cmd) {
		return c.sendSyncReply(socket, c.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()+", whitelist required"), "")
	}

	handleFunc, err := c.GetHandleFunc(cmd)
	if err != nil {
		return c.sendSyncReply(socket, req.Fail(fmt.Sprintf("GetHandleFunc(%s): %v", cmd, err)), matchedSecret)
	}

	if err := c.npacPushHandleContext(cmd, handleFunc); err != nil {
		c.LogError("npacPushHandleContext", "error", err)
	}

	reply := handleFunc(req)

	if err := c.npacPopHandleContext(cmd, handleFunc); err != nil {
		c.LogError("npacPopHandleContext", "error", err)
	}

	return c.sendSyncReply(socket, reply, matchedSecret)
}

func (c *SyncReplier) sendSyncReply(socket *zmq.Socket, reply message.ReplyInterface, matchedSecret string) error {
	var hmac string
	if matchedSecret != "" {
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
	takeAndCloseBoundSocket(&c.socket, c.Endpoint().HandlerUrl())
	c.wake = nil
	c.Control.SetSocketNil()
}

func (c *SyncReplier) finishRun(runSocket *zmq.Socket) {
	if runSocket != nil && c.socket == runSocket {
		takeAndCloseBoundSocket(&c.socket, c.Endpoint().HandlerUrl())
	}
	c.wake = nil
	c.Control.SetSocketNil()
}
