package handler

import (
	"fmt"
	"sync"
	"time"

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
}

func (c *SyncReplier) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		c.Control.SetSocketNil()
		c.workW.Wait()
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
		return c.sendSyncReply(socket, reply, "", "")
	}

	cmd := req.CommandName()
	matchedSecret := ""
	if c.IsWhitelistExist(cmd) {
		var ok bool
		matchedSecret, ok = c.getRequestSecret(req, hmacHash)
		if !ok {
			return c.sendSyncReply(socket, c.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()), cmd, "")
		}
	}

	handleFunc, err := c.GetHandleFunc(cmd)
	if err != nil {
		return c.sendSyncReply(socket, req.Fail(fmt.Sprintf("GetHandleFunc(%s): %v", cmd, err)), cmd, matchedSecret)
	}

	if err := c.npacPushHandleContext(cmd); err != nil {
		c.LogError("AddRoute", "error", err)
	}

	reply := handleFunc(req)

	if err := c.popHandleContext(cmd); err != nil {
		c.LogError("RemoveRoute", "error", err)
	}

	return c.sendSyncReply(socket, reply, cmd, matchedSecret)
}

func (c *SyncReplier) sendSyncReply(socket *zmq.Socket, reply message.ReplyInterface, cmd, matchedSecret string) error {
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
	if socket := c.socket; socket != nil {
		_ = socket.Close()
	}
	c.socket = nil
	c.Control.SetSocketNil()
}
