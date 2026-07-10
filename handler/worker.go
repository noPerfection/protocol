package handler

// Asynchronous worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// Worker is the socket wrapper for the service.
type Worker struct {
	*Handler
	*Autocontext
	*Security
	socket  *zmq.Socket
	Control *Control
	workW   sync.WaitGroup
}

var _ Interface = (*Worker)(nil)

// NewWorker asynchronous replying handler.
func NewWorker() *Worker {
	return &Worker{
		Handler:     New(),
		Control:     NewControl(),
		Autocontext: NewAutocontext(),
		Security:    NewSecurity(),
	}
}

// SetEndpoint adds the parameters of the handler from the config.
func (c *Worker) SetEndpoint(endpoint message.Endpoint) {
	c.Handler.SetEndpoint(endpoint)
	c.Control.SetEndpoint(endpoint)
}

func (c *Worker) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(ControlCategory))
}

// Type returns the handler type. If the configuration is not set, returns handler.UnknownType.
func (c *Worker) Type() HandlerType {
	return WorkerType
}

// Start the handler directly, not by goroutine.
func (c *Worker) Start() error {
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

func (c *Worker) setControlRoutes() {
	c.Control.Route(HandlerConfig, c.onControlConfig)
	c.Control.Route(HandlerStart, c.onControlStart)
	c.Control.Route(HandlerClose, c.onControlClose)
}

func (c *Worker) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		c.Control.SetSocketNil()
		c.workW.Wait()
	}
	_ = c.npacRemoveHandler(c.Endpoint().HandlerUrl())
	return req.Ok(datatype.New())
}

func (c *Worker) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Endpoint()))
}

func (c *Worker) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Control.Status()))
	}
	if err := c.restartWork(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Control.Status()))
}

func (c *Worker) restartWork() error {
	if err := c.bindExternal(); err != nil {
		return err
	}
	c.workW.Add(1)
	go c.run()
	return nil
}

func (c *Worker) bindExternal() error {
	socket, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket(PULL): %w", err)
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

	pubKey := c.publicKey()
	err = c.npacRegisterHandler(externalUrl, pubKey)
	if err != nil {
		return fmt.Errorf("npacRegisterHandler: %w", err)
	}
	return nil
}

func (c *Worker) run() {
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
				c.LogError("worker.handleRequest", "error", err)
			}
		}
	}

	_ = poller.RemoveBySocket(socket)
	c.cleanup()
}

func (c *Worker) handleRequest(socket *zmq.Socket) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, hmacHash, err := c.Packer().DeserializeRequest(raw)
	if err != nil {
		return fmt.Errorf("messageOps.DeserializeRequest: %w", err)
	}

	cmd := req.CommandName()
	matchedSecret := ""
	if c.IsWhitelistExist(cmd) {
		var ok bool
		matchedSecret, ok = c.getRequestSecret(req, hmacHash)
		if !ok {
			return fmt.Errorf("%w", message.ErrAccessDenied)
		}
	}

	handleFunc, err := c.GetHandleFunc(cmd)
	if err != nil {
		return fmt.Errorf("handler.GetHandleFunc(%s): %w", cmd, err)
	}

	handlerUrl := c.Endpoint().HandlerUrl()
	if matchedSecret != "" {
		if err := c.npacPushHandleContext(handlerUrl, cmd, matchedSecret); err != nil {
			c.LogError("AddRoute", "error", err)
		}
	}

	go func(matchedSecret, handlerUrl, cmd string) {
		handleFunc(req)
		if matchedSecret != "" {
			if err := c.popHandleContext(handlerUrl, cmd); err != nil {
				c.LogError("RemoveRoute", "error", err)
			}
		}
	}(matchedSecret, handlerUrl, cmd)

	return nil
}

func (c *Worker) cleanup() {
	if socket := c.socket; socket != nil {
		_ = socket.Close()
	}
	c.socket = nil
	c.Control.SetSocketNil()
}
