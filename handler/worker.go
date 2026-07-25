package handler

// Asynchronous worker

import (
	"fmt"
	"sync"
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
	wake    *wakePipe
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

func (worker *Worker) Secure(secretKey string) {
	worker.Security.Secure(secretKey)
	worker.Control.setSecretKey(secretKey)
}

func (worker *Worker) wireControlAutocontext() {
	if worker.Autocontext != nil {
		worker.Control.setNpacSecureEdgeCase(worker.NpacSecureEdgeCase)
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

func (c *Worker) setControlRoutes() {
	c.Control.Route(HandlerConfig, c.onControlConfig)
	c.Control.Route(HandlerStart, c.onControlStart)
	c.Control.Route(HandlerClose, c.onControlClose)
	c.Control.Route(HandlerCommands, func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("commands", c.Commands()))
	})
	c.Control.Route(HandlerRequireWhitelist, c.onControlRequireWhitelist)
	c.Control.Route(HandlerRequireSecure, c.onControlRequireSecure)
	c.Control.Route(HandlerAllow, func(req message.RequestInterface) message.ReplyInterface {
		publicKey, err := req.RouteParameters().StringValue("public-key")
		if err != nil || publicKey == "" {
			return req.Fail("public-key is required")
		}
		c.Allow(publicKey)
		return req.Ok(datatype.New())
	})
}

func (c *Worker) onControlRequireWhitelist(req message.RequestInterface) message.ReplyInterface {
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

func (c *Worker) onControlRequireSecure(req message.RequestInterface) message.ReplyInterface {
	if !c.IsSecure() {
		_, secret, err := message.GenerateCurveKey()
		if err != nil {
			return req.Fail(fmt.Sprintf("message.GenerateCurveKey: %v", err))
		}
		wasRunning := c.Control.Running()
		if wasRunning {
			c.Control.SetSocketNil()
			if wake := c.wake; wake != nil {
				wake.signal()
			}
			c.workW.Wait()
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

func (c *Worker) onControlClose(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Running() {
		c.Control.SetSocketNil()
		if wake := c.wake; wake != nil {
			wake.signal()
		}
		c.workW.Wait()
	}
	_ = c.npacRemoveHandler()
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

	err = c.auth(socket, c.Endpoint())
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

	wake, err := newWakePipe()
	if err != nil {
		c.LogError("worker.newWakePipe", "error", err)
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
	if c.IsWhitelistExist(cmd) {
		_, ok := c.getRequestSecret(req, hmacHash)
		if !ok {
			return fmt.Errorf("%w", message.ErrAccessDenied)
		}
	} else if c.IsWhitelistRequired(cmd) {
		return fmt.Errorf("%s", message.ErrAccessDenied.Error()+", whitelist required")
	}

	handleFunc, err := c.GetHandleFunc(cmd)
	if err != nil {
		return fmt.Errorf("handler.GetHandleFunc(%s): %w", cmd, err)
	}

	go func() {
		if err := c.npacPushHandleContext(cmd, handleFunc); err != nil {
			c.LogError("npacPushHandleContext", "error", err)
		}
		handleFunc(req)
		if err := c.npacPopHandleContext(cmd, handleFunc); err != nil {
			c.LogError("npacPopHandleContext", "error", err)
		}
	}()

	return nil
}

func (c *Worker) cleanup() {
	takeAndCloseSocket(&c.socket)
	c.wake = nil
	c.Control.SetSocketNil()
}
