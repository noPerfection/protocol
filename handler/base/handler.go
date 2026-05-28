// Package base keeps the generic Handler.
// It's not intended to be used independently.
// Other handlers should be defined based on this handler
package base

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/route"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	Incomplete  = "incomplete"
	Ready       = "ready"
	SocketIdle  = "idle"
	SocketReady = "ready"
	SocketNil   = "nil"
)

// The Handler is the socket wrapper for the zeromq socket.
type Handler struct {
	config *config.Handler
	socket *zmq.Socket
	logger *log.Logger
	Routes datatype.KeyValue
	status string
	close  bool
}

// New creates a handler.
// Optionally you can set the logger.
func New(logger ...*log.Logger) *Handler {
	h := &Handler{
		Routes: datatype.New(),
		status: SocketNil,
		close:  false,
	}

	if len(logger) > 0 && logger[0] != nil {
		h.logger = logger[0]
	}

	return h
}

// IsRouteExist returns true if the given route exists
func (c *Handler) IsRouteExist(command string) bool {
	return c.Routes.Exist(command)
}

// RouteCommands returns list of all route commands
func (c *Handler) RouteCommands() []string {
	commands := make([]string, len(c.Routes))

	i := 0
	for command := range c.Routes {
		commands[i] = command
		i++
	}

	return commands
}

func (c *Handler) Config() *config.Handler {
	return c.config
}

// Logger returns the handler logger.
func (c *Handler) Logger() *log.Logger {
	return c.logger
}

// SetConfig adds the parameters of the handler from the config.
func (c *Handler) SetConfig(handler *config.Handler) {
	c.config = handler
}

// SetLogger sets the logger (depends on context).
func (c *Handler) SetLogger(parent *log.Logger) error {
	if c.config == nil {
		return fmt.Errorf("missing configuration")
	}
	logger := parent.Child(c.config.Id)
	c.logger = logger

	return nil
}

// Route adds a route along with its handler to this handler
func (c *Handler) Route(cmd string, handle any) error {
	if !route.IsHandleFunc(handle) {
		return fmt.Errorf("handle is not a valid handle function")
	}

	c.Routes.Set(cmd, handle)

	return nil
}

// SetRoutes registers or overwrites multiple routes.
func (c *Handler) SetRoutes(routes map[string]route.HandleFunc) error {
	if c.status == SocketReady {
		return fmt.Errorf("can not overwrite handler when handler is running")
	}

	for cmd, handle := range routes {
		if err := c.Route(cmd, handle); err != nil {
			return err
		}
	}

	return nil
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Handler) Type() config.HandlerType {
	if c.config == nil {
		return config.UnknownType
	}
	return c.config.Type
}

func (c *Handler) Status() string {
	return c.status
}

// Closed returns true when the handler received a close signal.
func (c *Handler) Closed() bool {
	return c.close
}

// SetClose sets the handler close state.
func (c *Handler) SetClose(close bool) {
	c.close = close
}

// SetSocketIdle marks the handler socket as idle.
func (c *Handler) SetSocketIdle() {
	c.status = SocketIdle
}

// SetSocketReady marks the handler socket as ready.
func (c *Handler) SetSocketReady() {
	c.status = SocketReady
}

// SetSocketNil clears the handler socket and marks it nil.
func (c *Handler) SetSocketNil() {
	c.socket = nil
	c.status = SocketNil
}

// SetSocket assigns the handler's external ZeroMQ socket.
func (c *Handler) SetSocket(socket *zmq.Socket) {
	c.socket = socket
	if socket == nil {
		c.status = SocketNil
	} else {
		c.status = SocketIdle
	}
}

// Socket returns the handler's external ZeroMQ socket.
func (c *Handler) Socket() *zmq.Socket {
	return c.socket
}

// Does nothing, simply returns the data
var anyHandler = func(request message.RequestInterface) message.ReplyInterface {
	replyParameters := datatype.New().Set("route", request.CommandName())

	reply := request.Ok(replyParameters)
	return reply
}

func AnyRoute(handler *Handler) error {
	if err := handler.Route(route.Any, anyHandler); err != nil {
		return fmt.Errorf("failed to '%s' route into the handler: %w", route.Any, err)
	}
	return nil
}

func requiredMetadata() []string {
	return []string{"Identity", "pub_key"}
}
