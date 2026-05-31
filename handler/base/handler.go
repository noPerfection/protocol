// Package base keeps the generic Handler.
// It's not intended to be used independently.
// Other handlers should be defined based on this handler
package base

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/config"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	Incomplete  = "incomplete"
	SocketIdle  = "idle"
	SocketReady = "ready"
	SocketNil   = "nil"
)

// Any route name.
const Any = "*"

// HandleFunc is the function type that handles a request and returns a reply.
type HandleFunc = func(message.RequestInterface) message.ReplyInterface

// The Handler is the socket wrapper for the zeromq socket.
type Handler struct {
	config *config.Handler
	socket *zmq.Socket
	logger *log.Logger
	routes datatype.KeyValue
	status string
	close  bool
}

// New creates a handler.
// Optionally you can set the logger.
func New(logger ...*log.Logger) *Handler {
	h := &Handler{
		routes: datatype.New(),
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
	return c.routes.Exist(command)
}

// RouteCommands returns list of all route commands
func (c *Handler) RouteCommands() []string {
	commands := make([]string, len(c.routes))

	i := 0
	for command := range c.routes {
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

// SetLogger sets the logger. Passing nil disables logging.
func (c *Handler) SetLogger(parent *log.Logger) error {
	if parent == nil {
		c.logger = nil
		return nil
	}
	if c.config == nil {
		c.logger = parent
		return nil
	}
	c.logger = parent.Child(c.config.Id)

	return nil
}

// LogError writes an error log when a logger is configured.
func (c *Handler) LogError(msg string, args ...interface{}) {
	if c.logger == nil {
		return
	}
	c.logger.Error(msg, args...)
}

// LogWarn writes a warning log when a logger is configured.
func (c *Handler) LogWarn(msg string, args ...interface{}) {
	if c.logger == nil {
		return
	}
	c.logger.Warn(msg, args...)
}

// Route adds a route along with its handler to this handler.
func (c *Handler) Route(cmd string, handle HandleFunc) error {
	c.routes.Set(cmd, handle)

	return nil
}

// SetRoutes registers or overwrites multiple routes.
func (c *Handler) SetRoutes(routes map[string]HandleFunc) error {
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

func (c *Handler) FindRoute(cmd string) (HandleFunc, error) {
	return FindRoute(cmd, c.routes)
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

// FindRoute returns the command handler or the catch-all handler.
func FindRoute(cmd string, routeFuncs datatype.KeyValue) (HandleFunc, error) {
	var handle any

	if routeFuncs.Exist(cmd) {
		handle = routeFuncs[cmd]
	} else if routeFuncs.Exist(Any) {
		handle = routeFuncs[Any]
	} else {
		return nil, fmt.Errorf("the '%s' command handler not found", cmd)
	}

	handleFunc, ok := handle.(HandleFunc)
	if !ok {
		return nil, fmt.Errorf("the '%s' command handler is not a valid handle function", cmd)
	}

	return handleFunc, nil
}

// Handle calls the handle func for the request.
func Handle(req message.RequestInterface, handle HandleFunc) message.ReplyInterface {
	return handle(req)
}

// Does nothing, simply returns the data
var anyHandler HandleFunc = func(request message.RequestInterface) message.ReplyInterface {
	replyParameters := datatype.New().Set("route", request.CommandName())

	reply := request.Ok(replyParameters)
	return reply
}

func AnyRoute(handler *Handler) error {
	if err := handler.Route(Any, anyHandler); err != nil {
		return fmt.Errorf("failed to '%s' route into the handler: %w", Any, err)
	}
	return nil
}
