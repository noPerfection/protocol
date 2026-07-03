// Package base keeps the generic Handler.
// It's not intended to be used independently.
// Other handlers should be defined based on this handler
package base

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"

	"github.com/noPerfection/protocol/message"
)

const (
	Incomplete  = "incomplete"
	SocketIdle  = "idle"  // Socket is bind but not listening to receive messages
	SocketReady = "ready" // Socket is bind and started
	SocketNil   = "nil"   // Socket is removed and all clean
)

// Any route name.
const Any = "*"

// HandleFunc is the function type that handles a request and returns a reply.
type HandleFunc = func(message.RequestInterface) message.ReplyInterface

// The Handler is the socket wrapper for the zeromq socket.
type Handler struct {
	endpoint      message.Endpoint
	logger        *log.Logger
	messagePacker message.Packer
	routes        datatype.KeyValue
}

// New creates a handler.
// Optionally you can set the logger.
func New(logger ...*log.Logger) *Handler {
	h := &Handler{
		messagePacker: &message.MessagePacker{},
		routes:        datatype.New(),
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

// Commands returns list of all route commands.
func (c *Handler) Commands() []string {
	commands := make([]string, len(c.routes))

	i := 0
	for command := range c.routes {
		commands[i] = command
		i++
	}

	return commands
}

func (c *Handler) Endpoint() message.Endpoint {
	return c.endpoint
}

func (c *Handler) Packer() message.Packer {
	return c.messagePacker
}

// Logger returns the handler logger.
func (c *Handler) Logger() *log.Logger {
	return c.logger
}

// SetEndpoint adds the parameters of the handler from the config.
func (c *Handler) SetEndpoint(endpoint message.Endpoint) {
	c.endpoint = endpoint
}

func (c *Handler) SetPacker(packer message.Packer) {
	c.messagePacker = packer
}

// SetLogger sets the logger. Passing nil disables logging.
func (c *Handler) SetLogger(parent *log.Logger) error {
	if parent == nil {
		c.logger = nil
		return nil
	}
	if c.endpoint == (message.Endpoint{}) {
		c.logger = parent
		return nil
	}
	c.logger = parent.Child(c.endpoint.Id)

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

func (c *Handler) GetHandleFunc(cmd string) (HandleFunc, error) {
	var handle any

	if c.routes.Exist(cmd) {
		handle = c.routes[cmd]
	} else if c.routes.Exist(Any) {
		handle = c.routes[Any]
	} else {
		return nil, fmt.Errorf("the '%s' command handler not found", cmd)
	}

	handleFunc, ok := handle.(HandleFunc)
	if !ok {
		return nil, fmt.Errorf("the '%s' command handler is not a valid handle function", cmd)
	}

	return handleFunc, nil
}

// Type returns the handler type. If the configuration is not set, returns UnknownType.
func (c *Handler) Type() HandlerType {
	return UnknownType
}

func AnyRoute(handler *Handler) error {
	if err := handler.Route(Any, func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok()
	}); err != nil {
		return fmt.Errorf("failed to '%s' route into the handler: %w", Any, err)
	}
	return nil
}
