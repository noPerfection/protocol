// Package base keeps the generic Handler.
// It's not intended to be used independently.
// Other handlers should be defined based on this handler
package base

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/handler_manager"
	"github.com/noPerfection/protocol/handler/route"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// The Handler is the socket wrapper for the zeromq socket.
type Handler struct {
	config  *config.Handler
	socket  *zmq.Socket
	logger  *log.Logger
	Routes  datatype.KeyValue
	Manager *handler_manager.HandlerManager
	status  string
}

// New handler
func New() *Handler {
	return &Handler{
		logger:  nil,
		Routes:  datatype.New(),
		Manager: nil,
		status:  "",
	}
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

	c.Manager = handler_manager.New(parent, nil, nil, nil)
	c.Manager.SetConfig(&config.Concurrent{Handler: c.config})

	return nil
}

// Route adds a route along with its handler to this handler
func (c *Handler) Route(cmd string, handle any) error {
	if !route.IsHandleFunc(handle) {
		return fmt.Errorf("handle is not a valid handle function")
	}

	if c.Routes.Exist(cmd) {
		return nil
	}

	c.Routes.Set(cmd, handle)

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

// Start the handler directly, not by goroutine.
func (c *Handler) Start() error {
	if c.config == nil {
		return fmt.Errorf("configuration not set")
	}
	if err := c.Manager.Start(); err != nil {
		return fmt.Errorf("c.Manager.Start: %w", err)
	}

	return nil
}

// Does nothing, simply returns the data
var anyHandler = func(request message.RequestInterface) message.ReplyInterface {
	replyParameters := datatype.New().Set("route", request.CommandName())

	reply := request.Ok(replyParameters)
	return reply
}

func AnyRoute(handler Interface) error {
	if err := handler.Route(route.Any, anyHandler); err != nil {
		return fmt.Errorf("failed to '%s' route into the handler: %w", route.Any, err)
	}
	return nil
}

func requiredMetadata() []string {
	return []string{"Identity", "pub_key"}
}
