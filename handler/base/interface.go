package base

import (
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/config"
	zmq "github.com/pebbe/zmq4"
)

// Interface of the handler. Any handlers must be based on this.
// All handlers have
//
// handler.New(handler.Type)
// handler.SetConfig(Config)
// handler.Route("hello", onHello)
type Interface interface {
	Config() *config.Handler
	// SetConfig adds the parameters of the handler from the Config
	SetConfig(*config.Handler)

	// SetLogger adds the logger. The function accepts a parent, and function derives handler logger
	// Requires configuration to be set first
	SetLogger(*log.Logger) error

	// IsRouteExist returns true if the command is registered
	IsRouteExist(string) bool

	// RouteCommands returns list of all commands in this handler
	RouteCommands() []string

	// Route adds a new route and it's handlers for this handler
	Route(string, any) error

	// Type returns the type of the handler
	Type() config.HandlerType

	// Socket returns the handler's external ZeroMQ socket.
	Socket() *zmq.Socket

	// SetSocket assigns the handler's external ZeroMQ socket.
	SetSocket(*zmq.Socket)

	// SetSocketIdle marks the handler socket as idle.
	SetSocketIdle()

	// SetSocketReady marks the handler socket as ready.
	SetSocketReady()

	// SetSocketNil clears the handler socket and marks it nil.
	SetSocketNil()

	Start() error

	// The Status is empty is the handler is running.
	// Returns an error string if the Manager is not running
	Status() string
}
