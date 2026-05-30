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
	FindRoute(string) (HandleFunc, error)

	Config() *config.Handler
	// SetConfig adds the parameters of the handler from the Config
	SetConfig(*config.Handler)

	// SetLogger adds an optional logger. Passing nil disables logging.
	SetLogger(*log.Logger) error

	// IsRouteExist returns true if the command is registered
	IsRouteExist(string) bool

	// RouteCommands returns list of all commands in this handler
	RouteCommands() []string

	// Route adds a new route and its handler.
	Route(string, HandleFunc) error

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

	// Closed returns true when the handler received a close signal.
	Closed() bool

	// SetClose sets the handler close state.
	SetClose(bool)

	Start() error

	// The Status is empty is the handler is running.
	// Returns an error string if the control is not running
	Status() string
}
