package base

import (
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/message"
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

	Packer() message.Packer
	SetPacker(message.Packer)

	// SetLogger adds an optional logger. Passing nil disables logging.
	SetLogger(*log.Logger) error

	// IsRouteExist returns true if the command is registered
	IsRouteExist(string) bool

	// RouteCommands returns list of all commands in this handler
	RouteCommands() []string

	// Route adds a new route and its handler.
	Route(string, HandleFunc) error

	// Returns the handle function for the given command.
	FindRoute(string) (HandleFunc, error)

	// Type returns the type of the handler
	Type() config.HandlerType

	Start() error
}
