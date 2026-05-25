package base

import (
	"github.com/sds-framework/log-lib"
	clientConfig "github.com/sds-framework/protocol/client/config"
	"github.com/sds-framework/protocol/handler/config"
)

// Interface of the handler. Any handlers must be based on this.
// All handlers have
//
// The interface that it accepts is the *client.ClientSocket from the
// "github.com/sds-framework/protocol/client" package.
//
// handler.New(handler.Type)
// handler.SetConfig(Config)
// handler.Route("hello", onHello)
//
// The service will call:
// AddDepByService
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

	// AddDepByService adds the Config of the extension that the handler depends on.
	// This function is intended to be called by the service.
	//
	// If any route does not require the dependency, it returns an error.
	// If the configuration already added, it returns an error.
	AddDepByService(*clientConfig.Client) error

	// AddedDepByService returns true if the configuration already exists
	AddedDepByService(string) bool

	// DepIds return the list of dep ids collected from all Routes.
	DepIds() []string

	// Route adds a new route and it's handlers for this handler
	Route(string, any, ...string) error

	// Type returns the type of the handler
	Type() config.HandlerType

	Start() error

	// The Status is empty is the handler is running.
	// Returns an error string if the Manager is not running
	Status() string
}
