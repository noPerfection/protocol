package handler

import (
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
)

// HandlerType defines the available kind of handlers.
type HandlerType string

const (
	// SyncReplierType handlers process a one request at a time.
	SyncReplierType HandlerType = "SyncReplier"
	// PublisherType handlers broadcast messages to all subscribers.
	PublisherType HandlerType = "Publisher"
	// ReplierType handlers are the asynchronous ReplierType. It's a traditional client-server's server.
	ReplierType HandlerType = "Replier"
	PairType    HandlerType = "Pair"
	UnknownType HandlerType = ""
	WorkerType  HandlerType = "Worker" // Workers are receiving the messages but don't return any result to the caller.
)

// Interface of the handler. Any handlers must be based on this.
// All handlers have
//
// handler.New(handler.Type)
// handler.SetEndpoint(endpoint)
// handler.Route("hello", onHello)
type Interface interface {
	Endpoint() message.Endpoint
	// SetEndpoint adds the parameters of the handler from the endpoint config.
	SetEndpoint(message.Endpoint)

	Packer() message.Packer
	SetPacker(message.Packer)

	// SetLogger adds an optional logger. Passing nil disables logging.
	SetLogger(*log.Logger) error

	// Secure stores the CURVE server secret key. An empty key keeps the handler non-secure.
	Secure(secretKey string)

	// Allow registers a client CURVE public key permitted to connect when ZAP is active (zmq.AuthStart).
	Allow(clientPubKey string)

	// SetMushroomURL registers the handler URL.
	// Prefer to follow the convention of the noPerfection/topology config for the service config URL plus handler category:
	//
	//	pkg:golang/github.com/noPerfection/service#cmd/service?var=services[name:main]&category=main
	//
	// By following the topology convention, the handler can be resolved by the topology.
	SetMushroomURL(mushroomURL string)

	// IsRouteExist returns true if the command is registered
	IsRouteExist(string) bool

	// Commands returns list of all commands in this handler
	Commands() []string

	// Route adds a new route and its handler.
	Route(string, HandleFunc) error

	// Returns the handle function for the given command.
	GetHandleFunc(string) (HandleFunc, error)

	// Type returns the type of the handler
	Type() HandlerType

	Start() error
}
