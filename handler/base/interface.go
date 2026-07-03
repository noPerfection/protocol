package base

import (
	"fmt"

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

// IsValid checks whether the given string is the valid or not.
// If not valid, then returns the error otherwise returns nil.
func IsValid(t HandlerType) error {
	if t == SyncReplierType ||
		t == WorkerType ||
		t == PublisherType ||
		t == ReplierType ||
		t == PairType {
		return nil
	}

	return fmt.Errorf("'%s' is not valid handler type", t)
}

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
