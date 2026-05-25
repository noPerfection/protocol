package config

import "fmt"

// HandlerType defines the available kind of handlers
type HandlerType string

const (
	// SyncReplierType handlers process a one request at a time.
	SyncReplierType HandlerType = "SyncReplier"
	// PublisherType handlers broadcast the message to all subscribers. It's a trigger-able handler.
	PublisherType HandlerType = "Publisher"
	// ReplierType handlers are the asynchronous ReplierType. It's a traditional client-server's server.
	ReplierType HandlerType = "Replier"
	PairType    HandlerType = "Pair"
	UnknownType HandlerType = ""
	WorkerType              = "Worker" // Workers are receiving the messages but don't return any result to the caller.
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
