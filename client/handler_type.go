package client

import (
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// HandlerType defines the available kind of handlers.
type HandlerType string

const (
	// SyncReplierType handlers process one request at a time.
	SyncReplierType HandlerType = "SyncReplier"
	// PublisherType handlers broadcast messages to all subscribers.
	PublisherType HandlerType = "Publisher"
	// ReplierType handlers are asynchronous client-server handlers.
	ReplierType HandlerType = "Replier"
	PairType    HandlerType = "Pair"
	WorkerType  HandlerType = "Worker" // Workers receive messages but don't return any result to the caller.
)

type HandlerConfig struct {
	Type             HandlerType `json:"type" yaml:"type"`
	Category         string      `json:"category" yaml:"category"`
	message.Endpoint `json:",inline" yaml:",inline"`
}

// isTarget checks that the given handler type can be targeted by a client.
func isTarget(target HandlerType) bool {
	return target == SyncReplierType ||
		target == WorkerType ||
		target == PairType ||
		target == PublisherType ||
		target == ReplierType
}

// targetToClient gets the client socket type for the target handler type.
func targetToClient(target HandlerType) zmq.Type {
	switch target {
	case SyncReplierType:
		return zmq.REQ
	case WorkerType:
		return zmq.PUSH
	case PairType:
		return zmq.PAIR
	case PublisherType:
		return zmq.SUB
	case ReplierType:
		return zmq.DEALER
	default:
		return zmq.Type(-1)
	}
}
