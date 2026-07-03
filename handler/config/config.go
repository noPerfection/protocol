package config

import (
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type Handler struct {
	Type             HandlerType `json:"type" yaml:"type"`
	Category         string      `json:"category" yaml:"category"`
	message.Endpoint `json:",inline" yaml:",inline"`
}

func (handler *Handler) HandlerType() HandlerType {
	return handler.Type
}

// New returns a Handler configuration with the given HandlerType, ID, category, and port.
func New(as HandlerType, id string, category string, port uint64) *Handler {
	return &Handler{
		Type:     as,
		Category: category,
		Endpoint: message.NewEndpoint(id, port),
	}
}

// SocketType gets the ZMQ analog of the handler type
func SocketType(handlerType HandlerType) zmq.Type {
	switch handlerType {
	case SyncReplierType:
		return zmq.REP
	case ReplierType:
		return zmq.ROUTER
	case WorkerType:
		return zmq.PULL
	case PublisherType:
		return zmq.PUB
	case PairType:
		return zmq.PAIR
	}

	return zmq.Type(-1)
}

// CanReply returns true if the given Handler has to reply back to the user.
func CanReply(handlerType HandlerType) bool {
	return handlerType == ReplierType || handlerType == SyncReplierType || handlerType == PairType
}

// ByCategory returns handlers filtered by the category.
func ByCategory(handlers []*Handler, category string) []*Handler {
	filtered := make([]*Handler, 0, len(handlers))

	for i := range handlers {
		h := handlers[i]
		if h.Category == category {
			filtered = append(filtered, h)
		}
	}

	return filtered
}
