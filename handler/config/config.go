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

// NewHandler returns a Handler configuration with the given HandlerType, ID, category, and port.
func NewHandler(as HandlerType, id string, category string, port uint64) *Handler {
	return &Handler{
		Type:     as,
		Category: category,
		Endpoint: message.NewEndpoint(id, port),
	}
}

// SocketType gets the ZMQ analog of the handler type
func SocketType(handlerType HandlerType) zmq.Type {
	if handlerType == SyncReplierType {
		return zmq.REP
	} else if handlerType == ReplierType {
		return zmq.ROUTER
	} else if handlerType == WorkerType {
		return zmq.PULL
	} else if handlerType == PublisherType {
		return zmq.PUB
	} else if handlerType == PairType {
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
