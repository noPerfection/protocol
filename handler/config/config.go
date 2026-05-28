package config

import (
	"fmt"

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

type Trigger struct {
	*Handler
	BroadcastPort uint64      `json:"broadcast_port" yaml:"broadcast_port"`
	BroadcastId   string      `json:"broadcast_id" yaml:"broadcast_id"`
	BroadcastType HandlerType `json:"broadcast_type" yaml:"broadcast_type"`
}

// NewHandler returns a Handler configuration with the given HandlerType, ID, category, and port.
func NewHandler(as HandlerType, id string, category string, port uint64) *Handler {
	return &Handler{
		Type:     as,
		Category: category,
		Endpoint: message.NewEndpoint(id, port),
	}
}

// TriggerAble returns a Trigger configuration with the given handler and broadcast fields.
//
// The broadcast type defines the publishing parameter.
func TriggerAble(handlerType HandlerType, id string, category string, port uint64, broadcastType HandlerType, broadcastId string, broadcastPort uint64) (*Trigger, error) {
	if !CanTrigger(broadcastType) {
		return nil, fmt.Errorf("the '%s' handler type is not trigger-able", broadcastType)
	}

	trigger := &Trigger{
		Handler:       NewHandler(handlerType, id, category, port),
		BroadcastPort: broadcastPort,
		BroadcastType: broadcastType,
		BroadcastId:   broadcastId,
	}

	return trigger, nil
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
// It's the opposite of CanTrigger.
func CanReply(handlerType HandlerType) bool {
	return handlerType == ReplierType || handlerType == SyncReplierType || handlerType == PairType
}

// CanTrigger returns true if the given Handler must not reply back to the user.
// Only publishers are trigger-able.
func CanTrigger(handlerType HandlerType) bool {
	return handlerType == PublisherType
}

// IsInprocBroadcast returns true if the publisher is not a remote.
func (trigger *Trigger) IsInprocBroadcast() bool {
	return message.NewEndpoint(trigger.BroadcastId, trigger.BroadcastPort).IsInproc()
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
