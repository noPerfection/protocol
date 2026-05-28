package config

import (
	"fmt"
	"strings"

	zmq "github.com/pebbe/zmq4"
)

const (
	ManagerCategory = "control"
)

type Handler struct {
	Type        HandlerType `json:"type" yaml:"type"`
	Category    string      `json:"category" yaml:"category"`
	Port        uint64      `json:"port" yaml:"port"`
	Id          string      `json:"id" yaml:"id"`
	ManagerId   string      `json:"manager_id" yaml:"manager_id"`
	ManagerPort uint64      `json:"manager_port" yaml:"manager_port"`
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
		Type:        as,
		Category:    category,
		Id:          id,
		Port:        port,
		ManagerId:   DefaultManagerId(id),
		ManagerPort: 0,
	}
}

func DefaultManagerId(handlerId string) string {
	return "manager_" + handlerId
}

func (handler *Handler) ManagerHandler() *Handler {
	managerId := handler.ManagerId
	if managerId == "" {
		managerId = DefaultManagerId(handler.Id)
	}

	return NewHandler(handler.Type, managerId, ManagerCategory, handler.ManagerPort)
}

func (handler *Handler) ManagerExternalUrl() string {
	manager := handler.ManagerHandler()
	return ExternalUrl(manager.Id, manager.Port)
}

func (handler *Handler) ManagerConnectUrl() string {
	manager := handler.ManagerHandler()
	return ConnectUrl(manager.Id, manager.Port)
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

// ExternalUrl creates the ZeroMQ endpoint for the handler to bind.
//
// When Port is non-zero and Id is localhost or a 127.0.0.* address, returns tcp://*:{Port}.
// When Port is non-zero otherwise, returns tcp://{Id}:{Port}.
// When Port is 0 and Id has the prefix "tmp", returns ipc:///{Id} for a filesystem IPC socket.
// When Port is 0 otherwise, returns inproc://{Id} for in-process communication.
//
// Clients should connect using the same Id and Port with ConnectUrl.
func ExternalUrl(id string, port uint64) string {
	if port == 0 {
		if strings.HasPrefix(id, "tmp") {
			return fmt.Sprintf("ipc:///%s", id)
		}
		return fmt.Sprintf("inproc://%s", id)
	}
	if isLocalhost(id) {
		return fmt.Sprintf("tcp://*:%d", port)
	}
	return fmt.Sprintf("tcp://%s:%d", id, port)
}

func isLocalhost(id string) bool {
	return id == "localhost" || strings.HasPrefix(id, "127.0.0.")
}

// ConnectUrl creates the ZeroMQ endpoint for a subscriber to connect.
//
// When Port is non-zero, returns tcp://{Id}:{Port}.
// When Port is 0 and Id has the prefix "tmp", returns ipc:///{Id}.
// When Port is 0 otherwise, returns inproc://{Id}.
func ConnectUrl(id string, port uint64) string {
	if port == 0 {
		if strings.HasPrefix(id, "tmp") {
			return fmt.Sprintf("ipc:///%s", id)
		}
		return fmt.Sprintf("inproc://%s", id)
	}
	return fmt.Sprintf("tcp://%s:%d", id, port)
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

// IsInproc returns true if the handler is not a remote handler.
func (handler *Handler) IsInproc() bool {
	return handler.Port == 0
}

// IsInprocBroadcast returns true if the publisher is not a remote.
func (trigger *Trigger) IsInprocBroadcast() bool {
	return trigger.BroadcastPort == 0
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
