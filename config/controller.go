package config

import (
	"fmt"
	"strings"

	"github.com/sds-framework/os-lib/net"
	zmq "github.com/pebbe/zmq4"
)

const (
	HandlerStatus  = "status"
	ClosePart      = "close_part"
	RunPart        = "run-part"
	InstanceAmount = "instance-amount"
	MessageAmount  = "message-amount"
	AddInstance    = "add-instance"
	DeleteInstance = "delete-instance"
	Parts          = "parts"
	HandlerClose   = "close"  // Close the handler
	HandlerConfig  = "config" // Returns the handler configuration
)

type Handler struct {
	Type           HandlerType `json:"type" yaml:"type"`
	Category       string      `json:"category" yaml:"category"`
	InstanceAmount uint64      `json:"instance_amount" yaml:"instance_amount"`
	Port           uint64      `json:"port" yaml:"port"`
	Id             string      `json:"id" yaml:"id"`
}

type Trigger struct {
	*Handler
	BroadcastPort uint64      `json:"broadcast_port" yaml:"broadcast_port"`
	BroadcastId   string      `json:"broadcast_id" yaml:"broadcast_id"`
	BroadcastType HandlerType `json:"broadcast_type" yaml:"broadcast_type"`
}

// NewHandler configuration of the HandlerType and category.
// It generates the ID of the handler, as well as gets the free port.
//
// If not possible to get the port, it returns an error.
func NewHandler(as HandlerType, cat string) (*Handler, error) {
	port := net.GetFreePort()
	if port == 0 {
		return nil, fmt.Errorf("net.GetFreePort: no free port")
	}

	control := &Handler{
		Type:           as,
		Category:       cat,
		Id:             cat + "_1",
		InstanceAmount: 1,
		Port:           uint64(port),
	}

	return control, nil
}

// TriggerAble Converts the Handler to Trigger of the given type.
//
// The trigger's type defines the broadcasting parameter.
// If trigger is remote, then broadcast is remote as well.
func TriggerAble(handler *Handler, as HandlerType) (*Trigger, error) {
	if !CanTrigger(as) {
		return nil, fmt.Errorf("the '%s' handler type is not trigger-able", as)
	}

	port := 0
	if !handler.IsInproc() {
		port = net.GetFreePort()
		if port == 0 {
			return nil, fmt.Errorf("net.GetFreePort: no free port")
		}
	}

	trigger := &Trigger{
		Handler:       handler,
		BroadcastPort: uint64(port),
		BroadcastType: as,
		BroadcastId:   "broadcast_" + handler.Id,
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
// When Port is non-zero, returns tcp://*:{Port}.
// When Port is 0 and Id has the prefix "tmp", returns ipc:///{Id} for a filesystem IPC socket.
// When Port is 0 otherwise, returns inproc://{Id} for in-process communication.
//
// Clients should connect using the same Id and Port with client-lib/config.Url.
func ExternalUrl(id string, port uint64) string {
	if port == 0 {
		if strings.HasPrefix(id, "tmp") {
			return fmt.Sprintf("ipc:///%s", id)
		}
		return fmt.Sprintf("inproc://%s", id)
	}
	return fmt.Sprintf("tcp://*:%d", port)
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
