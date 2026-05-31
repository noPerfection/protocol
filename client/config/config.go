// Package config is the utility function that keeps the client-server
package config

import (
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// A Client parameters to connect to the dep
type Client struct {
	message.Endpoint `json:",inline" yaml:",inline"`
	TargetType       zmq.Type `json:"zmq_type,omitempty" yaml:"zmq_type,omitempty"` // The service's socket type
}

// New Client
func New(id string, port uint64, socketType zmq.Type) *Client {
	return &Client{
		Endpoint:   message.NewEndpoint(id, port),
		TargetType: socketType,
	}
}

// IsTarget checks that given zeromq socket type is the handler type
func IsTarget(target zmq.Type) bool {
	return target == zmq.REP || target == zmq.ROUTER || target == zmq.PUB || target == zmq.PULL || target == zmq.PAIR
}

// TargetToClient gets the ZMQ counter-part of the target.
// Returns zmq.REQ if target is not supported.
// Returns zmq.REQ for zmq.ROUTER and zmq.REP
func TargetToClient(target zmq.Type) zmq.Type {
	switch target {
	case zmq.PUB:
		return zmq.SUB
	case zmq.PULL:
		return zmq.PUSH
	case zmq.PAIR:
		return zmq.PAIR
	default:
		// For zmq.REP and zmq.ROUTER
		return zmq.REQ
	}
}

// IsEqual returns true if the clients match.
func IsEqual(first *Client, second *Client) bool {
	if first == nil || second == nil {
		return false
	}

	return first.Id == second.Id &&
		first.Port == second.Port &&
		first.TargetType == second.TargetType
}
