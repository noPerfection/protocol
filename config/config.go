// Package config is the utility function that keeps the client-server
package config

import (
	"fmt"
	"strings"

	zmq "github.com/pebbe/zmq4"
)

// A Client parameters to connect to the dep
type Client struct {
	ServiceUrl string   `json:"url" yaml:"url"` // Url link of the service
	Id         string   `json:"id" yaml:"id"`
	Port       uint64   `json:"port" yaml:"port"`
	TargetType zmq.Type `json:"zmq_type,omitempty" yaml:"zmq_type,omitempty"` // The service's socket type
	urlFunc    func(*Client) string
}

// New Client
func New(serviceUrl string, id string, port uint64, socketType zmq.Type) *Client {
	return &Client{
		ServiceUrl: serviceUrl,
		Id:         id,
		Port:       port,
		TargetType: socketType,
		urlFunc:    nil,
	}
}

// UrlFunc sets the context to generate the url
func (client *Client) UrlFunc(urlFunc func(*Client) string) *Client {
	client.urlFunc = urlFunc
	return client
}

// Url of the client
func (client *Client) Url() string {
	if client.urlFunc == nil {
		return ""
	}

	return client.urlFunc(client)
}

// Url creates the ZeroMQ endpoint for the client to connect to.
//
// When Port is non-zero, returns tcp://localhost:{Port}.
// When Port is 0 and Id has the prefix "tmp", returns ipc:///{Id} for a filesystem IPC socket.
// When Port is 0 otherwise, returns inproc://{Id} for in-process communication.
func Url(client *Client) string {
	if client.Port == 0 {
		if strings.HasPrefix(client.Id, "tmp") {
			return fmt.Sprintf("ipc:///%s", client.Id)
		}
		return fmt.Sprintf("inproc://%s", client.Id)
	}
	return fmt.Sprintf("tcp://localhost:%d", client.Port)
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
		first.TargetType == second.TargetType &&
		first.ServiceUrl == second.ServiceUrl
}
