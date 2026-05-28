package message

import (
	"fmt"
	"strings"
)

// Endpoint identifies where a ZeroMQ handler is bound or connected.
type Endpoint struct {
	Id   string `json:"id" yaml:"id"`
	Port uint64 `json:"port" yaml:"port"`
}

// NewEndpoint returns an Endpoint with the given id and port.
func NewEndpoint(id string, port uint64) Endpoint {
	return Endpoint{
		Id:   id,
		Port: port,
	}
}

// HandlerUrl creates the ZeroMQ endpoint for a handler to bind.
func (endpoint Endpoint) HandlerUrl() string {
	if endpoint.Port == 0 {
		if endpoint.IsIpc() {
			return fmt.Sprintf("ipc:///%s", endpoint.Id)
		}
		return fmt.Sprintf("inproc://%s", endpoint.Id)
	}
	if endpoint.IsLocalhost() {
		return fmt.Sprintf("tcp://*:%d", endpoint.Port)
	}
	return fmt.Sprintf("tcp://%s:%d", endpoint.Id, endpoint.Port)
}

// ClientUrl creates the ZeroMQ endpoint for a client to connect.
func (endpoint Endpoint) ClientUrl() string {
	if endpoint.Port == 0 {
		if endpoint.IsIpc() {
			return fmt.Sprintf("ipc:///%s", endpoint.Id)
		}
		return fmt.Sprintf("inproc://%s", endpoint.Id)
	}
	if endpoint.IsLocalhost() {
		return fmt.Sprintf("tcp://localhost:%d", endpoint.Port)
	}
	return fmt.Sprintf("tcp://%s:%d", endpoint.Id, endpoint.Port)
}

// IsInproc returns true when the endpoint uses ZeroMQ in-process transport.
func (endpoint Endpoint) IsInproc() bool {
	return endpoint.Port == 0 && !endpoint.IsIpc()
}

// IsIpc returns true when the endpoint uses ZeroMQ filesystem IPC transport.
func (endpoint Endpoint) IsIpc() bool {
	return endpoint.Port == 0 && strings.HasPrefix(endpoint.Id, "tmp")
}

// IsLocalhost returns true when the endpoint points at a local TCP host.
func (endpoint Endpoint) IsLocalhost() bool {
	return endpoint.Id == "" || endpoint.Id == "localhost" || strings.HasPrefix(endpoint.Id, "127.0.0.")
}

// IsRemote returns true when the endpoint uses a TCP transport.
func (endpoint Endpoint) IsRemote() bool {
	return endpoint.Port != 0
}
