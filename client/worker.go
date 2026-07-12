// Package worker is a client for Worker handlers (PUSH socket, Send only).
package client

import (
	"time"

	"github.com/noPerfection/protocol/message"
)

var (
	_ SendInterface = (*WorkerClient)(nil)
)

// WorkerClient talks to a Worker handler.
type WorkerClient struct {
	socket *Socket
}

// NewWorker creates a client for the given handler endpoint.
func NewWorker(id string, port uint64) (*WorkerClient, error) {
	socket, err := New(id, port, WorkerType)
	if err != nil {
		return nil, err
	}
	return &WorkerClient{socket: socket}, nil
}

func (c *WorkerClient) Send(req message.RequestInterface, hmac ...string) error {
	return c.socket.Send(req, hmac...)
}

func (c *WorkerClient) Close() error {
	return c.socket.Close()
}

func (c *WorkerClient) Timeout(timeout time.Duration) {
	c.socket.Timeout(timeout)
}

func (c *WorkerClient) Attempt(attempt uint8) {
	c.socket.Attempt(attempt)
}

func (c *WorkerClient) Packer(packer message.Packer) {
	c.socket.Packer(packer)
}

func (c *WorkerClient) Whitelist(cmd string, secrets ...string) error {
	return c.socket.Whitelist(cmd, secrets...)
}

func (c *WorkerClient) Secure(serverPublicKey string, clientSecretKey ...string) *WorkerClient {
	c.socket.Secure(serverPublicKey, clientSecretKey...)
	return c
}
