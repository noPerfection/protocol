// Package publisher is a client for Publisher handlers (SUB socket, channel Receive).
package client

import (
	"time"

	"github.com/noPerfection/protocol/message"
)

var (
	_ ReceiveInterface = (*PublisherClient)(nil)
)

// Client talks to a Publisher handler.
type PublisherClient struct {
	socket *Socket
}

// NewClient creates a client for the given handler endpoint.
func NewPublisher(id string, port uint64) (*PublisherClient, error) {
	socket, err := New(id, port, PublisherType)
	if err != nil {
		return nil, err
	}
	return &PublisherClient{socket: socket}, nil
}

func (c *PublisherClient) Receive() <-chan message.ReplyInterface {
	return c.socket.Receive()
}

func (c *PublisherClient) Close() error {
	return c.socket.Close()
}

func (c *PublisherClient) Timeout(timeout time.Duration) {
	c.socket.Timeout(timeout)
}

func (c *PublisherClient) Attempt(attempt uint8) {
	c.socket.Attempt(attempt)
}

func (c *PublisherClient) Packer(packer message.Packer) {
	c.socket.Packer(packer)
}

func (c *PublisherClient) Whitelist(cmd string, secrets ...string) error {
	return c.socket.Whitelist(cmd, secrets...)
}

func (c *PublisherClient) Secure(clientSecretKey string) *PublisherClient {
	c.socket.Secure(clientSecretKey)
	return c
}

func (c *PublisherClient) Allow(handlerPublicKey string) *PublisherClient {
	c.socket.Allow(handlerPublicKey)
	return c
}
