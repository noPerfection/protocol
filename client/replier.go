// Package replier is a client for Replier handlers (DEALER socket, Send and channel Receive).
package client

import (
	"time"

	"github.com/noPerfection/protocol/message"
)

var (
	_ SendInterface    = (*ReplierClient)(nil)
	_ ReceiveInterface = (*ReplierClient)(nil)
)

// ReplierClient talks to a Replier handler.
type ReplierClient struct {
	socket *Socket
}

// NewReplier creates a client for the given handler endpoint.
func NewReplier(id string, port uint64) (*ReplierClient, error) {
	socket, err := New(id, port, ReplierType)
	if err != nil {
		return nil, err
	}
	return &ReplierClient{socket: socket}, nil
}

func (c *ReplierClient) Send(req message.RequestInterface, hmac ...string) error {
	return c.socket.Send(req, hmac...)
}

func (c *ReplierClient) Receive() <-chan message.ReplyInterface {
	return c.socket.Receive()
}

func (c *ReplierClient) Close() error {
	return c.socket.Close()
}

func (c *ReplierClient) Timeout(timeout time.Duration) {
	c.socket.Timeout(timeout)
}

func (c *ReplierClient) Attempt(attempt uint8) {
	c.socket.Attempt(attempt)
}

func (c *ReplierClient) Packer(packer message.Packer) {
	c.socket.Packer(packer)
}

func (c *ReplierClient) Whitelist(cmd string, secrets ...string) error {
	return c.socket.Whitelist(cmd, secrets...)
}

func (c *ReplierClient) Secure(serverPublicKey string, clientSecretKey ...string) *ReplierClient {
	c.socket.Secure(serverPublicKey, clientSecretKey...)
	return c
}
