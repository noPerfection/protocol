// Package pair is a client for Pair handlers (PAIR socket, Send and channel Receive).
package client

import (
	"time"

	"github.com/noPerfection/protocol/message"
)

var (
	_ SendInterface    = (*PairClient)(nil)
	_ ReceiveInterface = (*PairClient)(nil)
)

// Client talks to a Pair handler.
type PairClient struct {
	socket *Socket
}

// NewClient creates a client for the given handler endpoint.
func NewPair(id string, port uint64) (*PairClient, error) {
	socket, err := New(id, port, PairType)
	if err != nil {
		return nil, err
	}
	return &PairClient{socket: socket}, nil
}

func (c *PairClient) Send(req message.RequestInterface, hmac ...string) error {
	return c.socket.Send(req, hmac...)
}

func (c *PairClient) Receive() <-chan message.ReplyInterface {
	return c.socket.Receive()
}

func (c *PairClient) Close() error {
	return c.socket.Close()
}

func (c *PairClient) Timeout(timeout time.Duration) {
	c.socket.Timeout(timeout)
}

func (c *PairClient) Attempt(attempt uint8) {
	c.socket.Attempt(attempt)
}

func (c *PairClient) Packer(packer message.Packer) {
	c.socket.Packer(packer)
}

func (c *PairClient) Whitelist(cmd string, secrets ...string) error {
	return c.socket.Whitelist(cmd, secrets...)
}

func (c *PairClient) Secure(clientSecretKey string) *PairClient {
	c.socket.Secure(clientSecretKey)
	return c
}

func (c *PairClient) Allow(handlerPublicKey string) *PairClient {
	c.socket.Allow(handlerPublicKey)
	return c
}
