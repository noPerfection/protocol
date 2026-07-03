// Package publisher is a client for Publisher handlers (SUB socket, channel Receive).
package publisher

import (
	"time"

	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/message"
)

var (
	_ client.ReceiveInterface = (*Client)(nil)
)

// Client talks to a Publisher handler.
type Client struct {
	socket *client.Socket
}

// NewClient creates a client for the given handler endpoint.
func NewClient(id string, port uint64) (*Client, error) {
	socket, err := client.New(id, port, client.PublisherType)
	if err != nil {
		return nil, err
	}
	return &Client{socket: socket}, nil
}

func (c *Client) Receive() <-chan message.ReplyInterface {
	return c.socket.Receive()
}

func (c *Client) Close() error {
	return c.socket.Close()
}

func (c *Client) Timeout(timeout time.Duration) {
	c.socket.Timeout(timeout)
}

func (c *Client) Attempt(attempt uint8) {
	c.socket.Attempt(attempt)
}

func (c *Client) Packer(packer message.Packer) {
	c.socket.Packer(packer)
}

func (c *Client) Whitelist(cmd string, secrets ...string) error {
	return c.socket.Whitelist(cmd, secrets...)
}

func (c *Client) Secure(serverPublicKey string, clientSecretKey ...string) *Client {
	c.socket.Secure(serverPublicKey, clientSecretKey...)
	return c
}
