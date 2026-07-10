// Package sync_replier is a client for SyncReplier handlers (REQ socket, Request only).
package sync_replier

import (
	"time"

	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/message"
)

var (
	_ client.RequestInterface = (*Client)(nil)
)

// Client talks to a SyncReplier handler.
type Client struct {
	socket *client.Socket
}

// NewClient creates a client for the given handler endpoint.
func NewClient(id string, port uint64) (*Client, error) {
	socket, err := client.New(id, port, client.SyncReplierType)
	if err != nil {
		return nil, err
	}
	return &Client{socket: socket}, nil
}

func (c *Client) Request(req message.RequestInterface, hmac ...string) (message.ReplyInterface, error) {
	return c.socket.Request(req, hmac...)
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
