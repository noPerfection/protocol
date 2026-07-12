// Package sync_replier is a client for SyncReplier handlers (REQ socket, Request only).
package client

import (
	"time"

	"github.com/noPerfection/protocol/message"
)

var (
	_ RequestInterface = (*SyncReplierClient)(nil)
)

// SyncReplierClient talks to a SyncReplier handler.
type SyncReplierClient struct {
	socket *Socket
}

// NewSyncReplier creates a client for the given handler endpoint.
func NewSyncReplier(id string, port uint64) (*SyncReplierClient, error) {
	socket, err := New(id, port, SyncReplierType)
	if err != nil {
		return nil, err
	}
	return &SyncReplierClient{socket: socket}, nil
}

func (c *SyncReplierClient) Request(req message.RequestInterface, hmac ...string) (message.ReplyInterface, error) {
	return c.socket.Request(req, hmac...)
}

func (c *SyncReplierClient) Close() error {
	return c.socket.Close()
}

func (c *SyncReplierClient) Timeout(timeout time.Duration) {
	c.socket.Timeout(timeout)
}

func (c *SyncReplierClient) Attempt(attempt uint8) {
	c.socket.Attempt(attempt)
}

func (c *SyncReplierClient) Packer(packer message.Packer) {
	c.socket.Packer(packer)
}

func (c *SyncReplierClient) Whitelist(cmd string, secrets ...string) error {
	return c.socket.Whitelist(cmd, secrets...)
}

func (c *SyncReplierClient) Secure(serverPublicKey string, clientSecretKey ...string) *SyncReplierClient {
	c.socket.Secure(serverPublicKey, clientSecretKey...)
	return c
}
