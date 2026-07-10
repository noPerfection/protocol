package client

import (
	"fmt"

	"github.com/noPerfection/protocol/message"
)

// Whitelist registers a shared secret for a command on the target handler.
// Use message.Any for a route-wide signing policy.
// When set, outbound requests are signed and inbound replies with an HMAC tail are verified.
func (socket *Socket) Whitelist(cmd string, secrets ...string) error {
	if len(secrets) == 0 {
		return fmt.Errorf("at least one secret is required for whitelist on '%s'", cmd)
	}

	socket.mu.Lock()
	defer socket.mu.Unlock()

	if socket.whitelists == nil {
		socket.whitelists = make(map[string]string)
	}
	socket.whitelists[cmd] = secrets[0]

	return nil
}

func (socket *Socket) secretFor(cmd string) (string, bool) {
	if socket.whitelists == nil {
		return "", false
	}
	if secret, ok := socket.whitelists[cmd]; ok {
		return secret, true
	}
	secret, ok := socket.whitelists[message.Any]
	return secret, ok
}

func (socket *Socket) serializeRequest(req message.RequestInterface, hmac ...string) ([]string, error) {
	if len(hmac) > 0 {
		return socket.messagePacker.SerializeRequest(req, hmac[0])
	}

	socket.mu.Lock()
	secret, sign := socket.secretFor(req.CommandName())
	socket.mu.Unlock()

	if !sign {
		return socket.messagePacker.SerializeRequest(req)
	}

	return socket.messagePacker.SerializeRequest(req, message.ComputeHMAC(req.String(), secret))
}

func (socket *Socket) validateReply(cmd string, reply message.ReplyInterface, replyHmac string) error {
	if replyHmac == "" {
		return nil
	}

	socket.mu.Lock()
	secret, verify := socket.secretFor(cmd)
	socket.mu.Unlock()

	if !verify {
		return nil
	}

	if !message.VerifyHMAC(reply.String(), secret, replyHmac) {
		return message.ErrAccessDenied
	}

	return nil
}

func (socket *Socket) validateReplyAny(reply message.ReplyInterface, replyHmac string) error {
	return socket.validateReply(message.Any, reply, replyHmac)
}
