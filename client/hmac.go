package client

import (
	"fmt"

	"github.com/noPerfection/protocol/message"
)

// Any applies a whitelist to all commands when no command-specific entry exists.
const Any = "*"

// Whitelist registers a shared secret for a command on the target handler.
// Use Any for a route-wide signing policy.
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
	secret, ok := socket.whitelists[Any]
	return secret, ok
}

func (socket *Socket) serializeRequest(packer message.Packer, req message.RequestInterface) ([]string, error) {
	socket.mu.Lock()
	secret, sign := socket.secretFor(req.CommandName())
	socket.mu.Unlock()

	if !sign {
		return packer.SerializeRequest(req)
	}

	hmac := message.ComputeHMAC(req.String(), secret)
	return packer.SerializeRequest(req, hmac)
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
	return socket.validateReply(Any, reply, replyHmac)
}
