package handler

import (
	"fmt"
	"slices"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type Security struct {
	curveSecretKey       string
	allowedClientPubKeys []string
	requireWhitelistCmds map[string]bool
	handshaked           bool
}

// NewPair Pair returned.
func NewSecurity() *Security {
	return &Security{}
}

func (security *Security) handshakeCompleted() {
	security.handshaked = true
}

func (security *Security) IsHandshaked() bool {
	return security.handshaked
}

func (security *Security) RequireWhitelist(cmd string) {
	if cmd == "" {
		return
	}
	if security.requireWhitelistCmds == nil {
		security.requireWhitelistCmds = make(map[string]bool)
	}
	security.requireWhitelistCmds[cmd] = true
}

func (security *Security) IsWhitelistRequired(cmd string) bool {
	if security.requireWhitelistCmds == nil {
		return false
	}
	if security.requireWhitelistCmds[cmd] {
		return true
	}
	return security.requireWhitelistCmds[message.Any]
}

func (security *Security) IsSecure() bool {
	return security.curveSecretKey != ""
}

// Secure stores the CURVE server secret key. An empty key keeps the handler non-secure.
func (security *Security) Secure(secretKey string) {
	security.curveSecretKey = secretKey
}

// Allow registers a client CURVE public key permitted to connect when ZAP is active (zmq.AuthStart).
func (security *Security) Allow(clientPubKey string) {
	if clientPubKey == "" {
		return
	}
	if slices.Contains(security.allowedClientPubKeys, clientPubKey) {
		return
	}
	security.allowedClientPubKeys = append(security.allowedClientPubKeys, clientPubKey)
}

func (security *Security) register(socket *zmq.Socket, endpoint message.Endpoint) error {
	if security.curveSecretKey != "" && !endpoint.IsInproc() {
		domain := endpoint.ZapDomain()
		if err := socket.ServerAuthCurve(domain, security.curveSecretKey); err != nil {
			return fmt.Errorf("socket.ServerAuthCurve: %w", err)
		}
		if len(security.allowedClientPubKeys) > 0 {
			zmq.AuthCurveAdd(domain, security.allowedClientPubKeys...)
		}
	}
	return nil
}
