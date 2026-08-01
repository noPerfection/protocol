package handler

import (
	"fmt"
	"slices"

	"github.com/noPerfection/protocol/handler/npac"
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

func (security *Security) IsWhitelistRequired(cmd string, dontUseAny ...bool) bool {
	if security.requireWhitelistCmds == nil {
		return false
	}
	if security.requireWhitelistCmds[cmd] {
		return true
	}
	if len(dontUseAny) > 0 && dontUseAny[0] {
		return false
	}
	return security.requireWhitelistCmds[message.Any]
}

func (security *Security) IsSecure() bool {
	return security.curveSecretKey != ""
}

// PublicKey returns the Z85 CURVE public key for the stored server secret key.
func (security *Security) PublicKey() (string, error) {
	if security.curveSecretKey == "" {
		return "", fmt.Errorf("handler is not secure")
	}
	return message.DerivePublicKey(security.curveSecretKey)
}

// Secure stores the CURVE server secret key. An empty key keeps the handler non-secure.
func (security *Security) Secure(secretKey string) {
	security.curveSecretKey = secretKey
}

// Allow registers a client CURVE public key permitted to connect when ZAP is active.
func (security *Security) Allow(clientPubKey string) {
	if clientPubKey == "" {
		return
	}
	if slices.Contains(security.allowedClientPubKeys, clientPubKey) {
		return
	}
	security.allowedClientPubKeys = append(security.allowedClientPubKeys, clientPubKey)
}

func (security *Security) auth(socket *zmq.Socket, handlerURL string) error {
	if security.curveSecretKey != "" {
		if err := socket.ServerAuthCurve(handlerURL, security.curveSecretKey); err != nil {
			return fmt.Errorf("socket.ServerAuthCurve: %w", err)
		}
		if len(security.allowedClientPubKeys) > 0 {
			npac.AuthCurveAdd(handlerURL, security.allowedClientPubKeys...)
		}
	}
	return nil
}
