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
}

// NewPair Pair returned.
func NewSecurity() *Security {
	return &Security{}
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
	if security.curveSecretKey != "" {
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
