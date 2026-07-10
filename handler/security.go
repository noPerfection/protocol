package handler

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// GenerateCurveKey returns a new Z85 CURVE public/secret keypair.
func GenerateCurveKey() (pub, secret string, err error) {
	return zmq.NewCurveKeypair()
}

// DerivePublicKey returns the Z85 CURVE public key for the given secret key.
func DerivePublicKey(secretKey string) (pubkey string, err error) {
	return zmq.AuthCurvePublic(secretKey)
}

// GenerateSecret returns a 32-byte cryptographically-random hex string.
// Each handler type calls this in its own New() to create an unexported
// npacSecret that is never exposed outside the handler package.
func GenerateSecret() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

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
	for _, key := range security.allowedClientPubKeys {
		if key == clientPubKey {
			return
		}
	}
	security.allowedClientPubKeys = append(security.allowedClientPubKeys, clientPubKey)
}

func (security *Security) publicKey() string {
	pubKey := ""
	if security.curveSecretKey != "" {
		if derivedKey, deriveErr := DerivePublicKey(security.curveSecretKey); deriveErr == nil {
			pubKey = derivedKey
		}
	}

	return pubKey
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
