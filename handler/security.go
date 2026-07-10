package handler

import (
	"crypto/rand"
	"encoding/hex"

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
