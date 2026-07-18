package message

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	zmq "github.com/pebbe/zmq4"
)

const signingKeyDomain = "noPerfection/manager-handshake/v1"

func curvePublicBytes(curvePublicKey string) ([]byte, error) {
	raw := zmq.Z85decode(curvePublicKey)
	if len(raw) != 32 {
		return nil, fmt.Errorf("invalid CURVE public key length %d", len(raw))
	}
	return []byte(raw), nil
}

func deriveSigningSeed(curvePublicKey string) ([]byte, error) {
	public, err := curvePublicBytes(curvePublicKey)
	if err != nil {
		return nil, err
	}
	seed := sha256.Sum256(append([]byte(signingKeyDomain+":"), public...))
	return seed[:], nil
}

func signingPrivateKeyFromCurveSecret(curveSecretKey string) (ed25519.PrivateKey, error) {
	pub, err := DerivePublicKey(curveSecretKey)
	if err != nil {
		return nil, fmt.Errorf("DerivePublicKey: %w", err)
	}
	seed, err := deriveSigningSeed(pub)
	if err != nil {
		return nil, err
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

func decodeSigningPublicKey(publicKey string) (ed25519.PublicKey, error) {
	if pub, err := hex.DecodeString(publicKey); err == nil && len(pub) == ed25519.PublicKeySize {
		return ed25519.PublicKey(pub), nil
	}
	seed, err := deriveSigningSeed(publicKey)
	if err != nil {
		return nil, err
	}
	return ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey), nil
}

// Sign signs body with an ed25519 key derived from the Z85 CURVE public key
// that corresponds to curveSecretKey.
func Sign(body, curveSecretKey string) (string, error) {
	if body == "" {
		return "", fmt.Errorf("empty request body")
	}

	privateKey, err := signingPrivateKeyFromCurveSecret(curveSecretKey)
	if err != nil {
		return "", err
	}

	sig := ed25519.Sign(privateKey, []byte(body))
	return hex.EncodeToString(sig), nil
}

// Verify reports whether signature is a valid ed25519 signature of body.
// publicKey may be either a hex-encoded ed25519 public key or a Z85 CURVE
// public key.
func Verify(body, signature, publicKey string) error {
	if body == "" {
		return fmt.Errorf("empty request body")
	}

	sig, err := hex.DecodeString(signature)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}

	edPub, err := decodeSigningPublicKey(publicKey)
	if err != nil {
		return err
	}

	if !ed25519.Verify(edPub, []byte(body), sig) {
		return fmt.Errorf("invalid signature for public key '%s'", publicKey)
	}
	return nil
}
