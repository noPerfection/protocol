package message

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// ComputeHMAC returns the HMAC-SHA256 hex digest of body signed with secret.
func ComputeHMAC(body, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(body))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyHMAC reports whether hash is a valid HMAC-SHA256 hex digest of body for secret.
func VerifyHMAC(body, secret, hash string) bool {
	expected := ComputeHMAC(body, secret)
	return hmac.Equal([]byte(expected), []byte(hash))
}

// ComputeCacheHash returns a SHA-256 hex digest of params.
// A 0-byte separator is written between values so "ab"+"c" and "a"+"bc" do not collide.
func ComputeCacheHash(params ...string) string {
	h := sha256.New()
	for i, p := range params {
		if i > 0 {
			_, _ = h.Write([]byte{0})
		}
		_, _ = h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}
