package message

import (
	"encoding/hex"
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/stretchr/testify/require"
)

func TestSignAndVerifyRequest(t *testing.T) {
	pub, secret, err := GenerateCurveKey()
	require.NoError(t, err)

	req := &Request{
		Command: "handshake",
		Parameters: datatype.New().
			Set("secret", secret).
			Set("inbound-url", "pkg:test/inbound"),
	}

	signature, err := Sign(req.String(), secret)
	require.NoError(t, err)
	require.NotEmpty(t, signature)

	require.NoError(t, Verify(req.String(), signature, pub))

	req.Parameters.Set("signature", signature)
	delete(req.Parameters, "signature")
	require.NoError(t, Verify(req.String(), signature, pub))
}

func TestVerifyRejectsWrongCurvePublicKey(t *testing.T) {
	pub, secret, err := GenerateCurveKey()
	require.NoError(t, err)
	otherPub, _, err := GenerateCurveKey()
	require.NoError(t, err)

	body := `{"command":"handshake","parameters":{}}`
	signature, err := Sign(body, secret)
	require.NoError(t, err)

	require.NoError(t, Verify(body, signature, pub))
	require.Error(t, Verify(body, signature, otherPub))
}

func TestDecodeSigningPublicKeyHex(t *testing.T) {
	_, secret, err := GenerateCurveKey()
	require.NoError(t, err)

	pub, err := DerivePublicKey(secret)
	require.NoError(t, err)

	edPub, err := signingPublicKey(pub)
	require.NoError(t, err)

	decoded, err := decodeSigningPublicKey(pub)
	require.NoError(t, err)
	require.Equal(t, edPub, decoded)
	require.Equal(t, hex.EncodeToString(edPub), hex.EncodeToString(decoded))
}
