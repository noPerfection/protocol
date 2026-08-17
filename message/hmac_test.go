package message

import (
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/stretchr/testify/require"
)

func TestComputeCacheHash(t *testing.T) {
	hash := ComputeCacheHash("hmac-secret", "curve-pubkey", "dep-url")
	require.Len(t, hash, 64)
	require.Equal(t, hash, ComputeCacheHash("hmac-secret", "curve-pubkey", "dep-url"))
	require.NotEqual(t, hash, ComputeCacheHash("hmac-secret", "other-pubkey", "dep-url"))
	require.NotEqual(t, ComputeCacheHash("ab", "c"), ComputeCacheHash("a", "bc"))
}

func TestComputeHMAC(t *testing.T) {
	body := `{"command":"hello","parameters":{}}`
	secret := "test-secret"

	hash := ComputeHMAC(body, secret)
	require.NotEmpty(t, hash)
	require.True(t, VerifyHMAC(body, secret, hash))
	require.False(t, VerifyHMAC(body, "wrong-secret", hash))
	require.False(t, VerifyHMAC("other-body", secret, hash))
}

func TestMessagePackerHMACRoundTrip(t *testing.T) {
	packer := &MessagePacker{}
	request := &Request{
		Command:    "secured",
		Parameters: datatype.New(),
	}
	secret := "route-secret"
	hmacHash := ComputeHMAC(request.String(), secret)

	envelope, err := packer.SerializeRequest(request, hmacHash)
	require.NoError(t, err)
	require.Equal(t, MessageToEnvelope("", request.String(), hmacHash), envelope)

	received, receivedHmac, err := packer.DeserializeRequest(envelope)
	require.NoError(t, err)
	require.Equal(t, hmacHash, receivedHmac)
	require.Equal(t, request.CommandName(), received.CommandName())
	require.True(t, VerifyHMAC(received.String(), secret, receivedHmac))

	reply := received.Ok(datatype.New().Set("ok", true))
	replyHmac := ComputeHMAC(reply.String(), secret)
	replyEnvelope, err := packer.SerializeReply(reply, replyHmac)
	require.NoError(t, err)

	receivedReply, receivedReplyHmac, err := packer.DeserializeReply(replyEnvelope)
	require.NoError(t, err)
	require.Equal(t, replyHmac, receivedReplyHmac)
	require.True(t, receivedReply.IsOK())
	require.True(t, VerifyHMAC(receivedReply.String(), secret, receivedReplyHmac))
}

func TestMessagePackerDeserializeWithoutHMAC(t *testing.T) {
	packer := &MessagePacker{}
	request := &Request{
		Command:    "open",
		Parameters: datatype.New(),
	}

	envelope, err := packer.SerializeRequest(request)
	require.NoError(t, err)

	_, hmacHash, err := packer.DeserializeRequest(envelope)
	require.NoError(t, err)
	require.Empty(t, hmacHash)
}
