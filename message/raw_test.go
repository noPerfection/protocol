package message

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawPacker(t *testing.T) {
	packer := RawMessage()
	envelope := []string{"con-id", "", "content", "hmac-hash", "tail"}

	request, hmacHash, err := packer.DeserializeRequest(envelope)
	require.NoError(t, err)
	require.Equal(t, "hmac-hash", hmacHash)
	require.IsType(t, &Raw{}, request)
	require.Equal(t, "con-id", request.ConId())
	require.Equal(t, "contenttail", request.String())

	reply, replyHmac, err := packer.DeserializeReply(envelope)
	require.NoError(t, err)
	require.Equal(t, "hmac-hash", replyHmac)
	require.IsType(t, &Raw{}, reply)
	require.Equal(t, "con-id", reply.ConId())
	require.Equal(t, "contenttail", reply.String())
}

func TestRawPackerSimpleEnvelope(t *testing.T) {
	packer := RawMessage()

	request, hmacHash, err := packer.DeserializeRequest([]string{"content"})
	require.NoError(t, err)
	require.Empty(t, hmacHash)
	require.Empty(t, request.ConId())
	require.Equal(t, "content", request.String())

	reply, replyHmac, err := packer.DeserializeReply([]string{"content"})
	require.NoError(t, err)
	require.Empty(t, replyHmac)
	require.Empty(t, reply.ConId())
	require.Equal(t, "content", reply.String())
}

func TestRawPackerSerializeRequest(t *testing.T) {
	packer := RawMessage()
	envelope := []string{"con-id", "", "content", "hmac-hash", "tail"}

	request, _, err := packer.DeserializeRequest(envelope)
	require.NoError(t, err)

	actual, err := packer.SerializeRequest(request, "hmac-hash")
	require.NoError(t, err)
	require.Equal(t, envelope, actual)

	request.SetConId("")
	actual, err = packer.SerializeRequest(request, "hmac-hash")
	require.NoError(t, err)
	require.Equal(t, []string{"", "content", "hmac-hash", "tail"}, actual)
}
