package message

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawPacker(t *testing.T) {
	packer := RawMessage()
	envelope := []string{"con-id", "", "content", "tail"}

	request, err := packer.DeserializeRequest(envelope)
	require.NoError(t, err)
	require.IsType(t, &Raw{}, request)
	require.Equal(t, "con-id", request.ConId())
	require.Equal(t, "contenttail", request.String())

	reply, err := packer.DeserializeReply(envelope)
	require.NoError(t, err)
	require.IsType(t, &Raw{}, reply)
	require.Equal(t, "con-id", reply.ConId())
	require.Equal(t, "contenttail", reply.String())
}

func TestRawPackerRejectsInvalidEnvelope(t *testing.T) {
	packer := RawMessage()

	_, err := packer.DeserializeRequest([]string{"content"})
	require.Error(t, err)

	_, err = packer.DeserializeReply([]string{"content"})
	require.Error(t, err)
}

func TestRawPackerSerializeRequest(t *testing.T) {
	packer := RawMessage()
	envelope := []string{"con-id", "", "content", "tail"}

	request, err := packer.DeserializeRequest(envelope)
	require.NoError(t, err)

	actual, err := packer.SerializeRequest(request)
	require.NoError(t, err)
	require.Equal(t, envelope, actual)

	request.SetConId("")
	actual, err = packer.SerializeRequest(request)
	require.NoError(t, err)
	require.Equal(t, []string{"", "content", "tail"}, actual)
}
