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

	reply, err := packer.DeseralizeReply(envelope)
	require.NoError(t, err)
	require.IsType(t, &Raw{}, reply)
	require.Equal(t, "con-id", reply.ConId())
	require.Equal(t, "contenttail", reply.String())
}

func TestRawPackerRejectsInvalidEnvelope(t *testing.T) {
	packer := RawMessage()

	_, err := packer.DeserializeRequest([]string{"content"})
	require.Error(t, err)

	_, err = packer.DeseralizeReply([]string{"content"})
	require.Error(t, err)
}

func TestRawZmqEnvelope(t *testing.T) {
	packer := RawMessage()
	envelope := []string{"con-id", "", "content", "tail"}

	request, err := packer.DeserializeRequest(envelope)
	require.NoError(t, err)

	actual, err := request.ZmqEnvelope()
	require.NoError(t, err)
	require.Equal(t, envelope, actual)

	request.SetConId("")
	actual, err = request.ZmqEnvelope()
	require.NoError(t, err)
	require.Equal(t, []string{"", "content", "tail"}, actual)
}
