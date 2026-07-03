package client

import (
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

func TestSocketWhitelistRequiresSecret(t *testing.T) {
	socket, err := New("test", 0, SyncReplierType)
	require.NoError(t, err)

	require.Error(t, socket.Whitelist("cmd"))
}

func TestSocketSerializeRequestWithWhitelist(t *testing.T) {
	socket, err := New("test", 0, SyncReplierType)
	require.NoError(t, err)
	require.NoError(t, socket.Whitelist("secured", "route-secret"))

	req := &message.Request{
		Command:    "secured",
		Parameters: datatype.New(),
	}

	envelope, err := socket.serializeRequest(socket.packer(), req)
	require.NoError(t, err)

	_, hmacHash, err := socket.packer().DeserializeRequest(envelope)
	require.NoError(t, err)
	require.NotEmpty(t, hmacHash)
	require.True(t, message.VerifyHMAC(req.String(), "route-secret", hmacHash))
}

func TestSocketSerializeRequestWithoutWhitelist(t *testing.T) {
	socket, err := New("test", 0, SyncReplierType)
	require.NoError(t, err)

	req := &message.Request{
		Command:    "open",
		Parameters: datatype.New(),
	}

	envelope, err := socket.serializeRequest(socket.packer(), req)
	require.NoError(t, err)

	_, hmacHash, err := socket.packer().DeserializeRequest(envelope)
	require.NoError(t, err)
	require.Empty(t, hmacHash)
}

func TestSocketValidateReply(t *testing.T) {
	socket, err := New("test", 0, SyncReplierType)
	require.NoError(t, err)
	require.NoError(t, socket.Whitelist("secured", "route-secret"))

	req := &message.Request{Command: "secured", Parameters: datatype.New()}
	reply := req.Ok(datatype.New())
	replyHmac := message.ComputeHMAC(reply.String(), "route-secret")

	require.NoError(t, socket.validateReply("secured", reply, replyHmac))
	require.NoError(t, socket.validateReply("secured", reply, ""))
	require.ErrorIs(t, socket.validateReply("secured", reply, "bad-hmac"), message.ErrAccessDenied)
}

func TestSocketValidateReplySkipsWhenNotWhitelisted(t *testing.T) {
	socket, err := New("test", 0, SyncReplierType)
	require.NoError(t, err)

	req := &message.Request{Command: "open", Parameters: datatype.New()}
	reply := req.Ok(datatype.New())

	require.NoError(t, socket.validateReply("open", reply, "any-hmac"))
}

func TestSocketValidateReplyAny(t *testing.T) {
	socket, err := New("test", 0, ReplierType)
	require.NoError(t, err)
	require.NoError(t, socket.Whitelist(Any, "broadcast-secret"))

	req := &message.Request{Command: "ignored", Parameters: datatype.New()}
	reply := req.Ok(datatype.New())
	replyHmac := message.ComputeHMAC(reply.String(), "broadcast-secret")

	require.NoError(t, socket.validateReplyAny(reply, replyHmac))
	require.ErrorIs(t, socket.validateReplyAny(reply, "bad-hmac"), message.ErrAccessDenied)
}

func TestSocketWhitelistAnyFallback(t *testing.T) {
	socket, err := New("test", 0, WorkerType)
	require.NoError(t, err)
	require.NoError(t, socket.Whitelist(Any, "global-secret"))

	req := &message.Request{
		Command:    "any-cmd",
		Parameters: datatype.New(),
	}

	envelope, err := socket.serializeRequest(socket.packer(), req)
	require.NoError(t, err)

	_, hmacHash, err := socket.packer().DeserializeRequest(envelope)
	require.NoError(t, err)
	require.True(t, message.VerifyHMAC(req.String(), "global-secret", hmacHash))
}
