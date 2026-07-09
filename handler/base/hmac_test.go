package base

import (
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

func TestRequiresWhitelist(t *testing.T) {
	handler := New()
	require.False(t, handler.RequiresWhitelist("cmd"))

	require.NoError(t, handler.Whitelist("cmd", "secret-a"))
	require.True(t, handler.RequiresWhitelist("cmd"))
	require.False(t, handler.RequiresWhitelist("other"))

	require.NoError(t, handler.Whitelist(Any, "global-secret"))
	handler2 := New()
	require.NoError(t, handler2.Whitelist(Any, "global-secret"))
	require.True(t, handler2.RequiresWhitelist("unknown-cmd"))
}

func TestWhitelistRequiresSecret(t *testing.T) {
	handler := New()
	require.Error(t, handler.Whitelist("cmd"))
}

func TestValidateRequestHmac(t *testing.T) {
	handler := New()
	require.NoError(t, handler.Whitelist("secured", "secret-a", "secret-b"))

	req := &message.Request{
		Command:    "secured",
		Parameters: datatype.New(),
	}
	validHash := message.ComputeHMAC(req.String(), "secret-a")

	require.True(t, handler.ValidateRequestHmac(req, validHash))
	require.True(t, handler.ValidateRequestHmac(req, message.ComputeHMAC(req.String(), "secret-b")))
	require.False(t, handler.ValidateRequestHmac(req, ""))
	require.False(t, handler.ValidateRequestHmac(req, "invalid"))

	matched, ok := handler.MatchRequestSecret(req, validHash)
	require.True(t, ok)
	require.Equal(t, "secret-a", matched)
}

func TestValidateRequestHmacAnyFallback(t *testing.T) {
	handler := New()
	require.NoError(t, handler.Whitelist(Any, "global-secret"))

	req := &message.Request{
		Command:    "any-cmd",
		Parameters: datatype.New(),
	}
	hash := message.ComputeHMAC(req.String(), "global-secret")

	require.True(t, handler.RequiresWhitelist("any-cmd"))
	require.True(t, handler.ValidateRequestHmac(req, hash))
}

func TestValidateReplyHmac(t *testing.T) {
	handler := New()
	require.NoError(t, handler.Whitelist(Any, "reply-secret"))

	req := &message.Request{Command: "cmd", Parameters: datatype.New()}
	reply := req.Ok(datatype.New())
	hash := message.ComputeHMAC(reply.String(), "reply-secret")

	require.True(t, handler.ValidateReplyHmac(reply, hash))
	require.False(t, handler.ValidateReplyHmac(reply, ""))
}

func TestComputeHMAC(t *testing.T) {
	req := &message.Request{
		Command:    "cmd",
		Parameters: datatype.New().Set("id", "1"),
	}

	hash := message.ComputeHMAC(req.String(), "secret")
	require.NotEmpty(t, hash)
	require.Equal(t, message.ComputeHMAC(req.String(), "secret"), hash)
}

func TestWhitelistRouteMessages(t *testing.T) {
	handler := New()
	const (
		cmd         = "secured"
		routeSecret = "route-secret"
		wrongSecret = "wrong-secret"
	)

	require.NoError(t, handler.Whitelist(cmd, routeSecret))
	require.NoError(t, handler.Route(cmd, func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("handled", true))
	}))

	req := &message.Request{
		Command:    cmd,
		Parameters: datatype.New().Set("id", "test"),
	}

	t.Run("signed with route secret", func(t *testing.T) {
		reply := dispatchWhitelistedRoute(t, handler, req, message.ComputeHMAC(req.String(), routeSecret))
		require.True(t, reply.IsOK())

		handled, err := reply.ReplyParameters().BoolValue("handled")
		require.NoError(t, err)
		require.True(t, handled)
	})

	t.Run("unsigned", func(t *testing.T) {
		reply := dispatchWhitelistedRoute(t, handler, req, "")
		require.False(t, reply.IsOK())
		require.Equal(t, message.ErrAccessDenied.Error(), reply.ErrorMessage())
	})

	t.Run("signed with wrong secret", func(t *testing.T) {
		reply := dispatchWhitelistedRoute(t, handler, req, message.ComputeHMAC(req.String(), wrongSecret))
		require.False(t, reply.IsOK())
		require.Equal(t, message.ErrAccessDenied.Error(), reply.ErrorMessage())
	})
}

// dispatchWhitelistedRoute mirrors handler inbound authorization before route dispatch.
func dispatchWhitelistedRoute(t *testing.T, handler *Handler, req *message.Request, hmacHash string) message.ReplyInterface {
	t.Helper()

	packer := handler.Packer()
	envelope, err := packer.SerializeRequest(req, hmacHash)
	require.NoError(t, err)

	received, receivedHmac, err := packer.DeserializeRequest(envelope)
	require.NoError(t, err)
	require.Equal(t, hmacHash, receivedHmac)

	cmd := received.CommandName()
	if handler.RequiresWhitelist(cmd) {
		if _, ok := handler.MatchRequestSecret(received, receivedHmac); !ok {
			return packer.EmptyRequest().Fail(message.ErrAccessDenied.Error())
		}
	}

	handleFunc, err := handler.GetHandleFunc(cmd)
	if err != nil {
		return received.Fail(err.Error())
	}

	return handleFunc(received)
}
