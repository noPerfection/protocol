package handler

import (
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

func TestRequiresWhitelist(t *testing.T) {
	handler := New()
	require.False(t, handler.IsWhitelistExist("cmd"))

	require.NoError(t, handler.Whitelist("cmd", "secret-a"))
	require.True(t, handler.IsWhitelistExist("cmd"))
	require.False(t, handler.IsWhitelistExist("other"))

	require.NoError(t, handler.Whitelist(message.Any, "global-secret"))
	handler2 := New()
	require.NoError(t, handler2.Whitelist(message.Any, "global-secret"))
	require.True(t, handler2.IsWhitelistExist("unknown-cmd"))
}

func TestWhitelistRequiresSecret(t *testing.T) {
	handler := New()
	require.Error(t, handler.Whitelist("cmd"))
}

func TestGetRequestSecret(t *testing.T) {
	handler := New()
	require.NoError(t, handler.Whitelist("secured", "secret-a", "secret-b"))

	req := &message.Request{
		Command:    "secured",
		Parameters: datatype.New(),
	}
	validHash := message.ComputeHMAC(req.String(), "secret-a")

	matched, ok := handler.getRequestSecret(req, validHash)
	require.True(t, ok)
	require.Equal(t, "secret-a", matched)

	_, ok = handler.getRequestSecret(req, message.ComputeHMAC(req.String(), "secret-b"))
	require.True(t, ok)
	_, ok = handler.getRequestSecret(req, "")
	require.False(t, ok)
	_, ok = handler.getRequestSecret(req, "invalid")
	require.False(t, ok)
}

func TestGetRequestSecretAnyFallback(t *testing.T) {
	handler := New()
	require.NoError(t, handler.Whitelist(message.Any, "global-secret"))

	req := &message.Request{
		Command:    "any-cmd",
		Parameters: datatype.New(),
	}
	hash := message.ComputeHMAC(req.String(), "global-secret")

	require.True(t, handler.IsWhitelistExist("any-cmd"))
	_, ok := handler.getRequestSecret(req, hash)
	require.True(t, ok)
}

func TestRequireWhitelistWithoutSecure(t *testing.T) {
	handler := NewSecurity()
	require.False(t, handler.IsWhitelistRequired("cmd"))
	handler.RequireWhitelist("cmd")
	require.True(t, handler.IsWhitelistRequired("cmd"))
	require.False(t, handler.IsWhitelistRequired("other"))
}

func TestRequireWhitelistSetsFlag(t *testing.T) {
	handler := NewSecurity()
	handler.Secure("server-secret")
	handler.RequireWhitelist("cmd")
	require.True(t, handler.IsWhitelistRequired("cmd"))
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
		reply := dispatchWhitelistedRoute(t, handler, req, message.ComputeHMAC(req.String(), routeSecret), nil)
		require.True(t, reply.IsOK())

		handled, err := reply.ReplyParameters().BoolValue("handled")
		require.NoError(t, err)
		require.True(t, handled)
	})

	t.Run("unsigned", func(t *testing.T) {
		reply := dispatchWhitelistedRoute(t, handler, req, "", nil)
		require.False(t, reply.IsOK())
		require.Equal(t, message.ErrAccessDenied.Error(), reply.ErrorMessage())
	})

	t.Run("signed with wrong secret", func(t *testing.T) {
		reply := dispatchWhitelistedRoute(t, handler, req, message.ComputeHMAC(req.String(), wrongSecret), nil)
		require.False(t, reply.IsOK())
		require.Equal(t, message.ErrAccessDenied.Error(), reply.ErrorMessage())
	})
}

func TestRequireWhitelistControlAcceptsAny(t *testing.T) {
	replier := NewReplier()
	require.NoError(t, replier.Route("cmd-a", func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New())
	}))

	reply := replier.onControlRequireWhitelist(&message.Request{
		Command: HandlerRequireWhitelist,
		Parameters: datatype.New().
			Set("command", message.Any).
			Set("secret", "route-secret"),
	})
	require.True(t, reply.IsOK(), reply.ErrorMessage())
	require.True(t, replier.IsWhitelistExist("cmd-a"))
	require.True(t, replier.IsWhitelistExist("other-cmd"))
}

func TestRequireWhitelistRouteMessages(t *testing.T) {
	handler := New()
	const (
		cmd    = "manager-cmd"
		secret = "manager-secret"
	)

	require.NoError(t, handler.Route(cmd, func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("handled", true))
	}))

	req := &message.Request{
		Command:    cmd,
		Parameters: datatype.New().Set("id", "test"),
	}

	t.Run("unsigned route denied when whitelist required", func(t *testing.T) {
		reply := dispatchWhitelistedRoute(t, handler, req, "", func(command string) bool {
			return command == cmd
		})
		require.False(t, reply.IsOK())
		require.Equal(t, message.ErrAccessDenied.Error()+", whitelist required", reply.ErrorMessage())
	})

	t.Run("signed route allowed when whitelist configured", func(t *testing.T) {
		require.NoError(t, handler.Whitelist(cmd, secret))
		reply := dispatchWhitelistedRoute(t, handler, req, message.ComputeHMAC(req.String(), secret), func(command string) bool {
			return command == cmd
		})
		require.True(t, reply.IsOK())
	})
}

// dispatchWhitelistedRoute mirrors handler inbound authorization before route dispatch.
func dispatchWhitelistedRoute(t *testing.T, handler *Handler, req *message.Request, hmacHash string, isRequired func(string) bool) message.ReplyInterface {
	t.Helper()

	packer := handler.Packer()
	envelope, err := packer.SerializeRequest(req, hmacHash)
	require.NoError(t, err)

	received, receivedHmac, err := packer.DeserializeRequest(envelope)
	require.NoError(t, err)
	require.Equal(t, hmacHash, receivedHmac)

	cmd := received.CommandName()
	if handler.IsWhitelistExist(cmd) {
		if _, ok := handler.getRequestSecret(received, receivedHmac); !ok {
			return packer.EmptyRequest().Fail(message.ErrAccessDenied.Error())
		}
	} else if isRequired != nil && isRequired(cmd) {
		return packer.EmptyRequest().Fail(message.ErrAccessDenied.Error() + ", whitelist required")
	}

	handleFunc, err := handler.GetHandleFunc(cmd)
	if err != nil {
		return received.Fail(err.Error())
	}

	return handleFunc(received)
}
