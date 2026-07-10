package protocoltest

import (
	"testing"
	"time"

	csyncreplier "github.com/noPerfection/protocol/client/sync_replier"
	"github.com/noPerfection/protocol/handler/npac"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

// TestClientHandlerHMAC exercises HMAC-based access control end-to-end.
// npac is started once for the suite so that clients can auto-discover
// secrets via the autocontext retry mechanism.
func TestClientHandlerHMAC(t *testing.T) {
	n := npac.New()
	require.NoError(t, n.Start())
	time.Sleep(10 * time.Millisecond)

	t.Run("unsigned request auto-corrected via autocontext", testHMACUnsignedAutoRetry)
	t.Run("wrong secret auto-corrected via autocontext", testHMACWrongSecretAutoRetry)
	t.Run("matching secret succeeds", testHMACMatchingSecretSucceeds)
}

// testHMACUnsignedAutoRetry: a client that sends no HMAC gets "access-denied"
// from the handler, then the built-in autocontext retry fetches the secret
// from npac and re-sends signed — the final reply is OK.
func testHMACUnsignedAutoRetry(t *testing.T) {
	req := require.New(t)
	const (
		cmd    = "echo"
		secret = "integration-secret"
	)

	id, _, control := startWhitelistedSyncReplier(t, cmd, secret)
	defer closeControl(t, control)

	client, err := csyncreplier.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(2 * time.Second)

	// Client sends without HMAC → access-denied → autocontext fetches secret → retry → success.
	reply, err := client.Request(newRequest(cmd, "unsigned"))
	req.NoError(err)
	req.True(reply.IsOK(), "expected autocontext retry to succeed, got: %s", reply.ErrorMessage())
}

// testHMACWrongSecretAutoRetry: a client configured with the wrong HMAC secret
// sends a signed request. The handler rejects it ("access-denied"). The
// autocontext retry fetches the real secret from npac, overwrites the client's
// local whitelist entry, and re-sends — the final reply is OK.
func testHMACWrongSecretAutoRetry(t *testing.T) {
	req := require.New(t)
	const (
		cmd           = "echo"
		handlerSecret = "handler-secret"
		clientSecret  = "wrong-client-secret"
	)

	id, _, control := startWhitelistedSyncReplier(t, cmd, handlerSecret)
	defer closeControl(t, control)

	client, err := csyncreplier.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(2 * time.Second)
	req.NoError(client.Whitelist(cmd, clientSecret))

	// Client sends with wrong secret → access-denied → autocontext overwrites → retry → success.
	reply, err := client.Request(newRequest(cmd, "wrong-secret"))
	req.NoError(err)
	req.True(reply.IsOK(), "expected autocontext retry to succeed, got: %s", reply.ErrorMessage())
}

func testHMACMatchingSecretSucceeds(t *testing.T) {
	req := require.New(t)
	const (
		cmd    = "echo"
		secret = "shared-secret"
	)

	id, _, control := startWhitelistedSyncReplier(t, cmd, secret)
	defer closeControl(t, control)

	client, err := csyncreplier.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(time.Second)
	req.NoError(client.Whitelist(cmd, secret))

	reply, err := client.Request(newRequest(cmd, "signed-value"))
	req.NoError(err)
	assertEchoReply(t, reply, cmd, "signed-value")
}

func startWhitelistedSyncReplier(t *testing.T, cmd, secret string) (string, *handler.SyncReplier, *csyncreplier.Control) {
	t.Helper()
	req := require.New(t)

	id := testID(t, "hmac")
	svc := handler.NewSyncReplier()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	req.NoError(svc.Whitelist(cmd, secret))
	req.NoError(svc.Route(cmd, echoRoute))
	req.NoError(svc.Start())

	control := newSyncControl(t, id)
	return id, svc, control
}
