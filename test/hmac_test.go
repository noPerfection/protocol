package protocoltest

import (
	"testing"
	"time"

	csyncreplier "github.com/noPerfection/protocol/client/sync_replier"
	hsyncreplier "github.com/noPerfection/protocol/handler/sync_replier"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

func TestClientHandlerHMAC(t *testing.T) {
	t.Run("unsigned request is rejected", testHMACUnsignedRejected)
	t.Run("wrong secret is rejected", testHMACWrongSecretRejected)
	t.Run("matching secret succeeds", testHMACMatchingSecretSucceeds)
}

func testHMACUnsignedRejected(t *testing.T) {
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
	client.Timeout(time.Second)

	reply, err := client.Request(newRequest(cmd, "unsigned"))
	req.NoError(err)
	req.False(reply.IsOK())
	req.Equal(message.ErrAccessDenied.Error(), reply.ErrorMessage())
}

func testHMACWrongSecretRejected(t *testing.T) {
	req := require.New(t)
	const (
		cmd           = "echo"
		handlerSecret = "handler-secret"
		clientSecret  = "client-secret"
	)

	id, _, control := startWhitelistedSyncReplier(t, cmd, handlerSecret)
	defer closeControl(t, control)

	client, err := csyncreplier.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(time.Second)
	req.NoError(client.Whitelist(cmd, clientSecret))

	reply, err := client.Request(newRequest(cmd, "wrong-secret"))
	req.NoError(err)
	req.False(reply.IsOK())
	req.Equal(message.ErrAccessDenied.Error(), reply.ErrorMessage())
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

func startWhitelistedSyncReplier(t *testing.T, cmd, secret string) (string, *hsyncreplier.SyncReplier, *csyncreplier.Control) {
	t.Helper()
	req := require.New(t)

	id := testID(t, "hmac")
	svc := hsyncreplier.New()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	req.NoError(svc.Whitelist(cmd, secret))
	req.NoError(svc.Route(cmd, echoRoute))
	req.NoError(svc.Start())

	control := newSyncControl(t, id)
	return id, svc, control
}
