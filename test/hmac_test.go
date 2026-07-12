package protocoltest

import (
	"testing"
	"time"

	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

// TestClientHandlerHMAC exercises HMAC between client and handler directly.
func TestClientHandlerHMAC(t *testing.T) {
	t.Run("unsigned request rejected", testHMACUnsignedRejected)
	t.Run("wrong secret rejected", testHMACWrongSecretRejected)
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

	syncClient, err := client.NewSyncReplier(id, 0)
	req.NoError(err)
	defer func() { req.NoError(syncClient.Close()) }()
	syncClient.Timeout(2 * time.Second)
	syncClient.Attempt(1)

	reply, err := syncClient.Request(newRequest(cmd, "unsigned"))
	req.Error(err)
	if reply != nil {
		req.False(reply.IsOK())
	}
}

func testHMACWrongSecretRejected(t *testing.T) {
	req := require.New(t)
	const (
		cmd           = "echo"
		handlerSecret = "handler-secret"
		clientSecret  = "wrong-client-secret"
	)

	id, _, control := startWhitelistedSyncReplier(t, cmd, handlerSecret)
	defer closeControl(t, control)

	syncClient, err := client.NewSyncReplier(id, 0)
	req.NoError(err)
	defer func() { req.NoError(syncClient.Close()) }()
	syncClient.Timeout(2 * time.Second)
	syncClient.Attempt(1)
	req.NoError(syncClient.Whitelist(cmd, clientSecret))

	reply, err := syncClient.Request(newRequest(cmd, "wrong-secret"))
	req.Error(err)
	if reply != nil {
		req.False(reply.IsOK())
	}
}

func testHMACMatchingSecretSucceeds(t *testing.T) {
	req := require.New(t)
	const (
		cmd    = "echo"
		secret = "shared-secret"
	)

	id, _, control := startWhitelistedSyncReplier(t, cmd, secret)
	defer closeControl(t, control)

	syncClient, err := client.NewSyncReplier(id, 0)
	req.NoError(err)
	defer func() { req.NoError(syncClient.Close()) }()
	syncClient.Timeout(time.Second)
	req.NoError(syncClient.Whitelist(cmd, secret))

	reply, err := syncClient.Request(newRequest(cmd, "signed-value"))
	req.NoError(err)
	assertEchoReply(t, reply, cmd, "signed-value")
}

func startWhitelistedSyncReplier(t *testing.T, cmd, secret string) (string, *handler.SyncReplier, *client.Control) {
	t.Helper()
	req := require.New(t)

	id := testID(t, "hmac")
	svc := handler.NewSyncReplier()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	setHandlerMushroomURL(svc, id)
	req.NoError(svc.Whitelist(cmd, secret))
	req.NoError(svc.Route(cmd, echoRoute))
	req.NoError(svc.Start())
	time.Sleep(10 * time.Millisecond)

	control := newSyncControl(t, id)
	return id, svc, control
}
