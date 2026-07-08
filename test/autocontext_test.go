package protocoltest

import (
	"testing"
	"time"

	csyncreplier "github.com/noPerfection/protocol/client/sync_replier"
	"github.com/noPerfection/protocol/handler/npac"
	hsyncreplier "github.com/noPerfection/protocol/handler/sync_replier"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

const (
	helloSecret = "hello-hmac-secret"
)

// TestAutocontextHmacRetry verifies the full autocontext HMAC-retry flow:
//
//  1. npac is started so the in-process security registry is available.
//  2. A sync_replier is started with two routes:
//     - "hello": requires HMAC (whitelisted with helloSecret)
//     - "age":   open (no HMAC required)
//
// 3. A client that has NO knowledge of any HMAC secrets sends a "hello"
//
//	request. The handler returns "access-denied". The client's built-in
//	autocontext retry fetches the secret from npac and re-sends signed.
//	The second attempt succeeds.
//
// 4. The same client sends an "age" request with no secret. It succeeds
//
//	directly (no retry needed).
func TestAutocontextHmacRetry(t *testing.T) {
	req := require.New(t)

	// Start npac (in-process security registry).
	n := npac.New()
	req.NoError(n.Start())
	time.Sleep(10 * time.Millisecond)

	// Start the handler with two routes.
	const handlerID = "autocontext-hmac-test"

	svc := hsyncreplier.New()
	svc.SetEndpoint(message.NewEndpoint(handlerID, 0))

	req.NoError(svc.Whitelist("hello", helloSecret))
	req.NoError(svc.Route("hello", func(r message.RequestInterface) message.ReplyInterface {
		return r.Ok(newRequest("hello", "world").Parameters)
	}))
	req.NoError(svc.Route("age", func(r message.RequestInterface) message.ReplyInterface {
		return r.Ok(newRequest("age", "18").Parameters)
	}))
	req.NoError(svc.Start())
	time.Sleep(10 * time.Millisecond)

	control := newSyncControl(t, handlerID)
	defer closeControl(t, control)

	// Client has NO pre-configured secrets.
	client, err := csyncreplier.NewClient(handlerID, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(2 * time.Second)

	t.Run("hello autocontext retry succeeds", func(t *testing.T) {
		r := require.New(t)
		// The client sends "hello" without HMAC.
		// Internally: access-denied → GetHmacSecret from npac → retry with HMAC → success.
		reply, err := client.Request(newRequest("hello", "world"))
		r.NoError(err)
		r.True(reply.IsOK(), "expected OK but got: %s", reply.ErrorMessage())
	})

	t.Run("age open route succeeds directly", func(t *testing.T) {
		r := require.New(t)
		reply, err := client.Request(newRequest("age", ""))
		r.NoError(err)
		r.True(reply.IsOK(), "expected OK but got: %s", reply.ErrorMessage())
	})
}
