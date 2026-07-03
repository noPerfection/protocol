package protocoltest

import (
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	cpair "github.com/noPerfection/protocol/client/pair"
	cpublisher "github.com/noPerfection/protocol/client/publisher"
	csyncreplier "github.com/noPerfection/protocol/client/sync_replier"
	"github.com/noPerfection/protocol/handler/base"
	hpair "github.com/noPerfection/protocol/handler/pair"
	hsyncreplier "github.com/noPerfection/protocol/handler/sync_replier"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

func TestProtocolEdgeCases(t *testing.T) {
	t.Run("wrong command returns handler failure", testWrongCommand)
	t.Run("wrong parameters return handler failure", testWrongParameters)
	t.Run("request times out when handler is missing", testRequestTimeoutWithoutHandler)
	t.Run("control times out when control handler is missing", testControlTimeoutWithoutHandler)
	t.Run("receive channel closes when publisher is idle", testReceiveClosesWhenPublisherIdle)
	t.Run("send after handler is stopped does not deliver", testSendAfterHandlerStoppedDoesNotDeliver)
}

func testWrongCommand(t *testing.T) {
	req := require.New(t)
	id := testID(t, "wrong-command")
	svc := hsyncreplier.New()
	svc.SetEndpoint(base.NewEndpoint(base.SyncReplierType, id, "test", 0))
	req.NoError(svc.Route("known", echoRoute))
	req.NoError(svc.Start())

	control := newSyncControl(t, id)
	defer closeControl(t, control)

	client, err := csyncreplier.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(50 * time.Millisecond)
	client.Attempt(1)

	reply, err := client.Request(newRequest("unknown", "value"))
	req.NoError(err)
	req.False(reply.IsOK())
	req.Contains(reply.ErrorMessage(), "command handler not found")
}

func testWrongParameters(t *testing.T) {
	req := require.New(t)
	id := testID(t, "wrong-parameters")
	svc := hsyncreplier.New()
	svc.SetEndpoint(base.NewEndpoint(base.SyncReplierType, id, "test", 0))
	req.NoError(svc.Route("needs-required", func(request message.RequestInterface) message.ReplyInterface {
		value, err := request.RouteParameters().StringValue("required")
		if err != nil {
			return request.Fail("required parameter missing")
		}
		return request.Ok(datatype.New().Set("required", value))
	}))
	req.NoError(svc.Start())

	control := newSyncControl(t, id)
	defer closeControl(t, control)

	client, err := csyncreplier.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(50 * time.Millisecond)
	client.Attempt(1)

	reply, err := client.Request(&message.Request{
		Command:    "needs-required",
		Parameters: datatype.New().Set("unexpected", "value"),
	})
	req.NoError(err)
	req.False(reply.IsOK())
	req.Equal("required parameter missing", reply.ErrorMessage())
}

func testRequestTimeoutWithoutHandler(t *testing.T) {
	req := require.New(t)
	client, err := csyncreplier.NewClient(testID(t, "missing-handler"), 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(20 * time.Millisecond)
	client.Attempt(1)

	reply, err := client.Request(newRequest("echo", "value"))
	req.Error(err)
	req.Nil(reply)
	req.Contains(err.Error(), "request_timeout")
}

func testControlTimeoutWithoutHandler(t *testing.T) {
	req := require.New(t)
	control, err := csyncreplier.NewControl(controlID(testID(t, "missing-control"), 0), 0)
	req.NoError(err)
	defer func() { req.NoError(control.Close()) }()
	control.Timeout(20 * time.Millisecond)
	control.Attempt(1)

	status, err := control.HandlerStatus()
	req.Error(err)
	req.Empty(status)
	req.Contains(err.Error(), "request_timeout")
}

func testReceiveClosesWhenPublisherIdle(t *testing.T) {
	req := require.New(t)
	client, err := cpublisher.NewClient(testID(t, "idle-publisher"), 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(20 * time.Millisecond)
	client.Attempt(2)

	replies := client.Receive()
	select {
	case reply, ok := <-replies:
		req.False(ok, "unexpected reply from missing publisher: %v", reply)
	case <-time.After(time.Second):
		t.Fatal("receive channel did not close after idle timeout")
	}
}

func testSendAfterHandlerStoppedDoesNotDeliver(t *testing.T) {
	req := require.New(t)
	id := testID(t, "stopped-pair")
	svc := hpair.New()
	svc.SetEndpoint(base.NewEndpoint(base.PairType, id, "test", 0))
	req.NoError(svc.Route("echo", echoRoute))
	req.NoError(svc.Start())

	control := newPairControl(t, id)
	defer func() { req.NoError(control.Close()) }()

	client, err := cpair.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(20 * time.Millisecond)
	client.Attempt(1)
	replies := client.Receive()

	req.NoError(control.HandlerClose())
	waitForStatus(t, control, base.SocketNil)

	_ = client.Send(newRequest("echo", "value"))
	select {
	case reply, ok := <-replies:
		req.False(ok, "unexpected reply after handler stopped: %v", reply)
	case <-time.After(time.Second):
		t.Fatal("receive channel did not close after stopped handler timeout")
	}
}
