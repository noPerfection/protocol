package protocoltest

import (
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler"
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
	svc := handler.NewSyncReplier()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	setHandlerMushroomURL(svc, id)
	req.NoError(svc.Route("known", echoRoute))
	req.NoError(svc.Start())

	control := newSyncControl(t, id)
	defer closeControl(t, control)

	syncClient, err := client.NewSyncReplier(id, 0)
	req.NoError(err)
	defer func() { req.NoError(syncClient.Close()) }()
	syncClient.Timeout(50 * time.Millisecond)
	syncClient.Attempt(1)

	reply, err := syncClient.Request(newRequest("unknown", "value"))
	req.NoError(err)
	req.False(reply.IsOK())
	req.Contains(reply.ErrorMessage(), "command handler not found")
}

func testWrongParameters(t *testing.T) {
	req := require.New(t)
	id := testID(t, "wrong-parameters")
	svc := handler.NewSyncReplier()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	setHandlerMushroomURL(svc, id)
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

	syncClient, err := client.NewSyncReplier(id, 0)
	req.NoError(err)
	defer func() { req.NoError(syncClient.Close()) }()
	syncClient.Timeout(50 * time.Millisecond)
	syncClient.Attempt(1)

	reply, err := syncClient.Request(&message.Request{
		Command:    "needs-required",
		Parameters: datatype.New().Set("unexpected", "value"),
	})
	req.NoError(err)
	req.False(reply.IsOK())
	req.Equal("required parameter missing", reply.ErrorMessage())
}

func testRequestTimeoutWithoutHandler(t *testing.T) {
	req := require.New(t)
	syncClient, err := client.NewSyncReplier(testID(t, "missing-handler"), 0)
	req.NoError(err)
	defer func() { req.NoError(syncClient.Close()) }()
	syncClient.Timeout(20 * time.Millisecond)
	syncClient.Attempt(1)

	reply, err := syncClient.Request(newRequest("echo", "value"))
	req.Error(err)
	req.Nil(reply)
	req.Contains(err.Error(), "request_timeout")
}

func testControlTimeoutWithoutHandler(t *testing.T) {
	req := require.New(t)
	control, err := client.NewControl(controlID(testID(t, "missing-control"), 0), 0)
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
	pubClient, err := client.NewPublisher(testID(t, "idle-publisher"), 0)
	req.NoError(err)
	defer func() { req.NoError(pubClient.Close()) }()
	pubClient.Timeout(20 * time.Millisecond)
	pubClient.Attempt(2)

	replies := pubClient.Receive()
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
	svc := handler.NewPair()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	setHandlerMushroomURL(svc, id)
	req.NoError(svc.Route("echo", echoRoute))
	req.NoError(svc.Start())

	control := newPairControl(t, id)
	defer func() { req.NoError(control.Close()) }()

	pairClient, err := client.NewPair(id, 0)
	req.NoError(err)
	defer func() { req.NoError(pairClient.Close()) }()
	pairClient.Timeout(20 * time.Millisecond)
	pairClient.Attempt(1)
	replies := pairClient.Receive()

	req.NoError(control.HandlerClose())
	waitForStatus(t, control, handler.SocketNil)

	_ = pairClient.Send(newRequest("echo", "value"))
	select {
	case reply, ok := <-replies:
		req.False(ok, "unexpected reply after handler stopped: %v", reply)
	case <-time.After(time.Second):
		t.Fatal("receive channel did not close after stopped handler timeout")
	}
}
