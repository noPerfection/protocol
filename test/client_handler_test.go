package protocoltest

import (
	"strings"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/handler/npac"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

func TestClientHandlerPairs(t *testing.T) {
	n := npac.New()
	require.NoError(t, n.Start())
	time.Sleep(10 * time.Millisecond)

	t.Run("sync replier request", testSyncReplierClientHandler)
	t.Run("replier send receive", testReplierClientHandler)
	t.Run("worker send", testWorkerClientHandler)
	t.Run("pair send receive", testPairClientHandler)
	t.Run("publisher receive broadcast", testPublisherClientHandler)
}

func testSyncReplierClientHandler(t *testing.T) {
	req := require.New(t)
	id := testID(t, "sync")
	svc := handler.NewSyncReplier()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	setHandlerMushroomURL(svc, id)
	req.NoError(svc.Route("echo", echoRoute))
	req.NoError(svc.Start())

	control := newSyncControl(t, id)
	defer closeControl(t, control)

	client, err := client.NewSyncReplier(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(time.Second)

	reply, err := client.Request(newRequest("echo", "sync-value"))
	req.NoError(err)
	assertEchoReply(t, reply, "echo", "sync-value")
}

func testReplierClientHandler(t *testing.T) {
	req := require.New(t)
	id := testID(t, "replier")
	svc := handler.NewReplier()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	setHandlerMushroomURL(svc, id)
	req.NoError(svc.Route("echo", echoRoute))
	req.NoError(svc.Start())

	control := newReplierControl(t, id)
	defer closeControl(t, control)

	client, err := client.NewReplier(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(time.Second)
	client.Attempt(3)
	replies := client.Receive()

	req.NoError(client.Send(newRequest("echo", "replier-value")))
	assertEchoReply(t, receiveReply(t, replies), "echo", "replier-value")
}

func testWorkerClientHandler(t *testing.T) {
	req := require.New(t)
	id := testID(t, "worker")
	handled := make(chan string, 1)
	svc := handler.NewWorker()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	setHandlerMushroomURL(svc, id)
	req.NoError(svc.Route("work", func(request message.RequestInterface) message.ReplyInterface {
		value, err := request.RouteParameters().StringValue("value")
		if err == nil {
			handled <- value
		}
		return request.Ok(datatype.New())
	}))
	req.NoError(svc.Start())

	control := newWorkerControl(t, id)
	defer closeControl(t, control)

	client, err := client.NewWorker(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(time.Second)

	req.NoError(client.Send(newRequest("work", "worker-value")))
	select {
	case value := <-handled:
		req.Equal("worker-value", value)
	case <-time.After(time.Second):
		t.Fatal("worker handler did not receive client message")
	}
}

func testPairClientHandler(t *testing.T) {
	req := require.New(t)
	id := testID(t, "pair")
	svc := handler.NewPair()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	setHandlerMushroomURL(svc, id)
	req.NoError(svc.Route("echo", echoRoute))
	req.NoError(svc.Start())

	control := newPairControl(t, id)
	defer closeControl(t, control)

	client, err := client.NewPair(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(time.Second)
	client.Attempt(3)
	replies := client.Receive()
	time.Sleep(time.Millisecond * 50)

	req.NoError(client.Send(newRequest("echo", "pair-value")))
	assertEchoReply(t, receiveReply(t, replies), "echo", "pair-value")
}

func testPublisherClientHandler(t *testing.T) {
	req := require.New(t)
	id := testID(t, "publisher")
	svc := handler.NewPublisher()
	svc.SetEndpoint(message.NewEndpoint(id, 0))
	setHandlerMushroomURL(svc, id)
	req.NoError(svc.Start())

	control := newPublisherControl(t, id)
	defer closeControl(t, control)

	client, err := client.NewPublisher(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(time.Second)
	client.Attempt(3)
	replies := client.Receive()
	time.Sleep(time.Millisecond * 50)

	req.NoError(control.Broadcast(*newReply("publisher-value")))
	reply := receiveReply(t, replies)
	req.True(reply.IsOK())
	value, err := reply.ReplyParameters().StringValue("value")
	req.NoError(err)
	req.Equal("publisher-value", value)
}

func echoRoute(request message.RequestInterface) message.ReplyInterface {
	return request.Ok(request.RouteParameters().Set("command", request.CommandName()))
}

func newRequest(command string, value string) *message.Request {
	return &message.Request{
		Command:    command,
		Parameters: datatype.New().Set("value", value),
	}
}

func newReply(value string) *message.Reply {
	return &message.Reply{
		Status:     message.OK,
		Parameters: datatype.New().Set("value", value),
	}
}

func assertEchoReply(t *testing.T, reply message.ReplyInterface, command string, value string) {
	t.Helper()
	req := require.New(t)
	req.True(reply.IsOK())
	actualCommand, err := reply.ReplyParameters().StringValue("command")
	req.NoError(err)
	req.Equal(command, actualCommand)
	actualValue, err := reply.ReplyParameters().StringValue("value")
	req.NoError(err)
	req.Equal(value, actualValue)
}

func receiveReply(t *testing.T, replies <-chan message.ReplyInterface) message.ReplyInterface {
	t.Helper()
	select {
	case reply, ok := <-replies:
		require.True(t, ok, "reply channel closed")
		return reply
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reply")
		return nil
	}
}

func testID(t *testing.T, suffix string) string {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return name + "_" + suffix
}

type mushroomURLSetter interface {
	SetMushroomURL(string)
}

func setHandlerMushroomURL(svc mushroomURLSetter, id string) {
	svc.SetMushroomURL("pkg:golang/protocol-test#" + id)
}

type handlerControl interface {
	HandlerClose() error
}

func closeControl(t *testing.T, control handlerControl) {
	t.Helper()
	require.NoError(t, control.HandlerClose())
	time.Sleep(100 * time.Millisecond)
}
