package protocoltest

import (
	"strings"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	cpair "github.com/noPerfection/protocol/client/pair"
	cpublisher "github.com/noPerfection/protocol/client/publisher"
	creplier "github.com/noPerfection/protocol/client/replier"
	csyncreplier "github.com/noPerfection/protocol/client/sync_replier"
	cworker "github.com/noPerfection/protocol/client/worker"
	"github.com/noPerfection/protocol/handler/config"
	hpair "github.com/noPerfection/protocol/handler/pair"
	hpublisher "github.com/noPerfection/protocol/handler/publisher"
	hreplier "github.com/noPerfection/protocol/handler/replier"
	hsyncreplier "github.com/noPerfection/protocol/handler/sync_replier"
	hworker "github.com/noPerfection/protocol/handler/worker"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

func TestClientHandlerPairs(t *testing.T) {
	t.Run("sync replier request", testSyncReplierClientHandler)
	t.Run("replier send receive", testReplierClientHandler)
	t.Run("worker send", testWorkerClientHandler)
	t.Run("pair send receive", testPairClientHandler)
	t.Run("publisher receive broadcast", testPublisherClientHandler)
}

func testSyncReplierClientHandler(t *testing.T) {
	req := require.New(t)
	id := testID(t, "sync")
	handler := hsyncreplier.New()
	handler.SetConfig(config.New(config.SyncReplierType, id, "test", 0))
	req.NoError(handler.Route("echo", echoRoute))
	req.NoError(handler.Start())

	control := newSyncControl(t, id)
	defer closeControl(t, control)

	client, err := csyncreplier.NewClient(id, 0)
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
	handler := hreplier.New()
	handler.SetConfig(config.New(config.ReplierType, id, "test", 0))
	req.NoError(handler.Route("echo", echoRoute))
	req.NoError(handler.Start())

	control := newReplierControl(t, id)
	defer closeControl(t, control)

	client, err := creplier.NewClient(id, 0)
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
	handler := hworker.New()
	handler.SetConfig(config.New(config.WorkerType, id, "test", 0))
	req.NoError(handler.Route("work", func(request message.RequestInterface) message.ReplyInterface {
		value, err := request.RouteParameters().StringValue("value")
		if err == nil {
			handled <- value
		}
		return request.Ok(datatype.New())
	}))
	req.NoError(handler.Start())

	control := newWorkerControl(t, id)
	defer closeControl(t, control)

	client, err := cworker.NewClient(id, 0)
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
	handler := hpair.New()
	handler.SetConfig(config.New(config.PairType, id, "test", 0))
	req.NoError(handler.Route("echo", echoRoute))
	req.NoError(handler.Start())

	control := newPairControl(t, id)
	defer closeControl(t, control)

	client, err := cpair.NewClient(id, 0)
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
	handler := hpublisher.New()
	handler.SetConfig(config.New(config.PublisherType, id, "test", 0))
	req.NoError(handler.Start())

	control := newPublisherControl(t, id)
	defer closeControl(t, control)

	client, err := cpublisher.NewClient(id, 0)
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

type handlerControl interface {
	HandlerClose() error
}

func closeControl(t *testing.T, control handlerControl) {
	t.Helper()
	require.NoError(t, control.HandlerClose())
	time.Sleep(100 * time.Millisecond)
}
