package handler

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/require"
)

func TestWorkerStressTest(t *testing.T) {
	const clientAmount = 1000

	processed := make(chan string, clientAmount)

	worker := NewWorker()
	err := worker.Route("db_request", func(request message.RequestInterface) message.ReplyInterface {
		time.Sleep(time.Millisecond * time.Duration(50+rand.Intn(51)))
		clientID, err := request.RouteParameters().StringValue("client_id")
		if err == nil {
			processed <- clientID
		}
		return request.Ok(datatype.New())
	})
	require.NoError(t, err)

	logger, err := log.New("worker-stress", false)
	require.NoError(t, err)

	testID := strings.ReplaceAll(t.Name(), "/", "_")
	handlerConfig := message.NewEndpoint(testID, 0)
	require.NoError(t, worker.SetLogger(logger))

	worker.SetEndpoint(handlerConfig)
	require.NoError(t, worker.SetLogger(logger))
	require.NoError(t, worker.Start())

	managerClient, err := zmq.NewSocket(zmq.REQ)
	require.NoError(t, err)
	defer func() { _ = managerClient.Close() }()

	managerConfig := NewInternalControlEndpoint(handlerConfig)
	require.NoError(t, managerClient.Connect(managerConfig.ClientUrl()))

	clients := make([]*zmq.Socket, clientAmount)
	for i := range clients {
		socket, err := zmq.NewSocket(zmq.PUSH)
		require.NoError(t, err)
		require.NoError(t, socket.SetSndtimeo(6*time.Second))
		require.NoError(t, socket.Connect(handlerConfig.ClientUrl()))
		clients[i] = socket
	}
	defer func() {
		for _, socket := range clients {
			if socket != nil {
				_ = socket.Close()
			}
		}
	}()

	sendErrors := make(chan error, clientAmount)
	startedAt := time.Now()
	for i, socket := range clients {
		clientID := fmt.Sprintf("client_%d", i)
		go func(socket *zmq.Socket, clientID string) {
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

			req := message.Request{
				Command:    "db_request",
				Parameters: datatype.New().Set("client_id", clientID),
			}
			packer := &message.MessagePacker{}
			envelope, err := packer.SerializeRequest(&req)
			if err != nil {
				sendErrors <- fmt.Errorf("%s: packer.SerializeRequest: %w", clientID, err)
				return
			}
			if _, err := socket.SendMessage(envelope); err != nil {
				sendErrors <- fmt.Errorf("%s: socket.SendMessage: %w", clientID, err)
				return
			}

			sendErrors <- nil
		}(socket, clientID)
	}

	for i := 0; i < clientAmount; i++ {
		require.NoError(t, <-sendErrors)
	}

	seen := make(map[string]struct{}, clientAmount)
	for i := 0; i < clientAmount; i++ {
		select {
		case clientID := <-processed:
			seen[clientID] = struct{}{}
		case <-time.After(6 * time.Second):
			t.Fatalf("timed out waiting for worker processing after %d/%d clients", i, clientAmount)
		}
	}
	require.Len(t, seen, clientAmount)
	require.Less(t, time.Since(startedAt), 3*time.Second)

	controlReq := message.Request{Command: HandlerClose, Parameters: datatype.New()}
	packger := &message.MessagePacker{}
	envelope, err := packger.SerializeRequest(&controlReq)
	require.NoError(t, err)
	_, err = managerClient.SendMessage(envelope)
	require.NoError(t, err)

	raw, err := managerClient.RecvMessage(0)
	require.NoError(t, err)
	reply, _, err := packger.DeserializeReply(raw)
	require.NoError(t, err)
	require.True(t, reply.IsOK())
}
