package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

func TestSendWithoutReceiveClosesIdleReceiver(t *testing.T) {
	endpoint := randomInprocPairEndpoint(t)
	stopBackend := startInprocPairEchoBackend(t, endpoint)
	defer stopBackend()

	socket, err := New(endpoint, 0, PairType)
	if err != nil {
		t.Fatalf("New(PairType): %v", err)
	}
	defer func() {
		if err := socket.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	receiveTimeout := 25 * time.Millisecond
	socket.Timeout(receiveTimeout).Attempt(2)

	req := &message.Request{
		Command:    "echo",
		Parameters: datatype.New().Set("value", "idle-without-receive"),
	}
	if err := socket.Send(req); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Send() activates the receiver, but this test never calls Receive().
	replies := socket.receiver.replies

	maxWait := 2*receiveTimeout + 3*receiverPollInterval + 200*time.Millisecond
	waitForReceiveChannelClose(t, replies, maxWait)
}

func randomInprocPairEndpoint(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("pair_%s_%d", t.Name(), time.Now().UnixNano())
}

func startInprocPairEchoBackend(t *testing.T, endpoint string) func() {
	t.Helper()

	packer := &message.MessagePacker{}

	sock, err := zmq.NewSocket(zmq.PAIR)
	if err != nil {
		t.Fatalf("zmq.NewSocket(PAIR): %v", err)
	}
	if err := sock.SetLinger(0); err != nil {
		_ = sock.Close()
		t.Fatalf("sock.SetLinger(0): %v", err)
	}

	url := message.NewEndpoint(endpoint, 0).HandlerUrl()
	if err := sock.Bind(url); err != nil {
		_ = sock.Close()
		t.Fatalf("sock.Bind(%q): %v", url, err)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = sock.Close() }()

		for {
			select {
			case <-stop:
				return
			default:
			}

			msg, err := sock.RecvMessage(zmq.DONTWAIT)
			if err != nil {
				time.Sleep(time.Millisecond)
				continue
			}

			req, _, err := packer.DeserializeRequest(msg)
			if err != nil {
				continue
			}

			reply := req.Ok(req.RouteParameters())
			envelope, err := packer.SerializeReply(reply)
			if err != nil {
				continue
			}
			if _, err := sock.SendMessage(envelope); err != nil {
				return
			}
		}
	}()

	return func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Log("pair echo backend did not stop cleanly")
		}
	}
}

func waitForReceiveChannelClose(
	t *testing.T,
	replies <-chan message.ReplyInterface,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-replies:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("receive channel did not close within %s", timeout)
		}
	}
}
