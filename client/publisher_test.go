package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

func TestReceiveSubscribesToInprocPublisher(t *testing.T) {
	endpoint := randomInprocEndpoint(t)
	pub := newInprocPublisher(t, endpoint)
	defer closePublisher(t, pub)

	client, err := NewPublisher(endpoint, 0)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Fatalf("client.Close: %v", err)
		}
	}()

	replies := client.Receive()
	got := receivePublishedReply(t, replies, pub, "subscription-live", time.Second*2)
	t.Logf("received publisher reply: %s", got)
}

func TestReceiveSubscriptionAcrossPublisherRestart(t *testing.T) {
	endpoint := randomInprocEndpoint(t)
	pub := newInprocPublisher(t, endpoint)

	client, err := NewPublisher(endpoint, 0)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Fatalf("client.Close: %v", err)
		}
	}()

	client.Timeout(time.Second)
	client.Attempt(2)

	replies := client.Receive()
	first := receivePublishedReply(t, replies, pub, "before-crash", time.Second*2)
	t.Logf("received publisher reply before crash: %s", first)

	closePublisher(t, pub)
	drainReplies(replies, time.Millisecond*200)

	stopRestartedPublisher := make(chan struct{})
	restartStarted := make(chan struct{})
	restartedErr := make(chan error, 1)
	go func() {
		select {
		case <-time.After(time.Second * 3):
		case <-stopRestartedPublisher:
			restartedErr <- nil
			return
		}

		restarted, err := zmq.NewSocket(zmq.PUB)
		if err != nil {
			restartedErr <- fmt.Errorf("zmq.NewSocket(PUB): %w", err)
			return
		}
		defer func() { _ = restarted.Close() }()

		if err := restarted.SetLinger(0); err != nil {
			restartedErr <- fmt.Errorf("restarted.SetLinger(0): %w", err)
			return
		}
		if err := restarted.Bind(message.NewEndpoint(endpoint, 0).HandlerUrl()); err != nil {
			restartedErr <- fmt.Errorf("restarted.Bind: %w", err)
			return
		}
		close(restartStarted)

		ticker := time.NewTicker(time.Millisecond * 20)
		defer ticker.Stop()

		for {
			select {
			case <-stopRestartedPublisher:
				restartedErr <- nil
				return
			case <-ticker.C:
				if err := publishReply(restarted, "after-restart"); err != nil {
					restartedErr <- err
					return
				}
			}
		}
	}()

	select {
	case reply, ok := <-replies:
		if ok {
			t.Fatalf("received reply instead of closing subscription: %s", reply)
		}
		t.Log("publisher receive channel closed before the service restarted")
	case err := <-restartedErr:
		if err != nil {
			t.Fatalf("restarted publisher: %v", err)
		}
		t.Fatal("restarted publisher exited before the subscription timed out")
	case <-time.After(time.Millisecond * 2500):
		t.Fatal("timed out waiting for publisher receive channel to close")
	}

	select {
	case <-restartStarted:
		t.Log("publisher restarted after the subscription had already closed")
	case err := <-restartedErr:
		if err != nil {
			t.Fatalf("restarted publisher: %v", err)
		}
		t.Fatal("restarted publisher exited before binding")
	case <-time.After(time.Second * 2):
		t.Fatal("timed out waiting for publisher restart")
	}

	close(stopRestartedPublisher)
	if err := <-restartedErr; err != nil {
		t.Fatalf("stopping restarted publisher: %v", err)
	}
}

func randomInprocEndpoint(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("publisher_%s_%d", t.Name(), time.Now().UnixNano())
}

func newInprocPublisher(t *testing.T, endpoint string) *zmq.Socket {
	t.Helper()

	pub, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		t.Fatalf("zmq.NewSocket(PUB): %v", err)
	}

	if err := pub.SetLinger(0); err != nil {
		_ = pub.Close()
		t.Fatalf("pub.SetLinger(0): %v", err)
	}

	if err := pub.Bind(message.NewEndpoint(endpoint, 0).HandlerUrl()); err != nil {
		_ = pub.Close()
		t.Fatalf("pub.Bind: %v", err)
	}

	return pub
}

func closePublisher(t *testing.T, pub *zmq.Socket) {
	t.Helper()

	if err := pub.Close(); err != nil {
		t.Fatalf("pub.Close: %v", err)
	}
}

func receivePublishedReply(
	t *testing.T,
	replies <-chan message.ReplyInterface,
	pub *zmq.Socket,
	event string,
	timeout time.Duration,
) message.ReplyInterface {
	t.Helper()

	ticker := time.NewTicker(time.Millisecond * 20)
	defer ticker.Stop()

	deadline := time.After(timeout)
	for {
		select {
		case reply := <-replies:
			requireReplyEvent(t, reply, event)
			return reply
		case <-ticker.C:
			if err := publishReply(pub, event); err != nil {
				t.Fatalf("publishReply: %v", err)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for publisher reply %q", event)
		}
	}
}

func publishReply(pub *zmq.Socket, event string) error {
	packer := &message.MessagePacker{}
	envelope, err := packer.SerializeReply(&message.Reply{
		Status: message.OK,
		Parameters: datatype.New().
			Set("event", event),
	})
	if err != nil {
		return fmt.Errorf("packer.SerializeReply: %w", err)
	}

	if _, err := pub.SendMessageDontwait(envelope); err != nil {
		return fmt.Errorf("pub.SendMessageDontwait: %w", err)
	}
	return nil
}

func requireReplyEvent(t *testing.T, reply message.ReplyInterface, want string) {
	t.Helper()

	if !reply.IsOK() {
		t.Fatalf("reply status is not OK: %s", reply)
	}

	got, err := reply.ReplyParameters().StringValue("event")
	if err != nil {
		t.Fatalf("reply.Parameters.StringValue('event'): %v", err)
	}
	if got != want {
		t.Fatalf("reply event = %q, want %q", got, want)
	}
}

func drainReplies(replies <-chan message.ReplyInterface, quiet time.Duration) {
	timer := time.NewTimer(quiet)
	defer timer.Stop()

	for {
		select {
		case <-replies:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(quiet)
		case <-timer.C:
			return
		}
	}
}
