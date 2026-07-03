package client

import (
	"testing"
	"time"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

func TestApplyCurveClientSkipsInproc(t *testing.T) {
	socket := &Socket{endpoint: message.NewEndpoint("svc", 0), serverPublicKey: "not-a-real-key"}

	raw, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	if err := socket.applyCurveClient(raw); err != nil {
		t.Fatalf("applyCurveClient: %v", err)
	}

	if mechanism, _ := raw.GetMechanism(); mechanism != zmq.NULL {
		t.Fatalf("expected NULL mechanism for inproc endpoint, got %v", mechanism)
	}
}

func TestApplyCurveClientEmptyKeyIsNonSecure(t *testing.T) {
	socket := &Socket{endpoint: message.NewEndpoint("127.0.0.1", 6000), serverPublicKey: ""}

	raw, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	if err := socket.applyCurveClient(raw); err != nil {
		t.Fatalf("applyCurveClient: %v", err)
	}

	if mechanism, _ := raw.GetMechanism(); mechanism != zmq.NULL {
		t.Fatalf("expected NULL mechanism for empty key, got %v", mechanism)
	}
}

// TestCurveClientServerInterop verifies the client CURVE configuration negotiates
// an encrypted CURVE session with a server configured the same way a handler is.
func TestCurveClientServerInterop(t *testing.T) {
	if !zmq.HasCurve() {
		t.Skip("CURVE not available in this libzmq build")
	}

	serverPublic, serverSecret, err := zmq.NewCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}

	server, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	endpoint := message.NewEndpoint("127.0.0.1", 6000)

	// Same CURVE configuration the handler applies on its bound socket.
	if err := server.ServerAuthCurve(endpoint.ZapDomain(), serverSecret); err != nil {
		t.Fatal(err)
	}
	if err := server.Bind("tcp://127.0.0.1:*"); err != nil {
		t.Fatal(err)
	}
	boundURL, err := server.GetLastEndpoint()
	if err != nil {
		t.Fatal(err)
	}

	socket := &Socket{endpoint: endpoint, serverPublicKey: serverPublic}
	client, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := socket.applyCurveClient(client); err != nil {
		t.Fatalf("applyCurveClient: %v", err)
	}
	if mechanism, _ := client.GetMechanism(); mechanism != zmq.CURVE {
		t.Fatalf("expected CURVE mechanism on client, got %v", mechanism)
	}
	if err := client.Connect(boundURL); err != nil {
		t.Fatal(err)
	}

	if _, err := client.SendMessage("ping"); err != nil {
		t.Fatal(err)
	}

	poller := zmq.NewPoller()
	poller.Add(server, zmq.POLLIN)
	polled, err := poller.Poll(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(polled) == 0 {
		t.Fatal("server did not receive encrypted message within timeout")
	}

	msg, err := server.RecvMessage(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msg) == 0 || msg[0] != "ping" {
		t.Fatalf("unexpected server message: %v", msg)
	}
}

func TestApplyCurveClientUsesPredefinedSecret(t *testing.T) {
	if !zmq.HasCurve() {
		t.Skip("CURVE not available in this libzmq build")
	}

	serverPublic, _, err := zmq.NewCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}

	clientPublic, clientSecret, err := zmq.NewCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}

	socket := &Socket{
		endpoint:        message.NewEndpoint("127.0.0.1", 6000),
		serverPublicKey: serverPublic,
		curveSecretKey:  clientSecret,
	}

	raw, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	if err := socket.applyCurveClient(raw); err != nil {
		t.Fatalf("applyCurveClient: %v", err)
	}

	gotPublic, err := raw.GetCurvePublickeykeyZ85()
	if err != nil {
		t.Fatal(err)
	}
	if gotPublic != clientPublic {
		t.Fatalf("expected predefined client public key %q, got %q", clientPublic, gotPublic)
	}
}
