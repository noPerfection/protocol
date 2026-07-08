// Package autocontext is the client-side half of the noPerfection AutoContext (npac).
// It looks up CURVE public keys and HMAC secrets from the in-process npac handler
// so that the client can recover from ErrNoCurveKey and access-denied replies automatically.
package autocontext

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	// NpacEndpointId is the inproc endpoint id of the npac handler (inproc://npac).
	NpacEndpointId = "npac"
	npacUrl        = "inproc://" + NpacEndpointId

	// NpacTimeout is the maximum time to wait for a reply from npac.
	NpacTimeout = 50 * time.Millisecond
)

// rawRequest sends a single request to npac using a fresh REQ socket and returns
// the raw reply. Transport or serialisation errors are returned directly; the
// caller is responsible for checking reply.IsOK().
func rawRequest(req message.RequestInterface) (message.ReplyInterface, error) {
	sock, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return nil, fmt.Errorf("autocontext: zmq.NewSocket: %w", err)
	}
	defer sock.Close()

	if err := sock.Connect(npacUrl); err != nil {
		return nil, fmt.Errorf("autocontext: Connect(%s): %w", npacUrl, err)
	}

	packer := &message.MessagePacker{}
	envelope, err := packer.SerializeRequest(req)
	if err != nil {
		return nil, fmt.Errorf("autocontext: SerializeRequest: %w", err)
	}

	if _, err := sock.SendMessage(envelope); err != nil {
		return nil, fmt.Errorf("autocontext: SendMessage: %w", err)
	}

	poller := zmq.NewPoller()
	poller.Add(sock, zmq.POLLIN)

	sockets, err := poller.Poll(NpacTimeout)
	if err != nil {
		return nil, fmt.Errorf("autocontext: poll error: %w", err)
	}
	if len(sockets) == 0 {
		return nil, fmt.Errorf("autocontext: poll timeout waiting for npac")
	}

	raw, err := sock.RecvMessage(0)
	if err != nil {
		return nil, fmt.Errorf("autocontext: RecvMessage: %w", err)
	}

	reply, _, err := packer.DeserializeReply(raw)
	if err != nil {
		return nil, fmt.Errorf("autocontext: DeserializeReply: %w", err)
	}
	return reply, nil
}

// GetPublicKey asks npac for the CURVE public key of the handler at the given URL.
func GetPublicKey(url string) (string, error) {
	reply, err := rawRequest(&message.Request{
		Command:    "get-public-key",
		Parameters: datatype.New().Set("url", url),
	})
	if err != nil {
		return "", err
	}
	if !reply.IsOK() {
		return "", fmt.Errorf(reply.ErrorMessage())
	}
	return reply.ReplyParameters().StringValue("public-key")
}

// GetHmacSecret asks npac for the HMAC secret for the given handler URL and command.
func GetHmacSecret(url, cmd string) (string, error) {
	reply, err := rawRequest(&message.Request{
		Command:    "get-hmac-secret",
		Parameters: datatype.New().Set("url", url).Set("command", cmd),
	})
	if err != nil {
		return "", err
	}
	if !reply.IsOK() {
		return "", fmt.Errorf(reply.ErrorMessage())
	}
	return reply.ReplyParameters().StringValue("secret")
}
