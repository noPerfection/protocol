// Package autocontext is the handler-side half of the noPerfection AutoContext (npac).
// Handlers call these functions to register their CURVE public key and HMAC secrets
// with the in-process npac handler so that clients can discover them automatically.
//
// All functions silently succeed (return nil) if npac is not running.
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
	// NpacTimeout is the maximum time to wait for a reply from npac.
	NpacTimeout = 50 * time.Millisecond
)

// rawRequest sends a single request to npac using a fresh REQ socket and returns
// the raw reply. Transport or serialisation errors are returned directly; the
// caller is responsible for checking reply.IsOK().
// An optional HMAC signature may be appended to the envelope.
func rawRequest(req message.RequestInterface, hmac ...string) (message.ReplyInterface, error) {
	sock, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return nil, fmt.Errorf("autocontext: zmq.NewSocket: %w", err)
	}
	defer sock.Close()

	npacUrl := "inproc://" + NpacEndpointId
	if err := sock.Connect(npacUrl); err != nil {
		return nil, fmt.Errorf("autocontext: Connect(%s): %w", npacUrl, err)
	}

	packer := &message.MessagePacker{}
	envelope, err := packer.SerializeRequest(req, hmac...)
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

// AddHandler registers a handler's URL and CURVE public key with npac.
// The handler's npacSecret is passed as a plain parameter; npac stores it and
// uses it to authenticate future add-route / remove-route calls from this handler.
// If the URL is already registered with a different secret, npac rejects the call.
func AddHandler(url, pubKey, npacSecret string) error {
	reply, err := rawRequest(&message.Request{
		Command: "add-handler",
		Parameters: datatype.New().
			Set("url", url).
			Set("public-key", pubKey).
			Set("secret", npacSecret),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// AddRoute registers an HMAC secret for a handler URL and command with npac.
// The request is HMAC-signed with npacSecret so npac can authenticate it.
func AddRoute(url, cmd, routeSecret, npacSecret string) error {
	req := &message.Request{
		Command: "add-route",
		Parameters: datatype.New().
			Set("url", url).
			Set("command", cmd).
			Set("secret", routeSecret),
	}
	hmac := message.ComputeHMAC(req.String(), npacSecret)
	reply, err := rawRequest(req, hmac)
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// RemoveHandler removes a handler's registration from npac.
// The request is HMAC-signed with npacSecret so npac can authenticate it.
func RemoveHandler(url, npacSecret string) error {
	req := &message.Request{
		Command:    "remove-handler",
		Parameters: datatype.New().Set("url", url),
	}
	hmac := message.ComputeHMAC(req.String(), npacSecret)
	reply, err := rawRequest(req, hmac)
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// RemoveRoute removes an HMAC secret registration for a handler URL and command.
// The request is HMAC-signed with npacSecret so npac can authenticate it.
func RemoveRoute(url, cmd, npacSecret string) error {
	req := &message.Request{
		Command: "remove-route",
		Parameters: datatype.New().
			Set("url", url).
			Set("command", cmd),
	}
	hmac := message.ComputeHMAC(req.String(), npacSecret)
	reply, err := rawRequest(req, hmac)
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}
