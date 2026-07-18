// Package client defines client zmqSocket that can access to the client service.
package client

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/protocol/message"

	zmq "github.com/pebbe/zmq4"
)

const Any = message.Any

const (
	minTimeout     = time.Millisecond * 2
	DefaultTimeout = time.Second * 100

	DefaultAttempt = uint8(5)
)

// infiniteAttempts reports whether send/request/receive retries are unbounded.
func infiniteAttempts(attempt uint8) bool {
	return attempt == 0
}

// A Socket is the structure that transmits the data to the handlers.
type Socket struct {
	mu              sync.Mutex
	zmqMu           sync.Mutex // serializes all zmq socket and poller access
	poller          *zmq.Poller
	zmqSocket       *zmq.Socket
	monitorSocket   *zmq.Socket // PAIR socket connected to zmqSocket's ZMQ monitor (CURVE only)
	closed          bool
	timeout         time.Duration
	attempt         uint8
	endpoint        message.Endpoint
	handlerType     HandlerType
	messagePacker   message.Packer
	dispatcher      *dispatcher
	receiver        *receiver
	whitelists      map[string]string
	serverPublicKey string
	curveSecretKey  string
}

// New creates a client for the given handler endpoint. Client type is determined by the target handler.
func New(id string, port uint64, handlerTargetType HandlerType) (*Socket, error) {
	if !isTarget(handlerTargetType) {
		return nil, fmt.Errorf("target is not supported")
	}

	endpoint := message.NewEndpoint(id, port)

	socket := &Socket{
		zmqSocket:     nil,
		timeout:       DefaultTimeout,
		attempt:       DefaultAttempt,
		endpoint:      endpoint,
		handlerType:   handlerTargetType,
		messagePacker: &message.MessagePacker{},
		whitelists:    make(map[string]string),
	}

	if supportsReceive(handlerTargetType) {
		socket.receiver = newReceiver(socket)
	}
	socket.dispatcher = newDispatcher(socket)
	return socket, nil
}

// Sets the message serializer and deserializer.
// Use the same as used by the handler.
func (socket *Socket) Packer(packer message.Packer) *Socket {
	socket.mu.Lock()
	defer socket.mu.Unlock()

	socket.messagePacker = packer
	return socket
}

// Attempts to connect to the endpoint.
// The difference from zmqSocket.reconnect() is that it will not authenticate if security is enabled.
func (socket *Socket) reconnect() (err error) {
	socketType := targetToClient(socket.handlerType)

	if socket.monitorSocket != nil {
		_ = socket.monitorSocket.Close()
		socket.monitorSocket = nil
	}

	if socket.zmqSocket != nil {
		if err := socket.zmqSocket.Close(); err != nil {
			return fmt.Errorf("failed to close zmqSocket in zmq: %w", err)
		}
	}

	socket.zmqSocket, err = zmq.NewSocket(socketType)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket('%s'): %w", socketType.String(), err)
	}

	if err := socket.zmqSocket.SetLinger(0); err != nil {
		return fmt.Errorf("zmqSocket.SetLinger(0): %w", err)
	}

	if err := socket.applyCurveClient(socket.zmqSocket); err != nil {
		_ = socket.zmqSocket.Close()
		socket.zmqSocket = nil
		return err
	}

	// Attach monitor before Connect so handshake events are always captured.
	// This covers both sides of auth failure: client with wrong/no CURVE key
	// connecting to a CURVE server, and client with CURVE connecting to a
	// non-CURVE server.
	if mon, monErr := attachMonitor(socket.zmqSocket); monErr == nil {
		socket.monitorSocket = mon
	}

	url := socket.endpoint.ClientUrl()
	if err := socket.zmqSocket.Connect(url); err != nil {
		return fmt.Errorf("zmqSocket.Connect('%s'): %w", url, err)
	}

	socket.afterReconnect(socketType)

	socket.poller = zmq.NewPoller()
	if socket.monitorSocket != nil {
		socket.poller.Add(socket.monitorSocket, zmq.POLLIN)
	}

	return nil
}

func (socket *Socket) updateToPollIn() {
	if _, err := socket.poller.UpdateBySocket(socket.zmqSocket, zmq.POLLIN); err != nil {
		_ = socket.poller.Add(socket.zmqSocket, zmq.POLLIN)
	}
}

func (socket *Socket) pollOut() {
	_ = socket.poller.Add(socket.zmqSocket, zmq.POLLOUT|zmq.POLLIN)
}

// Close the zmqSocket free the port and resources.
func (socket *Socket) Close() error {
	socket.mu.Lock()
	if socket.closed {
		socket.mu.Unlock()
		return nil
	}
	socket.closed = true
	recv := socket.receiver
	dispatcher := socket.dispatcher
	socket.mu.Unlock()

	if dispatcher != nil {
		dispatcher.stop()
	}

	if recv != nil {
		recv.close()
	}

	socket.zmqMu.Lock()
	zmqSocket := socket.zmqSocket
	monitorSocket := socket.monitorSocket
	socket.zmqSocket = nil
	socket.monitorSocket = nil
	socket.zmqMu.Unlock()

	if monitorSocket != nil {
		_ = monitorSocket.Close()
	}

	if zmqSocket == nil {
		return nil
	}
	err := zmqSocket.Close()
	if err != nil {
		return fmt.Errorf("error closing zmqSocket: %w", err)
	}

	return nil
}

// Timeout update. If the timeout is less than minTimeout, then minTimeout is set
func (socket *Socket) Timeout(timeout time.Duration) *Socket {
	if timeout < minTimeout {
		timeout = minTimeout
	}

	socket.mu.Lock()
	defer socket.mu.Unlock()

	socket.timeout = timeout
	return socket
}

// Attempt sets how many timeout windows are tried before giving up.
// Zero means retry indefinitely.
func (socket *Socket) Attempt(attempt uint8) *Socket {
	socket.mu.Lock()
	defer socket.mu.Unlock()

	socket.attempt = attempt
	return socket
}

func (socket *Socket) options() (time.Duration, uint8) {
	socket.mu.Lock()
	defer socket.mu.Unlock()

	return socket.timeout, socket.attempt
}

func (socket *Socket) isClosed() bool {
	socket.mu.Lock()
	defer socket.mu.Unlock()

	return socket.closed
}

func (socket *Socket) attemptRequesting(envelope []string) ([]string, error) {
	socket.zmqMu.Lock()
	defer socket.zmqMu.Unlock()

	timeoutDuration, maxAttempt := socket.options()

	triesLeft := uint8(0)
	if !infiniteAttempts(maxAttempt) {
		triesLeft = maxAttempt + 1
	}

	for {
		if !infiniteAttempts(maxAttempt) {
			triesLeft--
			if triesLeft == 0 {
				return nil, fmt.Errorf("%w: envelope=%v", message.RequestTimeoutError, envelope)
			}
		}

		timeout, err := socket.send(envelope)
		if err != nil {
			return nil, fmt.Errorf("socket.rawSend: %w", err)
		}

		if timeout {
			continue
		}

		socket.updateToPollIn()

		// Inner poll loop: keep waiting on the same socket until a reply arrives,
		// the timeout expires, or an auth error is detected. Non-error monitor
		// events (e.g. EVENT_CONNECTED) do not consume an attempt or reconnect.
		for {
			sockets, err := socket.poller.Poll(timeoutDuration)
			if err != nil {
				return nil, fmt.Errorf("poll error: %w", err)
			}

			// When the poll times out the monitor events have had the full timeout
			// window to accumulate, so drain now before reconnect destroys the socket.
			if len(sockets) == 0 {
				if socket.monitorSocket != nil {
					if authErr := drainMonitor(socket.monitorSocket); authErr != nil {
						return nil, authErr
					}
				}
				break // timed out — fall through to outer loop for reconnect + retry
			}

			for _, s := range sockets {
				if s.Socket == socket.monitorSocket {
					if authErr := drainMonitor(socket.monitorSocket); authErr != nil {
						return nil, authErr
					}
				}
			}

			for _, s := range sockets {
				if s.Socket == socket.zmqSocket {
					r, err := socket.zmqSocket.RecvMessage(0)
					if err != nil {
						return nil, fmt.Errorf("zmqSocket.RecvMessage: %w", err)
					}
					return r, nil
				}
			}
			// Monitor fired (non-error) but no reply yet — keep polling the same socket.
		}
	}
}

func (socket *Socket) attemptSending(envelope []string) error {
	socket.zmqMu.Lock()
	defer socket.zmqMu.Unlock()

	_, maxAttempt := socket.options()

	triesLeft := maxAttempt
	for {
		timeout, err := socket.send(envelope)
		if err != nil {
			return fmt.Errorf("socket.rawSend: %w", err)
		}

		if !timeout {
			socket.omitReplyIfPresent()
			break
		}

		if infiniteAttempts(maxAttempt) {
			continue
		}

		triesLeft--
		if triesLeft == 0 {
			return fmt.Errorf("send_timeout: envelope=%v", envelope)
		}
	}
	return nil
}

// send sends the message; it doesn't wait for a reply to see was it successfully sent.
//
// returns boolean for timeout.
func (socket *Socket) send(envelope []string) (bool, error) {
	timeoutDuration, _ := socket.options()

	err := socket.reconnect()
	if err != nil {
		return false, fmt.Errorf("initial  socket.reconnect: %w", err)
	}

	socket.pollOut()

	sockets, err := socket.poller.Poll(timeoutDuration)
	if err != nil {
		return false, fmt.Errorf("poll error: %w", err)
	}

	for _, s := range sockets {
		if s.Socket == socket.monitorSocket {
			if authErr := drainMonitor(socket.monitorSocket); authErr != nil {
				return false, authErr
			}
		}
	}

	for _, s := range sockets {
		if s.Socket == socket.zmqSocket {
			if _, err := socket.zmqSocket.SendMessage(envelope); err != nil {
				return false, fmt.Errorf("zmqSocket.SendMessage: %w", err)
			}
			return false, nil
		}
	}

	return true, nil
}

// omitReplyIfPresent drops an inbound reply so REQ submit can complete.
func (socket *Socket) omitReplyIfPresent() {
	timeoutDuration, _ := socket.options()

	socketType, err := socket.zmqSocket.GetType()
	if err != nil || socketType != zmq.REQ {
		return
	}

	socket.updateToPollIn()

	drain := timeoutDuration
	if drain > time.Second {
		drain = time.Second
	}

	sockets, err := socket.poller.Poll(drain)
	if err != nil || len(sockets) == 0 {
		return
	}

	_, _ = socket.zmqSocket.RecvMessage(0)
}

// Send transmits the request to the handler without waiting for a reply.
//
// When the connection is rejected due to a missing CURVE key (ErrNoCurveKey),
// the method looks up the server's public key from the npac autocontext and
// retries once with the new key.
func (socket *Socket) Send(req message.RequestInterface, hmac ...string) error {
	reqStr, err := socket.serializeRequest(req, hmac...)
	if err != nil {
		return fmt.Errorf("packer.SerializeRequest: %w", err)
	}

	sendErr := socket.dispatcher.send(reqStr)

	// On ErrNoCurveKey: fetch the server public key from npac and retry once.
	if sendErr != nil && errors.Is(sendErr, message.ErrNoCurveKey) {
		autocontext := NewAutocontext()
		if autocontext == nil {
			return fmt.Errorf("failed to create autocontext")
		}
		unregistered, publicKey, controlEndpoint, err := autocontext.HandlerContext(socket.endpoint, req.CommandName())

		autocontext.Close()

		if err != nil {
			return fmt.Errorf("autocontext.HandlerContext: %w", err)
		}
		if unregistered {
			return fmt.Errorf("%w: autocontext.HandlerContext(%s, %s): unregistered in 'npap'.", message.ErrNoCurveKey, socket.endpoint.HandlerUrl(), req.CommandName())
		}

		control, err := NewControl(controlEndpoint.Id, controlEndpoint.Port)
		if err != nil {
			return fmt.Errorf("NewControl: %w", err)
		}
		envelope, err := socket.messagePacker.SerializeRequest(req, hmac...)
		if err != nil {
			return fmt.Errorf("request: message.ErrNoCurveKey: packer.SerializeRequest: %w", err)
		}
		// endpoint, public-key, request, hmac optional
		_, err = control.RequestAsContext(socket.endpoint, socket.handlerType, publicKey, envelope, req.CommandName(), socket.attempt, socket.timeout, hmac...)
		if err != nil {
			return fmt.Errorf("control.Send: %w", err)
		}
		control.Close()
	}

	if sendErr != nil {
		return fmt.Errorf("socket.send: %w", sendErr)
	}

	return nil
}

// Request sends the request message to the zmqSocket.
// Returns the message.Reply.Parameters in case of success.
//
// When the connection is rejected due to a missing CURVE key (ErrNoCurveKey),
// the method looks up the server's public key from the npac autocontext and
// retries once with the new key.
//
// When the handler replies with "access-denied" (HMAC required but not sent),
// the method looks up the HMAC secret from the npac autocontext, re-signs the
// request, and retries once.
func (socket *Socket) Request(req message.RequestInterface, hmac ...string) (message.ReplyInterface, error) {
	reqStr, err := socket.serializeRequest(req, hmac...)
	if err != nil {
		return nil, fmt.Errorf("packer.SerializeRequest: %w", err)
	}

	rawReply, reqErr := socket.dispatcher.request(reqStr)

	// On ErrNoCurveKey: fetch the server public key from npac and retry once.
	if reqErr != nil && errors.Is(reqErr, message.ErrNoCurveKey) {
		autocontext := NewAutocontext()
		if autocontext == nil {
			return nil, fmt.Errorf("request: message.ErrNoCurveKey: failed to create autocontext")
		}
		unregistered, publicKey, controlEndpoint, err := autocontext.HandlerContext(socket.endpoint, req.CommandName())

		autocontext.Close()

		if err != nil {
			return nil, fmt.Errorf("request: message.ErrNoCurveKey: autocontext.HandlerContext: %w", err)
		}
		if unregistered {
			return nil, fmt.Errorf("%w: autocontext.HandlerContext(%s, %s): outbound is unregistered in 'npac'.", message.ErrNoCurveKey, socket.endpoint.HandlerUrl(), req.CommandName())
		}

		control, err := NewControl(controlEndpoint.Id, controlEndpoint.Port)
		if err != nil {
			return nil, fmt.Errorf("request: message.ErrNoCurveKey: NewControl: %w", err)
		}
		defer control.Close()
		envelope, err := socket.messagePacker.SerializeRequest(req, hmac...)
		if err != nil {
			return nil, fmt.Errorf("request: message.ErrNoCurveKey: packer.SerializeRequest: %w", err)
		}
		// endpoint, public-key, request, hmac optional
		reply, err := control.RequestAsContext(socket.endpoint, socket.handlerType, publicKey, envelope, req.CommandName(), socket.attempt, socket.timeout, hmac...)
		if err != nil {
			return nil, fmt.Errorf("request: message.ErrNoCurveKey: control.RequestAsContext: %w", err)
		}

		return reply, nil
	}

	if reqErr != nil {
		return nil, fmt.Errorf("socket.request: %w", reqErr)
	}

	reply, replyHmac, err := socket.messagePacker.DeserializeReply(rawReply)
	if err != nil {
		return nil, fmt.Errorf("packer.DeserializeReply('%v'): %w", rawReply, err)
	}

	// On "access-denied" reply body: the handler requires HMAC but the client
	// did not sign the request. Fetch the secret from npac, re-sign, and retry.
	if !reply.IsOK() && reply.ErrorMessage() == message.ErrAccessDenied.Error() {
		autocontext := NewAutocontext()
		if autocontext == nil {
			return nil, fmt.Errorf("reply: ErrAccessDenied: failed to create autocontext")
		}
		unregistered, publicKey, controlEndpoint, err := autocontext.HandlerContext(socket.endpoint, req.CommandName())

		autocontext.Close()

		if err != nil {
			return nil, fmt.Errorf("reply: ErrAccessDenied: autocontext.HandlerContext: %w", err)
		}
		if unregistered {
			return nil, fmt.Errorf("reply: ErrAccessDenied: %w: '%s' not registered in 'npap'.", message.ErrAccessDenied, socket.endpoint.HandlerUrl())
		}

		control, err := NewControl(controlEndpoint.Id, controlEndpoint.Port)
		if err != nil {
			return nil, fmt.Errorf("reply: ErrAccessDenied: NewControl: %w", err)
		}
		envelope, err := socket.messagePacker.SerializeRequest(req, hmac...)
		if err != nil {
			return nil, fmt.Errorf("reply: ErrAccessDenied: packer.SerializeRequest: %w", err)
		}
		// endpoint, public-key, request, hmac optional
		reply, err := control.RequestAsContext(socket.endpoint, socket.handlerType, publicKey, envelope, req.CommandName(), socket.attempt, socket.timeout, hmac...)
		if err != nil {
			return nil, fmt.Errorf("reply: ErrAccessDenied: control.RequestAsContext: %w", err)
		}

		control.Close()

		return reply, nil
	}

	if err := socket.validateReply(req.CommandName(), reply, replyHmac); err != nil {
		return nil, fmt.Errorf("reply hmac validation: %w", err)
	}

	return reply, nil
}
