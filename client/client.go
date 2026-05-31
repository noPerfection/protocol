// Package client defines client zmqSocket that can access to the client service.
package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/protocol/message"

	zmq "github.com/pebbe/zmq4"
)

const (
	minTimeout     = time.Millisecond * 2
	DefaultTimeout = time.Second * 100

	minAttempt     = uint8(1)
	DefaultAttempt = uint8(5)
)

// A Socket is the structure that transmits the data to the handlers.
type Socket struct {
	mu            sync.Mutex
	zmqMu         sync.Mutex // serializes all zmq socket and poller access
	poller        *zmq.Poller
	zmqSocket     *zmq.Socket
	closed        bool
	timeout       time.Duration
	attempt       uint8
	endpoint      message.Endpoint
	handlerType   HandlerType
	messagePacker message.Packer
	dispatcher    *dispatcher
	receiver      *receiver
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

	url := socket.endpoint.ClientUrl()
	if err := socket.zmqSocket.Connect(url); err != nil {
		return fmt.Errorf("zmqSocket.Connect('%s'): %w", url, err)
	}

	socket.afterReconnect(socketType)

	socket.poller = zmq.NewPoller()

	return nil
}

func (socket *Socket) updateToPollIn() {
	_, _ = socket.poller.UpdateBySocket(socket.zmqSocket, zmq.POLLIN)
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
	socket.zmqSocket = nil
	socket.zmqMu.Unlock()

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

// Attempt update. If the attempt is less than minAttempt, then minAttempt is set
func (socket *Socket) Attempt(attempt uint8) *Socket {
	if attempt < minAttempt {
		attempt = minAttempt
	}

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

	timeoutDuration, attempt := socket.options()

	attempt++

	for {
		attempt--
		if attempt == 0 {
			return nil, fmt.Errorf("request_timeout: envelope=%v", envelope)
		}

		timeout, err := socket.send(envelope)
		if err != nil {
			return nil, fmt.Errorf("socket.rawSend: %w", err)
		}

		if timeout {
			continue
		}

		socket.updateToPollIn()

		sockets, err := socket.poller.Poll(timeoutDuration)
		if err != nil {
			return nil, fmt.Errorf("poll error: %w", err)
		}

		if len(sockets) > 0 {
			r, err := socket.zmqSocket.RecvMessage(0)
			if err != nil {
				return nil, fmt.Errorf("zmqSocket.RecvMessage: %w", err)
			}

			return r, nil
		}
	}
}

func (socket *Socket) attemptSending(envelope []string) error {
	socket.zmqMu.Lock()
	defer socket.zmqMu.Unlock()

	_, attempt := socket.options()

	for {
		timeout, err := socket.send(envelope)
		if err != nil {
			return fmt.Errorf("socket.rawSend: %w", err)
		}

		if !timeout {
			socket.omitReplyIfPresent()
			break
		}

		attempt--
		if attempt == 0 {
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

	if len(sockets) > 0 {
		if _, err := socket.zmqSocket.SendMessage(envelope); err != nil {
			return false, fmt.Errorf("zmqSocket.SendMessage: %w", err)
		}

		return false, nil
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

func (socket *Socket) Send(req message.RequestInterface) error {
	packer := socket.packer()
	reqStr, err := packer.SerializeRequest(req)
	if err != nil {
		return fmt.Errorf("packer.SerializeRequest: %w", err)
	}

	err = socket.dispatcher.send(reqStr)
	if err != nil {
		return fmt.Errorf("socket.send: %w", err)
	}

	return nil
}

// Request sends the request message to the zmqSocket.
// Returns the message.Reply.Parameters in case of success.
func (socket *Socket) Request(req message.RequestInterface) (message.ReplyInterface, error) {
	packer := socket.packer()
	reqStr, err := packer.SerializeRequest(req)
	if err != nil {
		return nil, fmt.Errorf("packer.SerializeRequest: %w", err)
	}

	rawReply, err := socket.dispatcher.request(reqStr)
	if err != nil {
		return nil, fmt.Errorf("socket.request: %w", err)
	}

	reply, err := packer.DeserializeReply(rawReply)
	if err != nil {
		return nil, fmt.Errorf("packer.DeserializeReply('%v'): %w", rawReply, err)
	}

	return reply, nil
}
