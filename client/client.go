// Package client defines client zmqSocket that can access to the client service.
package client

import (
	"fmt"
	"os"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"

	zmq "github.com/pebbe/zmq4"
)

const (
	minTimeout     = time.Millisecond * 2
	DefaultTimeout = time.Second * 100

	minAttempt     = uint8(1)
	DefaultAttempt = uint8(5)
)

type Transmit struct {
	replyMsg   chan []string
	delayedErr chan error
	reqMsg     string
}

type Option struct {
	timeout time.Duration
	attempt uint8
}

// A Socket is the structure that transmits the data to the handlers.
type Socket struct {
	consumerId    uint64       // consume internal id assigned by zeromq
	poller        *zmq.Poller  // to receive messages
	schedulers    *zmq.Reactor // client keeps a zmqSocket for initialing a message transfer, and a queue consumer.
	zmqSocket     *zmq.Socket
	timeout       time.Duration
	attempt       uint8
	endpoint      message.Endpoint
	handlerType   HandlerType
	queue         *datatype.Queue
	sent          uint64         // used for push or pair for for msg counting.
	messagePacker message.Packer // client translates the message before and after transmitting.
}

func newSocket(handlerType HandlerType, endpoint message.Endpoint) *Socket {
	socket := &Socket{
		zmqSocket:     nil,
		timeout:       DefaultTimeout,
		attempt:       DefaultAttempt,
		endpoint:      endpoint,
		handlerType:   handlerType,
		queue:         datatype.NewQueue(),
		schedulers:    zmq.NewReactor(),
		consumerId:    0,
		sent:          1,
		messagePacker: &message.MessagePacker{},
	}

	// we can remove the following lines
	socket.consumerId = socket.schedulers.AddChannelTime(time.Tick(time.Microsecond), 0,
		func(_ interface{}) error { return socket.handleConsume() })

	go func() {
		err := socket.schedulers.Run(time.Microsecond * 2)
		if err != nil && err.Error() != "No sockets to poll, no channels to read" {
			_, _ = fmt.Fprintf(os.Stderr, "reactor exited with an error: %v\n", err)
		}
		socket.schedulers = nil
	}()

	return socket
}

// New creates a client for the given handler endpoint. Client type is determined by the target handler.
func New(id string, port uint64, handlerTargetType HandlerType) (*Socket, error) {
	if !isTarget(handlerTargetType) {
		return nil, fmt.Errorf("target is not supported")
	}

	endpoint := message.NewEndpoint(id, port)
	return newSocket(handlerTargetType, endpoint), nil
}

// Sets the message serializer and deserializer.
// Use the same as used by the handler.
func (socket *Socket) Packer(packer message.Packer) *Socket {
	socket.messagePacker = packer
	return socket
}

// handleConsume runs in a loop to read the queue.
// For the given queue, it will send the message to the handler.
func (socket *Socket) handleConsume() error {
	if socket.queue.IsEmpty() {
		return nil
	}

	msg := socket.queue.Pop().(*Transmit)

	// Send, not a reply message.
	if msg.replyMsg == nil {
		err := socket.rawSendByTimeout(msg.reqMsg)
		if err != nil {
			msg.delayedErr <- fmt.Errorf("socket.rawSendByTimeout: %w", err)
		} else {
			msg.delayedErr <- nil
		}
		return nil
	}

	reply, err := socket.rawRequestByTimeout(msg.reqMsg)
	if err != nil {
		msg.delayedErr <- err
		return nil
	}

	msg.delayedErr <- nil
	msg.replyMsg <- reply

	return nil
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
	if socket.zmqSocket == nil {
		return nil
	}
	err := socket.zmqSocket.Close()
	if err != nil {
		return fmt.Errorf("error closing zmqSocket: %w", err)
	}

	if socket.schedulers != nil {
		socket.schedulers.RemoveChannel(socket.consumerId)
	}

	return nil
}

// Timeout update. If the timeout is less than minTimeout, then minTimeout is set
func (socket *Socket) Timeout(timeout time.Duration) *Socket {
	if timeout < minTimeout {
		timeout = minTimeout
	}

	socket.timeout = timeout
	return socket
}

// Attempt update. If the attempt is less than minAttempt, then minAttempt is set
func (socket *Socket) Attempt(attempt uint8) *Socket {
	if attempt < minAttempt {
		attempt = minAttempt
	}

	socket.attempt = attempt
	return socket
}

func (socket *Socket) request(raw string) ([]string, error) {
	if socket.queue.IsFull() {
		return nil, fmt.Errorf("queue is full, try again later")
	}

	// todo, a message channel must return the error as well
	// todo, rename Transmit.reply a type to Reply.
	msg := &Transmit{
		replyMsg:   make(chan []string),
		delayedErr: make(chan error),
		reqMsg:     raw,
	}
	socket.queue.Push(msg)

	err := <-msg.delayedErr

	if err != nil {
		return nil, err
	}

	reply := <-msg.replyMsg
	return reply, nil
}

func (socket *Socket) rawRequestByTimeout(raw string) ([]string, error) {
	// Since we decrement before an attempt, it will be 0
	// If we had 1 attempt.
	attempt := socket.attempt + 1

	for {
		attempt--
		if attempt == 0 {
			return nil, fmt.Errorf("request_timeout: reqMsg='%s'", raw)
		}

		timeout, err := socket.rawSend(raw)
		if err != nil {
			return nil, fmt.Errorf("socket.rawSend: %w", err)
		}

		if timeout {
			continue
		}

		socket.updateToPollIn()

		// Poll zmqSocket for a reply, with timeout
		sockets, err := socket.poller.Poll(socket.timeout)
		if err != nil {
			return nil, fmt.Errorf("poll error: %w", err)
		}

		if len(sockets) > 0 {
			// Wait for a reply.
			r, err := socket.zmqSocket.RecvMessage(0)
			if err != nil {
				return nil, fmt.Errorf("zmqSocket.RecvMessage: %w", err)
			}

			return r, nil
		}
	}
}

func (socket *Socket) send(raw string) error {
	if socket.queue.IsFull() {
		return fmt.Errorf("queue is full, try again later")
	}

	msg := &Transmit{
		replyMsg:   nil,
		delayedErr: make(chan error),
		reqMsg:     raw,
	}
	socket.queue.Push(msg)

	err := <-msg.delayedErr

	return err
}

func (socket *Socket) rawSendByTimeout(raw string) error {
	attempt := socket.attempt

	for {
		timeout, err := socket.rawSend(raw)
		if err != nil {
			return fmt.Errorf("socket.rawSend: %w", err)
		}

		if !timeout {
			socket.omitReplyIfPresent()
			break
		}

		attempt--
		if attempt == 0 {
			return fmt.Errorf("send_timeout: reqMsg='%s'", raw)
		}
	}
	return nil
}

// rawSend sends the message; it doesn't wait for a reply to see was it successfully sent.
//
// returns boolean for timeout.
func (socket *Socket) rawSend(raw string) (bool, error) {
	// no need to reconnect every time.
	err := socket.reconnect()
	if err != nil {
		return false, fmt.Errorf("initial  socket.reconnect: %w", err)
	}

	socket.pollOut()

	messages := []string{raw}
	socketType, err := socket.zmqSocket.GetType()
	if err != nil {
		return false, fmt.Errorf("zmqSocket.GetType: %w", err)
	}

	if socketType == zmq.DEALER {
		messages = []string{"", raw}
	} else if socketType == zmq.PAIR || socketType == zmq.PUSH {
		messages = []string{fmt.Sprintf("%d", socket.sent), "", raw}
		socket.sent++
	}

	// Poll zmqSocket for a reply, with timeout
	sockets, err := socket.poller.Poll(socket.timeout)
	if err != nil {
		return false, fmt.Errorf("poll error: %w", err)
	}

	if len(sockets) > 0 {
		if _, err := socket.zmqSocket.SendMessage(messages); err != nil {
			return false, fmt.Errorf("zmqSocket.SendMessage: %w", err)
		}

		return false, nil
	}

	return true, nil
}

// omitReplyIfPresent drops an inbound reply so REQ submit can complete.
func (socket *Socket) omitReplyIfPresent() {
	socketType, err := socket.zmqSocket.GetType()
	if err != nil || socketType != zmq.REQ {
		return
	}

	socket.updateToPollIn()

	drain := socket.timeout
	if drain > time.Second {
		drain = time.Second
	}

	sockets, err := socket.poller.Poll(drain)
	if err != nil || len(sockets) == 0 {
		return
	}

	_, _ = socket.zmqSocket.RecvMessage(0)
}

//
// The client that works with the message.RequestInterface and message.ReplyInterface
//

func (socket *Socket) Send(req message.RequestInterface) error {
	reqStr, err := socket.messagePacker.SerializeRequest(req)
	if err != nil {
		return fmt.Errorf("packer.SerializeRequest: %w", err)
	}

	_, reqMsg, _ := message.EnvelopeToMessage(reqStr)
	err = socket.send(reqMsg)
	if err != nil {
		return fmt.Errorf("socket.send: %w", err)
	}

	return nil
}

// Request sends the request message to the zmqSocket.
// Returns the message.Reply.Parameters in case of success.
//
// Error is returned in other cases.
//
// If the client service returned a failure message, it's converted into an error.
//
// The zmqSocket type should be REQ or PUSH.
func (socket *Socket) Request(req message.RequestInterface) (message.ReplyInterface, error) {
	reqStr, err := socket.messagePacker.SerializeRequest(req)
	if err != nil {
		return nil, fmt.Errorf("packer.SerializeRequest: %w", err)
	}

	_, reqMsg, _ := message.EnvelopeToMessage(reqStr)
	rawReply, err := socket.request(reqMsg)
	if err != nil {
		return nil, fmt.Errorf("socket.request: %w", err)
	}

	reply, err := socket.messagePacker.DeserializeReply(rawReply)
	if err != nil {
		return nil, fmt.Errorf("packer.DeserializeReply('%v'): %w", rawReply, err)
	}

	return reply, nil
}
