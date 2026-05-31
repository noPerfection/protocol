package client

import (
	"sync"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const defaultReceiveBuffer = 64

type receiver struct {
	socket    *Socket
	replies   chan message.ReplyInterface
	active    bool
	closeOnce sync.Once
}

func newReceiver(socket *Socket) *receiver {
	return &receiver{
		socket:  socket,
		replies: make(chan message.ReplyInterface, defaultReceiveBuffer),
	}
}

func supportsReceive(handlerType HandlerType) bool {
	return handlerType == ReplierType ||
		handlerType == PairType ||
		handlerType == PublisherType
}

func (r *receiver) pollOnce() {
	r.socket.zmqMu.Lock()
	defer r.socket.zmqMu.Unlock()

	if r.socket.isClosed() || !r.isActive() {
		return
	}

	if r.socket.zmqSocket == nil {
		if err := r.socket.reconnect(); err != nil {
			return
		}
	}

	r.socket.updateToPollIn()

	sockets, err := r.socket.poller.Poll(0)
	if err != nil || len(sockets) == 0 {
		return
	}

	if r.socket.zmqSocket == nil {
		return
	}

	raw, err := r.socket.zmqSocket.RecvMessage(0)
	if err != nil {
		return
	}

	packer := r.socket.packer()
	reply, err := packer.DeserializeReply(raw)
	if err != nil {
		return
	}

	select {
	case r.replies <- reply:
	default:
	}
}

func (r *receiver) activate() {
	r.socket.mu.Lock()
	defer r.socket.mu.Unlock()

	r.active = true
}

func (r *receiver) isActive() bool {
	r.socket.mu.Lock()
	defer r.socket.mu.Unlock()

	return r.active
}

func (r *receiver) close() {
	r.closeOnce.Do(func() {
		close(r.replies)
	})
}

// Receive returns a channel of inbound handler replies.
// The channel is closed when the client socket is closed.
func (socket *Socket) Receive() <-chan message.ReplyInterface {
	if socket.receiver == nil {
		ch := make(chan message.ReplyInterface)
		close(ch)
		return ch
	}
	socket.receiver.activate()
	return socket.receiver.replies
}

func (socket *Socket) packer() message.Packer {
	socket.mu.Lock()
	defer socket.mu.Unlock()

	return socket.messagePacker
}

func (socket *Socket) afterReconnect(socketType zmq.Type) {
	if socketType == zmq.SUB && socket.receiver != nil {
		_ = socket.zmqSocket.SetSubscribe("")
	}
}
