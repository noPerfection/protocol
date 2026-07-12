package client

import (
	"errors"
	"sync"
	"time"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const defaultReceiveBuffer = 64

// It adds support of receiving message from handlers continuasly:
// useful for subscribing messages for example, to do it asynchronously.
type receiver struct {
	socket    *Socket
	replies   chan message.ReplyInterface
	active    bool
	timeouts  uint8
	nextRetry time.Time
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
			r.markIdle()
			return
		}
	}

	r.socket.updateToPollIn()

	sockets, err := r.socket.poller.Poll(0)
	if err != nil {
		r.markIdle()
		return
	}

	if authErr := r.socket.monitorAuthErr(); authErr != nil {
		if errors.Is(authErr, message.ErrNoCurveKey) && r.recoverNoCurveKey() {
			return
		}
		r.markIdle()
		return
	}

	if len(sockets) == 0 {
		r.markIdle()
		return
	}

	if r.socket.zmqSocket == nil {
		r.markIdle()
		return
	}

	raw, err := r.socket.zmqSocket.RecvMessage(0)
	if err != nil {
		r.markIdle()
		return
	}

	reply, replyHmac, err := r.socket.messagePacker.DeserializeReply(raw)
	if err != nil {
		r.markIdle()
		return
	}

	if err := r.socket.validateReplyAny(reply, replyHmac); err != nil {
		r.markIdle()
		return
	}

	r.markReceived()
	select {
	case r.replies <- reply:
	default:
	}
}

// recoverNoCurveKey looks up the handler's CURVE public key from npac, applies
// it with Secure, and reconnects once. Returns false when recovery is skipped
// or fails.
func (r *receiver) recoverNoCurveKey() bool {
	r.socket.mu.Lock()
	alreadySecure := r.socket.serverPublicKey != ""
	r.socket.mu.Unlock()
	if alreadySecure {
		return false
	}

	autocontext := NewAutocontext()
	if autocontext == nil {
		return false
	}
	defer func() { _ = autocontext.Close() }()

	unregistered, publicKey, _, err := autocontext.HandlerContext(r.socket.endpoint, message.Any)
	if err != nil || unregistered || publicKey == "" {
		return false
	}

	r.socket.Secure(publicKey)
	return r.socket.reconnect() == nil
}

func (r *receiver) activate() {
	timeout, _ := r.socket.options()

	r.socket.mu.Lock()
	defer r.socket.mu.Unlock()

	r.active = true
	r.timeouts = 0
	r.nextRetry = time.Now().Add(timeout)
}

func (r *receiver) isActive() bool {
	r.socket.mu.Lock()
	defer r.socket.mu.Unlock()

	return r.active
}

func (r *receiver) close() {
	r.socket.mu.Lock()
	r.active = false
	r.socket.mu.Unlock()

	r.closeOnce.Do(func() {
		close(r.replies)
	})
}

func (r *receiver) markIdle() {
	timeout, maxAttempt := r.socket.options()
	now := time.Now()
	shouldClose := false

	r.socket.mu.Lock()
	if r.active && !now.Before(r.nextRetry) {
		r.timeouts++
		r.nextRetry = now.Add(timeout)
		shouldClose = !infiniteAttempts(maxAttempt) && r.timeouts >= maxAttempt
	}
	r.socket.mu.Unlock()

	if shouldClose {
		r.close()
	}
}

func (r *receiver) markReceived() {
	timeout, _ := r.socket.options()

	r.socket.mu.Lock()
	defer r.socket.mu.Unlock()

	r.timeouts = 0
	r.nextRetry = time.Now().Add(timeout)
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

func (socket *Socket) afterReconnect(socketType zmq.Type) {
	if socketType == zmq.SUB && socket.receiver != nil {
		_ = socket.zmqSocket.SetSubscribe("")
	}
}
