package handler

import (
	"fmt"
	"sync/atomic"

	zmq "github.com/pebbe/zmq4"
)

// blockForever tells zmq.Poller to wait until a socket event arrives.
const blockForever = -1

var wakeCounter uint64

// interruptSocket closes socket with zero linger so a blocked Poll(-1) returns.
func interruptSocket(socket *zmq.Socket) {
	if socket == nil {
		return
	}
	_ = socket.SetLinger(0)
	_ = socket.Close()
}

// takeAndCloseSocket clears *socket and closes it to unblock a receive loop.
func takeAndCloseSocket(socket **zmq.Socket) {
	if socket == nil || *socket == nil {
		return
	}
	s := *socket
	*socket = nil
	interruptSocket(s)
}

// wakePipe wakes a blocked Poll via an inproc PUSH/PULL pair.
type wakePipe struct {
	pull *zmq.Socket
	push *zmq.Socket
}

func newWakePipe() (*wakePipe, error) {
	endpoint := fmt.Sprintf("inproc://handler-wake-%d", atomic.AddUint64(&wakeCounter, 1))

	pull, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		return nil, fmt.Errorf("wakePipe pull: %w", err)
	}
	push, err := zmq.NewSocket(zmq.PUSH)
	if err != nil {
		_ = pull.Close()
		return nil, fmt.Errorf("wakePipe push: %w", err)
	}

	if err := pull.Bind(endpoint); err != nil {
		_ = pull.Close()
		_ = push.Close()
		return nil, fmt.Errorf("wakePipe bind: %w", err)
	}
	if err := push.Connect(endpoint); err != nil {
		_ = pull.Close()
		_ = push.Close()
		return nil, fmt.Errorf("wakePipe connect: %w", err)
	}

	return &wakePipe{pull: pull, push: push}, nil
}

func (w *wakePipe) addToPoller(poller *zmq.Poller) {
	poller.Add(w.pull, zmq.POLLIN)
}

func (w *wakePipe) signal() {
	_, _ = w.push.SendBytes([]byte{0}, zmq.DONTWAIT)
}

func (w *wakePipe) drain() {
	for {
		_, err := w.pull.RecvBytes(zmq.DONTWAIT)
		if err != nil {
			return
		}
	}
}

func (w *wakePipe) close() {
	if w.push != nil {
		_ = w.push.Close()
		w.push = nil
	}
	if w.pull != nil {
		_ = w.pull.Close()
		w.pull = nil
	}
}

func isWakePoll(wake *wakePipe, polled zmq.Polled) bool {
	return wake != nil && polled.Socket == wake.pull
}
