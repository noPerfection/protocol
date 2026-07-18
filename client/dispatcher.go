package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
)

const receiverPollInterval = 50 * time.Millisecond

// Transmit is one queued client operation processed by the dispatcher.
type Transmit struct {
	replyMsg   chan []string
	delayedErr chan error
	envelope   []string
}

type dispatcher struct {
	socket *Socket
	queue  *datatype.Queue
	wake   chan struct{}
	wg     sync.WaitGroup
}

func newDispatcher(socket *Socket) *dispatcher {
	d := &dispatcher{
		socket: socket,
		queue:  datatype.NewQueue(),
		wake:   make(chan struct{}, 1),
	}

	d.wg.Add(1)
	go d.runLoop()

	return d
}

func (d *dispatcher) signalWake() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *dispatcher) stop() {
	d.signalWake()
	d.wg.Wait()
}

func (d *dispatcher) runLoop() {
	defer d.wg.Done()

	receiverTick := time.NewTicker(receiverPollInterval)
	defer receiverTick.Stop()

	for {
		if d.socket.isClosed() {
			return
		}

		if err := d.drainQueue(); err != nil {
			return
		}

		select {
		case <-d.wake:
		case <-receiverTick.C:
			if recv := d.socket.receiver; recv != nil && recv.isActive() {
				recv.pollOnce()
			}
		}
	}
}

func (d *dispatcher) drainQueue() error {
	for {
		if d.socket.isClosed() {
			return fmt.Errorf("client socket closed")
		}

		msg := d.popTransmit()
		if msg == nil {
			return nil
		}

		if msg.replyMsg == nil {
			err := d.socket.attemptSending(msg.envelope)
			if err != nil {
				msg.delayedErr <- fmt.Errorf("socket.rawSendByTimeout: %w", err)
			} else {
				msg.delayedErr <- nil
			}
			continue
		}

		reply, err := d.socket.attemptRequesting(msg.envelope)
		if err != nil {
			msg.delayedErr <- err
			continue
		}

		msg.delayedErr <- nil
		msg.replyMsg <- reply
	}
}

func (d *dispatcher) enqueueTransmit(msg *Transmit) error {
	d.socket.mu.Lock()
	defer d.socket.mu.Unlock()

	if d.socket.closed {
		return fmt.Errorf("client socket closed")
	}
	if d.queue.IsFull() {
		return fmt.Errorf("queue is full, try again later")
	}

	d.queue.Push(msg)
	d.signalWake()
	return nil
}

func (d *dispatcher) popTransmit() *Transmit {
	d.socket.mu.Lock()
	defer d.socket.mu.Unlock()

	if d.queue.IsEmpty() {
		return nil
	}

	return d.queue.Pop().(*Transmit)
}

func (d *dispatcher) request(envelope []string) ([]string, error) {
	msg := &Transmit{
		replyMsg:   make(chan []string),
		delayedErr: make(chan error),
		envelope:   envelope,
	}
	if err := d.enqueueTransmit(msg); err != nil {
		return nil, err
	}

	err := <-msg.delayedErr
	if err != nil {
		return nil, err
	}

	reply := <-msg.replyMsg
	return reply, nil
}

func (d *dispatcher) send(envelope []string) error {
	msg := &Transmit{
		replyMsg:   nil,
		delayedErr: make(chan error),
		envelope:   envelope,
	}
	if err := d.enqueueTransmit(msg); err != nil {
		return err
	}

	return <-msg.delayedErr
}
