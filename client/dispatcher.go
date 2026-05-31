package client

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
	zmq "github.com/pebbe/zmq4"
)

// Transmit is one queued client operation processed by the dispatcher.
type Transmit struct {
	replyMsg   chan []string
	delayedErr chan error
	envelope   []string
}

type dispatcher struct {
	socket     *Socket
	queue      *datatype.Queue
	schedulers *zmq.Reactor
	consumerId uint64
	wg         sync.WaitGroup
}

func newDispatcher(socket *Socket) *dispatcher {
	d := &dispatcher{
		socket:     socket,
		queue:      datatype.NewQueue(),
		schedulers: zmq.NewReactor(),
	}

	d.consumerId = d.schedulers.AddChannelTime(time.Tick(time.Microsecond), 0,
		func(_ interface{}) error { return d.handleConsume() })

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		err := d.schedulers.Run(time.Microsecond * 2)
		if err != nil &&
			err.Error() != "No sockets to poll, no channels to read" &&
			err.Error() != "client socket closed" {
			_, _ = fmt.Fprintf(os.Stderr, "reactor exited with an error: %v\n", err)
		}
	}()

	return d
}

func (d *dispatcher) stop() {
	// Wait for handleConsume to observe socket.closed and exit Run.
	// Do not call Reactor.RemoveChannel from this goroutine; the reactor is not thread-safe.
	d.wg.Wait()
}

func (d *dispatcher) handleConsume() error {
	if d.socket.isClosed() {
		return fmt.Errorf("client socket closed")
	}

	msg := d.popTransmit()
	if msg == nil {
		if recv := d.socket.receiver; recv != nil {
			recv.pollOnce()
		}
		return nil
	}

	if msg.replyMsg == nil {
		err := d.socket.attemptSending(msg.envelope)
		if err != nil {
			msg.delayedErr <- fmt.Errorf("socket.rawSendByTimeout: %w", err)
		} else {
			msg.delayedErr <- nil
		}
		return nil
	}

	reply, err := d.socket.attemptRequesting(msg.envelope)
	if err != nil {
		msg.delayedErr <- err
		return nil
	}

	msg.delayedErr <- nil
	msg.replyMsg <- reply

	return nil
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
