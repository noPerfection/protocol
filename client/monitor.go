package client

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

var monitorCounter uint64

// attachMonitor creates a ZMQ socket monitor on zmqSock and returns a PAIR socket
// connected to it. Must be called before zmqSock.Connect().
func attachMonitor(zmqSock *zmq.Socket) (*zmq.Socket, error) {
	addr := fmt.Sprintf("inproc://client-monitor-%d", atomic.AddUint64(&monitorCounter, 1))

	// Monitor connection lifecycle and all handshake outcomes.
	// EVENT_CONNECTED + EVENT_DISCONNECTED (without HANDSHAKE_SUCCEEDED) is how
	// a plain client hitting a CURVE server appears — the server drops the
	// connection without sending explicit HANDSHAKE_FAILED_* events.
	events := zmq.EVENT_CONNECTED |
		zmq.EVENT_DISCONNECTED |
		zmq.EVENT_HANDSHAKE_SUCCEEDED |
		zmq.EVENT_HANDSHAKE_FAILED_AUTH |
		zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL |
		zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL

	if err := zmqSock.Monitor(addr, events); err != nil {
		return nil, fmt.Errorf("zmqSock.Monitor: %w", err)
	}

	mon, err := zmq.NewSocket(zmq.PAIR)
	if err != nil {
		return nil, fmt.Errorf("zmq.NewSocket(PAIR): %w", err)
	}

	if err := mon.Connect(addr); err != nil {
		_ = mon.Close()
		return nil, fmt.Errorf("monitor.Connect(%s): %w", addr, err)
	}

	return mon, nil
}

// drainMonitor reads all pending events from mon (non-blocking) and returns
// ErrNoCurveKey when an auth/handshake failure is detected.
//
// Two failure patterns are recognised:
//  1. Explicit: EVENT_HANDSHAKE_FAILED_{AUTH,NO_DETAIL,PROTOCOL}
//  2. Implicit: EVENT_CONNECTED followed by EVENT_DISCONNECTED without an
//     intervening EVENT_HANDSHAKE_SUCCEEDED — this is what a plain client sees
//     when the server silently drops the connection after rejecting the CURVE
//     greeting.
func drainMonitor(mon *zmq.Socket) error {
	connected := false
	succeeded := false

	for {
		event, _, _, err := mon.RecvEvent(zmq.DONTWAIT)
		if err != nil {
			// EAGAIN — no more events queued
			break
		}
		switch event {
		case zmq.EVENT_CONNECTED:
			connected = true
		case zmq.EVENT_HANDSHAKE_SUCCEEDED:
			succeeded = true
		case zmq.EVENT_DISCONNECTED:
			if connected && !succeeded {
				return fmt.Errorf("%w: connected then disconnected without handshake", message.ErrNoCurveKey)
			}
		case zmq.EVENT_HANDSHAKE_FAILED_AUTH,
			zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL,
			zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL:
			return fmt.Errorf("%w: zmq handshake event %v", message.ErrNoCurveKey, event)
		}
	}

	return nil
}

// waitMonitorHandshake blocks until CURVE handshake succeeds or timeout.
func waitMonitorHandshake(mon *zmq.Socket, poller *zmq.Poller, timeout time.Duration) error {
	if mon == nil {
		return nil
	}

	deadline := time.Now().Add(timeout)
	connected := false

	for time.Now().Before(deadline) {
		for {
			event, _, _, err := mon.RecvEvent(zmq.DONTWAIT)
			if err != nil {
				break
			}
			switch event {
			case zmq.EVENT_CONNECTED:
				connected = true
			case zmq.EVENT_HANDSHAKE_SUCCEEDED:
				return nil
			case zmq.EVENT_DISCONNECTED:
				if connected {
					return fmt.Errorf("%w: connected then disconnected without handshake", message.ErrNoCurveKey)
				}
			case zmq.EVENT_HANDSHAKE_FAILED_AUTH,
				zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL,
				zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL:
				return fmt.Errorf("%w: zmq handshake event %v", message.ErrNoCurveKey, event)
			}
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if _, err := poller.Poll(remaining); err != nil {
			return fmt.Errorf("poll error: %w", err)
		}
	}

	return fmt.Errorf("send-timeout waiting for handshake")
}

// monitorAuthErr drains pending monitor events and reports handshake failures.
func (socket *Socket) monitorAuthErr() error {
	if socket.monitorSocket == nil {
		return nil
	}
	return drainMonitor(socket.monitorSocket)
}
