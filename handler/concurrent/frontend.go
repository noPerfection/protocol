// Package frontend forwards incoming messages to the instances and vice versa.
package concurrent

import (
	"fmt"
	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/pair"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"time"
)

const (
	CREATED = "created"
	RUNNING = "running"
)

type Frontend struct {
	external        *zmq.Socket
	sockets         *zmq.Reactor
	id              string // Handler ID
	status          string
	paired          bool
	instanceType    config.HandlerType
	externalConfig  *Config
	queue           *datatype.Queue
	processing      *datatype.List // Tracking messages processed by the instances
	instanceManager *Parent
	consumerId      uint64
	close           bool
}

// New frontend is created
func NewFrontend() *Frontend {
	return &Frontend{
		external:        nil,
		status:          CREATED,
		paired:          false,
		externalConfig:  nil,
		queue:           datatype.NewQueue(),
		processing:      datatype.NewList(),
		instanceManager: nil,
		consumerId:      0,
		close:           false,
	}
}

func (f *Frontend) SetConfig(externalConfig *Config) {
	if f.paired {
		f.instanceType = externalConfig.Type
		paired := pair.Config(externalConfig.Handler)
		f.externalConfig = &Config{
			Handler:        paired,
			InstanceAmount: externalConfig.InstanceAmount,
		}
	} else {
		f.externalConfig = externalConfig
	}
}

func (f *Frontend) SetInstanceManager(manager *Parent) {
	f.instanceManager = manager
}

func (f *Frontend) Status() string {
	return f.status
}

func (f *Frontend) QueueLen() uint {
	return f.queue.Len()
}

func (f *Frontend) ProcessingLen() uint {
	return f.processing.Len()
}

// IsError returns true if the frontend got issue during the running.
func (f *Frontend) IsError() bool {
	return f.status != CREATED && f.status != RUNNING
}

// prepareExternalSocket sets up the external socket.
// the user requests are coming to external socket.
// this socket is bound to the url from externalConfig.
func (f *Frontend) prepareExternalSocket() error {
	if err := f.closeExternal(); err != nil {
		return fmt.Errorf("frontend.closeExternal: %w", err)
	}

	socketType := config.SocketType(f.externalConfig.Type)
	if f.externalConfig.Type == config.SyncReplierType {
		socketType = config.SocketType(config.ReplierType)
	}

	external, err := zmq.NewSocket(socketType)
	if err != nil {
		return fmt.Errorf("zmq.NewSocket(%s): %v", f.externalConfig.Type, err)
	}

	// Keep in memory one message
	// Rather than queueing the messages in the Zeromq, we queue in Frontend
	if err := external.SetRcvhwm(1); err != nil {
		return fmt.Errorf("external.SetRcvhwm(1): %w", err)
	}

	url := f.externalConfig.HandlerUrl()
	err = external.Bind(url)
	if err != nil {
		return fmt.Errorf("external(%s).Bind(%s): %v", f.externalConfig.Type, url, err)
	}
	f.external = external

	return nil
}

// prepareSockets sets up the zeromq's frontend to handle all External, instances in a one thread
func (f *Frontend) prepareSockets() {
	f.sockets = zmq.NewReactor()
}

// startExternalSocket adds the external socket to zeromq frontend to invoke handleExternal when it receives a message.
func (f *Frontend) startExternalSocket() {
	f.sockets.AddSocket(f.external, zmq.POLLIN, func(e zmq.State) error { return f.handleExternal() })
}

// startConsuming adds the consumer to the zeromq frontend to check for queue and send the messages to instances.
// It runs every millisecond, or 1000 times in a second.
func (f *Frontend) startConsuming() {
	f.consumerId = f.sockets.AddChannelTime(time.Tick(time.Millisecond), 0,
		func(_ interface{}) error { return f.handleConsume() })
}

// receiveInstanceMessage makes sure to invoke handleInstance when an instance returns the result.
func (f *Frontend) receiveInstanceMessage(id string, socket *zmq.Socket) {
	f.sockets.AddSocket(socket, zmq.POLLIN, func(e zmq.State) error { return f.handleInstance(id, socket) })
}

// Start the frontend
func (f *Frontend) Start() error {
	if f.externalConfig == nil {
		return fmt.Errorf("no externalConfig. call frontend.SetConfig()")
	}

	// Instance Manager maybe not running. We won't block the frontend from it.
	if f.instanceManager == nil {
		return fmt.Errorf("no instanceManager. call frontend.SetInstanceManager")
	}

	if err := f.prepareExternalSocket(); err != nil {
		return fmt.Errorf("prepareExternalSocket: %w", err)
	}

	f.prepareSockets()
	f.startExternalSocket()
	f.startConsuming()

	f.status = RUNNING
	go func() {
		for {
			// Received a direct close channel
			if f.close {
				break
			}

			// Zeromq's frontend (sockets) sets the poll timeout not as infinite, for two reasons.
			// First, zeromq's frontend requires a timeout if we add a channel into the zeromq.Reactor.
			// The Frontend consumer is channel-based.
			// The second reason is due to external socket.
			// If the external socket receives a message, but the queue is full, then the message will be returned back
			if err := f.sockets.Run(time.Millisecond); err != nil {
				if !f.close {
					f.status = fmt.Sprintf("sockets.Start (mark as closed? %v): %v", f.close, err)
				}
				break // break if marked as close
			}
		}

		// exited due to closing? reset it
		if f.close {
			f.close = false
			f.status = CREATED
		}
	}()
	return nil
}

// handleExternal is an event invoked by the zmq4.Reactor whenever a new client request happens.
//
// This function will forward the messages to the backend.
// Since backend is calling the workers, which means the worker will be busy, this function removes the worker from the queue.
// Since the queue is removed, it will remove the external from the frontend.
// The external socket will still receive the messages, however, they will be queued until external will not be added to the frontend.
func (f *Frontend) handleExternal() error {
	msg, err := f.external.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("handleExternal: external.RecvMessage: %w", err)
	}

	if len(msg) < 3 {
		reply, err := (&message.Request{}).Fail("handleExternal: id, separator in the message are not set").ZmqEnvelope()
		if err != nil {
			return fmt.Errorf("handleExternal: reply.String: %w", err)
		}

		if _, err := f.external.SendMessageDontwait(reply); err != nil {
			return fmt.Errorf("handleExternal: len(msg) < 3: frontend.external.SendMessageDontwait: %w", err)
		}
		return nil
	}

	req, err := message.NewReq(msg[2:])
	if err != nil {
		return fmt.Errorf("handleExternal: message.NewReq: %w", err)
	}

	if f.queue.IsFull() {
		reply, err := req.Fail("queue is full").ZmqEnvelope()

		if err != nil {
			return fmt.Errorf("handleExternal: reply.String: %w", err)
		}
		if _, err := f.external.SendMessageDontwait(msg[0], msg[1], reply); err != nil {
			return fmt.Errorf("handleExternal: frontend.external.SendMessageDontwait: %w", err)
		}
		return nil
	}

	f.queue.Push(msg)

	return nil
}

func (f *Frontend) closeExternal() error {
	if f.external != nil {
		f.sockets.RemoveSocket(f.external)
		if err := f.external.Close(); err != nil {
			return fmt.Errorf("closing already running external socket: %w", err)
		}

		f.external = nil
	}

	return nil
}

// Close stops all sockets, and clear out the queue, processing
func (f *Frontend) Close() error {
	if f.close {
		return nil
	}

	if f.status != RUNNING {
		return nil
	}

	f.close = true // stop zeromq's frontend running

	if f.sockets == nil {
		return nil
	}

	if err := f.closeExternal(); err != nil {
		return fmt.Errorf("frontend.closeExternal: %w", err)
	}

	// remove consumer from zeromq's frontend
	if f.consumerId > 0 {
		f.sockets.RemoveChannel(f.consumerId)
		f.consumerId = 0
	}

	// remove instance sockets from zeromq's frontend
	if !f.processing.IsEmpty() && f.instanceManager != nil {
		list := f.processing.List()
		for raw := range list {
			instanceId := raw.(string)
			sock := f.instanceManager.Handler(instanceId)
			if sock != nil {
				f.sockets.RemoveSocket(sock)
			}
		}

		f.processing = datatype.NewList()
	}

	return nil
}

// PairExternal runs the external socket as the PAIR.
// Requires frontend not be running.
// Requires configuration to be set.
func (f *Frontend) PairExternal() error {
	if f.externalConfig == nil {
		return fmt.Errorf("set the configuration first")
	}
	if f.status == RUNNING {
		return fmt.Errorf("frontend is running")
	}

	f.instanceType = f.externalConfig.Type
	// frontend uses the paired config while paired interface will use external config.
	paired := pair.Config(f.externalConfig.Handler)
	f.externalConfig = &Config{
		Handler:        paired,
		InstanceAmount: f.externalConfig.InstanceAmount,
	}
	f.paired = true

	return nil
}

// Invoked when instances return a response.
// The invoked response returned to back to the external socket.
// The external socket will reply it back to the user.
func (f *Frontend) handleInstance(id string, sock *zmq.Socket) error {
	messages, err := sock.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("instance(%s).socket.ReceiveMessage: %w", id, err)
	}

	messageIdRaw, err := f.processing.Get(id)
	if err != nil {
		return fmt.Errorf("processing.Get(%s): %w", id, err)
	}

	messageId := messageIdRaw.(string)

	if _, err := f.external.SendMessageDontwait(messageId, "", messages); err != nil {
		return fmt.Errorf("external.SendMessageDontWaint(%v): %w", []byte(messageId), err)
	}

	// Delete the instance from the list
	_, err = f.processing.Take(id)
	if err != nil {
		return fmt.Errorf("processing.Take(%s): %w", id, err)
	}

	// avoiding receiving the incoming messages from instances again
	f.sockets.RemoveSocket(sock)

	return nil
}

// handleConsume forwards the messages from queue to the ready instances.
// requires instance manager and zeromq sockets to be set first.
//
// the forwards messages are taken from the queue and added to the processing list.
// it also registers instance in the zeromq frontend, so that Frontend could handle when the instance
// finishes its handling.
func (f *Frontend) handleConsume() error {
	if f.instanceManager == nil {
		return fmt.Errorf("instanceManager not set")
	}
	// Maybe it's still loading
	if f.instanceManager.Status() != Running {
		return nil
	}

	if f.sockets == nil {
		return fmt.Errorf("zmq.Frontend not set")
	}

	if f.queue.IsEmpty() {
		return nil
	}

	id, sock := f.instanceManager.Ready()
	if f.processing.Exist(id) {
		return nil
	}

	if sock == nil {
		return nil
	}

	messages := f.queue.Pop().([]string)
	if err := f.processing.Add(id, messages[0]); err != nil {
		return fmt.Errorf("processing.Add: %w", err)
	}

	if f.instanceType == config.ReplierType {
		if _, err := sock.SendMessageDontwait("", messages[2:]); err != nil {
			return fmt.Errorf("instance.SendMessageDontWait: %w", err)
		}
	} else {
		if _, err := sock.SendMessageDontwait(messages[2:]); err != nil {
			return fmt.Errorf("instance.SendMessageDontWait: %w", err)
		}
	}
	f.receiveInstanceMessage(id, sock)

	return nil
}
