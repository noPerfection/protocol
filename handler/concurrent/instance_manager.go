// Package instance_manager manages the instances
package concurrent

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	clientConfig "github.com/noPerfection/protocol/client/config"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	InstanceCreated = "created" // Instance is created, but not initialized yet. Used for child instances
	Running         = "running"
	Idle            = "idle"
)

type Child struct {
	status        string      // instance status
	managerSocket *zmq.Socket // interact with the instance
	handleSocket  *zmq.Socket // instance's handler
}

type Parent struct {
	instances      map[string]*Child
	eventSock      *zmq.Socket
	MessageOps     *message.Operations
	lastInstanceId uint
	id             string
	parentLogger   *log.Logger
	logger         *log.Logger
	status         string
	close          bool
}

// New instance manager is created with the handler id
func NewInstanceManager(id string, parent *log.Logger) *Parent {
	logger := parent.Child("instance_manager")

	return &Parent{
		id:             id,
		lastInstanceId: 0,
		instances:      make(map[string]*Child, 0),
		parentLogger:   parent,
		logger:         logger,
		eventSock:      nil,
		status:         Idle,
		close:          false,
		MessageOps:     message.DefaultMessage(),
	}
}

// Status of the instance manager.
func (parent *Parent) Status() string {
	return parent.status
}

func (parent *Parent) cleanDeletedInstance(instanceId string) error {
	child, ok := parent.instances[instanceId]
	if !ok {
		return fmt.Errorf("instances[%s] not found", instanceId)
	}

	if child == nil {
		return fmt.Errorf("instances[%s] is null", instanceId)
	}

	err := child.managerSocket.Close()
	delete(parent.instances, instanceId)
	if err != nil {
		return fmt.Errorf("child(%s).managerSocket.Close: %w", instanceId, err)
	}
	err = child.handleSocket.Close()
	if err != nil {
		return fmt.Errorf("child(%s).handleSocket.Close: %w", instanceId, err)
	}

	return nil
}

// onInstanceStatus updates the instance status.
// since our socket is one directional, there is no point to reply.
func (parent *Parent) onInstanceStatus(req message.RequestInterface) message.ReplyInterface {
	instanceId, err := req.RouteParameters().StringValue("id")
	if err != nil {
		return req.Fail(fmt.Sprintf("req.Parameters.GetString('id'): %v", err))
	}

	_, ok := parent.instances[instanceId]
	if !ok {
		return req.Fail(fmt.Sprintf("instances[%s] not found", instanceId))
	}

	status, err := req.RouteParameters().StringValue("status")
	if err != nil {
		return req.Fail(fmt.Sprintf("req.Parameters.GetString('status'): %v", err))
	}

	if status == CLOSED {
		err = parent.cleanDeletedInstance(instanceId)
		if err != nil {
			return req.Fail(fmt.Sprintf("parent.cleanDeletedInstance('%s'): %v", instanceId, err))
		}
		if err := parent.pubInstanceDeleted(instanceId); err != nil {
			parent.logger.Error("parent.pubInstanceDeleted", "instanceId", instanceId, "error", err)
		}

	} else if status == READY {
		if err := parent.pubInstanceAdded(instanceId); err != nil {
			parent.logger.Error("parent.pubInstanceAdded", "instanceId", instanceId, "error", err)
		}
	}

	// Might be deleted when received CLOSE signal
	_, ok = parent.instances[instanceId]
	if ok {
		parent.instances[instanceId].status = status
	}

	return req.Ok(datatype.New())
}

// newPullSocket returns a socket that receives the instance status created by this Parent.
func (parent *Parent) newPullSocket() (*zmq.Socket, error) {
	// This socket is receiving messages from the parents.
	sock, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		err = fmt.Errorf("zmq.NewSocket('PULL'): %w", err)
		if pubErr := parent.pubError(); pubErr != nil {
			err = fmt.Errorf("%w: parent.pubError: %w", err, pubErr)
		}
		return nil, err
	}

	err = sock.Bind(ParentUrl(parent.id))
	if err != nil {
		err = fmt.Errorf("socket('PULL').Bind: %w", err)
		if pubErr := parent.pubError(); pubErr != nil {
			err = fmt.Errorf("%w: parent.pubError: %w", err, pubErr)
		}
		return nil, err
	}

	return sock, nil
}

// SetMessageOperations overwrites the default message type.
func (parent *Parent) SetMessageOperations(messageOps *message.Operations) {
	parent.MessageOps = messageOps
}

// Start the instance manager to receive the data from the instances
// Use the goroutine.
// The operations of the instance manager are not done via socket.
func (parent *Parent) Start() error {
	ready := make(chan error)

	parent.close = false
	if parent.status == Running {
		return fmt.Errorf("instance manager already running")
	}

	go func(ready chan error) {
		eventSock, err := parent.newEventSocket()
		if err != nil {
			ready <- fmt.Errorf("parent.newEventSocket: %w", err)
			return
		}
		parent.eventSock = eventSock

		// This socket is receiving messages from the handler.
		sock, err := parent.newPullSocket()
		if err != nil {
			// failed to create a pull socket, so free the bound endpoint.
			closeErr := eventSock.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: eventSock.Close: %w", err, closeErr)
			}
			ready <- fmt.Errorf("parent.newPullSocket: %w", err)
			return
		}

		if err := parent.pubReady(); err != nil {
			closeErr := eventSock.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: eventSock.Close: %w", err, closeErr)
			}
			closeErr = sock.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: sock.Close: %w", err, closeErr)
			}
			ready <- fmt.Errorf("parent.pubError: %w", err)
			return
		}
		parent.status = Running

		poller := zmq.NewPoller()
		poller.Add(sock, zmq.POLLIN)

		// exit from the parent.Start()
		// any error received from hereafter are set in the parent.status.
		ready <- nil

		for {
			if parent.close {
				break
			}

			sockets, err := poller.Poll(time.Millisecond)
			if err != nil {
				err = fmt.Errorf("poller.Poll: %v", err)
				if pubErr := parent.pubError(); pubErr != nil {
					parent.logger.Error("parent.pubErr", "argument", err, "error", pubErr)
				}
				break
			}

			if len(sockets) == 0 {
				continue
			}

			raw, err := sock.RecvMessage(0)
			if err != nil {
				err = fmt.Errorf("managerSocket.RecvMessage: %v", err)
				if pubErr := parent.pubError(); pubErr != nil {
					parent.logger.Error("parent.pubErr", "argument", err, "error", pubErr)
				}
				break
			}

			req, err := message.NewReq(raw)
			if err != nil {
				err = fmt.Errorf("message.NewRaw(%s): %v", raw, req)
				if pubErr := parent.pubError(); pubErr != nil {
					parent.logger.Error("parent.pubErr", "argument", err, "error", pubErr)
				}
				break
			}

			// Only set_status is supported. If it's not set_status, throw an error.
			if req.CommandName() != "set_status" {
				err = fmt.Errorf("command '%s' not supported", req.CommandName())
				if pubErr := parent.pubError(); pubErr != nil {
					parent.logger.Error("parent.pubErr", "argument", err, "error", pubErr)
				}
				break
			}

			reply := parent.onInstanceStatus(req)
			if !reply.IsOK() {
				err = fmt.Errorf("onInstanceStatus: %s [%v]", reply.ErrorMessage(), req.RouteParameters())
				if pubErr := parent.pubError(); pubErr != nil {
					parent.logger.Error("parent.pubErr", "argument", err, "error", pubErr)
				}
				break
			}
		}

		parent.close = false

		// removing all running instances
		for instanceId, child := range parent.instances {
			if child.status == CLOSED {
				continue
			}
			err := parent.DeleteInstance(instanceId, true)
			if err != nil {
				parent.logger.Error("parent.DeleteInstance", "instanceId", instanceId, "error", err)
			}
			err = parent.cleanDeletedInstance(instanceId)
			if err != nil {
				parent.logger.Error("parent.cleanDeletedInstance", "instanceId", instanceId, "error", err)
			}
		}

		err = poller.RemoveBySocket(sock)
		if err != nil {
			err = fmt.Errorf("poller.RemoveBySocket: %v", err)
			if pubErr := parent.pubError(); pubErr != nil {
				parent.logger.Error("parent.pubErr", "argument", err, "error", pubErr)
			}
			return
		}

		err = sock.Close()
		if err != nil {
			err = fmt.Errorf("managerSocket.Close: %v", err)
			if pubErr := parent.pubError(); pubErr != nil {
				parent.logger.Error("parent.pubErr", "argument", err, "error", pubErr)
			}
			return
		}

		parent.status = Idle
		if err := parent.pubIdle(true); err != nil {
			parent.logger.Error("parent.pubIdle", "closeSignal", true, "error", err)
		}

		err = eventSock.Close()
		if err != nil {
			err = fmt.Errorf("eventSock.Close: %v", err)
			if pubErr := parent.pubError(); pubErr != nil {
				parent.logger.Error("parent.pubErr", "argument", err, "error", pubErr)
			}
			return
		}
	}(ready)

	return <-ready
}

// Close the instance manager.
// It deletes all instances.
// The reason why it doesn't return an error is because
// it broadcasts them through pub socket.
// Also, it needs a time for closing the instance manager sockets.
func (parent *Parent) Close() {
	parent.close = true

	err := parent.pubClose()
	if err != nil {
		parent.logger.Error("parent.pubClose", "error", err)
	}
}

func (parent *Parent) NewInstanceId() string {
	instanceNum := parent.lastInstanceId + 1
	return fmt.Sprintf("instance_%d", instanceNum)
}

// Handler socket of the instance.
//
// Don't do any operations on the socket, as it could lead to the unexpected behaviors of the instance manager.
func (parent *Parent) Handler(instanceId string) *zmq.Socket {
	child, ok := parent.instances[instanceId]
	if child == nil || !ok || child.handleSocket == nil {
		return nil
	}

	return child.handleSocket
}

// AddInstance to the handler.
// Returns generated instance id and error.
// Returns error if instance manager is not running.
// Returns error if instance client socket creation fails.
func (parent *Parent) AddInstance(handlerType config.HandlerType, router base.Router) (string, error) {
	if parent.Status() != Running {
		return "", fmt.Errorf("instance_manager is not running. unexpected status: %s", parent.Status())
	}

	id := parent.NewInstanceId()

	added := NewInstance(handlerType, id, parent.id, parent.parentLogger)
	added.SetRouter(router)
	added.SetMessageOps(parent.MessageOps)

	childSock, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return id, fmt.Errorf("new childSocket(%s): %v", id, err)
	}
	err = childSock.Connect(InstanceUrl(parent.id, id))
	if err != nil {
		return id, fmt.Errorf("connect childSocket(%s): %v", id, err)
	}

	handleSock, err := zmq.NewSocket(clientConfig.TargetToClient(config.SocketType(handlerType)))
	if err != nil {
		return "", fmt.Errorf("new handleSocket(%s): %v", id, err)
	}
	err = handleSock.Connect(InstanceHandleUrl(parent.id, id))
	if err != nil {
		return "", fmt.Errorf("connect handleSocket(%s): %v", id, err)
	}

	parent.instances[id] = &Child{
		status:        InstanceCreated,
		managerSocket: childSock,
		handleSocket:  handleSock,
	}
	err = added.Start()
	if err != nil {
		return "", fmt.Errorf("instance['%s'].Start: %w", id, err)
	}

	parent.lastInstanceId++

	return id, nil
}

// DeleteInstance closes the instance.
// Sends the signal to the instance thread.
// Instance sends back to instance manager a status update.
//
// instanceId must be registered in the instance manager.
//
// If instant is set true, then instance is closed without replying back
func (parent *Parent) DeleteInstance(instanceId string, instant bool) error {
	child, ok := parent.instances[instanceId]
	if !ok {
		return fmt.Errorf("instance[%s] not found", instanceId)
	}
	if child == nil {
		return fmt.Errorf("instance[%s] is null", instanceId)
	}
	if child.status == CLOSED {
		return fmt.Errorf("instance[%s] is closed", instanceId)
	}

	req := message.Request{
		Command:    "close",
		Parameters: datatype.New().Set("instant", instant),
	}
	reqStr, err := req.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("req.String: %w", err)
	}

	_, err = child.managerSocket.SendMessage(reqStr)
	if err != nil {
		return fmt.Errorf("child(%s).SendMessage(%s): %w", instanceId, reqStr, err)
	}

	if instant {
		return nil
	}
	replyStr, err := child.managerSocket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("child(%s).RecvMessage: %w", instanceId, err)
	}
	reply, err := message.NewRep(replyStr)
	if err != nil {
		return fmt.Errorf("parseReply(%s): %w", replyStr, err)
	}

	if !reply.IsOK() {
		return fmt.Errorf("instance close failed: %s", reply.ErrorMessage())
	}

	return nil
}

// Instances returns all instances as instance_id => instance_status
func (parent *Parent) Instances() map[string]string {
	instances := make(map[string]string, len(parent.instances))

	for id, child := range parent.instances {
		instances[id] = child.status
	}

	return instances
}

// Ready returns an instance that's ready to handle requests
func (parent *Parent) Ready() (string, *zmq.Socket) {
	for id, child := range parent.instances {
		if child.status == READY {
			return id, child.handleSocket
		}
	}

	return "", nil
}
