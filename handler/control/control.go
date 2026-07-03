// Package control creates a socket that controls the handler.
package control

import (
	"fmt"
	"strconv"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	HandlerStatus   = "status" // Returns the handler status
	HandlerStart    = "start"  // Starts the handler
	HandlerClose    = "close"  // Closes the handler
	HandlerConfig   = "config" // Returns the handler configuration
	ControlCategory = "control"
)

// Manager is the control ROUTER socket for a handler.
type Manager struct {
	*base.Handler
	socket *zmq.Socket
	status string
}

var _ base.Interface = (*Manager)(nil)

// New creates a control handler.
func New(parent ...*log.Logger) *Manager {
	return &Manager{
		Handler: base.New(parent...),
		status:  base.SocketNil,
	}
}

// NewInternalControlEndpoint derives the control endpoint from a handler endpoint.
func NewInternalControlEndpoint(handlerEndpoint message.Endpoint) message.Endpoint {
	handlerEndpoint.Id = handlerEndpoint.Id + strconv.FormatUint(handlerEndpoint.Port, 10) + "_control"
	return handlerEndpoint
}

// Converts the handler endpoint into a control endpoint and stores it.
func (m *Manager) SetEndpoint(handlerEndpoint message.Endpoint) {
	m.Handler.SetEndpoint(NewInternalControlEndpoint(handlerEndpoint))
}

func (m *Manager) Status() string {
	return m.status
}

// Running returns true while the handler socket is ready to serve.
func (m *Manager) Running() bool {
	return m.status == base.SocketReady
}

func (m *Manager) SetSocketIdle() {
	m.status = base.SocketIdle
}

func (m *Manager) SetSocketReady() {
	m.status = base.SocketReady
}

func (m *Manager) SetSocketNil() {
	m.status = base.SocketNil
}

func (m *Manager) onBuiltinStatus(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("status", m.Status()))
}

// Start binds the control ROUTER socket and serves registered routes.
func (m *Manager) Start() error {
	if m.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("no config")
	}

	m.Route(HandlerStatus, m.onBuiltinStatus)

	ready := make(chan error)

	go func(ready chan error) {
		socket, err := zmq.NewSocket(zmq.REP)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket: %w", err)
			return
		}

		url := m.Endpoint().HandlerUrl()
		if err := socket.Bind(url); err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("socket.Bind('%s'): %w", url, err)
			return
		}

		m.socket = socket

		poller := zmq.NewPoller()
		poller.Add(socket, zmq.POLLIN)

		m.SetSocketReady()

		ready <- nil

		for {
			sockets, err := poller.Poll(time.Millisecond)
			if err != nil {
				m.LogError("poller.Poll", "error", err)
				break
			}

			if len(sockets) == 0 {
				continue
			}

			raw, err := socket.RecvMessage(0)
			if err != nil {
				m.LogError("socket.RecvMessage", "error", err)
				break
			}

			req, err := m.Packer().DeserializeRequest(raw)
			if err != nil {
				m.LogError("Packer().DeserializeRequest", "messages", raw, "error", err)
				continue
			}

			handleFunc, err := m.GetHandleFunc(req.CommandName())
			if err != nil {
				m.sendReply(socket, req, req.Fail(fmt.Sprintf("base.GetHandleFunc(%s): %v", req.CommandName(), err)))
				continue
			}

			m.sendReply(socket, req, handleFunc(req))
		}

		if err := socket.Close(); err != nil {
			m.LogError("socket.Close", "error", err)
		}
		m.socket = nil
		m.status = base.SocketNil
	}(ready)

	return <-ready
}

func (m *Manager) sendReply(socket *zmq.Socket, req message.RequestInterface, reply message.ReplyInterface) {
	replyStr, err := m.Packer().SerializeReply(reply)
	if err != nil {
		fail := req.Fail(fmt.Sprintf("failed to convert reply [%v] to string", reply))
		replyStr, err = m.Packer().SerializeReply(fail)
		if err != nil {
			m.LogError("Packer.SerializeReply", "request", req, "reply", reply, "error", err)
			return
		}
	}

	if _, err := socket.SendMessage(replyStr); err != nil {
		m.LogError("socket.SendMessage", "reply", reply, "error", err)
	}
}
