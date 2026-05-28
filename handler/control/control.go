// Package control creates a socket that controls the handler.
package control

import (
	"fmt"
	"time"

	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	HandlerStatus   = "status"
	HandlerStart    = "start"  // Close the handler
	HandlerClose    = "close"  // Close the handler
	HandlerConfig   = "config" // Returns the handler configuration
	ControlCategory = "control"
)

// Manager is the control ROUTER socket for a handler.
type Manager struct {
	*base.Handler
}

var _ base.Interface = (*Manager)(nil)

// New creates a control handler.
func New(parent *log.Logger) base.Interface {
	logger := parent.Child("control")

	return &Manager{
		Handler: base.New(logger),
	}
}

func DefaultManagerId(handlerId string) string {
	return handlerId + "_control"
}

func CreateInternalConfig(handler *config.Handler) *config.Handler {
	id := handler.Id
	if handler.Port == 0 {
		id = DefaultManagerId(handler.Id)
	}

	return config.NewHandler(handler.Type, id, ControlCategory, 0)
}

// SetClose is intentionally disabled for control handlers.
func (m *Manager) SetClose(_ bool) {
	m.Handler.Logger().Warn("Handler controls can not be closed. Please don't call it")
}

// Start binds the control ROUTER socket and serves registered routes.
func (m *Manager) Start() error {
	if m.Config() == nil {
		return fmt.Errorf("no config")
	}
	if m.Config().Category != ControlCategory {
		return fmt.Errorf("I cant start a handler in a %s category, It must be %s", m.Config().Category, ControlCategory)
	}

	ready := make(chan error)

	go func(ready chan error) {
		socket, err := zmq.NewSocket(zmq.ROUTER)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket: %w", err)
			return
		}

		url := m.Config().HandlerUrl()
		if err := socket.Bind(url); err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("socket.Bind('%s'): %w", url, err)
			return
		}

		m.SetSocket(socket)

		poller := zmq.NewPoller()
		poller.Add(socket, zmq.POLLIN)

		m.SetSocketReady()

		ready <- nil

		for {
			if m.Closed() {
				if err := poller.RemoveBySocket(socket); err != nil {
					m.Logger().Error("poller.RemoveBySocket", "error", err)
				}
				break
			}

			sockets, err := poller.Poll(time.Millisecond)
			if err != nil {
				m.Logger().Error("poller.Poll", "error", err)
				break
			}

			if len(sockets) == 0 {
				continue
			}

			raw, err := socket.RecvMessage(0)
			if err != nil {
				m.Logger().Error("socket.RecvMessage", "error", err)
				break
			}

			req, err := message.NewReq(raw)
			if err != nil {
				m.Logger().Error("message.NewReq", "messages", raw, "error", err)
				continue
			}

			handleFunc, err := base.FindRoute(req.CommandName(), m.Routes)
			if err != nil {
				m.sendReply(socket, req, req.Fail(fmt.Sprintf("base.FindRoute(%s): %v", req.CommandName(), err)))
				continue
			}

			m.sendReply(socket, req, base.Handle(req, handleFunc))
		}

		m.SetClose(false)

		if err := socket.Close(); err != nil {
			m.Logger().Error("socket.Close", "error", err)
		}
		m.SetSocket(nil)
	}(ready)

	return <-ready
}

func (m *Manager) sendReply(socket *zmq.Socket, req message.RequestInterface, reply message.ReplyInterface) {
	replyStr, err := reply.ZmqEnvelope()
	if err != nil {
		fail := req.Fail(fmt.Sprintf("failed to convert reply [%v] to string", reply))
		replyStr, err = fail.ZmqEnvelope()
		if err != nil {
			m.Logger().Error("req.Fail.ZmqEnvelope", "request", req, "reply", reply, "error", err)
			return
		}
	}

	if _, err := socket.SendMessage(replyStr); err != nil {
		m.Logger().Error("socket.SendMessage", "reply", reply, "error", err)
	}
}
