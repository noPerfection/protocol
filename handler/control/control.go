// Package control creates a socket that controls the handler.
package control

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/route"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	HandlerStatus   = "status"
	HandlerClose    = "close"  // Close the handler
	HandlerConfig   = "config" // Returns the handler configuration
	ControlCategory = "control"

	socketIdle  = "idle"
	socketReady = "ready"
)

type Manager struct {
	logger *log.Logger
	config *config.Handler
	routes datatype.KeyValue
	status string // It's the socket status, not the handler status
	close  bool
}

// New creates a new Manager.
func New(parent *log.Logger) *Manager {
	logger := parent.Child("control")

	return &Manager{
		routes: datatype.New(),
		status: socketIdle,
		logger: logger,
	}
}

// SetConfig sets the link to the configuration of the Manager.
func (m *Manager) SetConfig(config *config.Handler) {
	m.config = config
}

// Status returns the socket status of the Manager.
func (m *Manager) Status() string {
	return m.status
}

// PartStatuses returns statuses of the base handler parts.
//
// Intended to be used by the extending handlers.
func (m *Manager) PartStatuses() datatype.KeyValue {
	return datatype.New()
}

// Close adds a close signal to the Manager socket loop.
func (m *Manager) Close() {
	m.close = true
}

// SetRoutes registers or overwrites control routes.
func (m *Manager) SetRoutes(routes map[string]route.HandleFunc) error {
	if m.status == socketReady {
		return fmt.Errorf("can not overwrite handler when Manager is running")
	}

	for cmd, handle := range routes {
		m.routes.Set(cmd, handle)
	}

	return nil
}

// Route registers or overwrites one control route.
func (m *Manager) Route(cmd string, handle route.HandleFunc) error {
	return m.SetRoutes(map[string]route.HandleFunc{cmd: handle})
}

// Start the Manager.
func (m *Manager) Start() error {
	if m.config == nil {
		return fmt.Errorf("no config")
	}
	if m.config.Category != ControlCategory {
		return fmt.Errorf("I cant start a handler in a %s category, It must be %s", m.config.Category, ControlCategory)
	}

	ready := make(chan error)

	go func(ready chan error) {
		socket, err := zmq.NewSocket(zmq.ROUTER)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket: %w", err)
			return
		}

		url := config.ExternalUrl(m.config.Id, m.config.Port)
		err = socket.Bind(url)
		if err != nil {
			ready <- fmt.Errorf("socket.Bind('%s'): %w", url, err)
			return
		}

		poller := zmq.NewPoller()
		poller.Add(socket, zmq.POLLIN)

		m.status = socketReady

		// Exit from Start function
		ready <- nil

		for {
			if m.close {
				err = poller.RemoveBySocket(socket)
				if err != nil {
					m.logger.Error("poller.RemoveBySocket", "error", err)
				}
				break
			}

			sockets, err := poller.Poll(time.Millisecond)
			if err != nil {
				m.logger.Error("poller.Poll", "error", err)
				break
			}

			if len(sockets) == 0 {
				continue
			}

			raw, err := socket.RecvMessage(0)
			if err != nil {
				m.logger.Error("socket.RecvMessage", "error", err)
				break
			}

			req, err := message.NewReq(raw)
			if err != nil {
				m.logger.Error("message.NewReq", "messages", raw, "error", err)
				continue
			}

			handleInterface, err := route.Route(req.CommandName(), m.routes)
			if err != nil {
				reply := req.Fail(fmt.Sprintf("route.Route(%s): %v", req.CommandName(), err))
				replyStr, err := reply.ZmqEnvelope()
				if err != nil {
					reply := req.Fail(fmt.Sprintf("failed to convert reply [%v] to string", reply))
					replyStr, err := reply.ZmqEnvelope()
					if err != nil {
						m.logger.Error("req.Fail.String", "request", req, "reply", reply, "error", err)
						continue
					}
					_, err = socket.SendMessage(replyStr)
					if err != nil {
						m.logger.Error("socket.SendMessage", "reply", reply, "error", err)
					}
				} else {
					_, err = socket.SendMessage(replyStr)
					if err != nil {
						m.logger.Error("socket.SendMessage", "reply", reply, "error", err)
					}
				}
				continue
			}

			reply := route.Handle(req, handleInterface)
			replyStr, err := reply.ZmqEnvelope()
			if err != nil {
				reply := req.Fail(fmt.Sprintf("failed to convert handle reply [%v] to string", reply))
				replyStr, err := reply.ZmqEnvelope()
				if err != nil {
					m.logger.Error("req.Fail.String", "request", req, "reply", reply, "error", err)
					continue
				}
				_, err = socket.SendMessage(replyStr)
				if err != nil {
					m.logger.Error("socket.SendMessage", "reply", reply, "error", err)
					continue
				}
			} else {
				_, err = socket.SendMessage(replyStr)
				if err != nil {
					m.logger.Error("socket.SendMessage", "reply", reply, "error", err)
					continue
				}
			}
		}

		m.status = socketIdle
		m.close = false

		closeErr := socket.Close()
		if closeErr != nil {
			m.logger.Error("socket.Close", "error", err)
		}
	}(ready)

	return <-ready
}
