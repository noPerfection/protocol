package handler

import (
	"fmt"
	"time"

	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

// Control is the control ROUTER socket for a handler.
type PublisherControl struct {
	*Control
	*Autocontext
}

var _ Interface = (*PublisherControl)(nil)

// NewPublisherControl creates a publisher control.
// Its identical to Control, but it sets the autocontext for the publishing commands
func NewPublisherControl(parent ...*log.Logger) *PublisherControl {
	return &PublisherControl{
		Control:     NewControl(parent...),
		Autocontext: NewAutocontext(),
	}
}

// Start binds the control ROUTER socket, and registers HandlerStatus route.
func (m *PublisherControl) Start() error {
	if m.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("no config")
	}

	m.Route(HandlerStatus, m.onBuiltinStatus)

	ready := make(chan error)

	go func(ready chan error) {
		socket, err := zmq.NewSocket(zmq.ROUTER)
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

			req, hmacHash, err := m.Packer().DeserializeRequest(raw)
			if err != nil {
				m.LogError("Packer().DeserializeRequest", "messages", raw, "error", err)
				failReq := m.Packer().EmptyRequest()
				if conId, _, _ := message.EnvelopeToMessage(raw); conId != "" {
					failReq.SetConId(conId)
				}
				m.sendControlReply(socket, failReq, failReq.Fail(err.Error()), "", "")
				continue
			}

			cmd := req.CommandName()
			matchedSecret := ""
			if m.IsWhitelistExist(cmd) {
				var ok bool
				matchedSecret, ok = m.getRequestSecret(req, hmacHash)
				if !ok {
					m.sendControlReply(socket, req, req.Fail(message.ErrAccessDenied.Error()), cmd, "")
					continue
				}
			}

			useNpacContext := cmd != HandlerClose && cmd != HandlerConfig && cmd != HandlerStart && cmd != HandlerStatus
			if useNpacContext {
				err = m.npacPushHandleContext(cmd)
				if err != nil {
					m.sendControlReply(socket, req, req.Fail(fmt.Sprintf("npacPushHandleContext(%s): %v", cmd, err)), cmd, matchedSecret)
					continue
				}
			}

			handleFunc, err := m.GetHandleFunc(cmd)
			if err != nil {
				m.sendControlReply(socket, req, req.Fail(fmt.Sprintf("GetHandleFunc(%s): %v", cmd, err)), cmd, matchedSecret)
				continue
			}

			reply := handleFunc(req)

			if useNpacContext {
				err = m.popHandleContext(cmd)
				if err != nil {
					m.sendControlReply(socket, req, req.Fail(fmt.Sprintf("popHandleContext(%s): %v", cmd, err)), cmd, matchedSecret)
					continue
				}
			}

			m.sendControlReply(socket, req, reply, cmd, matchedSecret)
		}

		if err := socket.Close(); err != nil {
			m.LogError("socket.Close", "error", err)
		}
		m.socket = nil
		m.status = SocketNil
	}(ready)

	return <-ready
}
