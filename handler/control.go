package handler

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
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

// Control is the control REP socket for a handler.
type Control struct {
	*Handler
	socket *zmq.Socket
	status string
}

var _ Interface = (*Control)(nil)

// NewControl creates a control handler.
func NewControl(parent ...*log.Logger) *Control {
	return &Control{
		Handler: New(parent...),
		status:  SocketNil,
	}
}

// NewInternalControlEndpoint derives the control endpoint from a handler endpoint.
// Control sockets are always in-process, so Port is set to 0 to use the inproc
// transport regardless of the original handler's transport.
func NewInternalControlEndpoint(handlerEndpoint message.Endpoint) message.Endpoint {
	handlerEndpoint.Id = handlerEndpoint.ZapDomain() + "_control"
	handlerEndpoint.Port = 0
	return handlerEndpoint
}

// SetEndpoint converts the handler endpoint into a control endpoint and stores it.
func (m *Control) SetEndpoint(handlerEndpoint message.Endpoint) {
	m.Handler.SetEndpoint(NewInternalControlEndpoint(handlerEndpoint))
}

// Secure is a no-op; control sockets are inproc and do not use CURVE.
func (m *Control) Secure(_ string) {}

// Allow is a no-op; control sockets are inproc and do not use CURVE client allowlists.
func (m *Control) Allow(_ string) {}

func (m *Control) Status() string {
	return m.status
}

// Running returns true while the handler socket is ready to serve.
func (m *Control) Running() bool {
	return m.status == SocketReady
}

func (m *Control) SetSocketIdle() {
	m.status = SocketIdle
}

func (m *Control) SetSocketReady() {
	m.status = SocketReady
}

func (m *Control) SetSocketNil() {
	m.status = SocketNil
}

func (m *Control) onBuiltinStatus(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("status", m.Status()))
}

// Start binds the control REP socket and serves registered routes.
func (m *Control) Start() error {
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

			req, hmacHash, err := m.Packer().DeserializeRequest(raw)
			if err != nil {
				m.LogError("Packer().DeserializeRequest", "messages", raw, "error", err)
				continue
			}

			cmd := req.CommandName()
			matchedSecret := ""
			if m.IsWhitelistExist(cmd) {
				var ok bool
				matchedSecret, ok = m.getRequestSecret(req, hmacHash)
				if !ok {
					m.sendControlReply(socket, req, m.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()), cmd, matchedSecret)
					continue
				}
			}

			handleFunc, err := m.GetHandleFunc(cmd)
			if err != nil {
				m.sendControlReply(socket, req, req.Fail(fmt.Sprintf("GetHandleFunc(%s): %v", cmd, err)), cmd, matchedSecret)
				continue
			}

			m.sendControlReply(socket, req, handleFunc(req), cmd, matchedSecret)
		}

		if err := socket.Close(); err != nil {
			m.LogError("socket.Close", "error", err)
		}
		m.socket = nil
		m.status = SocketNil
	}(ready)

	return <-ready
}

func (m *Control) sendControlReply(socket *zmq.Socket, req message.RequestInterface, reply message.ReplyInterface, cmd, matchedSecret string) {
	var hmac string
	if m.IsWhitelistExist(cmd) && matchedSecret != "" {
		hmac = message.ComputeHMAC(reply.String(), matchedSecret)
	}
	replyStr, err := m.Packer().SerializeReply(reply, hmac)
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
