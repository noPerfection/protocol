package handler

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	// Registers the outbound handlers that can be called within this handler context.
	//
	// Arguments:
	// 	- endpoint: the endpoint of the outbound
	// 	- commands: map[command]: secret.
	// Then call the control's request-as-context command to request the outbound
	HandlerRegisterOutbounds = "register-outbounds"
	// receives the request, and endpoint and client type, and client parameters such as timeout and attempt
	HandlerRequestAsContext = "request-as-context" // Requests an outbound
	HandlerStatus           = "status"             // Returns the handler status
	HandlerStart            = "start"              // Starts the handler
	HandlerClose            = "close"              // Closes the handler
	HandlerConfig           = "config"             // Returns the handler configuration
	HandlerCommands         = "commands"           // Returns the handler route commands
	HandlerRequireWhitelist = "require-whitelist"  // Marks a route as requiring whitelist; optional secret param whitelists it
	HandlerRequireSecure    = "require-secure"     // Ensures the handler socket is secure and returns its public key
	HandlerSecureOutbound   = "secure-outbound"    // Ensures control outbound identity and returns its CURVE public key
	HandlerAllow            = "allow"              // Allows a CURVE client public key
	ControlCategory         = "control"
)

type npacSecureEdgeCaseFunc func(outbound, localCmd string) error

// Control is the control ROUTER socket for a handler.
type Control struct {
	*Handler
	socket               *zmq.Socket
	status               string
	curveSecretKey       string
	mushroomURL          string
	npacSecureEdgeCase   npacSecureEdgeCaseFunc
	requireWhitelistCmds map[string]bool
	// endpoint -> command: secret
	outbounds map[string]map[string]string
	// endpoint -> outbound handler public key
	outboundPublicKeys map[string]string
}

var _ Interface = (*Control)(nil)

// NewControl creates a control handler.
func NewControl(parent ...*log.Logger) *Control {
	return &Control{
		Handler:            New(parent...),
		status:             SocketNil,
		outbounds:          make(map[string]map[string]string),
		outboundPublicKeys: make(map[string]string),
	}
}

func (m *Control) setSecretKey(secretKey string) {
	m.curveSecretKey = secretKey
}

func (m *Control) setNpacSecureEdgeCase(fn npacSecureEdgeCaseFunc) {
	m.npacSecureEdgeCase = fn
}

// NewInternalControlEndpoint derives the control endpoint from a handler endpoint.
// Control sockets are always in-process, so Port is set to 0 to use the inproc
// transport regardless of the original handler's transport.
func NewInternalControlEndpoint(handlerEndpoint message.Endpoint) message.Endpoint {
	handlerEndpoint.Id = handlerEndpoint.Id + strconv.FormatUint(handlerEndpoint.Port, 10) + "_control"
	handlerEndpoint.Port = 0
	return handlerEndpoint
}

// SetEndpoint converts the handler endpoint into a control endpoint and stores it.
func (m *Control) SetEndpoint(handlerEndpoint message.Endpoint) {
	m.Handler.SetEndpoint(NewInternalControlEndpoint(handlerEndpoint))
}

// Secure is a no-op; control sockets are inproc and do not use CURVE.
func (m *Control) Secure(_ string) {
}

func (m *Control) IsSecure() bool {
	return m.curveSecretKey != ""
}

func (m *Control) PublicKey() (string, error) {
	if m.curveSecretKey == "" {
		return "", fmt.Errorf("handler is not secure")
	}
	return message.DerivePublicKey(m.curveSecretKey)
}

// SecureOutbound returns the control outbound CURVE public key.
// When the control already has a secret key, it is reused; otherwise an ephemeral key is generated.
func (m *Control) SecureOutbound() (string, error) {
	if m.IsSecure() {
		return m.PublicKey()
	}
	_, secret, err := message.GenerateCurveKey()
	if err != nil {
		return "", fmt.Errorf("message.GenerateCurveKey: %w", err)
	}
	m.setSecretKey(secret)
	return m.PublicKey()
}

func (m *Control) onSecureOutbound(req message.RequestInterface) message.ReplyInterface {
	pubKey, err := m.SecureOutbound()
	if err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("public-key", pubKey))
}

func (m *Control) RequireWhitelist(cmd string) {
	if cmd == "" {
		return
	}
	if m.requireWhitelistCmds == nil {
		m.requireWhitelistCmds = make(map[string]bool)
	}
	m.requireWhitelistCmds[cmd] = true
}

func (m *Control) IsWhitelistRequired(cmd string, dontUseAny ...bool) bool {
	if m.requireWhitelistCmds == nil {
		return false
	}
	if m.requireWhitelistCmds[cmd] {
		return true
	}
	if len(dontUseAny) > 0 && dontUseAny[0] {
		return false
	}
	return m.requireWhitelistCmds[message.Any]
}

// Allow is a no-op; control sockets are inproc and do not use CURVE client allowlists.
func (m *Control) Allow(_ string) {}

// SetMushroomURL stores the handler mushroom URL (ZAP domain for the external socket).
func (m *Control) SetMushroomURL(mushroomURL string) {
	m.mushroomURL = mushroomURL
}

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

// If commands argument's secret are empty or nil, then it will generate them which is recommended.
func (m *Control) onRegisterOutbounds(req message.RequestInterface) message.ReplyInterface {
	endpointKv, err := req.RouteParameters().NestedValue("endpoint")
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to get endpoint from route parameters: %v", err))
	}
	var endpoint message.Endpoint
	err = endpointKv.Interface(&endpoint)
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to get endpoint from route parameters: %v", err))
	}
	endpointUrl := endpoint.HandlerUrl()
	commandsKv, err := req.RouteParameters().NestedValue("commands")
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to get commands from route parameters: %v", err))
	}
	var commands map[string]string
	err = commandsKv.Interface(&commands)
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to get commands from route parameters: %v", err))
	}
	if _, ok := m.outbounds[endpointUrl]; !ok {
		m.outbounds[endpointUrl] = make(map[string]string)
	}
	for command, secret := range commands {
		if secret == "" {
			return req.Fail(fmt.Sprintf("secret for command %q is empty", command))
		}
		m.outbounds[endpointUrl][command] = secret
	}
	publicKey, err := req.RouteParameters().StringValue("public-key")
	if err == nil && publicKey != "" {
		m.outboundPublicKeys[endpointUrl] = publicKey
	}

	if m.npacSecureEdgeCase != nil {
		outboundURL, outboundErr := req.RouteParameters().StringValue("outbound-url")
		localCmd, localErr := req.RouteParameters().StringValue("local-command")
		if outboundErr == nil && outboundURL != "" && localErr == nil && localCmd != "" {
			if err := m.npacSecureEdgeCase(outboundURL, localCmd); err != nil {
				if !errors.Is(err, ErrAlreadyWhitelisted) {
					return req.Fail(fmt.Sprintf("NpacSecureEdgeCase(%q, %q): %v", outboundURL, localCmd, err))
				}
			}
		}
	}

	return req.Ok(datatype.New())
}

// RequestAsContext creates instance client and to request-as-context request.
// Accepted parameters:
// - endpoint: the endpoint of the outbound
// - public-key: the public key of the outbound, can be empty
// - envelope: the array of messages after serializing the message.RequestInterface.
// - command: request.CommandName()
// - client-type: the type of the client
// - timeout: poll timeout in nanoseconds (uint64)
// - attempt: the number of attempts to send the request (uint64)
//
// HMAC is always computed locally from register-outbounds secrets; callers never pass it.
func (m *Control) onRequestAsContext(req message.RequestInterface) message.ReplyInterface {
	endpointKv, err := req.RouteParameters().NestedValue("endpoint")
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to get endpoint from route parameters: %v", err))
	}
	var endpoint message.Endpoint
	err = endpointKv.Interface(&endpoint)
	if err != nil {
		return req.Fail(fmt.Sprintf("endpoint param: %v", err))
	}
	if _, ok := m.outbounds[endpoint.HandlerUrl()]; !ok {
		return req.Fail(fmt.Sprintf("outbound endpoint not registered: %s", endpoint.HandlerUrl()))
	}
	publicKey, err := req.RouteParameters().StringValue("public-key")
	if err != nil || publicKey == "" {
		if stored, ok := m.outboundPublicKeys[endpoint.HandlerUrl()]; ok {
			publicKey = stored
		}
	}
	if m.curveSecretKey == "" {
		return req.Fail("handler doesn't know what secret key to use to identify itself, please call handler.Control.SetSecretKey")
	}
	rawClientType, err := req.RouteParameters().StringValue("client-type")
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to get client type from route parameters: %v", err))
	}
	clientType := client.HandlerType(rawClientType)
	command, err := req.RouteParameters().StringValue("command")
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to get command from route parameters: %v", err))
	}
	var timeout time.Duration
	timeoutRaw, err := req.RouteParameters().Uint64Value("timeout")
	if err != nil {
		timeout = client.DefaultTimeout
	} else {
		timeout = time.Duration(timeoutRaw)
	}
	var attempt uint8
	attemptRaw, err := req.RouteParameters().Uint64Value("attempt")
	if err != nil {
		attempt = client.DefaultAttempt
	} else {
		attempt = uint8(attemptRaw)
	}
	envelope, err := req.RouteParameters().StringsValue("envelope")
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to get request from route parameters: %v", err))
	}

	secret, err := m.outboundHmacSecret(endpoint.HandlerUrl(), command)
	if err != nil {
		return req.Fail(err.Error())
	}

	// Now we set a client:
	c, err := client.New(endpoint.Id, endpoint.Port, clientType)
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to create client: %v", err))
	}
	defer c.Close()

	if publicKey != "" {
		c.Allow(publicKey)
	}
	if m.curveSecretKey != "" {
		c.Secure(m.curveSecretKey)
	}
	// Now we send the request:
	packer := message.RawPacker{}
	r, _, err := packer.DeserializeRequest(envelope)
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to deserialize request: %v", err))
	}

	c.Attempt(attempt).Timeout(timeout)
	c.Packer(&packer)

	hmac := message.ComputeHMAC(r.String(), secret)
	reply, err := c.Request(r, hmac)
	if err != nil {
		return req.Fail(fmt.Sprintf("failed to send request: %v", err))
	}
	return reply
}

func (m *Control) outboundHmacSecret(endpointURL, command string) (string, error) {
	commands, ok := m.outbounds[endpointURL]
	if !ok {
		return "", fmt.Errorf("outbound endpoint not registered: %s", endpointURL)
	}
	if secret, ok := commands[command]; ok && secret != "" {
		return secret, nil
	}
	if secret, ok := commands[message.Any]; ok && secret != "" {
		return secret, nil
	}
	return "", fmt.Errorf("no HMAC secret registered for outbound %q command %q", endpointURL, command)
}

// Start binds the control ROUTER socket, and registers HandlerStatus route.
func (m *Control) Start() error {
	if m.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("no config")
	}

	m.Route(HandlerStatus, m.onBuiltinStatus)
	m.Route(HandlerRegisterOutbounds, m.onRegisterOutbounds)
	m.Route(HandlerRequestAsContext, m.onRequestAsContext)
	m.Route(HandlerSecureOutbound, m.onSecureOutbound)

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
			sockets, err := poller.Poll(blockForever)
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
			} else if m.IsWhitelistRequired(cmd) {
				m.sendControlReply(socket, req, req.Fail(message.ErrAccessDenied.Error()+", whitelist required"), cmd, "")
				continue
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
	// ROUTER must reply with the requester's routing identity. Handlers such as
	// request-as-context return the inner outbound reply, which carries a different
	// (often empty) conId from the nested client socket.
	if req != nil {
		if conId := req.ConId(); conId != "" {
			reply.SetConId(conId)
		}
	}

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
