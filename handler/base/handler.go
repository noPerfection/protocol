package base

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	Incomplete  = "incomplete"
	SocketIdle  = "idle"  // Socket is bind but not listening to receive messages
	SocketReady = "ready" // Socket is bind and started
	SocketNil   = "nil"   // Socket is removed and all clean
)

// Any route name.
const Any = "*"

// HandleFunc is the function type that handles a request and returns a reply.
type HandleFunc = func(message.RequestInterface) message.ReplyInterface

// The Handler is the socket wrapper for the zeromq socket.
type Handler struct {
	endpoint      message.Endpoint
	logger        *log.Logger
	messagePacker message.Packer
	routes        datatype.KeyValue
	// command -> secret -> true
	whitelists map[string]map[string]bool
}

// New creates a handler.
// Optionally you can set the logger.
func New(logger ...*log.Logger) *Handler {
	h := &Handler{
		messagePacker: &message.MessagePacker{},
		routes:        datatype.New(),
		whitelists:    make(map[string]map[string]bool),
	}

	if len(logger) > 0 && logger[0] != nil {
		h.logger = logger[0]
	}

	return h
}

// IsRouteExist returns true if the given route exists
func (c *Handler) IsRouteExist(command string) bool {
	return c.routes.Exist(command)
}

// Commands returns list of all route commands.
func (c *Handler) Commands() []string {
	commands := make([]string, len(c.routes))

	i := 0
	for command := range c.routes {
		commands[i] = command
		i++
	}

	return commands
}

func (c *Handler) Endpoint() message.Endpoint {
	return c.endpoint
}

func (c *Handler) Packer() message.Packer {
	return c.messagePacker
}

// Logger returns the handler logger.
func (c *Handler) Logger() *log.Logger {
	return c.logger
}

// SetEndpoint adds the parameters of the handler from the config.
func (c *Handler) SetEndpoint(endpoint message.Endpoint) {
	c.endpoint = endpoint
}

func (c *Handler) SetPacker(packer message.Packer) {
	c.messagePacker = packer
}

// SetLogger sets the logger. Passing nil disables logging.
func (c *Handler) SetLogger(parent *log.Logger) error {
	if parent == nil {
		c.logger = nil
		return nil
	}
	if c.endpoint == (message.Endpoint{}) {
		c.logger = parent
		return nil
	}
	c.logger = parent.Child(c.endpoint.Id)

	return nil
}

// LogError writes an error log when a logger is configured.
func (c *Handler) LogError(msg string, args ...interface{}) {
	if c.logger == nil {
		return
	}
	c.logger.Error(msg, args...)
}

// LogWarn writes a warning log when a logger is configured.
func (c *Handler) LogWarn(msg string, args ...interface{}) {
	if c.logger == nil {
		return
	}
	c.logger.Warn(msg, args...)
}

// Whitelist registers one or more shared secrets for a command.
// Use Any for a route-wide policy that applies when no command-specific whitelist exists.
func (c *Handler) Whitelist(cmd string, secrets ...string) error {
	if len(secrets) == 0 {
		return fmt.Errorf("at least one secret is required for whitelist on '%s'", cmd)
	}
	if c.whitelists[cmd] == nil {
		c.whitelists[cmd] = make(map[string]bool)
	}
	for _, secret := range secrets {
		c.whitelists[cmd][secret] = true
	}
	return nil
}

// RequiresWhitelist reports whether the given command requires HMAC validation.
func (c *Handler) RequiresWhitelist(cmd string) bool {
	if _, ok := c.whitelists[cmd]; ok {
		return true
	}
	_, ok := c.whitelists[Any]
	return ok
}

func (c *Handler) whitelistFor(cmd string) map[string]bool {
	if secrets, ok := c.whitelists[cmd]; ok {
		return secrets
	}
	return c.whitelists[Any]
}

// ValidateRequestHmac reports whether hash is valid for the request command whitelist.
func (c *Handler) ValidateRequestHmac(req message.RequestInterface, hash string) bool {
	_, ok := c.MatchRequestSecret(req, hash)
	return ok
}

// ValidateReplyHmac reports whether hash is valid for the Any-route whitelist.
func (c *Handler) ValidateReplyHmac(reply message.ReplyInterface, hash string) bool {
	secrets := c.whitelistFor(Any)
	if secrets == nil {
		return true
	}
	if hash == "" {
		return false
	}
	body := reply.String()
	for secret := range secrets {
		if message.VerifyHMAC(body, secret, hash) {
			return true
		}
	}
	return false
}

// SignRequestHmac returns the HMAC hash for a request signed with secret.
func (c *Handler) SignRequestHmac(req message.RequestInterface, secret string) string {
	return message.ComputeHMAC(req.String(), secret)
}

// SignReplyHmac returns the HMAC hash for a reply signed with secret.
func (c *Handler) SignReplyHmac(reply message.ReplyInterface, secret string) string {
	return message.ComputeHMAC(reply.String(), secret)
}

func (c *Handler) MatchRequestSecret(req message.RequestInterface, hash string) (string, bool) {
	secrets := c.whitelistFor(req.CommandName())
	if secrets == nil {
		return "", true
	}
	if hash == "" {
		return "", false
	}
	body := req.String()
	for secret := range secrets {
		if message.VerifyHMAC(body, secret, hash) {
			return secret, true
		}
	}
	return "", false
}

// Route adds a route along with its handler to this handler.
func (c *Handler) Route(cmd string, handle HandleFunc) error {
	c.routes.Set(cmd, handle)

	return nil
}

func (c *Handler) GetHandleFunc(cmd string) (HandleFunc, error) {
	var handle any

	if c.routes.Exist(cmd) {
		handle = c.routes[cmd]
	} else if c.routes.Exist(Any) {
		handle = c.routes[Any]
	} else {
		return nil, fmt.Errorf("the '%s' command handler not found", cmd)
	}

	handleFunc, ok := handle.(HandleFunc)
	if !ok {
		return nil, fmt.Errorf("the '%s' command handler is not a valid handle function", cmd)
	}

	return handleFunc, nil
}

// Type returns the handler type. If the configuration is not set, returns UnknownType.
func (c *Handler) Type() HandlerType {
	return UnknownType
}

func AnyRoute(handler *Handler) error {
	if err := handler.Route(Any, func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok()
	}); err != nil {
		return fmt.Errorf("failed to '%s' route into the handler: %w", Any, err)
	}
	return nil
}

// GenerateCurveKey returns a new Z85 CURVE public/secret keypair.
func GenerateCurveKey() (pub, secret string, err error) {
	return zmq.NewCurveKeypair()
}

// DerivePublicKey returns the Z85 CURVE public key for the given secret key.
func DerivePublicKey(secretKey string) (pubkey string, err error) {
	return zmq.AuthCurvePublic(secretKey)
}
