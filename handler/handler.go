package handler

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"

	"github.com/noPerfection/protocol/message"
)

const (
	SocketIdle  = "idle"  // Socket is bind but not listening to receive messages
	SocketReady = "ready" // Socket is bind and started
	SocketNil   = "nil"   // Socket is removed and all clean
)

// HandleFunc is re-exported from message so callers can use either package name.
type HandleFunc = message.HandleFunc

// The Handler is the socket wrapper for the zeromq socket.
type Handler struct {
	endpoint      message.Endpoint
	logger        *log.Logger
	messagePacker message.Packer
	routes        datatype.KeyValue
	// command -> secret -> true, one command may have multiple secrets
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
	c.logger = parent.Child(c.endpoint.ZapDomain())

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
// Use message.Any for a route-wide policy that applies when no command-specific whitelist exists.
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

// IsWhitelistExist reports whether the given command requires HMAC validation.
func (c *Handler) IsWhitelistExist(cmd string) bool {
	if _, ok := c.whitelists[cmd]; ok {
		return true
	}
	_, ok := c.whitelists[message.Any]
	return ok
}

func (c *Handler) getHmacSecrets(cmd string) map[string]bool {
	if secrets, ok := c.whitelists[cmd]; ok {
		return secrets
	}
	return c.whitelists[message.Any]
}

func (c *Handler) getRequestSecret(req message.RequestInterface, hash string) (string, bool) {
	secrets := c.getHmacSecrets(req.CommandName())
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
	} else if c.routes.Exist(message.Any) {
		handle = c.routes[message.Any]
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
	if err := handler.Route(message.Any, func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok()
	}); err != nil {
		return fmt.Errorf("failed to '%s' route into the handler: %w", message.Any, err)
	}
	return nil
}
