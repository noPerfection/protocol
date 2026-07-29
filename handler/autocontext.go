package handler

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/ahmetson/mushroom"
	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler/npac"
	"github.com/noPerfection/protocol/message"
)

// ErrAlreadyWhitelisted is returned when NpacSecureEdgeCase is called for an
// already authorized handler route on an outbound.
var ErrAlreadyWhitelisted = errors.New("already whitelisted")

type Autocontext struct {
	npacSecret  string
	mushroomURL string
	started     bool
	client      *client.Socket
	clientMu    sync.Mutex // serializes npac REQ socket use (not goroutine-safe)
}

// AutocontextHandler is implemented by handlers that embed Autocontext.
type AutocontextHandler interface {
	NpacPushAnyContext(functionPath any) error
	NpacPopAnyContext(functionPath any) error
}

// AsAutocontextHandler reports whether h embeds an Autocontext.
func AsAutocontextHandler(h Interface) (AutocontextHandler, bool) {
	ac, ok := h.(AutocontextHandler)
	return ac, ok
}

func NewAutocontext() *Autocontext {
	secret := message.GenerateSecret()
	c, _ := client.New("npac", 0, client.SyncReplierType)
	if c != nil {
		c.Timeout(50 * time.Millisecond)
		_ = c.Whitelist(npac.RemoveHandler, secret)
		_ = c.Whitelist(npac.SecureEdgeCase, secret)
		_ = c.Whitelist(npac.PushHandlerContext, secret)
		_ = c.Whitelist(npac.PopHandlerContext, secret)
	}
	return &Autocontext{
		npacSecret: secret,
		client:     c,
	}
}

func (c *Autocontext) SetMushroomURL(mushroomURL string) {
	c.mushroomURL = mushroomURL
}

// routeURL returns the handler mushroom URL with cmd as the "command" additional
// property. When handleFunc is non-empty it is stored in "handle-func".
func (c *Autocontext) routeURL(cmd string, handleFunc string) (string, error) {
	hypha, err := (&mushroom.Soil{}).Hypha(c.mushroomURL)
	if err != nil {
		return "", fmt.Errorf("routeURL: parse %q: %w", c.mushroomURL, err)
	}
	if hypha.AdditionalProps == nil {
		hypha.AdditionalProps = make(map[string]string)
	}
	hypha.AdditionalProps["command"] = cmd
	if handleFunc != "" {
		hypha.AdditionalProps["handle-func"] = handleFunc
	}
	return hypha.String(), nil
}

func (c *Autocontext) npacRequest(req message.RequestInterface, hmac ...string) (message.ReplyInterface, error) {
	if c.client == nil {
		return nil, fmt.Errorf("npac client not initialized")
	}
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	return c.client.Request(req, hmac...)
}

func handleFuncName(fn any) string {
	if fn == nil {
		return ""
	}
	ptr := reflect.ValueOf(fn).Pointer()
	rf := runtime.FuncForPC(ptr)
	if rf == nil {
		return ""
	}
	return strings.TrimSuffix(rf.Name(), "-fm")
}

// npacRegisterHandler registers a handler's mushroom URL, CURVE public key, and
// control endpoint with npac, and caches the mushroom URL for subsequent calls.
func (c *Autocontext) npacRegisterHandler(controlEndpoint message.Endpoint) error {
	if c.mushroomURL == "" {
		return fmt.Errorf("mushroom URL not set, call SetMushroomURL first")
	}
	req := &message.Request{
		Command: npac.RegisterHandler,
		Parameters: datatype.New().
			Set("mushroom-url", c.mushroomURL).
			Set("control-endpoint", controlEndpoint).
			Set("npac-secret", c.npacSecret),
	}
	hmac := message.ComputeHMAC(req.String(), c.npacSecret)
	reply, err := c.npacRequest(req, hmac)
	if err != nil {
		if errors.Is(err, message.RequestTimeoutError) {
			c.started = false
			return nil
		}
		return err
	}
	if !reply.IsOK() {
		if strings.Contains(reply.ErrorMessage(), "already registered") {
			c.started = true
			return nil
		}
		return fmt.Errorf(reply.ErrorMessage())
	}
	c.started = true
	return nil
}

// npacPushHandleContext pushes a route URL (mushroomURL + cmd) onto npac's
// context stack so outbound calls can reach this handler's control endpoint.
func (c *Autocontext) npacPushHandleContext(cmd string, handleFunc any) error {
	if !c.started {
		return nil
	}
	url, err := c.routeURL(cmd, handleFuncName(handleFunc))
	reply, err := c.npacRequest(&message.Request{
		Command:    npac.PushHandlerContext,
		Parameters: datatype.New().Set("mushroom-url", url),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// NpacPushAnyContext pushes message.Any with functionPath onto npac's context stack.
func (c *Autocontext) NpacPushAnyContext(functionPath any) error {
	if !c.started {
		return nil
	}
	return c.npacPushHandleContext(message.Any, functionPath)
}

// NpacSecureEdgeCase authorizes the cmd's context to call the outbound.
func (c *Autocontext) NpacSecureEdgeCase(outbound, cmd string) error {
	mushroomURL, err := c.routeURL(cmd, "")
	if err != nil {
		return err
	}
	reply, err := c.npacRequest(&message.Request{
		Command: npac.SecureEdgeCase,
		Parameters: datatype.New().
			Set("outbound", outbound).
			Set("mushroom-url", mushroomURL),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		if strings.Contains(reply.ErrorMessage(), "already whitelisted") {
			return ErrAlreadyWhitelisted
		}
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// npacRemoveHandler removes this handler's registration from npac.
func (c *Autocontext) npacRemoveHandler() error {
	if !c.started {
		return nil
	}
	reply, err := c.npacRequest(&message.Request{
		Command:    npac.RemoveHandler,
		Parameters: datatype.New().Set("mushroom-url", c.mushroomURL),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// HandlerContext resolves outbound access from this handler's npac context.
func (c *Autocontext) HandlerContext(endpoint message.Endpoint, cmd string) (bool, string, message.Endpoint, error) {
	autocontext := client.NewAutocontext()
	if autocontext == nil {
		return false, "", message.Endpoint{}, fmt.Errorf("failed to create autocontext")
	}
	defer func() { _ = autocontext.Close() }()
	return autocontext.HandlerContext(endpoint, cmd)
}

// npacPopHandleContext pops the route URL (mushroomURL + cmd) from npac's context stack.
func (c *Autocontext) npacPopHandleContext(cmd string, handleFunc any) error {
	if !c.started {
		return nil
	}
	url, err := c.routeURL(cmd, handleFuncName(handleFunc))
	if err != nil {
		return err
	}
	reply, err := c.npacRequest(&message.Request{
		Command:    npac.PopHandlerContext,
		Parameters: datatype.New().Set("mushroom-url", url),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// NpacPopAnyContext pops the message.Any route for functionPath from npac's context stack.
func (c *Autocontext) NpacPopAnyContext(functionPath any) error {
	return c.npacPopHandleContext(message.Any, functionPath)
}
