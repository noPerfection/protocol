// Package npac implements the noPerfection AutoContext (npac) handler.
// It is a singleton in-process SyncReplier that acts as a security registry:
// handlers register their CURVE public key and HMAC secrets so that clients
// can look them up when a connection is rejected.
package npac

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	// Route command constants.
	GetPublicKeyCmd  = "get-public-key"
	GetHmacSecretCmd = "get-hmac-secret"
	AddHandlerCmd    = "add-handler"
	AddRouteCmd      = "add-route"
	RemoveHandlerCmd = "remove-handler"
	RemoveRouteCmd   = "remove-route"
)

// Endpoint is the inproc endpoint the npac handler binds to.
var Endpoint = message.NewEndpoint("npac", 0)

// handlerEntry stores security info for one registered handler.
type handlerEntry struct {
	PubKey string
	Secret string            // shared secret between the handler and npac
	Routes map[string]string // command -> HMAC secret
}

// Npac wraps a SyncReplier and maintains a datatype.List of handler registrations.
// Use New() to construct; the inner replier is not accessible from outside the package.
type Npac struct {
	sr      *handler.SyncReplier
	list    *datatype.List
	started bool
	mu      sync.Mutex
}

// New creates a ready-to-start Npac handler bound to the default inproc endpoint.
func New() *Npac {
	h := &Npac{
		sr:   handler.NewSyncReplier(),
		list: datatype.NewList(),
	}
	h.sr.SetEndpoint(Endpoint)

	_ = h.sr.Route(GetPublicKeyCmd, h.onGetPublicKey)
	_ = h.sr.Route(GetHmacSecretCmd, h.onGetHmacSecret)
	_ = h.sr.Route(AddHandlerCmd, h.onAddHandler)
	_ = h.sr.Route(AddRouteCmd, h.onAddRoute)
	_ = h.sr.Route(RemoveHandlerCmd, h.onRemoveHandler)
	_ = h.sr.Route(RemoveRouteCmd, h.onRemoveRoute)

	return h
}

// isAlreadyRunning probes the npac inproc endpoint with a short timeout.
func isAlreadyRunning() bool {
	sock, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return false
	}
	defer sock.Close()

	if err := sock.Connect(Endpoint.ClientUrl()); err != nil {
		return false
	}

	packer := &message.MessagePacker{}
	envelope, err := packer.SerializeRequest(&message.Request{
		Command:    GetPublicKeyCmd,
		Parameters: datatype.New().Set("url", "probe"),
	})
	if err != nil {
		return false
	}

	if _, err := sock.SendMessage(envelope); err != nil {
		return false
	}

	poller := zmq.NewPoller()
	poller.Add(sock, zmq.POLLIN)
	sockets, err := poller.Poll(50 * time.Millisecond)
	return err == nil && len(sockets) > 0
}

// Start starts the npac handler and the ZAP authentication goroutine.
// If npac is already running in this process, Start returns nil without
// starting a second instance, matching the topology.Handler pattern.
func (h *Npac) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.started {
		return nil
	}
	if isAlreadyRunning() {
		h.started = true
		return nil
	}
	if err := h.sr.Start(); err != nil {
		return fmt.Errorf("npac.Start: %w", err)
	}
	if err := zmq.AuthStart(); err != nil {
		return fmt.Errorf("npac.Start: zmq.AuthStart: %w", err)
	}
	h.started = true
	return nil
}

// Stop shuts down the ZAP authentication goroutine started by Start.
// It is safe to call Stop even if Start was not called or returned an error.
func (h *Npac) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.started {
		return
	}
	zmq.AuthStop()
	h.started = false
}

// onGetPublicKey returns the CURVE public key for the given handler URL.
func (h *Npac) onGetPublicKey(req message.RequestInterface) message.ReplyInterface {
	url, err := req.RouteParameters().StringValue("url")
	if err != nil {
		return req.Fail(fmt.Sprintf("url param: %v", err))
	}

	if !h.list.Exist(url) {
		return req.Fail(fmt.Sprintf("handler %q not registered", url))
	}

	val, err := h.list.Get(url)
	if err != nil {
		return req.Fail(fmt.Sprintf("list.Get: %v", err))
	}

	entry, ok := val.(*handlerEntry)
	if !ok {
		return req.Fail("invalid entry type")
	}

	return req.Ok(datatype.New().Set("public-key", entry.PubKey))
}

// onGetHmacSecret returns the HMAC secret for the given handler URL and command.
func (h *Npac) onGetHmacSecret(req message.RequestInterface) message.ReplyInterface {
	url, err := req.RouteParameters().StringValue("url")
	if err != nil {
		return req.Fail(fmt.Sprintf("url param: %v", err))
	}
	cmd, err := req.RouteParameters().StringValue("command")
	if err != nil {
		return req.Fail(fmt.Sprintf("command param: %v", err))
	}

	if !h.list.Exist(url) {
		return req.Fail(fmt.Sprintf("handler %q not registered", url))
	}

	val, err := h.list.Get(url)
	if err != nil {
		return req.Fail(fmt.Sprintf("list.Get: %v", err))
	}

	entry, ok := val.(*handlerEntry)
	if !ok {
		return req.Fail("invalid entry type")
	}

	secret, exists := entry.Routes[cmd]
	if !exists {
		return req.Fail(fmt.Sprintf("command %q not registered for %q", cmd, url))
	}

	return req.Ok(datatype.New().Set("secret", secret))
}

// onAddHandler registers a handler's CURVE public key and shared secret by URL.
// The call is open (no HMAC required). If the URL is already registered the
// secret must match; otherwise the request is rejected (prevents hijacking).
// On success the handler's secret is added to the whitelist for add-route and
// remove-route so future calls from this handler can be HMAC-authenticated.
func (h *Npac) onAddHandler(req message.RequestInterface) message.ReplyInterface {
	url, err := req.RouteParameters().StringValue("url")
	if err != nil {
		return req.Fail(fmt.Sprintf("url param: %v", err))
	}
	pubKey, err := req.RouteParameters().StringValue("public-key")
	if err != nil {
		return req.Fail(fmt.Sprintf("public-key param: %v", err))
	}
	secret, err := req.RouteParameters().StringValue("secret")
	if err != nil {
		return req.Fail(fmt.Sprintf("secret param: %v", err))
	}

	if h.list.Exist(url) {
		val, getErr := h.list.Get(url)
		if getErr != nil {
			return req.Fail(fmt.Sprintf("list.Get: %v", getErr))
		}
		entry, ok := val.(*handlerEntry)
		if !ok {
			return req.Fail("invalid entry type")
		}
		if entry.Secret != secret {
			return req.Fail(fmt.Sprintf("handler %q already registered with a different secret", url))
		}
		entry.PubKey = pubKey
		return req.Ok(datatype.New())
	}

	entry := &handlerEntry{
		PubKey: pubKey,
		Secret: secret,
		Routes: make(map[string]string),
	}
	if addErr := h.list.Add(url, entry); addErr != nil {
		return req.Fail(fmt.Sprintf("list.Add: %v", addErr))
	}

	// Whitelist write commands with this handler's secret so the framework
	// validates HMAC before those handler functions are called.
	_ = h.sr.Whitelist(AddRouteCmd, secret)
	_ = h.sr.Whitelist(RemoveRouteCmd, secret)
	_ = h.sr.Whitelist(RemoveHandlerCmd, secret)

	return req.Ok(datatype.New())
}

// onAddRoute registers an HMAC secret for a handler URL and command.
// The call is HMAC-protected: the framework validates the request HMAC against
// the handler's registered secret before this function is reached.
func (h *Npac) onAddRoute(req message.RequestInterface) message.ReplyInterface {
	url, err := req.RouteParameters().StringValue("url")
	if err != nil {
		return req.Fail(fmt.Sprintf("url param: %v", err))
	}
	cmd, err := req.RouteParameters().StringValue("command")
	if err != nil {
		return req.Fail(fmt.Sprintf("command param: %v", err))
	}
	secret, err := req.RouteParameters().StringValue("secret")
	if err != nil {
		return req.Fail(fmt.Sprintf("secret param: %v", err))
	}

	if !h.list.Exist(url) {
		return req.Fail(fmt.Sprintf("handler %q not registered", url))
	}

	val, err := h.list.Get(url)
	if err != nil {
		return req.Fail(fmt.Sprintf("list.Get: %v", err))
	}

	entry, ok := val.(*handlerEntry)
	if !ok {
		return req.Fail("invalid entry type")
	}

	entry.Routes[cmd] = secret
	return req.Ok(datatype.New())
}

// onRemoveHandler removes a handler registration.
// The call is HMAC-protected: the framework validates the request HMAC against
// the handler's registered secret before this function is reached.
// On success the handler's secret is removed from all whitelists.
func (h *Npac) onRemoveHandler(req message.RequestInterface) message.ReplyInterface {
	url, err := req.RouteParameters().StringValue("url")
	if err != nil {
		return req.Fail(fmt.Sprintf("url param: %v", err))
	}

	if !h.list.Exist(url) {
		return req.Ok(datatype.New())
	}

	_, _ = h.list.Take(url)

	return req.Ok(datatype.New())
}

// onRemoveRoute removes the HMAC secret for a handler URL and command.
// The call is HMAC-protected: the framework validates the request HMAC before
// this function is reached.
func (h *Npac) onRemoveRoute(req message.RequestInterface) message.ReplyInterface {
	url, err := req.RouteParameters().StringValue("url")
	if err != nil {
		return req.Fail(fmt.Sprintf("url param: %v", err))
	}
	cmd, err := req.RouteParameters().StringValue("command")
	if err != nil {
		return req.Fail(fmt.Sprintf("command param: %v", err))
	}

	if !h.list.Exist(url) {
		return req.Ok(datatype.New())
	}

	val, err := h.list.Get(url)
	if err != nil {
		return req.Fail(fmt.Sprintf("list.Get: %v", err))
	}

	if entry, ok := val.(*handlerEntry); ok {
		delete(entry.Routes, cmd)
	}

	return req.Ok(datatype.New())
}
