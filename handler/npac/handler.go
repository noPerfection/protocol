// Package npac implements the noPerfection AutoContext (npac) handler.
// It is a singleton in-process REP socket that acts as a handler registry:
// handlers register their mushroom URL, npac secret, and control endpoint
// so that clients can discover them by mushroom URL.
package npac

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ahmetson/mushroom"
	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	// Route command constants.
	RegisterHandler    = "register-handler"
	RemoveHandler      = "remove-handler"
	RegisterOutbound   = "register-outbound"
	RemoveOutbound     = "remove-outbound"
	SecureEdgeCase     = "secure-edge-case"
	PushHandlerContext = "push-handler-context"
	PopHandlerContext  = "pop-handler-context"
	HandlerContext     = "handler-context"
)

// Endpoint is the inproc endpoint the npac handler binds to.
var Endpoint = message.NewEndpoint("npac", 0)

// handlerEntry stores the identity info for one registered handler.
type handlerEntry struct {
	NpacSecret      string // shared secret used to verify the handler with npac
	ControlEndpoint message.Endpoint
}

// outbound the outbounds from the handlers.
// public key is the curve key that all handlers could use.
// mushroom url is the absolute path of the handler: `pkg:golang/package#module?var=service&category`
// Whitelists are list of handler's mushroom urls that allowed to access the command
type outbound struct {
	PublicKey   string              // CURVE public key of the target handler
	MushroomURL string              // mushroom URL that identifies the handler
	Whitelist   map[string][]string // command -> allowed route URLs for this outbound
}

// Npac is a minimal inproc REP socket with a route table.
// Use New() to construct.
type Npac struct {
	socket  *zmq.Socket
	routes  map[string]message.HandleFunc
	started bool
	mu      sync.Mutex
	wg      sync.WaitGroup
	// handlers maps each mushroom URL to its registered handler entry.
	handlers map[string]*handlerEntry
	// outbounds maps each handler URL to its resolved outbound identity.
	// e.g.: "inproc://entrypoint" -> outbound
	outbounds map[string]*outbound
	// mushroomUrlToOutbounds is the reverse index: mushroom URL -> handler URL,
	// allowing npac to resolve "which real endpoint backs this mushroom URL?"
	mushroomUrlToOutbounds map[string]string
	// contexts is a stack of handler route mushroom URLs.
	// Push appends; Pop verifies the top matches before removing.
	contexts []string
}

// getHandleFunc retrieves the handler for cmd, falling back to message.Any.
func (h *Npac) getHandleFunc(cmd string) (message.HandleFunc, error) {
	if fn, ok := h.routes[cmd]; ok {
		return fn, nil
	}
	if fn, ok := h.routes[message.Any]; ok {
		return fn, nil
	}
	return nil, fmt.Errorf("the '%s' command handler not found", cmd)
}

// New creates a ready-to-start Npac handler bound to the default inproc endpoint.
func New() *Npac {
	h := &Npac{
		routes:                 make(map[string]message.HandleFunc),
		handlers:               make(map[string]*handlerEntry),
		outbounds:              make(map[string]*outbound),
		mushroomUrlToOutbounds: make(map[string]string),
		contexts:               []string{},
	}

	h.routes[RegisterOutbound] = h.onRegisterOutbound
	h.routes[RemoveOutbound] = h.onRemoveOutbound
	h.routes[RegisterHandler] = h.onRegisterHandler       // HMAC protected
	h.routes[RemoveHandler] = h.onRemoveHandler           // HMAC protected
	h.routes[SecureEdgeCase] = h.onSecureEdgeCase         // HMAC protected
	h.routes[PushHandlerContext] = h.onPushHandlerContext // HMAC protected
	h.routes[PopHandlerContext] = h.onPopHandlerContext   // HMAC protected
	h.routes[HandlerContext] = h.onHandlerContext         // Public, called by clients

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
		Command:    HandlerContext,
		Parameters: datatype.New(),
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

// Start binds the inproc REP socket and launches the receive loop.
// If npac is already running in this process, Start returns nil without
// starting a second instance.
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

	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		return fmt.Errorf("npac.Start: zmq.NewSocket: %w", err)
	}
	if err := socket.Bind(Endpoint.HandlerUrl()); err != nil {
		_ = socket.Close()
		return fmt.Errorf("npac.Start: Bind(%s): %w", Endpoint.HandlerUrl(), err)
	}
	h.socket = socket

	h.wg.Add(1)
	go h.run()

	h.started = true
	return nil
}

// Stop closes the socket and shuts down the ZAP authentication goroutine.
// It is safe to call Stop even if Start was not called or returned an error.
func (h *Npac) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.started {
		return
	}
	if h.socket != nil {
		_ = h.socket.Close()
		h.socket = nil
	}
	h.started = false
	h.wg.Wait()
}

func (h *Npac) run() {
	defer h.wg.Done()

	packer := &message.MessagePacker{}

	for {
		raw, err := h.socket.RecvMessage(0)
		if err != nil {
			break
		}

		req, hash, err := packer.DeserializeRequest(raw)
		if err != nil {
			reply := packer.EmptyRequest().Fail(fmt.Sprintf("DeserializeRequest: %v", err))
			envelope, _ := packer.SerializeReply(reply, "")
			_, _ = h.socket.SendMessage(envelope)
			continue
		}

		var reply message.ReplyInterface
		if err := h.verifyHMAC(req, hash); err != nil {
			reply = req.Fail(err.Error())
		} else {
			handleFunc, err := h.getHandleFunc(req.CommandName())
			if err != nil {
				reply = req.Fail(fmt.Sprintf("unknown command %q", req.CommandName()))
			} else {
				reply = handleFunc(req)
			}
		}

		envelope, err := packer.SerializeReply(reply, "")
		if err != nil {
			continue
		}
		_, _ = h.socket.SendMessage(envelope)
	}
}

// verifyHMAC validates the request HMAC for commands that require it.
// AddHandlerCmd: secret is taken from the npac-secret request parameter.
// RemoveHandlerCmd/SecureEdgeCaseCmd: secret is looked up from the registered handler entry.
// PushHandlerContextCmd/PopHandlerContextCmd: the mushroom-url is a route URL; the handler
// entry is resolved by stripping the "command" additional property and converting to a link.
// Returns an error if validation fails, nil otherwise.
func (h *Npac) verifyHMAC(req message.RequestInterface, hash string) error {
	var secret string

	switch req.CommandName() {
	case RegisterHandler:
		s, err := req.RouteParameters().StringValue("npac-secret")
		if err != nil {
			return fmt.Errorf("npac-secret param: %w", err)
		}
		secret = s
	case RemoveHandler:
		mushroomURL, err := req.RouteParameters().StringValue("mushroom-url")
		if err != nil {
			return fmt.Errorf("mushroom-url param: %w", err)
		}
		entry, exists := h.handlers[mushroomURL]
		if !exists {
			return fmt.Errorf("handler %q not registered", mushroomURL)
		}
		secret = entry.NpacSecret
	case SecureEdgeCase, PushHandlerContext, PopHandlerContext:
		routeURL, err := req.RouteParameters().StringValue("mushroom-url")
		if err != nil {
			return fmt.Errorf("mushroom-url param: %w", err)
		}
		handlerURL, _, err := routeURLToHandlerURL(routeURL)
		if err != nil {
			return err
		}
		entry, exists := h.handlers[handlerURL]
		if !exists {
			return fmt.Errorf("handler %q not registered", handlerURL)
		}
		secret = entry.NpacSecret
	default:
		return nil
	}

	if !message.VerifyHMAC(req.String(), secret, hash) {
		return fmt.Errorf("invalid hmac")
	}
	return nil
}

// routeURLToHandlerURL converts a route mushroom URL to its handler mushroom URL
// by removing the "command" additional property and returning the link form.
// The second return value is the command that was present, or "" if absent.
// e.g. pkg:golang/pkg#mod?command=push&service=svc  →  ("pkg:golang/pkg#mod?service=svc", "push", nil)
func routeURLToHandlerURL(routeURL string) (handlerURL, command string, err error) {
	hypha, err := (&mushroom.Soil{}).Hypha(routeURL)
	if err != nil {
		return "", "", fmt.Errorf("mushroom-url parse: %w", err)
	}
	if !hypha.URL {
		return "", "", fmt.Errorf("mushroom-url %q is not a mushroom URL", routeURL)
	}
	command = hypha.AdditionalProps["command"]
	delete(hypha.AdditionalProps, "command")
	return hypha.AsLink().String(), command, nil
}

// onRegisterHandler registers a handler by its mushroom URL, npac secret, and control endpoint.
func (h *Npac) onRegisterHandler(req message.RequestInterface) message.ReplyInterface {
	mushroomURL, err := req.RouteParameters().StringValue("mushroom-url")
	if err != nil {
		return req.Fail(fmt.Sprintf("mushroom-url param: %v", err))
	}
	npacSecret, err := req.RouteParameters().StringValue("npac-secret")
	if err != nil {
		return req.Fail(fmt.Sprintf("npac-secret param: %v", err))
	}
	controlKV, err := req.RouteParameters().NestedValue("control-endpoint")
	if err != nil {
		return req.Fail(fmt.Sprintf("control-endpoint param: %v", err))
	}
	var controlEndpoint message.Endpoint
	if err := controlKV.Interface(&controlEndpoint); err != nil {
		return req.Fail(fmt.Sprintf("control-endpoint: %v", err))
	}

	if _, exists := h.handlers[mushroomURL]; exists {
		return req.Fail(fmt.Sprintf("handler %q already registered", mushroomURL))
	}

	h.handlers[mushroomURL] = &handlerEntry{
		NpacSecret:      npacSecret,
		ControlEndpoint: controlEndpoint,
	}

	return req.Ok(datatype.New())
}

// onRemoveHandler deletes the handler entry for the given mushroom URL.
// HMAC is verified in dispatch before this function is reached.
func (h *Npac) onRemoveHandler(req message.RequestInterface) message.ReplyInterface {
	mushroomURL, _ := req.RouteParameters().StringValue("mushroom-url")
	delete(h.handlers, mushroomURL)

	// Remove any whitelist entries across all outbounds whose URL starts with mushroomURL.
	for _, ob := range h.outbounds {
		for cmd, urls := range ob.Whitelist {
			filtered := urls[:0]
			for _, u := range urls {
				if !strings.HasPrefix(u, mushroomURL) {
					filtered = append(filtered, u)
				}
			}
			ob.Whitelist[cmd] = filtered
		}
	}

	return req.Ok(datatype.New())
}

// onRegisterOutbound registers an outbound handler entry keyed by its handler URL.
// Parameters: endpoint (nested {id, port}), mushroom-url (pkg: URL), public-key (string).
func (h *Npac) onRegisterOutbound(req message.RequestInterface) message.ReplyInterface {
	endpointKV, err := req.RouteParameters().NestedValue("endpoint")
	if err != nil {
		return req.Fail(fmt.Sprintf("endpoint param: %v", err))
	}
	var endpoint message.Endpoint
	if err := endpointKV.Interface(&endpoint); err != nil {
		return req.Fail(fmt.Sprintf("endpoint: %v", err))
	}
	mushroomURL, err := req.RouteParameters().StringValue("mushroom-url")
	if err != nil {
		return req.Fail(fmt.Sprintf("mushroom-url param: %v", err))
	}
	pubKey, _ := req.RouteParameters().StringValue("public-key")

	handlerURL := endpoint.HandlerUrl()

	hypha, err := (&mushroom.Soil{}).Hypha(mushroomURL)
	if err != nil {
		return req.Fail(fmt.Sprintf("mushroom-url parse: %v", err))
	}
	if !hypha.URL {
		return req.Fail(fmt.Sprintf("mushroom-url %q is not a mushroom URL", mushroomURL))
	}

	if _, exists := h.outbounds[handlerURL]; exists {
		return req.Fail(fmt.Sprintf("outbound for endpoint %q already registered", handlerURL))
	}
	h.outbounds[handlerURL] = &outbound{
		PublicKey:   pubKey,
		MushroomURL: mushroomURL,
	}
	h.mushroomUrlToOutbounds[mushroomURL] = handlerURL

	return req.Ok(datatype.New())
}

// onSecureEdgeCase adds a mushroom URL to a specific command's whitelist on an
// outbound. HMAC is verified using the npac secret of the handler whose mushroom
// URL is the base of the "outbound" route URL.
//
// Parameters:
//   - outbound:     route URL whose base identifies the outbound and whose
//     "command" additional property names the whitelist key
//   - mushroom-url: mushroom URL to add to the command's whitelist
func (h *Npac) onSecureEdgeCase(req message.RequestInterface) message.ReplyInterface {
	outbound, err := req.RouteParameters().StringValue("outbound")
	if err != nil {
		return req.Fail(fmt.Sprintf("outbound param: %v", err))
	}
	mushroomURL, err := req.RouteParameters().StringValue("mushroom-url")
	if err != nil {
		return req.Fail(fmt.Sprintf("mushroom-url param: %v", err))
	}

	outboundMushroomURL, command, err := routeURLToHandlerURL(outbound)
	if err != nil {
		return req.Fail(fmt.Sprintf("outbound parse: %v", err))
	}

	handlerURL, exists := h.mushroomUrlToOutbounds[outboundMushroomURL]
	if !exists {
		return req.Fail(fmt.Sprintf("outbound for mushroom-url %q not found", outboundMushroomURL))
	}
	ob := h.outbounds[handlerURL]

	if ob.Whitelist == nil {
		ob.Whitelist = make(map[string][]string)
	}

	if _, commandExists := ob.Whitelist[command]; !commandExists {
		ob.Whitelist[command] = []string{}
	}

	urls := ob.Whitelist[command]
	if slices.Contains(urls, mushroomURL) {
		return req.Fail(fmt.Sprintf("mushroom-url %q already whitelisted for command %q", mushroomURL, command))
	}

	ob.Whitelist[command] = append(urls, mushroomURL)
	return req.Ok(datatype.New())
}

// onPushHandlerContext appends the given route mushroom URL onto the context stack.
// HMAC is verified via the handler resolved by stripping the "command" additional
// property from the route URL.
func (h *Npac) onPushHandlerContext(req message.RequestInterface) message.ReplyInterface {
	mushroomURL, err := req.RouteParameters().StringValue("mushroom-url")
	if err != nil {
		return req.Fail(fmt.Sprintf("mushroom-url param: %v", err))
	}
	h.contexts = append(h.contexts, mushroomURL)
	return req.Ok(datatype.New())
}

// onPopHandlerContext checks that the top of the context stack matches
// mushroom-url and, if so, removes it. Returns an error when the stack is
// empty or the top does not match.
func (h *Npac) onPopHandlerContext(req message.RequestInterface) message.ReplyInterface {
	mushroomURL, err := req.RouteParameters().StringValue("mushroom-url")
	if err != nil {
		return req.Fail(fmt.Sprintf("mushroom-url param: %v", err))
	}
	if len(h.contexts) == 0 {
		return req.Fail("context stack is empty")
	}
	top := h.contexts[len(h.contexts)-1]
	if top != mushroomURL {
		return req.Fail(fmt.Sprintf("context top %q does not match %q", top, mushroomURL))
	}
	h.contexts = h.contexts[:len(h.contexts)-1]
	return req.Ok(datatype.New())
}

// onHandlerContext is the public route called by clients to resolve whether a
// given entrypoint/cmd pair is reachable from the current handler context.
//
// Parameters:
//   - entrypoint: route mushroom URL identifying the caller's outbound
//   - cmd:        command the caller intends to invoke
//
// Reply parameters on success (unregistered=false):
//   - unregistered  bool
//   - public-key    string
//   - control-endpoint nested Endpoint
//
// Failure modes:
//   - unregistered=true when the entrypoint is not in npac or cmd has no whitelist entry
//   - error "cross-access-denied: …" when the last context is not authorised
func (h *Npac) onHandlerContext(req message.RequestInterface) message.ReplyInterface {
	endpointKV, err := req.RouteParameters().NestedValue("endpoint")
	if err != nil {
		return req.Fail(fmt.Sprintf("entrypoint param: %v", err))
	}
	var endpoint message.Endpoint
	if err := endpointKV.Interface(&endpoint); err != nil {
		return req.Fail(fmt.Sprintf("entrypoint: %v", err))
	}
	cmd, err := req.RouteParameters().StringValue("command")
	if err != nil {
		return req.Fail(fmt.Sprintf("command param: %v", err))
	}

	// Resolve the outbound registered for this endpoint.
	ob, exists := h.outbounds[endpoint.HandlerUrl()]
	if !exists {
		return req.Ok(datatype.New().Set("unregistered", true))
	}

	// Confirm the command (or the catch-all) has a whitelist entry.
	_, cmdWhitelisted := ob.Whitelist[cmd]
	_, anyWhitelisted := ob.Whitelist[message.Any]
	if !cmdWhitelisted && !anyWhitelisted {
		return req.Ok(datatype.New().Set("unregistered", true))
	}

	// Verify the last handler context is authorised to call this command.
	if len(h.contexts) == 0 {
		return req.Fail("no-context, please call it within the handler context")
	}
	lastContext := h.contexts[len(h.contexts)-1]

	inCmdWhitelist := cmdWhitelisted && slices.Contains(ob.Whitelist[cmd], lastContext)
	if !inCmdWhitelist {
		if !anyWhitelisted || !slices.Contains(ob.Whitelist[message.Any], lastContext) {
			return req.Fail(fmt.Sprintf("cross-access-denied: command doesn't support to be accessed from the '%s' context", lastContext))
		}
	}

	// Resolve the handler entry for the last context to obtain the control endpoint.
	contextHandlerURL, _, err := routeURLToHandlerURL(lastContext)
	if err != nil {
		return req.Fail(fmt.Sprintf("context resolve: %v", err))
	}
	entry, exists := h.handlers[contextHandlerURL]
	if !exists {
		return req.Fail(fmt.Sprintf("handler %q not registered", contextHandlerURL))
	}

	controlKV, err := datatype.NewFromInterface(entry.ControlEndpoint)
	if err != nil {
		return req.Fail(fmt.Sprintf("control-endpoint serialize: %v", err))
	}

	return req.Ok(datatype.New().
		Set("unregistered", false).
		Set("public-key", ob.PublicKey).
		Set("control-endpoint", controlKV))
}

// onRemoveOutbound removes an outbound entry by handler URL.
func (h *Npac) onRemoveOutbound(req message.RequestInterface) message.ReplyInterface {
	entrypointKV, err := req.RouteParameters().NestedValue("entrypoint")
	if err != nil {
		return req.Fail(fmt.Sprintf("entrypoint param: %v", err))
	}
	var entrypoint message.Endpoint
	if err := entrypointKV.Interface(&entrypoint); err != nil {
		return req.Fail(fmt.Sprintf("entrypoint: %v", err))
	}
	handlerURL := entrypoint.HandlerUrl()

	entry, exists := h.outbounds[handlerURL]
	if !exists {
		return req.Fail(fmt.Sprintf("outbound for entrypoint %q not found", handlerURL))
	}
	i := 0
	for cmd := range entry.Whitelist {
		for range entry.Whitelist[cmd] {
			i++
		}
	}
	if i > 0 {
		return req.Fail(fmt.Sprintf("outbound for entrypoint %q has %d whitelisted mushroom URLs, please remove them first", handlerURL, i))
	}

	delete(h.mushroomUrlToOutbounds, entry.MushroomURL)
	delete(h.outbounds, handlerURL)

	return req.Ok(datatype.New())
}
