package protocoltest

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/handler/npac"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

const (
	helloCmd     = "hello"
	ageCmd       = "age"
	pingCmd      = "ping"
	pongCmd      = "pong"
	echoCmd      = "echo"
	timeCmd      = "time"
	upperCaseCmd = "upper-case"

	upperCaseSecret = "upper-case-secret"
)

var concurrentClientRoutes = []string{helloCmd, ageCmd, pingCmd, pongCmd, echoCmd, timeCmd}

type autocontextHandlers struct {
	nameUtilsEndpoint  message.Endpoint
	helloWorldEndpoint message.Endpoint
	nameUtilsMushroom  string
	helloWorldMushroom string
	nameUtilsControl   *client.Control
	helloWorldControl  *client.Control
	helloWorldClient   *client.SyncReplierClient
}

// TestAutocontextOutboundAccess verifies npac outbound context rules with
// hello-world and name-utils handlers. name-utils is open; only the hello
// route context may reach upper-case through autocontext.
func TestAutocontextOutboundAccess(t *testing.T) {
	h := startAutocontextHandlers(t, autocontextOptions{})
	defer h.close(t)

	reply, err := h.outsideUpperCase("alice")
	assertUpperCaseDenied(t, reply, err)

	reply, err = h.routeUpperCase(t, ageCmd, "bob")
	assertUpperCaseDenied(t, reply, err)

	reply, err = h.routeUpperCase(t, helloCmd, "carol")
	assertUpperCaseOK(t, reply, err, "CAROL")
}

// TestAutocontextOutboundWhitelist repeats the same client flow when name-utils
// whitelists upper-case. Client code is unchanged; only the hello route succeeds.
func TestAutocontextOutboundWhitelist(t *testing.T) {
	h := startAutocontextHandlers(t, autocontextOptions{whitelistNameUtils: true})
	defer h.close(t)

	reply, err := h.outsideUpperCase("alice")
	assertUpperCaseDenied(t, reply, err)
	reply, err = h.routeUpperCase(t, ageCmd, "bob")
	assertUpperCaseDenied(t, reply, err)
	reply, err = h.routeUpperCase(t, helloCmd, "carol")
	assertUpperCaseOK(t, reply, err, "CAROL")
}

// TestAutocontextOutboundLifecycle verifies outbound calls fail while name-utils
// is stopped and succeed again after it is restarted.
func TestAutocontextOutboundLifecycle(t *testing.T) {
	h := startAutocontextHandlers(t, autocontextOptions{
		whitelistNameUtils: true,
		secureNameUtils:    true,
	})
	defer h.close(t)

	reply, err := h.routeUpperCase(t, helloCmd, "dave")
	assertUpperCaseOK(t, reply, err, "DAVE")

	req := require.New(t)
	req.NoError(h.nameUtilsControl.HandlerClose())
	waitForStatus(t, h.nameUtilsControl, handler.SocketNil)

	reply, err = h.routeUpperCase(t, helloCmd, "erin")
	assertUpperCaseDenied(t, reply, err)

	status, err := h.nameUtilsControl.StartHandler()
	req.NoError(err)
	req.Equal(handler.SocketReady, status)
	waitForStatus(t, h.nameUtilsControl, handler.SocketReady)

	reply, err = h.routeUpperCase(t, helloCmd, "frank")
	assertUpperCaseOK(t, reply, err, "FRANK")
}

// TestAutocontextConcurrentHandlers runs two handlers with routes that sleep
// randomly (5-100ms) and call each other through npac autocontext.
// Multiple clients fire concurrent random requests to reproduce real overlap.
func TestAutocontextConcurrentHandlers(t *testing.T) {
	const (
		clientCount       = 4
		requestsPerClient = 12
		sleepMin          = 5 * time.Millisecond
		sleepMax          = 100 * time.Millisecond
	)

	h := startConcurrentAutocontextHandlers(t)
	defer h.close(t)

	var wg sync.WaitGroup
	errCh := make(chan error, clientCount*requestsPerClient)

	for clientIdx := 0; clientIdx < clientCount; clientIdx++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()

			syncClient, err := client.NewSyncReplier(h.helloWorldEndpoint.Id, h.helloWorldEndpoint.Port)
			if err != nil {
				errCh <- fmt.Errorf("client %d: NewSyncReplier: %w", clientIdx, err)
				return
			}
			defer func() { _ = syncClient.Close() }()
			syncClient.Timeout(5 * time.Second)
			syncClient.Attempt(2)

			clientRNG := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(clientIdx)))

			for reqIdx := 0; reqIdx < requestsPerClient; reqIdx++ {
				route := concurrentClientRoutes[clientRNG.Intn(len(concurrentClientRoutes))]
				value := fmt.Sprintf("c%d-r%d", clientIdx, reqIdx)

				reply, err := syncClient.Request(newRequest(route, value))
				if err != nil {
					errCh <- fmt.Errorf("client %d req %d route %s: %w", clientIdx, reqIdx, route, err)
					continue
				}
				if err := assertConcurrentRouteReply(route, value, reply); err != nil {
					errCh <- fmt.Errorf("client %d req %d route %s: %w", clientIdx, reqIdx, route, err)
				}

				time.Sleep(time.Duration(clientRNG.Intn(int(sleepMax-sleepMin)+1)) + sleepMin)
			}
		}(clientIdx)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
}

type concurrentAutocontextHandlers struct {
	npac               *npac.Npac
	nameUtilsEndpoint  message.Endpoint
	helloWorldEndpoint message.Endpoint
	nameUtilsMushroom  string
	helloWorldMushroom string
	nameUtilsControl   *client.Control
	helloWorldControl  *client.Control
}

func startConcurrentAutocontextHandlers(t *testing.T) *concurrentAutocontextHandlers {
	t.Helper()
	req := require.New(t)

	n := npac.New()
	req.NoError(n.Start())
	time.Sleep(10 * time.Millisecond)

	nameUtilsID := testID(t, "concurrent-name-utils")
	helloWorldID := testID(t, "concurrent-hello-world")
	nameUtilsEndpoint := message.NewEndpoint(nameUtilsID, 0)
	helloWorldEndpoint := message.NewEndpoint(helloWorldID, 0)
	nameUtilsMushroom := autocontextMushroomURL(nameUtilsID)
	helloWorldMushroom := autocontextMushroomURL(helloWorldID)

	var nameUtilsPublicKey string
	nameUtils := handler.NewReplier()
	nameUtils.SetEndpoint(nameUtilsEndpoint)
	nameUtils.SetMushroomURL(nameUtilsMushroom)
	req.NoError(nameUtils.Whitelist(upperCaseCmd, upperCaseSecret))
	req.NoError(nameUtils.Route(upperCaseCmd, slowUpperCaseRoute))
	req.NoError(nameUtils.Start())
	time.Sleep(10 * time.Millisecond)

	helloWorld := handler.NewSyncReplier()
	helloWorld.SetEndpoint(helloWorldEndpoint)
	helloWorld.SetMushroomURL(helloWorldMushroom)

	nameUtilsEndpointRef := nameUtilsEndpoint
	slowOutboundCall := func(r message.RequestInterface) message.ReplyInterface {
		value, err := r.RouteParameters().StringValue("value")
		if err != nil {
			return r.Fail(err.Error())
		}
		randomSleep()
		reply, err := requestUpperCaseViaHandler(helloWorld, nameUtilsEndpointRef, value)
		if err != nil {
			return r.Fail(err.Error())
		}
		if !reply.IsOK() {
			return r.Fail(reply.ErrorMessage())
		}
		return r.Ok(reply.ReplyParameters())
	}

	req.NoError(helloWorld.Route(helloCmd, slowOutboundCall))
	req.NoError(helloWorld.Route(ageCmd, slowOutboundCall))
	req.NoError(helloWorld.Route(pingCmd, slowOutboundCall))
	req.NoError(helloWorld.Route(pongCmd, slowOutboundCall))
	req.NoError(helloWorld.Route(echoCmd, slowOutboundCall))
	req.NoError(helloWorld.Route(timeCmd, slowOutboundCall))
	req.NoError(helloWorld.Start())
	time.Sleep(10 * time.Millisecond)

	autocontext := client.NewAutocontext()
	req.NotNil(autocontext)
	defer func() { req.NoError(autocontext.Close()) }()
	req.NoError(autocontext.RegisterOutbound(nameUtilsEndpoint, nameUtilsMushroom, nameUtilsPublicKey))

	helloWorldControl := newSyncControl(t, helloWorldID)
	req.NoError(controlRegisterOutbounds(
		t,
		helloWorldControl,
		nameUtilsEndpoint,
		map[string]string{upperCaseCmd: upperCaseSecret},
	))

	outboundRouteURL := routeMushroomURL(nameUtilsMushroom, upperCaseCmd)
	req.NoError(helloWorld.NpacSecureEdgeCase(outboundRouteURL, helloCmd))

	nameUtilsControl := newReplierControl(t, nameUtilsID)

	return &concurrentAutocontextHandlers{
		npac:               n,
		nameUtilsEndpoint:  nameUtilsEndpoint,
		helloWorldEndpoint: helloWorldEndpoint,
		nameUtilsMushroom:  nameUtilsMushroom,
		helloWorldMushroom: helloWorldMushroom,
		nameUtilsControl:   nameUtilsControl,
		helloWorldControl:  helloWorldControl,
	}
}

func (h *concurrentAutocontextHandlers) close(t *testing.T) {
	t.Helper()
	if h.helloWorldControl != nil {
		closeControl(t, h.helloWorldControl)
	}
	if h.nameUtilsControl != nil {
		closeControl(t, h.nameUtilsControl)
	}
	if h.npac != nil {
		h.npac.Stop()
	}
	time.Sleep(50 * time.Millisecond)
}

func slowUpperCaseRoute(request message.RequestInterface) message.ReplyInterface {
	randomSleep()
	return upperCaseRoute(request)
}

func randomSleep() {
	sleepMin := 5 * time.Millisecond
	sleepMax := 100 * time.Millisecond
	jitter := time.Duration(rand.Intn(int(sleepMax-sleepMin)+1)) + sleepMin
	time.Sleep(jitter)
}

func assertConcurrentRouteReply(route, value string, reply message.ReplyInterface) error {
	if reply == nil {
		return fmt.Errorf("nil reply")
	}
	switch route {
	case helloCmd:
		if !reply.IsOK() {
			return fmt.Errorf("expected ok reply, got: %s", reply.ErrorMessage())
		}
		actual, err := reply.ReplyParameters().StringValue("value")
		if err != nil {
			return err
		}
		if actual != strings.ToUpper(value) {
			return fmt.Errorf("expected %q, got %q", strings.ToUpper(value), actual)
		}
	case ageCmd, pingCmd, pongCmd, echoCmd, timeCmd:
		if reply.IsOK() {
			return fmt.Errorf("expected age route to be denied, got ok with %v", reply.ReplyParameters())
		}
		msg := reply.ErrorMessage()
		if !strings.Contains(msg, message.ErrAccessDenied.Error()) &&
			!strings.Contains(msg, "cross-access-denied") &&
			!strings.Contains(msg, "no-context") {
			return fmt.Errorf("expected access denied, got: %s", msg)
		}
	default:
		return fmt.Errorf("unknown route %q", route)
	}
	return nil
}

type autocontextOptions struct {
	whitelistNameUtils bool
	secureNameUtils    bool
}

func startAutocontextHandlers(t *testing.T, opts autocontextOptions) *autocontextHandlers {
	t.Helper()
	req := require.New(t)

	n := npac.New()
	req.NoError(n.Start())
	time.Sleep(10 * time.Millisecond)

	nameUtilsID := testID(t, "name-utils")
	helloWorldID := testID(t, "hello-world")
	nameUtilsEndpoint := message.NewEndpoint(nameUtilsID, 0)
	helloWorldEndpoint := message.NewEndpoint(helloWorldID, 0)
	nameUtilsMushroom := autocontextMushroomURL(nameUtilsID)
	helloWorldMushroom := autocontextMushroomURL(helloWorldID)

	var nameUtilsPublicKey string
	nameUtils := handler.NewSyncReplier()
	nameUtils.SetEndpoint(nameUtilsEndpoint)
	nameUtils.SetMushroomURL(nameUtilsMushroom)
	if opts.secureNameUtils {
		var serverSecret string
		var err error
		_, serverSecret, err = message.GenerateCurveKey()
		req.NoError(err)
		nameUtils.Secure(serverSecret)
	}
	if opts.whitelistNameUtils {
		req.NoError(nameUtils.Whitelist(upperCaseCmd, upperCaseSecret))
	}
	req.NoError(nameUtils.Route(upperCaseCmd, upperCaseRoute))
	req.NoError(nameUtils.Start())
	time.Sleep(10 * time.Millisecond)

	helloWorld := handler.NewSyncReplier()
	helloWorld.SetEndpoint(helloWorldEndpoint)
	helloWorld.SetMushroomURL(helloWorldMushroom)

	nameUtilsEndpointRef := nameUtilsEndpoint
	req.NoError(helloWorld.Route(helloCmd, func(r message.RequestInterface) message.ReplyInterface {
		value, err := r.RouteParameters().StringValue("value")
		if err != nil {
			return r.Fail(err.Error())
		}
		reply, err := requestUpperCaseViaHandler(helloWorld, nameUtilsEndpointRef, value)
		if err != nil {
			return r.Fail(err.Error())
		}
		if !reply.IsOK() {
			return r.Fail(reply.ErrorMessage())
		}
		return r.Ok(reply.ReplyParameters())
	}))
	req.NoError(helloWorld.Route(ageCmd, func(r message.RequestInterface) message.ReplyInterface {
		value, err := r.RouteParameters().StringValue("value")
		if err != nil {
			return r.Fail(err.Error())
		}
		reply, err := requestUpperCaseViaHandler(helloWorld, nameUtilsEndpointRef, value)
		if err != nil {
			return r.Fail(err.Error())
		}
		if !reply.IsOK() {
			return r.Fail(reply.ErrorMessage())
		}
		return r.Ok(reply.ReplyParameters())
	}))
	req.NoError(helloWorld.Start())
	time.Sleep(10 * time.Millisecond)

	autocontext := client.NewAutocontext()
	req.NotNil(autocontext)
	defer func() { req.NoError(autocontext.Close()) }()
	req.NoError(autocontext.RegisterOutbound(nameUtilsEndpoint, nameUtilsMushroom, nameUtilsPublicKey))

	helloWorldControl := newSyncControl(t, helloWorldID)
	req.NoError(controlRegisterOutbounds(
		t,
		helloWorldControl,
		nameUtilsEndpoint,
		map[string]string{upperCaseCmd: upperCaseSecret},
	))

	outboundRouteURL := routeMushroomURL(nameUtilsMushroom, upperCaseCmd)
	req.NoError(helloWorld.NpacSecureEdgeCase(outboundRouteURL, helloCmd))

	helloWorldClient, err := client.NewSyncReplier(helloWorldID, 0)
	req.NoError(err)
	helloWorldClient.Timeout(3 * time.Second)
	helloWorldClient.Attempt(1)

	nameUtilsControl := newSyncControl(t, nameUtilsID)

	return &autocontextHandlers{
		nameUtilsEndpoint:  nameUtilsEndpoint,
		helloWorldEndpoint: helloWorldEndpoint,
		nameUtilsMushroom:  nameUtilsMushroom,
		helloWorldMushroom: helloWorldMushroom,
		nameUtilsControl:   nameUtilsControl,
		helloWorldControl:  helloWorldControl,
		helloWorldClient:   helloWorldClient,
	}
}

func (h *autocontextHandlers) close(t *testing.T) {
	t.Helper()
	if h.helloWorldClient != nil {
		require.NoError(t, h.helloWorldClient.Close())
	}
	if h.helloWorldControl != nil {
		closeControl(t, h.helloWorldControl)
	}
	if h.nameUtilsControl != nil {
		closeControl(t, h.nameUtilsControl)
	}
	time.Sleep(50 * time.Millisecond)
}

func (h *autocontextHandlers) outsideUpperCase(value string) (message.ReplyInterface, error) {
	return requestUpperCase(h.nameUtilsEndpoint, value)
}

func (h *autocontextHandlers) routeUpperCase(t *testing.T, route, value string) (message.ReplyInterface, error) {
	t.Helper()
	return h.helloWorldClient.Request(newRequest(route, value))
}

// requestUpperCase resolves the outbound through npac handler context, then
// calls upper-case with the secret registered on hello-world control.
func requestUpperCase(endpoint message.Endpoint, value string) (message.ReplyInterface, error) {
	req := newRequest(upperCaseCmd, value)
	return retryUpperCaseViaAutocontext(endpoint, req)
}

type handlerAutocontext interface {
	HandlerContext(message.Endpoint, string) (bool, string, message.Endpoint, error)
}

func requestUpperCaseViaHandler(caller handlerAutocontext, endpoint message.Endpoint, value string) (message.ReplyInterface, error) {
	req := newRequest(upperCaseCmd, value)
	return retryUpperCaseViaHandlerContext(caller, endpoint, req)
}

func retryUpperCaseViaHandlerContext(caller handlerAutocontext, endpoint message.Endpoint, req *message.Request) (message.ReplyInterface, error) {
	unregistered, _, _, err := caller.HandlerContext(endpoint, req.Command)
	if err != nil {
		return nil, err
	}
	if unregistered {
		return nil, fmt.Errorf("%w: '%s' not registered in 'npac'", message.ErrAccessDenied, endpoint.HandlerUrl())
	}
	return sendUpperCaseRequest(endpoint, req)
}

func retryUpperCaseViaAutocontext(endpoint message.Endpoint, req *message.Request) (message.ReplyInterface, error) {
	autocontext := client.NewAutocontext()
	if autocontext == nil {
		return nil, fmt.Errorf("failed to create autocontext")
	}
	defer func() { _ = autocontext.Close() }()

	unregistered, _, _, err := autocontext.HandlerContext(endpoint, req.Command)
	if err != nil {
		return nil, err
	}
	if unregistered {
		return nil, fmt.Errorf("%w: '%s' not registered in 'npac'", message.ErrAccessDenied, endpoint.HandlerUrl())
	}
	return sendUpperCaseRequest(endpoint, req)
}

func sendUpperCaseRequest(endpoint message.Endpoint, req *message.Request) (message.ReplyInterface, error) {
	syncClient, err := client.NewSyncReplier(endpoint.Id, endpoint.Port)
	if err != nil {
		return nil, err
	}
	defer func() { _ = syncClient.Close() }()
	syncClient.Timeout(2 * time.Second)
	syncClient.Attempt(1)
	if err := syncClient.Whitelist(req.Command, upperCaseSecret); err != nil {
		return nil, err
	}
	return syncClient.Request(req)
}

func controlRegisterOutbounds(
	t *testing.T,
	control *client.Control,
	endpoint message.Endpoint,
	commands map[string]string,
) error {
	t.Helper()
	req := require.New(t)

	endpointKV, err := datatype.NewFromInterface(endpoint)
	req.NoError(err)
	commandsKV, err := datatype.NewFromInterface(commands)
	req.NoError(err)

	reply, err := control.Request(&message.Request{
		Command: handler.HandlerRegisterOutbounds,
		Parameters: datatype.New().
			Set("endpoint", endpointKV).
			Set("commands", commandsKV),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return errReply(reply)
	}
	return nil
}

func upperCaseRoute(request message.RequestInterface) message.ReplyInterface {
	value, err := request.RouteParameters().StringValue("value")
	if err != nil {
		return request.Fail(err.Error())
	}
	return request.Ok(datatype.New().Set("value", strings.ToUpper(value)))
}

func autocontextMushroomURL(id string) string {
	return "pkg:golang/autocontext-test#" + id
}

func routeMushroomURL(mushroomURL, command string) string {
	return mushroomURL + "?command=" + command
}

func assertUpperCaseDenied(t *testing.T, reply message.ReplyInterface, err error) {
	t.Helper()
	req := require.New(t)
	if err != nil {
		return
	}
	req.NotNil(reply)
	req.False(reply.IsOK(), "expected upper-case call to fail, got: %s", reply.ErrorMessage())
}

func assertUpperCaseOK(t *testing.T, reply message.ReplyInterface, err error, expected string) {
	t.Helper()
	req := require.New(t)
	req.NoError(err)
	req.True(reply.IsOK(), "expected upper-case call to succeed, got: %s", reply.ErrorMessage())
	value, err := reply.ReplyParameters().StringValue("value")
	req.NoError(err)
	req.Equal(expected, value)
}

func errReply(reply message.ReplyInterface) error {
	return &replyError{message: reply.ErrorMessage()}
}

type replyError struct {
	message string
}

func (e *replyError) Error() string {
	return e.message
}
