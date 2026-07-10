package npac_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	csyncreplier "github.com/noPerfection/protocol/client/sync_replier"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/handler/npac"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/suite"
)

// testActor simulates an independent handler process that talks to npac
// directly via a sync-replier client, signing its requests with its own
// internally-generated npac secret.
type testActor struct {
	c      *csyncreplier.Client
	secret string
}

func newTestActor(t *testing.T) *testActor {
	t.Helper()

	secret := handler.GenerateSecret()

	c, err := csyncreplier.NewClient(npac.Endpoint.Id, 0)
	if err != nil {
		t.Fatalf("newTestActor: NewClient: %v", err)
	}
	c.Timeout(100 * time.Millisecond)
	c.Attempt(1)
	_ = c.Whitelist(npac.AddHandlerCmd, secret)
	_ = c.Whitelist(npac.RemoveHandlerCmd, secret)
	_ = c.Whitelist(npac.SecureEdgeCaseCmd, secret)
	_ = c.Whitelist(npac.PushHandlerContextCmd, secret)
	_ = c.Whitelist(npac.PopHandlerContextCmd, secret)
	t.Cleanup(func() { _ = c.Close() })
	return &testActor{c: c, secret: secret}
}

func (a *testActor) addHandler(mushroomURL string, controlEndpoint message.Endpoint) error {
	controlKV, err := datatype.NewFromInterface(controlEndpoint)
	if err != nil {
		return fmt.Errorf("addHandler: NewFromInterface: %w", err)
	}
	reply, err := a.c.Request(&message.Request{
		Command: npac.AddHandlerCmd,
		Parameters: datatype.New().
			Set("mushroom-url", mushroomURL).
			Set("npac-secret", a.secret).
			Set("control-endpoint", controlKV),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

func (a *testActor) addOutbound(endpoint message.Endpoint, mushroomURL, publicKey string) error {
	endpointKV, err := datatype.NewFromInterface(endpoint)
	if err != nil {
		return fmt.Errorf("addOutbound: NewFromInterface: %w", err)
	}
	reply, err := a.c.Request(&message.Request{
		Command: npac.AddOutboundCmd,
		Parameters: datatype.New().
			Set("endpoint", endpointKV).
			Set("mushroom-url", mushroomURL).
			Set("public-key", publicKey),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

// secureEdgeCase sends SecureEdgeCaseCmd.
// outbound is a route URL whose base identifies the outbound and whose
// "command" additional property names the whitelist key.
// mushroomURL is the URL to add to that command's whitelist.
func (a *testActor) secureEdgeCase(outbound, mushroomURL string) error {
	reply, err := a.c.Request(&message.Request{
		Command: npac.SecureEdgeCaseCmd,
		Parameters: datatype.New().
			Set("outbound", outbound).
			Set("mushroom-url", mushroomURL),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

func (a *testActor) pushHandlerContext(routeURL string) error {
	reply, err := a.c.Request(&message.Request{
		Command:    npac.PushHandlerContextCmd,
		Parameters: datatype.New().Set("mushroom-url", routeURL),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

func (a *testActor) popHandlerContext(routeURL string) error {
	reply, err := a.c.Request(&message.Request{
		Command:    npac.PopHandlerContextCmd,
		Parameters: datatype.New().Set("mushroom-url", routeURL),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

func (a *testActor) handlerContext(endpoint message.Endpoint, cmd string) (message.ReplyInterface, error) {
	endpointKV, err := datatype.NewFromInterface(endpoint)
	if err != nil {
		return nil, fmt.Errorf("handlerContext: NewFromInterface: %w", err)
	}
	return a.c.Request(&message.Request{
		Command: npac.HandlerContextCmd,
		Parameters: datatype.New().
			Set("entrypoint", endpointKV).
			Set("cmd", cmd),
	})
}

func (a *testActor) removeHandler(mushroomURL string) error {
	reply, err := a.c.Request(&message.Request{
		Command:    npac.RemoveHandlerCmd,
		Parameters: datatype.New().Set("mushroom-url", mushroomURL),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

// TestNpacSuite covers handler registration and HMAC protection.
type TestNpacSuite struct {
	suite.Suite
	h *npac.Npac
}

func (s *TestNpacSuite) SetupSuite() {
	s.h = npac.New()
	s.Require().NoError(s.h.Start())
	// Give npac a moment to bind its inproc socket.
	time.Sleep(10 * time.Millisecond)
}

// uniqueMushroomURL returns a test-scoped pkg: mushroom URL that won't collide
// with other sub-tests.
func (s *TestNpacSuite) uniqueMushroomURL(suffix string) string {
	return "pkg:golang/npac-test#" + s.T().Name() + "-" + suffix
}

// TestAddAndRemoveHandler verifies the happy path: a handler can register
// with its own npac secret and later remove itself.
func (s *TestNpacSuite) TestAddAndRemoveHandler() {
	mushroomURL := s.uniqueMushroomURL("happy")
	controlEndpoint := message.NewEndpoint("npac-test-control", 0)

	actor := newTestActor(s.T())
	s.Require().NoError(actor.addHandler(mushroomURL, controlEndpoint))
	s.Require().NoError(actor.removeHandler(mushroomURL))
}

// TestAddHandlerWithWrongHMAC verifies that a request whose HMAC was signed
// with a different secret than the one declared in npac-secret is rejected.
func (s *TestNpacSuite) TestAddHandlerWithWrongHMAC() {
	mushroomURL := s.uniqueMushroomURL("wrong-hmac")
	controlEndpoint := message.NewEndpoint("npac-test-control-wrong", 0)

	// Build a client that signs AddHandlerCmd with a different secret than
	// what it declares in the npac-secret parameter.
	declaredSecret := handler.GenerateSecret()
	signingSecret := handler.GenerateSecret() // intentionally different

	c, err := csyncreplier.NewClient(npac.Endpoint.Id, 0)
	s.Require().NoError(err)
	c.Timeout(100 * time.Millisecond)
	c.Attempt(1)
	_ = c.Whitelist(npac.AddHandlerCmd, signingSecret) // signs with signingSecret
	defer func() { _ = c.Close() }()

	controlKV, err := datatype.NewFromInterface(controlEndpoint)
	s.Require().NoError(err)

	reply, err := c.Request(&message.Request{
		Command: npac.AddHandlerCmd,
		Parameters: datatype.New().
			Set("mushroom-url", mushroomURL).
			Set("npac-secret", declaredSecret). // HMAC will be verified against this
			Set("control-endpoint", controlKV),
	})
	s.Require().NoError(err)
	s.Require().False(reply.IsOK())
	s.Contains(reply.ErrorMessage(), "invalid hmac")
}

// TestSecureEdgeCase verifies the SecureEdgeCaseCmd route end-to-end.
// The outbound mushroom URL is also registered as a handler so that the HMAC
// verification (which derives the handler from the "outbound" route URL) can
// find the npac secret.
func (s *TestNpacSuite) TestSecureEdgeCase() {
	// callerURL is the actor's own mushroom URL — used for HMAC and as the whitelisted entry.
	callerURL := s.uniqueMushroomURL("sec-edge-caller")
	outboundURL := s.uniqueMushroomURL("sec-edge-outbound")
	outboundEndpoint := message.NewEndpoint("npac-sec-edge-outbound", 0)
	const cmd = "my-command"
	// outbound route URL: base outbound URL + command as an additional property.
	outboundRouteURL := outboundURL + "?command=" + cmd

	actor := newTestActor(s.T())

	// Register the CALLER as a handler so HMAC verification can find the secret.
	s.Require().NoError(actor.addHandler(callerURL, message.NewEndpoint("npac-sec-edge-control", 0)))
	// Register the outbound so the whitelist lookup can find it.
	s.Require().NoError(actor.addOutbound(outboundEndpoint, outboundURL, ""))

	s.Run("outbound not found", func() {
		noSuchRoute := s.uniqueMushroomURL("no-such-outbound") + "?command=" + cmd
		err := actor.secureEdgeCase(noSuchRoute, callerURL)
		s.Require().Error(err)
		s.Contains(err.Error(), "not found")
	})

	s.Run("success adds caller url to command whitelist", func() {
		s.Require().NoError(actor.secureEdgeCase(outboundRouteURL, callerURL))
	})

	s.Run("idempotency: adding same caller url again fails", func() {
		err := actor.secureEdgeCase(outboundRouteURL, callerURL)
		s.Require().Error(err)
		s.Contains(err.Error(), "already whitelisted")
	})

	s.Run("different url can be added to same command", func() {
		otherCaller := s.uniqueMushroomURL("sec-edge-other-caller")
		otherActor := newTestActor(s.T())
		s.Require().NoError(otherActor.addHandler(otherCaller, message.NewEndpoint("npac-sec-edge-other", 0)))
		s.Require().NoError(otherActor.secureEdgeCase(outboundRouteURL, otherCaller))
	})

	s.Run("hmac fails for unregistered caller", func() {
		unregistered := s.uniqueMushroomURL("unregistered-caller")
		err := actor.secureEdgeCase(outboundRouteURL, unregistered)
		s.Require().Error(err)
		s.Contains(err.Error(), "not registered")
	})
}

// TestPushPopHandlerContext verifies that handler route URLs can be pushed into
// and popped out of the contexts list, with HMAC derived from the base handler URL.
func (s *TestNpacSuite) TestPushPopHandlerContext() {
	// Base handler URL (no command property).
	handlerURL := s.uniqueMushroomURL("ctx-handler")
	// Route URL: the same base URL with a "command" additional property.
	// After stripping "command", npac resolves this back to handlerURL for HMAC.
	routeURL := handlerURL + "?command=my-route"

	actor := newTestActor(s.T())
	s.Require().NoError(actor.addHandler(handlerURL, message.NewEndpoint("npac-ctx-control", 0)))

	s.Run("push succeeds", func() {
		s.Require().NoError(actor.pushHandlerContext(routeURL))
	})

	s.Run("pushing same url again is allowed (stacks)", func() {
		s.Require().NoError(actor.pushHandlerContext(routeURL))
	})

	s.Run("pop top matches and succeeds", func() {
		s.Require().NoError(actor.popHandlerContext(routeURL))
	})

	s.Run("pop wrong url fails", func() {
		otherURL := handlerURL + "?command=other"
		err := actor.popHandlerContext(otherURL)
		s.Require().Error(err)
		s.Contains(err.Error(), "does not match")
	})

	s.Run("pop remaining entry succeeds", func() {
		s.Require().NoError(actor.popHandlerContext(routeURL))
	})

	s.Run("popping empty stack fails", func() {
		err := actor.popHandlerContext(routeURL)
		s.Require().Error(err)
		s.Contains(err.Error(), "empty")
	})

	s.Run("hmac rejected for unregistered base handler", func() {
		unregisteredRouteURL := s.uniqueMushroomURL("ctx-no-handler") + "?command=my-route"
		err := actor.pushHandlerContext(unregisteredRouteURL)
		s.Require().Error(err)
		s.Contains(err.Error(), "not registered")
	})
}

// TestHandlerContext exercises the full handler-context route cycle.
// It registers an outbound, a handler (caller), whitelists the caller for a
// command, pushes the caller as the active context, and then queries npac via
// HandlerContextCmd covering every branch of onHandlerContext.
func (s *TestNpacSuite) TestHandlerContext() {
	const pubKey    = "test-pubkey-hc"
	const targetCmd = "hc-target-cmd"

	outboundURL      := s.uniqueMushroomURL("hc-outbound")
	callerURL        := s.uniqueMushroomURL("hc-caller")
	outboundRouteURL := outboundURL + "?command=" + targetCmd
	callerRouteURL   := callerURL + "?command=some-route"

	outboundEndpoint := message.NewEndpoint("npac-hc-outbound", 0)
	controlEndpoint  := message.NewEndpoint("npac-hc-control", 0)

	actor := newTestActor(s.T())
	s.Require().NoError(actor.addOutbound(outboundEndpoint, outboundURL, pubKey))
	s.Require().NoError(actor.addHandler(callerURL, controlEndpoint))
	// Whitelist callerRouteURL for targetCmd on the outbound.
	s.Require().NoError(actor.secureEdgeCase(outboundRouteURL, callerRouteURL))

	s.Run("unregistered entrypoint", func() {
		reply, err := actor.handlerContext(message.NewEndpoint("npac-hc-unknown", 0), targetCmd)
		s.Require().NoError(err)
		s.Require().True(reply.IsOK())
		unregistered, err := reply.ReplyParameters().BoolValue("unregistered")
		s.Require().NoError(err)
		s.True(unregistered)
	})

	s.Run("cmd not whitelisted", func() {
		reply, err := actor.handlerContext(outboundEndpoint, "unknown-cmd")
		s.Require().NoError(err)
		s.Require().True(reply.IsOK())
		unregistered, err := reply.ReplyParameters().BoolValue("unregistered")
		s.Require().NoError(err)
		s.True(unregistered)
	})

	s.Run("no context", func() {
		reply, err := actor.handlerContext(outboundEndpoint, targetCmd)
		s.Require().NoError(err)
		s.Require().False(reply.IsOK())
		s.Contains(reply.ErrorMessage(), "no-context")
	})

	s.Run("cross-access-denied", func() {
		// Register a handler that is NOT whitelisted on the outbound.
		deniedURL      := s.uniqueMushroomURL("hc-denied")
		deniedRouteURL := deniedURL + "?command=some-route"
		deniedActor    := newTestActor(s.T())
		s.Require().NoError(deniedActor.addHandler(deniedURL, message.NewEndpoint("npac-hc-denied-ctl", 0)))
		s.Require().NoError(deniedActor.pushHandlerContext(deniedRouteURL))
		defer func() { _ = deniedActor.popHandlerContext(deniedRouteURL) }()

		reply, err := actor.handlerContext(outboundEndpoint, targetCmd)
		s.Require().NoError(err)
		s.Require().False(reply.IsOK())
		s.Contains(reply.ErrorMessage(), "cross-access-denied")
	})

	s.Run("success via cmd whitelist", func() {
		s.Require().NoError(actor.pushHandlerContext(callerRouteURL))
		defer func() { _ = actor.popHandlerContext(callerRouteURL) }()

		reply, err := actor.handlerContext(outboundEndpoint, targetCmd)
		s.Require().NoError(err)
		s.Require().True(reply.IsOK(), reply.ErrorMessage())

		unregistered, err := reply.ReplyParameters().BoolValue("unregistered")
		s.Require().NoError(err)
		s.False(unregistered)

		pk, err := reply.ReplyParameters().StringValue("public-key")
		s.Require().NoError(err)
		s.Equal(pubKey, pk)

		controlKV, err := reply.ReplyParameters().NestedValue("control-endpoint")
		s.Require().NoError(err)
		var gotControl message.Endpoint
		s.Require().NoError(controlKV.Interface(&gotControl))
		s.Equal(controlEndpoint, gotControl)
	})

	s.Run("success via any whitelist", func() {
		anyCallerURL        := s.uniqueMushroomURL("hc-any-caller")
		anyCallerRouteURL   := anyCallerURL + "?command=some-route"
		anyOutboundURL      := s.uniqueMushroomURL("hc-any-outbound")
		anyOutboundEndpoint := message.NewEndpoint("npac-hc-any-ob", 0)
		anyControlEndpoint  := message.NewEndpoint("npac-hc-any-ctl", 0)
		const anyTestCmd    = "hc-any-cmd"

		anyActor := newTestActor(s.T())
		s.Require().NoError(anyActor.addOutbound(anyOutboundEndpoint, anyOutboundURL, pubKey))
		s.Require().NoError(anyActor.addHandler(anyCallerURL, anyControlEndpoint))
		// Whitelist under message.Any so the catch-all covers any command.
		s.Require().NoError(anyActor.secureEdgeCase(anyOutboundURL+"?command="+message.Any, anyCallerRouteURL))
		s.Require().NoError(anyActor.pushHandlerContext(anyCallerRouteURL))
		defer func() { _ = anyActor.popHandlerContext(anyCallerRouteURL) }()

		reply, err := actor.handlerContext(anyOutboundEndpoint, anyTestCmd)
		s.Require().NoError(err)
		s.Require().True(reply.IsOK(), reply.ErrorMessage())

		unregistered, err := reply.ReplyParameters().BoolValue("unregistered")
		s.Require().NoError(err)
		s.False(unregistered)

		pk, err := reply.ReplyParameters().StringValue("public-key")
		s.Require().NoError(err)
		s.Equal(pubKey, pk)

		controlKV, err := reply.ReplyParameters().NestedValue("control-endpoint")
		s.Require().NoError(err)
		var gotAnyControl message.Endpoint
		s.Require().NoError(controlKV.Interface(&gotAnyControl))
		s.Equal(anyControlEndpoint, gotAnyControl)
	})
}

func TestNpac(t *testing.T) {
	suite.Run(t, new(TestNpacSuite))
}
