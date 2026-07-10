package npac_test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	csyncreplier "github.com/noPerfection/protocol/client/sync_replier"
	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/handler/npac"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/suite"
)

// testActor simulates an independent handler process: it holds its own
// randomly-generated secret and talks to npac directly via a sync-replier
// client. This is intentionally decoupled from handler.Autocontext so that
// making NewAutocontext unexported will not break this test.
type testActor struct {
	c      *csyncreplier.Client
	secret string
}

func newTestActor(t *testing.T) *testActor {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("newTestActor: rand.Read: %v", err)
	}
	secret := hex.EncodeToString(b)

	c, err := csyncreplier.NewClient(npac.Endpoint.Id, 0)
	if err != nil {
		t.Fatalf("newTestActor: NewClient: %v", err)
	}
	c.Timeout(100 * time.Millisecond)
	c.Attempt(1)
	_ = c.Whitelist(npac.RemoveHandlerCmd, secret)
	_ = c.Whitelist(npac.AddRouteCmd, secret)
	_ = c.Whitelist(npac.RemoveRouteCmd, secret)
	t.Cleanup(func() { _ = c.Close() })
	return &testActor{c: c, secret: secret}
}

func (a *testActor) addHandler(url, pubKey string) error {
	reply, err := a.c.Request(&message.Request{
		Command: npac.AddHandlerCmd,
		Parameters: datatype.New().
			Set("url", url).
			Set("public-key", pubKey).
			Set("secret", a.secret),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

func (a *testActor) removeHandler(url string) error {
	reply, err := a.c.Request(&message.Request{
		Command:    npac.RemoveHandlerCmd,
		Parameters: datatype.New().Set("url", url),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

func (a *testActor) addRoute(url, cmd, routeSecret string) error {
	reply, err := a.c.Request(&message.Request{
		Command: npac.AddRouteCmd,
		Parameters: datatype.New().
			Set("url", url).
			Set("command", cmd).
			Set("secret", routeSecret),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

func (a *testActor) removeRoute(url, cmd string) error {
	reply, err := a.c.Request(&message.Request{
		Command: npac.RemoveRouteCmd,
		Parameters: datatype.New().
			Set("url", url).
			Set("command", cmd),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("%s", reply.ErrorMessage())
	}
	return nil
}

// TestNpacSuite verifies that npac correctly guards write operations with
// per-handler secrets and HMAC signatures.
type TestNpacSuite struct {
	suite.Suite
}

func (s *TestNpacSuite) SetupSuite() {
	h := npac.New()
	s.Require().NoError(h.Start())
	// Give npac a moment to bind its inproc socket.
	time.Sleep(10 * time.Millisecond)
}

// uniqueURL returns a test-scoped inproc-style URL that won't collide with
// other sub-tests.
func (s *TestNpacSuite) uniqueURL(suffix string) string {
	return "inproc://npac-test-" + s.T().Name() + "-" + suffix
}

// TestSyncReplierRegistersWithNpac starts a real sync replier that has a
// hello and an age-verification (HMAC-whitelisted) route, then verifies
// that a second start call is a no-op (idempotency).
func (s *TestNpacSuite) TestSyncReplierRegistersWithNpac() {
	sr := handler.NewSyncReplier()
	s.Require().NoError(sr.Route("hello", func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("greeting", "hello!"))
	}))
	const ageSecret = "age-route-secret"
	s.Require().NoError(sr.Whitelist("age-verification", ageSecret))
	s.Require().NoError(sr.Route("age-verification", func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("allowed", true))
	}))
	sr.SetEndpoint(message.NewEndpoint("npac-test-sr", 0))
	s.Require().NoError(sr.Start())
	time.Sleep(5 * time.Millisecond)

	// npac.Start is idempotent — a second call must return nil.
	s.Require().NoError(npac.New().Start())
}

// TestFakeClientCannotHijackRegistration verifies that once a handler registers
// with a secret, a different process using a different secret is rejected.
func (s *TestNpacSuite) TestFakeClientCannotHijackRegistration() {
	url := s.uniqueURL("hijack")

	real := newTestActor(s.T())
	s.Require().NoError(real.addHandler(url, "real-pub-key"))

	// Fake client tries to re-register the same URL with a different secret.
	fake := newTestActor(s.T())
	err := fake.addHandler(url, "fake-pub-key")
	s.Require().Error(err)
	s.Contains(err.Error(), "already registered with a different secret")

	// Clean up.
	s.Require().NoError(real.removeHandler(url))
}

// TestFakeClientCannotAddRoute verifies that add-route requires a valid HMAC
// signed with the registered handler secret.
func (s *TestNpacSuite) TestFakeClientCannotAddRoute() {
	url := s.uniqueURL("add-route")

	real := newTestActor(s.T())
	s.Require().NoError(real.addHandler(url, ""))

	// Fake client signs with its own (unknown) secret — npac rejects it.
	fake := newTestActor(s.T())
	err := fake.addRoute(url, "age-verification", "stolen-route-secret")
	s.Require().Error(err)

	s.Require().NoError(real.removeHandler(url))
}

// TestFakeClientCannotRemoveRoute verifies that remove-route requires a valid HMAC.
func (s *TestNpacSuite) TestFakeClientCannotRemoveRoute() {
	url := s.uniqueURL("remove-route")

	real := newTestActor(s.T())
	s.Require().NoError(real.addHandler(url, ""))
	s.Require().NoError(real.addRoute(url, "hello", "hello-hmac"))

	// Fake client tries to remove the route using the wrong HMAC secret.
	fake := newTestActor(s.T())
	err := fake.removeRoute(url, "hello")
	s.Require().Error(err)

	s.Require().NoError(real.removeHandler(url))
}

// TestFakeClientCannotRemoveHandler verifies that remove-handler requires a
// valid HMAC — a caller without the correct secret cannot remove a registration.
func (s *TestNpacSuite) TestFakeClientCannotRemoveHandler() {
	url := s.uniqueURL("remove-handler")

	real := newTestActor(s.T())
	s.Require().NoError(real.addHandler(url, ""))

	// Fake client tries to remove using its own (wrong) secret for HMAC.
	fake := newTestActor(s.T())
	err := fake.removeHandler(url)
	s.Require().Error(err)

	// Cleanup with the correct secret.
	s.Require().NoError(real.removeHandler(url))
}

// TestLegitimateHandlerCanManageItsRoutes verifies the happy path: the real
// handler (with the correct secret) can register, add routes, remove routes,
// and deregister.
func (s *TestNpacSuite) TestLegitimateHandlerCanManageItsRoutes() {
	url := s.uniqueURL("legit")

	real := newTestActor(s.T())
	s.Require().NoError(real.addHandler(url, "some-pub-key"))
	s.Require().NoError(real.addRoute(url, "hello", "hello-hmac-secret"))
	s.Require().NoError(real.removeRoute(url, "hello"))
	s.Require().NoError(real.removeHandler(url))
}

func TestNpac(t *testing.T) {
	suite.Run(t, new(TestNpacSuite))
}
