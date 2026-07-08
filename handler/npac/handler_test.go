package npac_test

import (
	"testing"
	"time"

	"github.com/noPerfection/protocol/handler/autocontext"
	"github.com/noPerfection/protocol/handler/npac"
	"github.com/noPerfection/protocol/handler/sync_replier"
	"github.com/noPerfection/protocol/message"
	"github.com/noPerfection/datatype"
	"github.com/stretchr/testify/suite"
)

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
	sr := sync_replier.New()
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
	realSecret := "real-handler-secret"

	s.Require().NoError(autocontext.AddHandler(url, "real-pub-key", realSecret))

	// Fake client tries to re-register the same URL with a different secret.
	err := autocontext.AddHandler(url, "fake-pub-key", "fake-secret")
	s.Require().Error(err)
	s.Contains(err.Error(), "already registered with a different secret")

	// Clean up.
	s.Require().NoError(autocontext.RemoveHandler(url, realSecret))
}

// TestFakeClientCannotAddRoute verifies that add-route requires a valid HMAC
// signed with the registered handler secret.
func (s *TestNpacSuite) TestFakeClientCannotAddRoute() {
	url := s.uniqueURL("add-route")
	realSecret := "real-secret-for-add-route"

	s.Require().NoError(autocontext.AddHandler(url, "", realSecret))

	// Fake client signs with its own (unknown) secret — npac rejects it.
	err := autocontext.AddRoute(url, "age-verification", "stolen-route-secret", "attacker-secret")
	s.Require().Error(err)

	s.Require().NoError(autocontext.RemoveHandler(url, realSecret))
}

// TestFakeClientCannotRemoveRoute verifies that remove-route requires a valid HMAC.
func (s *TestNpacSuite) TestFakeClientCannotRemoveRoute() {
	url := s.uniqueURL("remove-route")
	realSecret := "real-secret-for-remove-route"

	s.Require().NoError(autocontext.AddHandler(url, "", realSecret))
	s.Require().NoError(autocontext.AddRoute(url, "hello", "hello-hmac", realSecret))

	// Fake client tries to remove the route using the wrong HMAC secret.
	err := autocontext.RemoveRoute(url, "hello", "attacker-secret")
	s.Require().Error(err)

	s.Require().NoError(autocontext.RemoveHandler(url, realSecret))
}

// TestFakeClientCannotRemoveHandler verifies that remove-handler requires a
// valid HMAC — a caller without the correct secret cannot remove a registration.
func (s *TestNpacSuite) TestFakeClientCannotRemoveHandler() {
	url := s.uniqueURL("remove-handler")
	realSecret := "real-secret-for-remove-handler"

	s.Require().NoError(autocontext.AddHandler(url, "", realSecret))

	// Fake client tries to remove using its own (wrong) secret for HMAC.
	err := autocontext.RemoveHandler(url, "attacker-secret")
	s.Require().Error(err)

	// Cleanup with the correct secret.
	s.Require().NoError(autocontext.RemoveHandler(url, realSecret))
}

// TestLegitimateHandlerCanManageItsRoutes verifies the happy path: the real
// handler (with the correct secret) can register, add routes, remove routes,
// and deregister.
func (s *TestNpacSuite) TestLegitimateHandlerCanManageItsRoutes() {
	url := s.uniqueURL("legit")
	realSecret := "legitimate-secret"

	s.Require().NoError(autocontext.AddHandler(url, "some-pub-key", realSecret))
	s.Require().NoError(autocontext.AddRoute(url, "hello", "hello-hmac-secret", realSecret))
	s.Require().NoError(autocontext.RemoveRoute(url, "hello", realSecret))
	s.Require().NoError(autocontext.RemoveHandler(url, realSecret))
}

func TestNpac(t *testing.T) {
	suite.Run(t, new(TestNpacSuite))
}
