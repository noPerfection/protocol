package config

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestConfigSuite struct {
	suite.Suite
}

func (test *TestConfigSuite) SetupTest() {
}

// Test_10_IsValid tests handler type validation.
func (test *TestConfigSuite) Test_10_IsValid() {
	s := test.Suite.Require

	s().Error(IsValid(UnknownType))
	s().NoError(IsValid(PublisherType))
}

// Test_11_IsLocal tests the handler is remote or not with Handler.
func (test *TestConfigSuite) Test_11_IsLocal() {
	s := test.Require

	id := "example.com"
	category := "category"
	port := uint64(6000)

	// Testing the remote handler
	handler := New(SyncReplierType, id, category, port)
	s().Equal(category, handler.Category)
	s().Equal(id, handler.Id)
	s().Equal(port, handler.Port)
	s().False(handler.IsInproc())

	// Testing the inproc handler
	handler = New(SyncReplierType, id, category, 0)
	s().Equal(category, handler.Category)
	s().Equal(id, handler.Id)
	s().True(handler.IsInproc())
}

// a normal test function and pass our suite to suite.Run
func TestConfig(t *testing.T) {
	suite.Run(t, new(TestConfigSuite))
}
