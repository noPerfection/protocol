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

// Test_10_IsValid tests setting of the route dependencies
func (test *TestConfigSuite) Test_10_IsValid() {
	s := test.Suite.Require

	s().Error(IsValid(UnknownType))
	s().NoError(IsValid(PublisherType))
}

// Test_11_IsLocal tests the handler is remote or not with Handler.
// Tests the Handler.IsInproc and Trigger.IsInprocBroadcast methods.
func (test *TestConfigSuite) Test_11_IsLocal() {
	s := test.Require

	category := "category"

	// Testing the remote handler
	handler, err := NewHandler(SyncReplierType, category)
	s().NoError(err)
	s().False(handler.IsInproc())

	trigger, err := TriggerAble(handler, PublisherType)
	s().NoError(err)
	s().False(trigger.IsInproc())
	s().False(trigger.IsInprocBroadcast())

	// Testing the inproc handler
	handler = NewInternalHandler(SyncReplierType, category)
	s().True(handler.IsInproc())

	trigger, err = TriggerAble(handler, PublisherType)
	s().NoError(err)
	s().True(trigger.IsInproc())
	s().True(trigger.IsInprocBroadcast())
}

func (test *TestConfigSuite) Test_12_ExternalUrl_tcp() {
	test.Require().Equal("tcp://*:6000", ExternalUrl("sample", 6000))
}

func (test *TestConfigSuite) Test_13_ExternalUrl_inproc() {
	test.Require().Equal("inproc://my-service", ExternalUrl("my-service", 0))
}

func (test *TestConfigSuite) Test_14_ExternalUrl_ipc_tmp() {
	test.Require().Equal("ipc:///tmp-test.sock", ExternalUrl("tmp-test.sock", 0))
}

// a normal test function and pass our suite to suite.Run
func TestConfig(t *testing.T) {
	suite.Run(t, new(TestConfigSuite))
}
