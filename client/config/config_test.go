package config

import (
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
	"testing"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestConfigSuite struct {
	suite.Suite
	client *Client
}

// Make sure that Account is set to five
// before each test
func (test *TestConfigSuite) SetupTest() {
	serviceUrl := "github.com/sds-framework/service"
	id := "sample"
	port := uint64(0)
	socketType := zmq.REQ
	test.client = New(serviceUrl, id, port, socketType)
}

// Test_10_IsTarget checks the target zeromq sockets are supported
//
// Version 1 supported sockets:
//
//	zmq.REP || zmq.ROUTER || zmq.PUB || zmq.PUSH || zmq.PULL
func (test *TestConfigSuite) Test_10_IsTarget() {
	require := test.Require
	require().True(IsTarget(zmq.REP))
	require().True(IsTarget(zmq.ROUTER))
	require().True(IsTarget(zmq.PUB))
	require().True(IsTarget(zmq.PAIR))
	require().True(IsTarget(zmq.PULL))

	// Invalid
	require().False(IsTarget(zmq.REQ))
}

// Test_11_TargetToClient checks client socket derivation from target.
func (test *TestConfigSuite) Test_11_TargetToClient() {
	require := test.Require

	require().Equal(zmq.REQ, TargetToClient(zmq.REP))
	require().Equal(zmq.REQ, TargetToClient(zmq.ROUTER))
	require().Equal(zmq.SUB, TargetToClient(zmq.PUB))
	require().Equal(zmq.PAIR, TargetToClient(zmq.PAIR))
	require().Equal(zmq.PUSH, TargetToClient(zmq.PULL))

	// Invalid socket types return request
	require().Equal(zmq.REQ, TargetToClient(zmq.REQ))
	require().Equal(zmq.REQ, TargetToClient(zmq.DEALER))
}

// Test_12_Url tests the url generation
func (test *TestConfigSuite) Test_12_Url() {
	require := test.Require

	require().Empty(test.client.Url())

	test.client.UrlFunc(Url)
	require().Equal("inproc://sample", test.client.Url())
}

func (test *TestConfigSuite) Test_13_Url_tcp() {
	require := test.Require

	client := New("github.com/sds-framework/service", "sample", 6000, zmq.REP)
	require().Equal("tcp://sample:6000", Url(client))
}

func (test *TestConfigSuite) Test_14_Url_ipc_tmp() {
	require := test.Require

	client := New("github.com/sds-framework/service", "tmp-test.sock", 0, zmq.REP)
	require().Equal("ipc:///tmp-test.sock", Url(client))
}

func (test *TestConfigSuite) Test_15_Url_inproc_port_zero() {
	require := test.Require

	client := New("github.com/sds-framework/service", "my-service", 0, zmq.REP)
	require().Equal("inproc://my-service", Url(client))
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestConfig(t *testing.T) {
	suite.Run(t, new(TestConfigSuite))
}
