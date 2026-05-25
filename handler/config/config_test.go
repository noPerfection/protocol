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

	id := "example.com"
	category := "category"
	port := uint64(6000)

	// Testing the remote handler
	handler := NewHandler(SyncReplierType, id, category, port)
	s().Equal(category, handler.Category)
	s().Equal(id, handler.Id)
	s().Equal(port, handler.Port)
	s().Equal(DefaultManagerId(id), handler.ManagerId)
	s().Zero(handler.ManagerPort)
	s().Equal("inproc://manager_"+id, handler.ManagerExternalUrl())
	s().Equal("inproc://manager_"+id, handler.ManagerConnectUrl())
	s().False(handler.IsInproc())

	broadcastId := "broadcast.example.com"
	broadcastPort := uint64(6001)

	trigger, err := TriggerAble(SyncReplierType, id, category, port, PublisherType, broadcastId, broadcastPort)
	s().NoError(err)
	s().Equal(broadcastId, trigger.BroadcastId)
	s().Equal(broadcastPort, trigger.BroadcastPort)
	s().False(trigger.IsInproc())
	s().False(trigger.IsInprocBroadcast())

	// Testing the inproc handler
	handler = NewInternalHandler(SyncReplierType, id, category)
	s().Equal(category, handler.Category)
	s().Equal(id, handler.Id)
	s().True(handler.IsInproc())

	trigger, err = TriggerAble(SyncReplierType, id, category, 0, PublisherType, broadcastId, 0)
	s().NoError(err)
	s().True(trigger.IsInproc())
	s().True(trigger.IsInprocBroadcast())
}

func (test *TestConfigSuite) Test_12_ExternalUrl_tcp() {
	test.Require().Equal("tcp://sample:6000", ExternalUrl("sample", 6000))
}

func (test *TestConfigSuite) Test_12_ExternalUrl_tcp_localhost() {
	test.Require().Equal("tcp://*:6000", ExternalUrl("localhost", 6000))
	test.Require().Equal("tcp://*:6000", ExternalUrl("127.0.0.1", 6000))
}

func (test *TestConfigSuite) Test_13_ExternalUrl_inproc() {
	test.Require().Equal("inproc://my-service", ExternalUrl("my-service", 0))
}

func (test *TestConfigSuite) Test_14_ExternalUrl_ipc_tmp() {
	test.Require().Equal("ipc:///tmp-test.sock", ExternalUrl("tmp-test.sock", 0))
}

func (test *TestConfigSuite) Test_15_ConnectUrl_tcp() {
	test.Require().Equal("tcp://sample:6000", ConnectUrl("sample", 6000))
	test.Require().Equal("tcp://localhost:6000", ConnectUrl("localhost", 6000))
	test.Require().Equal("tcp://127.0.0.1:6000", ConnectUrl("127.0.0.1", 6000))
}

func (test *TestConfigSuite) Test_15_ManagerUrl_tcp() {
	handler := NewHandler(SyncReplierType, "example.com", "category", 6000)
	handler.ManagerId = "manager.example.com"
	handler.ManagerPort = 7000

	manager := handler.ManagerHandler()

	test.Require().Equal("manager.example.com", manager.Id)
	test.Require().Equal(ManagerCategory, manager.Category)
	test.Require().Equal(uint64(7000), manager.Port)
	test.Require().Equal("tcp://manager.example.com:7000", handler.ManagerExternalUrl())
	test.Require().Equal("tcp://manager.example.com:7000", handler.ManagerConnectUrl())
}

func (test *TestConfigSuite) Test_16_ConnectUrl_inproc() {
	test.Require().Equal("inproc://my-service", ConnectUrl("my-service", 0))
}

func (test *TestConfigSuite) Test_17_ConnectUrl_ipc_tmp() {
	test.Require().Equal("ipc:///tmp-test.sock", ConnectUrl("tmp-test.sock", 0))
}

// a normal test function and pass our suite to suite.Run
func TestConfig(t *testing.T) {
	suite.Run(t, new(TestConfigSuite))
}
