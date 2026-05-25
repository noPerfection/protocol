package instance_manager

import (
	"github.com/sds-framework/client-lib"
	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/datatype-lib/message"
	"github.com/sds-framework/handler-lib/config"
	"github.com/sds-framework/handler-lib/instance"
	"github.com/sds-framework/log-lib"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestInstanceSuite struct {
	suite.Suite

	parent   *Parent
	handle0  interface{}
	handle1  interface{}
	parentId string

	clients   key_value.KeyValue
	routes    key_value.KeyValue
	routeDeps key_value.KeyValue
}

// Make sure that Account is set to five
// before each test
func (test *TestInstanceSuite) SetupTest() {
	handle0 := func(request message.RequestInterface) message.ReplyInterface {
		time.Sleep(time.Millisecond * 200)
		return request.Ok(key_value.New())
	}
	// delays 1 second for testing ready instances
	handle1 := func(request message.RequestInterface, _ *client.Socket) message.ReplyInterface {
		time.Sleep(time.Second)
		return request.Ok(key_value.New())
	}

	test.handle0 = handle0
	test.handle1 = handle1
	test.parentId = "parent_0"
}

func (test *TestInstanceSuite) Test_0_New() {
	s := &test.Suite

	logger, _ := log.New("parent_test", true)

	test.parent = New(test.parentId, logger)

	s.Require().Equal(Idle, test.parent.Status())
}

// Test_10_Close starts, then closes the instance manager
func (test *TestInstanceSuite) Test_10_Close() {
	s := &test.Suite

	// First, it should be prepared
	s.Require().Equal(Idle, test.parent.Status())

	// Let's start the instance manager
	s.Require().NoError(test.parent.Start())
	time.Sleep(time.Millisecond * 100) // waiting a time for initialization

	// Make sure that the instance manager is running
	s.Require().Equal(Running, test.parent.Status())

	// Sending a close message
	test.parent.Close()

	// Waiting
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(Idle, test.parent.Status())
}

// Test_11_AddInstance tests adding a new instance.
//
// We won't test DeleteInstance, as it's already done by parent.Close().
func (test *TestInstanceSuite) Test_11_AddInstance() {
	s := &test.Suite

	test.routes = key_value.New().
		Set("handle_0", test.handle0).
		Set("handle_1", test.handle1)
	test.routeDeps = key_value.New().
		Set("handle_1", []string{"dep_1"})
	test.clients = key_value.New().
		Set("handle_1", &client.Socket{})

	// Make sure that there are no instances
	s.Require().Len(test.parent.instances, 0)

	// Adding a new instance when manager not yet started must fail
	_, err := test.parent.AddInstance(config.SyncReplierType, &test.routes, &test.routeDeps, &test.clients)
	s.Require().Error(err)

	// Start instance manager
	s.Require().NoError(test.parent.Start())

	// waiting a bit for initialization
	time.Sleep(time.Millisecond * 10)
	s.Require().Equal(Running, test.parent.Status())

	// Now adding a new instance should work
	instanceId, err := test.parent.AddInstance(config.SyncReplierType, &test.routes, &test.routeDeps, &test.clients)
	s.Require().NoError(err)

	// Wait a bit for instance initialization
	time.Sleep(time.Millisecond * 100)

	// The instance should be created
	s.Require().Len(test.parent.instances, 1)
	s.Equal(instance.READY, test.parent.instances[instanceId].status)

	// Clean out after adding a new instance
	test.parent.Close()
	time.Sleep(time.Millisecond * 200)
	s.Equal(Idle, test.parent.Status())
	s.Require().Len(test.parent.instances, 0)
}

// Test_12_Ready tests the handling requests by ready worker.
func (test *TestInstanceSuite) Test_12_Ready() {
	s := &test.Suite

	// request to send
	req := message.Request{Command: "handle_1", Parameters: key_value.New()}
	reqStr, err := req.ZmqEnvelope()
	s.Require().NoError(err)

	// Make sure that there are no instances
	s.Require().Len(test.parent.instances, 0)
	s.Require().Empty(test.parent.Ready())

	// The instance manager should be idle before start
	s.Require().Equal(Idle, test.parent.Status())

	// Start instance manager
	s.Require().NoError(test.parent.Start())

	// waiting a bit for initialization
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(Running, test.parent.Status())

	// Now adding a new instance should work
	instanceId, err := test.parent.AddInstance(config.SyncReplierType, &test.routes, &test.routeDeps, &test.clients)
	s.Require().NoError(err)
	instanceId2, err := test.parent.AddInstance(config.SyncReplierType, &test.routes, &test.routeDeps, &test.clients)
	s.Require().NoError(err)

	// Instances are in the parent
	s.Require().Len(test.parent.instances, 2)

	// Instance should be ready
	time.Sleep(time.Millisecond * 100)
	s.Equal(instance.READY, test.parent.instances[instanceId].status)
	s.Equal(instance.READY, test.parent.instances[instanceId2].status)

	// Let's test the ready instances
	_, handler := test.parent.Ready()
	s.Require().NotNil(handler)

	_, err = handler.SendMessageDontwait(reqStr)
	s.Require().NoError(err)

	// Waiting the instance will notify instance manager that it's busy
	// Since, we are sending messages without waiting their update
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(instance.HANDLING, test.parent.instances[instanceId].status)

	// Get the second ready worker
	_, handler2 := test.parent.Ready()
	s.Require().NotNil(handler2)

	_, err = handler2.SendMessageDontwait(reqStr)
	s.Require().NoError(err)

	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(instance.HANDLING, test.parent.instances[instanceId2].status)

	// There should not be any ready worker
	_, handler3 := test.parent.Ready()
	s.Require().Nil(handler3)

	// After handling, the first worker should be READY again
	_, err = handler.RecvMessage(0)
	s.Require().NoError(err)

	time.Sleep(time.Millisecond * 10)
	_, handler4 := test.parent.Ready()
	s.Require().NotNil(handler4)

	// Clean out after adding a new instance
	test.parent.Close()
	time.Sleep(time.Millisecond * 200)
	s.Equal(Idle, test.parent.Status())
	s.Require().Len(test.parent.instances, 0)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInstance(t *testing.T) {
	suite.Run(t, new(TestInstanceSuite))
}
