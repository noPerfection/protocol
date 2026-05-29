package concurrent

import (
	"fmt"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"

	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestInstanceSuite struct {
	suite.Suite

	instance0 *Instance
	instance1 *Instance
	handle0   interface{}
	handle1   interface{}
	parentId  string

	router *base.Handler
}

// Make sure that Account is set to five
// before each test
func (test *TestInstanceSuite) SetupTest() {
	handle0 := func(request message.RequestInterface) message.ReplyInterface {
		time.Sleep(time.Millisecond * 200)
		return request.Ok(datatype.New())
	}
	handle1 := func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(datatype.New())
	}

	test.handle0 = handle0
	test.handle1 = handle1
	test.parentId = "parent_0"
}

func (test *TestInstanceSuite) testRouter() *base.Handler {
	router := base.New()
	test.Require().NoError(router.Route("handle_0", test.handle0.(base.HandleFunc)))
	test.Require().NoError(router.Route("handle_1", test.handle1.(base.HandleFunc)))
	return router
}

func (test *TestInstanceSuite) Test_0_New() {
	s := &test.Suite

	handlerType := config.SyncReplierType
	id := "instance_0"

	logger, _ := log.New("instance_test", true)

	test.instance0 = NewInstance(handlerType, id, test.parentId, logger)
	test.instance0.SetMessageOps(message.DefaultMessage())

	s.Require().Equal(PREPARE, test.instance0.Status())
}

// Test_10_SetRouter tests the setting router references from handler.
// If the routes are changed by the parent, then instances should see updated lookups.
// Let's test it here. We imitate a parent. And set the routes.
// Then we update the route.
func (test *TestInstanceSuite) Test_10_SetRouter() {
	s := &test.Suite

	test.router = base.New()
	// Before setting the router, the instance should have a nil there
	s.Require().Nil(test.instance0.router)

	// Update the routes
	test.instance0.SetRouter(test.router)

	// Now, the instance should have the empty router since we added empty routes
	s.Require().NotNil(test.instance0.router)
	s.Require().Empty(test.router.RouteCommands())

	// Let's imitate the handler updated the routes
	s.Require().NoError(test.router.Route("handle_0", test.handle0.(base.HandleFunc)))
	s.Require().Len(test.router.RouteCommands(), 1)

	s.Require().NoError(test.router.Route("handle_1", test.handle1.(base.HandleFunc)))
	s.Require().Len(test.router.RouteCommands(), 2)

	// Make sure that instance's routes lint to the valid parameters.
	for _, routeCmdName := range test.router.RouteCommands() {
		_, err := test.instance0.router.FindRoute(routeCmdName)
		s.Require().NoError(err, fmt.Sprintf("the '%s' not found", routeCmdName))
	}
}

// Test_12_Close tests starting and closing the instance
func (test *TestInstanceSuite) Test_12_Close() {
	s := &test.Suite

	// First, it should be prepared
	s.Require().Equal(PREPARE, test.instance0.Status())

	// Let's start the instance
	s.Require().NoError(test.instance0.Start())
	time.Sleep(time.Millisecond * 100) // waiting a time for initialization

	// Make sure that instance is ready
	s.Require().Equal(READY, test.instance0.Status())

	// Sending a close message
	instanceManager, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	err = instanceManager.Connect(InstanceUrl(test.instance0.parentId, test.instance0.Id))
	s.Require().NoError(err)
	req := message.Request{Command: "close", Parameters: datatype.New().Set("instant", false)}
	reqStr, err := req.ZmqEnvelope()
	s.Require().NoError(err)

	_, err = instanceManager.SendMessage(reqStr)
	s.Require().NoError(err)

	// Waiting
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(CLOSED, test.instance0.Status())

	// Clean out the things
	err = instanceManager.Close()
	s.Require().NoError(err)
}

// Test_13_Handle tests that instance can handle the messages
func (test *TestInstanceSuite) Test_13_Handle() {
	s := &test.Suite

	// Let's start the instance
	s.Require().NoError(test.instance0.Start())
	time.Sleep(time.Millisecond * 100) // waiting a time for initialization

	// Make sure that instance is ready
	s.Require().Equal(READY, test.instance0.Status())

	// Now we will send some random requests
	// Sending a close message
	handleClient, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	err = handleClient.Connect(InstanceHandleUrl(test.instance0.parentId, test.instance0.Id))
	s.Require().NoError(err)
	for i := 0; i < 2; i++ {
		req := message.Request{Command: "handle_0", Parameters: datatype.New()}
		reqStr, err := req.ZmqEnvelope()
		s.Require().NoError(err)

		_, err = handleClient.SendMessage(reqStr)
		s.Require().NoError(err)

		_, err = handleClient.RecvMessage(0)
		s.Require().NoError(err)
	}

	// Sending a close message
	instanceManager, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	err = instanceManager.Connect(InstanceUrl(test.instance0.parentId, test.instance0.Id))
	s.Require().NoError(err)
	req := message.Request{Command: "close", Parameters: datatype.New().Set("instant", false)}
	reqStr, err := req.ZmqEnvelope()
	s.Require().NoError(err)

	_, err = instanceManager.SendMessage(reqStr)
	s.Require().NoError(err)

	// Then we will close it
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(CLOSED, test.instance0.Status())

	// Clean out the things
	err = instanceManager.Close()
	s.Require().NoError(err)
}

// Test_14_HandleRouter tests that asynchronous instances can handle the messages
func (test *TestInstanceSuite) Test_14_HandleRouter() {
	s := &test.Suite

	test.router = test.testRouter()

	handlerType := config.ReplierType
	id := "instance_1"
	logger, _ := log.New("instance_test", true)

	test.instance1 = NewInstance(handlerType, id, test.parentId, logger)
	test.instance1.SetRouter(test.router)
	test.instance1.SetMessageOps(message.DefaultMessage())

	// Let's start the instance
	s.Require().NoError(test.instance1.Start())
	time.Sleep(time.Millisecond * 100) // waiting a time for initialization

	// Make sure that instance is ready
	s.Require().Equal(READY, test.instance1.Status())

	// Now we will send some random requests
	// Sending a close message
	handleClient, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	err = handleClient.Connect(InstanceHandleUrl(test.instance1.parentId, test.instance1.Id))
	s.Require().NoError(err)
	for i := 0; i < 2; i++ {
		req := message.Request{Command: "handle_0", Parameters: datatype.New()}
		reqStr, err := req.ZmqEnvelope()
		s.Require().NoError(err)

		_, err = handleClient.SendMessage(reqStr)
		s.Require().NoError(err)

		_, err = handleClient.RecvMessage(0)
		s.Require().NoError(err)
	}

	// Sending a close message
	instanceManager, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	err = instanceManager.Connect(InstanceUrl(test.instance1.parentId, test.instance1.Id))
	s.Require().NoError(err)
	req := message.Request{Command: "close", Parameters: datatype.New().Set("instant", false)}
	reqStr, err := req.ZmqEnvelope()
	s.Require().NoError(err)

	_, err = instanceManager.SendMessage(reqStr)
	s.Require().NoError(err)

	// Then we will close it
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(CLOSED, test.instance1.Status())

	// Clean out the things
	err = instanceManager.Close()
	s.Require().NoError(err)
}

// Test_15_HandleDealer tests that asynchronous clients can send the messages
func (test *TestInstanceSuite) Test_15_HandleDealer() {
	s := &test.Suite

	test.router = test.testRouter()

	handlerType := config.SyncReplierType
	id := "instance_1"
	logger, _ := log.New("instance_test", true)

	test.instance1 = NewInstance(handlerType, id, test.parentId, logger)
	test.instance1.SetRouter(test.router)
	test.instance1.SetMessageOps(message.DefaultMessage())

	// Let's start the instance
	s.Require().NoError(test.instance1.Start())
	time.Sleep(time.Millisecond * 100) // waiting a time for initialization

	// Make sure that instance is ready
	s.Require().Equal(READY, test.instance1.Status())

	// Now we will send some random requests
	// Sending a close message
	handleClient, err := zmq.NewSocket(zmq.DEALER)
	s.Require().NoError(err)
	err = handleClient.Connect(InstanceHandleUrl(test.instance1.parentId, test.instance1.Id))
	s.Require().NoError(err)
	for i := 0; i < 2; i++ {
		req := message.Request{Command: "handle_0", Parameters: datatype.New()}
		reqStr, err := req.ZmqEnvelope()
		s.Require().NoError(err)

		_, err = handleClient.SendMessage("", reqStr)
		s.Require().NoError(err)

		_, err = handleClient.RecvMessage(0)
		s.Require().NoError(err)
	}

	// Sending a close message
	instanceManager, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	err = instanceManager.Connect(InstanceUrl(test.instance1.parentId, test.instance1.Id))
	s.Require().NoError(err)
	req := message.Request{Command: "close", Parameters: datatype.New().Set("instant", false)}
	reqStr, err := req.ZmqEnvelope()
	s.Require().NoError(err)

	_, err = instanceManager.SendMessage(reqStr)
	s.Require().NoError(err)

	// Then we will close it
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(CLOSED, test.instance1.Status())

	// Clean out the things
	err = instanceManager.Close()
	s.Require().NoError(err)
}

// Test_16_HandleDealerRouter tests that asynchronous clients communicate with asynchronous instances
func (test *TestInstanceSuite) Test_15_HandleDealerRouter() {
	s := &test.Suite

	test.router = test.testRouter()

	handlerType := config.ReplierType
	id := "instance_1"
	logger, _ := log.New("instance_test", true)

	test.instance1 = NewInstance(handlerType, id, test.parentId, logger)
	test.instance1.SetRouter(test.router)
	test.instance1.SetMessageOps(message.DefaultMessage())

	// Let's start the instance
	s.Require().NoError(test.instance1.Start())
	time.Sleep(time.Millisecond * 100) // waiting a time for initialization

	// Make sure that instance is ready
	s.Require().Equal(READY, test.instance1.Status())

	// Now we will send some random requests
	// Sending a close message
	handleClient, err := zmq.NewSocket(zmq.DEALER)
	s.Require().NoError(err)
	err = handleClient.Connect(InstanceHandleUrl(test.instance1.parentId, test.instance1.Id))
	s.Require().NoError(err)
	for i := 0; i < 2; i++ {
		req := message.Request{Command: "handle_0", Parameters: datatype.New()}
		reqStr, err := req.ZmqEnvelope()
		s.Require().NoError(err)

		_, err = handleClient.SendMessage("", reqStr)
		s.Require().NoError(err)

		_, err = handleClient.RecvMessage(0)
		s.Require().NoError(err)
	}

	// Sending a close message
	instanceManager, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	err = instanceManager.Connect(InstanceUrl(test.instance1.parentId, test.instance1.Id))
	s.Require().NoError(err)
	req := message.Request{Command: "close", Parameters: datatype.New().Set("instant", false)}
	reqStr, err := req.ZmqEnvelope()
	s.Require().NoError(err)

	_, err = instanceManager.SendMessage(reqStr)
	s.Require().NoError(err)

	// Then we will close it
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(CLOSED, test.instance1.Status())

	// Clean out the things
	err = instanceManager.Close()
	s.Require().NoError(err)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInstance(t *testing.T) {
	suite.Run(t, new(TestInstanceSuite))
}
