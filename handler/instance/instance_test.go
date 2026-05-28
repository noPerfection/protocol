package instance

import (
	"fmt"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
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

	routes datatype.KeyValue
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

func (test *TestInstanceSuite) Test_0_New() {
	s := &test.Suite

	handlerType := config.SyncReplierType
	id := "instance_0"

	logger, _ := log.New("instance_test", true)

	test.instance0 = New(handlerType, id, test.parentId, logger)
	test.instance0.SetMessageOps(message.DefaultMessage())

	s.Require().Equal(PREPARE, test.instance0.Status())
}

// Test_10_SetRoutes tests the setting routes references from handler.
// If the routes are changed by the parent, then instances should have the updated routes.
// Let's test it here. We imitate a parent. And set the routes.
// Then we update the route.
func (test *TestInstanceSuite) Test_10_SetRoutes() {
	s := &test.Suite

	test.routes = datatype.New()
	// Before setting the routes, the instance should have a nil there
	s.Require().Nil(test.instance0.routes)

	// Update the routes
	test.instance0.SetRoutes(&test.routes)

	// Now, the instance should have the empty routes since we added empty routes
	s.Require().NotNil(test.instance0.routes)
	s.Require().Len(*test.instance0.routes, 0)

	// Let's imitate the handler updated the routes
	test.routes.Set("handle_0", test.handle0)
	s.Require().Len(*test.instance0.routes, 1)

	test.routes.Set("handle_1", test.handle1)
	s.Require().Len(*test.instance0.routes, 2)

	// Make sure that instance's routes lint to the valid parameters.
	for routeCmdName := range test.routes {
		found := false
		for cmdName := range *test.instance0.routes {
			if routeCmdName == cmdName {
				found = true
				break
			}
		}
		s.Require().True(found, fmt.Sprintf("the '%s' not found", routeCmdName))
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
	err = instanceManager.Connect(config.InstanceUrl(test.instance0.parentId, test.instance0.Id))
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
	err = handleClient.Connect(config.InstanceHandleUrl(test.instance0.parentId, test.instance0.Id))
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
	err = instanceManager.Connect(config.InstanceUrl(test.instance0.parentId, test.instance0.Id))
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

	test.routes = datatype.New()
	test.routes.Set("handle_0", test.handle0)
	test.routes.Set("handle_1", test.handle1)

	handlerType := config.ReplierType
	id := "instance_1"
	logger, _ := log.New("instance_test", true)

	test.instance1 = New(handlerType, id, test.parentId, logger)
	test.instance1.SetRoutes(&test.routes)
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
	err = handleClient.Connect(config.InstanceHandleUrl(test.instance1.parentId, test.instance1.Id))
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
	err = instanceManager.Connect(config.InstanceUrl(test.instance1.parentId, test.instance1.Id))
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

	test.routes = datatype.New()
	test.routes.Set("handle_0", test.handle0)
	test.routes.Set("handle_1", test.handle1)

	handlerType := config.SyncReplierType
	id := "instance_1"
	logger, _ := log.New("instance_test", true)

	test.instance1 = New(handlerType, id, test.parentId, logger)
	test.instance1.SetRoutes(&test.routes)
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
	err = handleClient.Connect(config.InstanceHandleUrl(test.instance1.parentId, test.instance1.Id))
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
	err = instanceManager.Connect(config.InstanceUrl(test.instance1.parentId, test.instance1.Id))
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

	test.routes = datatype.New()
	test.routes.Set("handle_0", test.handle0)
	test.routes.Set("handle_1", test.handle1)

	handlerType := config.ReplierType
	id := "instance_1"
	logger, _ := log.New("instance_test", true)

	test.instance1 = New(handlerType, id, test.parentId, logger)
	test.instance1.SetRoutes(&test.routes)
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
	err = handleClient.Connect(config.InstanceHandleUrl(test.instance1.parentId, test.instance1.Id))
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
	err = instanceManager.Connect(config.InstanceUrl(test.instance1.parentId, test.instance1.Id))
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
