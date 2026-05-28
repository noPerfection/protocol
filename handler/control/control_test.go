package control_test

import (
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	clientConfig "github.com/noPerfection/protocol/client/config"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/concurrent"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestControlSuite struct {
	suite.Suite

	handler         *concurrent.Concurrent
	instanceManager *concurrent.Parent
	frontend        *concurrent.Frontend

	handlerManager *control.Manager

	inprocConfig *concurrent.Config
	inprocClient *zmq.Socket
	logger       *log.Logger
	routes       datatype.KeyValue
}

// Make sure that Account is set to five
// before each test unit
func (test *TestControlSuite) SetupTest() {
	s := &test.Suite

	test.inprocConfig = concurrent.NewInternalConfig(config.SyncReplierType, "test", "test")

	logger, err := log.New("handler", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.routes = datatype.New()
	test.routes.Set("command_1", func(request message.RequestInterface) message.ReplyInterface {
		// Used for testing 'message_amount' command.
		// While handling, the queue length should decrease.
		// While handling, the processing length should increase.
		time.Sleep(time.Second)
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	})
	test.routes.Set("command_2", func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	})

	test.handler = concurrent.NewConcurrent()
	test.handler.SetConfig(test.inprocConfig)
	for command, handle := range test.routes {
		s.Require().NoError(test.handler.Route(command, handle))
	}
	s.Require().NoError(test.handler.SetLogger(test.logger))

	test.frontend = test.handler.Frontend
	test.instanceManager = test.handler.InstanceManager
	test.handlerManager = test.handler.Manager

	s.Require().NoError(test.instanceManager.Start())
	s.Require().NoError(test.frontend.Start())
	s.Require().NoError(test.handlerManager.Start())

	// Wait a bit before parts are initialized
	time.Sleep(time.Millisecond * 100)

	// make sure that parts are running
	s.Require().Equal(concurrent.Running, test.instanceManager.Status())
	s.Require().Equal(concurrent.RUNNING, test.frontend.Status())
	s.Require().Equal(base.SocketReady, test.handlerManager.Status())

	// Client that will imitate the service
	inprocClient, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	err = inprocClient.Connect(test.inprocConfig.ManagerConnectUrl())
	s.Require().NoError(err)
	test.inprocClient = inprocClient
}

// Limitation of Zeromq, the inproc client can not reconnect if the backend restarted
func (test *TestControlSuite) reconnectClient() {
	s := &test.Suite
	url := test.inprocConfig.ManagerConnectUrl()

	err := test.inprocClient.Disconnect(url)
	s.Require().NoError(err)

	err = test.inprocClient.Connect(url)
	s.Require().NoError(err)
}

// cleanOut everything
func (test *TestControlSuite) cleanOut() {
	s := &test.Suite

	err := test.inprocClient.Close()
	s.Require().NoError(err)

	if test.instanceManager.Status() == concurrent.Running {
		test.instanceManager.Close()
	}

	if test.frontend.Status() == concurrent.RUNNING {
		err = test.frontend.Close()
		s.Require().NoError(err)
	}

	if test.handlerManager.Status() == base.SocketReady {
		test.handlerManager.SetClose(&message.Request{Command: control.HandlerClose, Parameters: datatype.New()})
	}

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything is closed
	s.Require().Equal(concurrent.Idle, test.instanceManager.Status())
	s.Require().Equal(concurrent.CREATED, test.frontend.Status())
	s.Require().Equal(base.SocketIdle, test.handlerManager.Status())
}

func (test *TestControlSuite) req(request message.Request) message.ReplyInterface {
	s := &test.Suite

	reqStr, err := request.ZmqEnvelope()
	s.Require().NoError(err)

	_, err = test.inprocClient.SendMessage(reqStr)
	s.Require().NoError(err)

	raw, err := test.inprocClient.RecvMessage(0)
	s.Require().NoError(err)

	reply, err := message.NewRep(raw)
	s.Require().NoError(err)

	return reply
}

// Test_10_InvalidCommand tries to send an invalid command
func (test *TestControlSuite) Test_10_InvalidCommand() {
	s := &test.Suite

	// must fail since the command is invalid
	req := message.Request{Command: "no_command", Parameters: datatype.New()}
	reply := test.req(req)
	s.Require().False(reply.IsOK())

	test.cleanOut()
}

// Test_12_ClosePart stops the parts
func (test *TestControlSuite) Test_12_ClosePart() {
	s := &test.Suite
	params := datatype.New()
	req := message.Request{Command: concurrent.ClosePart, Parameters: params}

	// Trying to stop without a part must fail
	reply := test.req(req)
	s.Require().False(reply.IsOK())

	// Trying to stop a part that doesn't exist must fail
	params.Set("part", "no_part")
	req.Parameters = params
	reply = test.req(req)
	s.Require().False(reply.IsOK())

	// Stopping the frontend must succeed
	params.Set("part", "frontend")
	req.Parameters = params
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Re-stopping the frontend must fail
	time.Sleep(time.Millisecond * 100)
	reply = test.req(req)
	s.Require().False(reply.IsOK())

	// Stopping the instance manager must succeed
	params.Set("part", "instance_manager")
	req.Parameters = params
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Re-stopping the frontend must fail
	time.Sleep(time.Millisecond * 100)
	reply = test.req(req)
	s.Require().False(reply.IsOK())

	test.cleanOut()
}

// Test_13_RunPart trying to run some parts
func (test *TestControlSuite) Test_13_RunPart() {
	s := &test.Suite
	params := datatype.New()
	req := message.Request{Command: concurrent.ClosePart, Parameters: params}

	// Stopping the frontend that was run during test setup
	params.Set("part", "frontend")
	req.Parameters = params
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	// Make sure the frontend stopped
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(concurrent.CREATED, test.frontend.Status())

	// Let's test running it
	req.Command = concurrent.RunPart
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Make sure it's running
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(concurrent.RUNNING, test.frontend.Status())

	//
	// Testing with the instance manager
	//

	// stop the instance manager that was run during test setup
	req.Command = concurrent.ClosePart
	params.Set("part", "instance_manager")
	req.Parameters = params

	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Make sure that instance manager stopped
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(concurrent.Idle, test.instanceManager.Status())

	// Start the instance manager
	req.Command = concurrent.RunPart
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Make sure that instance manager is running
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(concurrent.Running, test.instanceManager.Status())

	//
	// Re-running must fail
	//
	reply = test.req(req)
	s.Require().False(reply.IsOK())

	test.cleanOut()
}

// Test_14_InstanceAmount trying check that instance amount is correct
func (test *TestControlSuite) Test_14_InstanceAmount() {
	s := &test.Suite
	req := message.Request{Command: concurrent.InstanceAmount, Parameters: datatype.New()}

	// No instances were added, so it must return 0
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	instanceAmount, err := reply.ReplyParameters().Uint64Value("instance_amount")
	s.Require().NoError(err)
	s.Require().Zero(instanceAmount)

	// Add a new instance
	instanceId, err := test.instanceManager.AddInstance(test.inprocConfig.Type, &test.routes)
	s.Require().NoError(err)

	// Wait a bit for instance initialization
	time.Sleep(time.Millisecond * 100)

	// The instance amount is not 0
	reply = test.req(req)
	s.Require().True(reply.IsOK())
	instanceAmount, err = reply.ReplyParameters().Uint64Value("instance_amount")
	s.Require().NoError(err)
	s.Require().NotZero(instanceAmount)

	//
	// After instance deletion, the instance_amount should return a correct result
	//
	err = test.instanceManager.DeleteInstance(instanceId, false)
	s.Require().NoError(err)

	// Wait a bit for the closing of the instance thread
	time.Sleep(time.Millisecond * 100)

	// Must be 0 instances
	reply = test.req(req)
	s.Require().True(reply.IsOK())
	instanceAmount, err = reply.ReplyParameters().Uint64Value("instance_amount")
	s.Require().NoError(err)
	s.Require().Zero(instanceAmount)

	test.cleanOut()
}

// Test_15_InstanceAmount checks that instance amount is correct when instances come and go
func (test *TestControlSuite) Test_15_InstanceAmount() {
	s := &test.Suite
	req := message.Request{Command: concurrent.InstanceAmount, Parameters: datatype.New()}

	// No instances were added, so it must return 0
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	instanceAmount, err := reply.ReplyParameters().Uint64Value("instance_amount")
	s.Require().NoError(err)
	s.Require().Zero(instanceAmount)

	// Add a new instance
	instanceId, err := test.instanceManager.AddInstance(test.inprocConfig.Type, &test.routes)
	s.Require().NoError(err)

	// Wait a bit for instance initialization
	time.Sleep(time.Millisecond * 100)

	// The instance amount is not 0
	reply = test.req(req)
	s.Require().True(reply.IsOK())
	instanceAmount, err = reply.ReplyParameters().Uint64Value("instance_amount")
	s.Require().NoError(err)
	s.Require().NotZero(instanceAmount)

	//
	// After instance deletion, the instance_amount should return a correct result
	//
	err = test.instanceManager.DeleteInstance(instanceId, false)
	s.Require().NoError(err)

	// Wait a bit for the closing of the instance thread
	time.Sleep(time.Millisecond * 100)

	// Must be 0 instances
	reply = test.req(req)
	s.Require().True(reply.IsOK())
	instanceAmount, err = reply.ReplyParameters().Uint64Value("instance_amount")
	s.Require().NoError(err)
	s.Require().Zero(instanceAmount)

	test.cleanOut()
}

// Test_16_MessageAmount checks that queue and processing messages amount are correct
func (test *TestControlSuite) Test_16_MessageAmount() {
	s := &test.Suite
	req := message.Request{Command: concurrent.MessageAmount, Parameters: datatype.New()}

	// Imitating the user that sends the message
	clientType := clientConfig.TargetToClient(config.SocketType(test.inprocConfig.Type))
	clientSocket, err := zmq.NewSocket(clientType)
	s.Require().NoError(err)
	clientUrl := config.ExternalUrl(test.inprocConfig.Id, test.inprocConfig.Port)
	err = clientSocket.Connect(clientUrl)
	s.Require().NoError(err)

	// No instances were added, so it must return 0
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	queueAmount, err := reply.ReplyParameters().Uint64Value("queue_length")
	s.Require().NoError(err)
	s.Require().Zero(queueAmount)
	procAmount, err := reply.ReplyParameters().Uint64Value("processing_length")
	s.Require().NoError(err)
	s.Require().Zero(procAmount)

	// User sends a message
	extReq := message.Request{Command: "command_1", Parameters: datatype.New()}
	extReqStr, err := extReq.ZmqEnvelope()
	s.Require().NoError(err)
	_, err = clientSocket.SendMessageDontwait(extReqStr)
	s.Require().NoError(err)

	// Wait a bit for transfer between threads
	time.Sleep(time.Millisecond * 100)

	// Queue has one message
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	queueAmount, err = reply.ReplyParameters().Uint64Value("queue_length")
	s.Require().NoError(err)
	s.Require().NotZero(queueAmount)
	procAmount, err = reply.ReplyParameters().Uint64Value("processing_length")
	s.Require().NoError(err)
	s.Require().Zero(procAmount)

	// Add a new instance that will start processing the message
	_, err = test.instanceManager.AddInstance(test.inprocConfig.Type, &test.routes)
	s.Require().NoError(err)

	// Wait a bit for instance initialization
	time.Sleep(time.Millisecond * 100)

	// The instance handles the request, so queue must be empty.
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	queueAmount, err = reply.ReplyParameters().Uint64Value("queue_length")
	s.Require().NoError(err)
	s.Require().Zero(queueAmount)
	procAmount, err = reply.ReplyParameters().Uint64Value("processing_length")
	s.Require().NoError(err)
	s.Require().NotZero(procAmount)

	// After handling, the queue and processing must be empty
	_, err = clientSocket.RecvMessage(0) // handling finished

	reply = test.req(req)
	s.Require().True(reply.IsOK())

	queueAmount, err = reply.ReplyParameters().Uint64Value("queue_length")
	s.Require().NoError(err)
	s.Require().Zero(queueAmount)
	procAmount, err = reply.ReplyParameters().Uint64Value("processing_length")
	s.Require().NoError(err)
	s.Require().Zero(procAmount)

	// clean out
	err = clientSocket.Close()
	s.Require().NoError(err)

	test.cleanOut()
}

// Test_17_MessageAmount checks that message amounts are correct
func (test *TestControlSuite) Test_17_MessageAmount() {
	s := &test.Suite
	req := message.Request{Command: control.HandlerStatus, Parameters: datatype.New()}

	// Test setup runs all parts, status must be base.Ready
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	status, err := reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.Ready, status)

	//
	// Turn the status to incomplete
	//
	partReq := message.Request{Command: concurrent.ClosePart, Parameters: datatype.New().Set("part", "frontend")}
	reply = test.req(partReq)
	s.Require().True(reply.IsOK())

	// Wait a bit for the frontend closes itself
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(concurrent.CREATED, test.frontend.Status())

	// Status must be incomplete
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	status, err = reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.Incomplete, status)

	// Only frontend must be incomplete
	parts, err := reply.ReplyParameters().NestedValue("parts")
	s.Require().NoError(err)
	frontendStatus, err := parts.StringValue("frontend")
	s.Require().NoError(err)
	s.Require().Equal(concurrent.CREATED, frontendStatus)
	instanceManager, err := parts.StringValue("instance_manager")
	s.Require().NoError(err)
	s.Require().Equal(concurrent.Running, instanceManager)

	//
	// Absolutely incomplete if instance manager stopped
	//
	partReq.Parameters.Set("part", "instance_manager")
	reply = test.req(partReq)
	s.Require().True(reply.IsOK())

	// Wait a bit for the frontend closes itself
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(concurrent.Idle, test.instanceManager.Status())

	// Status must be incomplete
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	status, err = reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.Incomplete, status)

	// Frontend and instance manager are incomplete
	parts, err = reply.ReplyParameters().NestedValue("parts")
	s.Require().NoError(err)
	frontendStatus, err = parts.StringValue("frontend")
	s.Require().NoError(err)
	s.Require().Equal(concurrent.CREATED, frontendStatus)
	instanceManager, err = parts.StringValue("instance_manager")
	s.Require().NoError(err)
	s.Require().Equal(concurrent.Idle, instanceManager)

	//
	// base.Incomplete turns to ready when processes are running
	//

	// Start the instance manager
	partReq.Command = concurrent.RunPart
	reply = test.req(partReq)
	s.Require().True(reply.IsOK())

	// Wait a bit for instance manager initialization
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(concurrent.Running, test.instanceManager.Status())

	// Status must be incomplete
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	status, err = reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.Incomplete, status)

	// Only frontend is incomplete
	parts, err = reply.ReplyParameters().NestedValue("parts")
	s.Require().NoError(err)
	frontendStatus, err = parts.StringValue("frontend")
	s.Require().NoError(err)
	s.Require().Equal(concurrent.CREATED, frontendStatus)
	instanceManager, err = parts.StringValue("instance_manager")
	s.Require().NoError(err)
	s.Require().Equal(concurrent.Running, instanceManager)

	// Start Frontend
	partReq.Parameters.Set("part", "frontend")
	reply = test.req(partReq)
	s.Require().True(reply.IsOK())

	// Wait a bit for frontend initialization
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(concurrent.RUNNING, test.frontend.Status())

	// Status must be ready
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	status, err = reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.Ready, status)

	// Clean
	test.cleanOut()
}

// Test_18_OverwriteRoute checks that routes can be overwritten
func (test *TestControlSuite) Test_18_OverwriteRoute() {
	s := &test.Suite
	req := message.Request{Command: control.HandlerStatus, Parameters: datatype.New()}

	// The default route must work as designed
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	status, err := reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.Ready, status)

	// Overriding must fail when handler manager is running
	overwritten := "overwritten"
	onStatus := func(req message.RequestInterface) message.ReplyInterface {
		params := datatype.New().Set("status", overwritten)
		return req.Ok(params)
	}
	err = test.handlerManager.Route("status", onStatus)
	s.Require().Error(err)

	// Close the handler manager
	test.handlerManager.SetClose(&message.Request{Command: control.HandlerClose, Parameters: datatype.New()})
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(base.SocketIdle, test.handlerManager.Status())

	// Overwriting must work when the handler manager is not running
	err = test.handlerManager.Route("status", onStatus)
	s.Require().NoError(err)

	// Start handler manager to apply route effects
	s.Require().NoError(test.handlerManager.Start())
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(base.SocketReady, test.handlerManager.Status())

	// reconnect the client
	test.reconnectClient()

	// Requesting status must return result from overwritten handler
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	status, err = reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(overwritten, status)

	// Clean
	test.cleanOut()
}

// Test_19_AddInstance checks that instances can be added
func (test *TestControlSuite) Test_19_AddInstance() {
	s := &test.Suite
	req := message.Request{Command: concurrent.AddInstance, Parameters: datatype.New()}

	// There must not be any instances before adding
	s.Require().Len(test.instanceManager.Instances(), 0)

	// Adding an instance must be successful
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	_, err := reply.ReplyParameters().StringValue("instance_id")
	s.Require().NoError(err)

	// Wait a bit for instance initialization
	time.Sleep(time.Millisecond * 100)

	// Confirming instance exist
	s.Require().Len(test.instanceManager.Instances(), 1)

	// Clean
	test.cleanOut()
}

// Test_20_DeleteInstance deletes the instance
func (test *TestControlSuite) Test_20_DeleteInstance() {
	s := &test.Suite
	req := message.Request{Command: concurrent.DeleteInstance, Parameters: datatype.New()}

	// There must not be any instances before adding
	s.Require().Len(test.instanceManager.Instances(), 0)

	// Delete must fail as no instance id
	reply := test.req(req)
	s.Require().False(reply.IsOK())

	// Deleting non existence instance must fail
	req.Parameters.Set("instance_id", "no_id")
	reply = test.req(req)
	s.Require().False(reply.IsOK())

	// Let's add a new instance for deleting
	req.Command = concurrent.AddInstance
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	instanceId, err := reply.ReplyParameters().StringValue("instance_id")
	s.Require().NoError(err)

	// Wait a bit for initialization of the instance
	time.Sleep(time.Millisecond * 100)
	s.Require().Len(test.instanceManager.Instances(), 1)

	// Delete the instance
	req.Command = concurrent.DeleteInstance
	req.Parameters.Set("instance_id", instanceId)
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Wait a bit for deleting of the instance thread
	time.Sleep(time.Millisecond * 100)
	s.Require().Len(test.instanceManager.Instances(), 0)

	// Clean
	test.cleanOut()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestControl(t *testing.T) {
	suite.Run(t, new(TestControlSuite))
}
