package frontend

import (
	"os"

	zmq "github.com/pebbe/zmq4"
	"github.com/sds-framework/datatype-lib/data_type"
	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/log-lib"
	"github.com/sds-framework/protocol/client"
	"github.com/sds-framework/protocol/handler/config"
	"github.com/sds-framework/protocol/handler/instance_manager"
	"github.com/sds-framework/protocol/handler/pair"
	"github.com/sds-framework/protocol/message"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type CustomExternal struct {
	pairClient *client.Socket
}

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestFrontendSuite struct {
	suite.Suite

	frontend     *Frontend
	handleConfig *config.Handler
}

// Create an instance manager. Returns a test command, instance id and instance manager itself
func (test *TestFrontendSuite) instanceManager() (string, string, *instance_manager.Parent) {
	s := &test.Suite

	cmd := "hello"
	handleHello := func(req message.RequestInterface) message.ReplyInterface {
		time.Sleep(time.Second) // just to test consuming since there are no ready instances
		id, err := req.RouteParameters().Uint64Value("id")
		if err != nil {
			id = 0
		}
		return req.Ok(key_value.New().Set("id", id))
	}
	routes := key_value.New().Set(cmd, handleHello)
	routeDeps := key_value.New()
	depClients := key_value.New()

	// Added Instance Manager
	logger, err := log.New(test.handleConfig.Id, true)
	s.Require().NoError(err)
	instanceManager := instance_manager.New(test.handleConfig.Id, logger)

	s.Require().NoError(instanceManager.Start())

	time.Sleep(time.Millisecond * 50) // wait until it updates the status
	instanceId, err := instanceManager.AddInstance(test.handleConfig.Type, &routes, &routeDeps, &depClients)
	s.Require().NoError(err)

	time.Sleep(time.Millisecond * 50) // wait until the instance will be loaded

	return cmd, instanceId, instanceManager
}

// Create a custom external layer
func (test *TestFrontendSuite) customExternal() *CustomExternal {
	s := &test.Suite

	pairClient, err := pair.NewClient(test.handleConfig)
	s.Require().NoError(err)

	externalLayer := CustomExternal{
		pairClient: pairClient,
	}

	return &externalLayer
}

func (test *TestFrontendSuite) SetupTest() {
	test.handleConfig = &config.Handler{Type: config.SyncReplierType, Category: "test", Id: "sample", Port: 0, InstanceAmount: 1}
	test.frontend = New()
}

func (test *TestFrontendSuite) Test_0_New() {
	test.frontend = New()
}

func (test *TestFrontendSuite) Test_10_SetConfig() {
	s := &test.Suite

	// The frontend has no info about the handler
	s.Require().Nil(test.frontend.externalConfig)

	test.frontend.SetConfig(test.handleConfig)

	// The frontend has the configuration of the handler
	s.Require().NotNil(test.frontend.externalConfig)
}

// Test_11_External tests the external socket is running and the messages are queued.
//
// It doesn't send the messages to the consumer.
func (test *TestFrontendSuite) Test_11_External() {
	s := &test.Suite

	test.frontend.SetConfig(test.handleConfig)

	// Queue is populated by External socket, so let's test it.
	err := test.frontend.queue.SetCap(2)
	s.Require().NoError(err)

	// Let's prepare the external socket from the handle configuration
	err = test.frontend.prepareExternalSocket()
	s.Require().NoError(err)

	// user that will send the messages
	clientUrl := config.ExternalUrl(test.frontend.externalConfig.Id, test.frontend.externalConfig.Port)
	user, err := zmq.NewSocket(zmq.DEALER)
	s.Require().NoError(err)
	err = user.Connect(clientUrl)
	s.Require().NoError(err)

	// before external socket receives the messages, we must be sure queue is valid
	s.Require().True(test.frontend.queue.IsEmpty())
	s.Require().EqualValues(test.frontend.queue.Len(), uint(0))

	for i := 1; i <= 2; i++ {
		msg := message.Request{Command: "cmd", Parameters: key_value.New().Set("id", i).Set("cap", 2)}
		msgStr, err := msg.ZmqEnvelope()
		s.Require().NoError(err)
		_, err = user.SendMessage("", msgStr)
		s.Require().NoError(err)
	}

	// A delay a bit, until external socket will receive the user messages
	time.Sleep(time.Millisecond * 50)
	err = test.frontend.handleExternal()
	s.Require().NoError(err)
	err = test.frontend.handleExternal()
	s.Require().NoError(err)

	s.Require().False(test.frontend.queue.IsEmpty())
	s.Require().True(test.frontend.queue.IsFull())
	s.Require().EqualValues(test.frontend.queue.Len(), uint(2))

	// If we try to send a message, it should fail
	i := 3
	msg := message.Request{Command: "cmd", Parameters: key_value.New().Set("id", i).Set("cap", 2)}
	msgStr, err := msg.ZmqEnvelope()
	s.Require().NoError(err)
	_, err = user.SendMessage("", msgStr)
	s.Require().NoError(err)

	err = test.frontend.handleExternal()
	s.Require().NoError(err)

	// However, the third message that user receives should be a failure
	reply, err := user.RecvMessage(0)
	s.Require().NoError(err)
	rep, err := message.NewRep(reply)
	s.Require().NoError(err)
	s.Require().False(rep.IsOK())

	// Trying to decrease the length should fail
	err = test.frontend.queue.SetCap(1)
	s.Require().Error(err)

	// Clean out
	err = user.Close()
	s.Require().NoError(err)

	err = test.frontend.external.Close()
	s.Require().NoError(err)

	// clean out the queue
	test.frontend.queue = data_type.NewQueue()
}

// Test_11_External_ipc tests the external socket over a filesystem IPC endpoint.
func (test *TestFrontendSuite) Test_11_External_ipc() {
	s := &test.Suite

	ipcId := "tmp/handler-lib-frontend.sock"
	ipcPath := "/" + ipcId
	_ = os.Remove(ipcPath)
	defer os.Remove(ipcPath)

	test.handleConfig = &config.Handler{
		Type: config.SyncReplierType, Category: "test", Id: ipcId, Port: 0, InstanceAmount: 1,
	}
	test.frontend = New()
	test.frontend.SetConfig(test.handleConfig)

	err := test.frontend.queue.SetCap(1)
	s.Require().NoError(err)
	err = test.frontend.prepareExternalSocket()
	s.Require().NoError(err)

	clientUrl := config.ExternalUrl(test.frontend.externalConfig.Id, test.frontend.externalConfig.Port)
	s.Require().Equal("ipc:///"+ipcId, clientUrl)

	user, err := zmq.NewSocket(zmq.DEALER)
	s.Require().NoError(err)
	err = user.Connect(clientUrl)
	s.Require().NoError(err)

	msg := message.Request{Command: "cmd", Parameters: key_value.New().Set("id", 1)}
	msgStr, err := msg.ZmqEnvelope()
	s.Require().NoError(err)
	_, err = user.SendMessage("", msgStr)
	s.Require().NoError(err)

	time.Sleep(time.Millisecond * 50)
	err = test.frontend.handleExternal()
	s.Require().NoError(err)
	s.Require().EqualValues(uint(1), test.frontend.queue.Len())

	err = user.Close()
	s.Require().NoError(err)
	err = test.frontend.external.Close()
	s.Require().NoError(err)
}

// Test_12_Consumer tests the consuming
func (test *TestFrontendSuite) Test_12_Consumer() {
	s := &test.Suite

	// Consumer requires Instance Manager
	err := test.frontend.handleConsume()
	s.Require().Error(err)

	// Added Instance Manager
	cmd, instanceId, instanceManager := test.instanceManager()

	test.frontend.SetInstanceManager(instanceManager) // adding

	// Empty queue does nothing
	s.Require().True(test.frontend.queue.IsEmpty())

	// Instance manager is set, zeromq frontend is set, but no queue should do nothing
	err = test.frontend.handleConsume()
	s.Require().Error(err)

	// Also, consumer requires zeromq frontend
	test.frontend.prepareSockets()

	// Instance manager is set, zeromq frontend is set, but no queue should do nothing
	err = test.frontend.handleConsume()
	s.Require().NoError(err)

	// add a message into the queue
	req := message.Request{Command: cmd, Parameters: key_value.New()}
	reqStr, err := req.ZmqEnvelope()
	s.Require().NoError(err)
	messageId := "msg_1"

	s.Require().True(test.frontend.queue.IsEmpty())
	test.frontend.queue.Push([]string{messageId, "", reqStr[1]})
	s.Require().False(test.frontend.queue.IsEmpty())

	// Before testing handle consume with queue,
	// make sure that processing is empty.
	// Consuming the message should remove the message from queue and add it to the processing list
	s.Require().True(test.frontend.processing.IsEmpty())

	// Consuming the message
	err = test.frontend.handleConsume()
	s.Require().NoError(err)

	// The consumed message should be moved from queue to the processing
	s.Require().True(test.frontend.queue.IsEmpty())
	s.Require().False(test.frontend.processing.IsEmpty())

	// The processing list has the expected tracking. Such as instance handles the message id
	rawMessageId, err := test.frontend.processing.Get(instanceId)
	s.Require().NoError(err)
	s.Require().EqualValues(messageId, rawMessageId.(string))

	// clean out
	instanceManager.Close()
	test.frontend.processing = key_value.NewList()

	time.Sleep(time.Millisecond * 100) // wait until instance manager ends
}

// Test_13_Run runs the Frontend
func (test *TestFrontendSuite) Test_13_Run() {
	s := &test.Suite

	test.frontend.SetConfig(test.handleConfig)

	// Queue is populated by External socket, so let's test it.
	err := test.frontend.queue.SetCap(1)
	s.Require().NoError(err)

	// The frontend runs for the first time
	s.Require().Equal(CREATED, test.frontend.Status())

	// The frontend has the configuration of the handler
	s.Require().NotNil(test.frontend.externalConfig)

	cmd, _, instanceManager := test.instanceManager()
	test.frontend.SetInstanceManager(instanceManager)

	// Start
	s.Require().NoError(test.frontend.Start())

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 10)

	// Running?
	s.Require().Equal(RUNNING, test.frontend.Status())

	// The external user sends the request.
	// The request is handled by one instance.
	//
	// Assuming each request is handled in 1 second.
	// User sends the first request to make the instance busy.
	// He then sends the second request that should be queued
	// (before 1 second of instance handling expires).
	// User sends the third request
	// (before 1 second of instance handling expires).
	// The third request should not be added to the queue, since the queue is full.
	// User will be idle
	clientUrl := config.ExternalUrl(test.frontend.externalConfig.Id, test.frontend.externalConfig.Port)
	user, err := zmq.NewSocket(zmq.DEALER)
	s.Require().NoError(err)
	err = user.Connect(clientUrl)
	s.Require().NoError(err)

	req := message.Request{Command: cmd, Parameters: key_value.New().Set("id", 1)}
	reqStr, err := req.ZmqEnvelope()
	s.Require().NoError(err)

	// Everything should be empty
	s.Require().True(test.frontend.queue.IsEmpty())
	s.Require().True(test.frontend.processing.IsEmpty())

	// The first message consumed instantly
	_, err = user.SendMessage(reqStr)
	s.Require().NoError(err)

	// Delay a bit for socket transfers
	time.Sleep(time.Millisecond * 50)

	// It's processing?
	s.Require().True(!test.frontend.queue.IsEmpty() || !test.frontend.processing.IsEmpty())

	// Sending the second message
	s.Require().True(test.frontend.queue.IsEmpty())
	req.Parameters.Set("id", 2)
	reqStr, err = req.ZmqEnvelope()
	s.Require().NoError(err)

	_, err = user.SendMessage(reqStr)
	s.Require().NoError(err)

	// Wait a bit for transmission between sockets
	time.Sleep(time.Millisecond * 50)

	// The frontend queued
	s.Require().True(test.frontend.queue.IsFull())

	// Sending the third message
	req.Parameters.Set("id", 3)
	reqStr, err = req.ZmqEnvelope()
	s.Require().NoError(err)

	_, err = user.SendMessage(reqStr)
	s.Require().NoError(err)

	// Wait a bit for transmission between sockets
	time.Sleep(time.Millisecond * 50)

	// Now we receive the messages
	repl, err := user.RecvMessage(0)
	s.Require().NoError(err)
	errorRepl, err := message.NewRep(repl)
	s.Require().NoError(err)
	s.Require().False(errorRepl.IsOK())
	_, err = user.RecvMessage(0)
	s.Require().NoError(err)
	_, err = user.RecvMessage(0)
	s.Require().NoError(err)

	// Close the frontend
	err = test.frontend.Close()
	s.Require().NoError(err)

	// wait a bit before the socket runner stops
	time.Sleep(time.Millisecond * 50)
	s.Require().Equal(CREATED, test.frontend.Status())

	err = user.Close()
	s.Require().NoError(err)

	instanceManager.Close()
	test.frontend.processing = key_value.NewList()
	// wait a bit for thread closing
	time.Sleep(time.Millisecond * 50)
}

// Test_14_PairSocket tests over-writing the external socket
func (test *TestFrontendSuite) Test_14_PairSocket() {
	s := &test.Suite

	test.frontend = New()

	cmd, _, instanceManager := test.instanceManager()
	test.frontend.SetInstanceManager(instanceManager)

	// Can not set the pair socket without configuration
	s.Require().Error(test.frontend.PairExternal())
	s.Require().False(test.frontend.paired)

	// Pair the external socket.
	test.frontend.SetConfig(test.handleConfig)
	s.Require().NoError(test.frontend.PairExternal())
	s.Require().True(test.frontend.paired)
	s.Require().Equal(config.PairType, test.frontend.externalConfig.Type)

	// Start
	s.Require().NoError(test.frontend.Start())

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 50)

	// Running?
	s.Require().Equal(RUNNING, test.frontend.Status())

	customExternal := test.customExternal()

	req := message.Request{Command: cmd, Parameters: key_value.New().Set("id", 1)}
	reqStr, err := req.ZmqEnvelope()
	s.Require().NoError(err)

	repl, err := customExternal.pairClient.RawRequest(message.JoinMessages(reqStr))
	s.Require().NoError(err)

	errorRepl, err := message.NewRep(repl)
	s.Require().NoError(err)
	s.Require().True(errorRepl.IsOK())

	// Close the frontend
	err = test.frontend.Close()
	s.Require().NoError(err)

	// wait a bit before the socket runner stops
	time.Sleep(time.Millisecond * 50)
	s.Require().Equal(CREATED, test.frontend.Status())

	err = customExternal.pairClient.Close()
	s.Require().NoError(err)

	instanceManager.Close()
	test.frontend.processing = key_value.NewList()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestFrontend(t *testing.T) {
	suite.Run(t, new(TestFrontendSuite))
}
