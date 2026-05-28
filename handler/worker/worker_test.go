package worker

import (
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/handler_manager"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestWorkerSuite struct {
	suite.Suite
	worker         *Worker
	handlerConfig  *config.Concurrent
	managerClient  *zmq.Socket
	externalClient *client.Socket
	logger         *log.Logger
	routes         map[string]interface{}
	cmd1Result     string
	cmd2Result     string
}

// Make sure that Account is set to five
// before each test
func (test *TestWorkerSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("sync-replier", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.worker = New()

	// Socket to talk to clients
	test.routes = make(map[string]interface{}, 2)
	test.routes["command_1"] = func(request message.RequestInterface) message.ReplyInterface {
		id, err := request.RouteParameters().StringValue("id")
		if err != nil {
			return request.Fail("missing id parameter")
		}
		test.cmd1Result = id

		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	test.routes["command_2"] = func(request message.RequestInterface) message.ReplyInterface {
		id, err := request.RouteParameters().StringValue("id")
		if err != nil {
			return request.Fail("missing id parameter")
		}
		test.cmd2Result = id

		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}

	err = test.worker.Route("command_1", test.routes["command_1"])
	s.Require().NoError(err)
	err = test.worker.Route("command_2", test.routes["command_2"])
	s.Require().NoError(err)

	test.handlerConfig = config.NewInternalConcurrent(config.WorkerType, "test", "test")
	inprocUrl := config.ExternalUrl(test.handlerConfig.Id, test.handlerConfig.Port)

	// Setting a logger should fail since we don't have a configuration set
	s.Require().Error(test.worker.SetLogger(test.logger))

	// Setting the configuration
	// Setting the logger should be successful
	test.worker.SetConfig(test.handlerConfig)
	s.Require().NoError(test.worker.SetLogger(test.logger))

	test.managerClient, err = zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	managerUrl := test.handlerConfig.ManagerConnectUrl()
	err = test.managerClient.Connect(managerUrl)
	s.Require().NoError(err)

	externalClient, err := client.NewRaw(zmq.PULL, inprocUrl)
	s.Require().NoError(err)
	test.externalClient = externalClient
}

func (test *TestWorkerSuite) req(client *zmq.Socket, request message.Request) message.ReplyInterface {
	s := &test.Suite

	reqStr, err := request.ZmqEnvelope()
	s.Require().NoError(err)

	_, err = client.SendMessage(reqStr)
	s.Require().NoError(err)

	raw, err := client.RecvMessage(0)
	s.Require().NoError(err)

	reply, err := message.NewRep(raw)
	s.Require().NoError(err)

	return reply
}

func (test *TestWorkerSuite) cleanOut() {
	s := &test.Suite

	err := test.managerClient.Close()
	s.Require().NoError(err)

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 100)
}

// Test_10_Start starts the sync replier and makes sure that it can not have more than 1 instance.
func (test *TestWorkerSuite) Test_10_Start() {
	s := &test.Suite

	cmd1Id := "test_10_start"

	err := test.worker.Start()
	s.Require().NoError(err)

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything works
	req := message.Request{Command: config.HandlerStatus, Parameters: datatype.New()}
	reply := test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	status, err := reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(handler_manager.Ready, status)

	req = message.Request{Command: config.InstanceAmount, Parameters: datatype.New()}
	reply = test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	// By default, the handler creates a socket.
	// Trying to add a new socket, it will throw an error
	s.Require().Len(test.worker.InstanceManager.Instances(), 1)

	// Testing the external connection
	s.Require().Empty(test.cmd1Result)
	req = message.Request{
		Command:    "command_1",
		Parameters: datatype.New().Set("id", cmd1Id),
	}
	err = test.externalClient.Submit(&req)
	s.Require().NoError(err)

	// Wait a bit for the processing
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(cmd1Id, test.cmd1Result)

	// Close the handler
	req.Command = config.HandlerClose
	reply = test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	// clean out
	test.cleanOut()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestWorker(t *testing.T) {
	suite.Run(t, new(TestWorkerSuite))
}
