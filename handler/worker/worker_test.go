package worker

import (
	"strings"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/control"
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
	handlerConfig  message.Endpoint
	managerClient  *zmq.Socket
	externalClient *zmq.Socket
	logger         *log.Logger
	routes         map[string]base.HandleFunc
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
	test.routes = make(map[string]base.HandleFunc, 2)
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

	testID := strings.ReplaceAll(test.T().Name(), "/", "_")
	test.handlerConfig = message.NewEndpoint(testID, 0)

	s.Require().NoError(test.worker.SetLogger(test.logger))

	// Setting the configuration
	// Setting the logger should be successful
	test.worker.SetEndpoint(test.handlerConfig)
	s.Require().NoError(test.worker.SetLogger(test.logger))

	test.managerClient, err = zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	managerConfig := control.NewInternalControlEndpoint(test.handlerConfig)
	managerUrl := managerConfig.ClientUrl()
	err = test.managerClient.Connect(managerUrl)
	s.Require().NoError(err)

	externalClient, err := test.newExternalClient()
	s.Require().NoError(err)
	test.externalClient = externalClient
}

func (test *TestWorkerSuite) newExternalClient() (*zmq.Socket, error) {
	externalClient, err := zmq.NewSocket(zmq.PUSH)
	if err != nil {
		return nil, err
	}
	if err := externalClient.SetLinger(0); err != nil {
		_ = externalClient.Close()
		return nil, err
	}
	if err := externalClient.Connect(test.handlerConfig.ClientUrl()); err != nil {
		_ = externalClient.Close()
		return nil, err
	}
	return externalClient, nil
}

func (test *TestWorkerSuite) req(client *zmq.Socket, request message.Request) message.ReplyInterface {
	s := &test.Suite

	reqStr, err := test.worker.Packer().SerializeRequest(&request)
	s.Require().NoError(err)

	_, err = client.SendMessage(reqStr)
	s.Require().NoError(err)

	raw, err := client.RecvMessage(0)
	s.Require().NoError(err)

	reply, err := test.worker.Packer().DeserializeReply(raw)
	s.Require().NoError(err)

	return reply
}

func (test *TestWorkerSuite) submit(client *zmq.Socket, request message.Request) error {
	reqStr, err := test.worker.Packer().SerializeRequest(&request)
	if err != nil {
		return err
	}
	_, err = client.SendMessage(reqStr)
	return err
}

func (test *TestWorkerSuite) cleanOut() {
	s := &test.Suite

	if test.externalClient != nil {
		s.Require().NoError(test.externalClient.Close())
	}

	err := test.managerClient.Close()
	s.Require().NoError(err)

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 100)
}

func (test *TestWorkerSuite) Test_10_StartHandlesSubmittedMessages() {
	s := &test.Suite

	cmd1Id := "test_10_start"

	err := test.worker.Start()
	s.Require().NoError(err)

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything works
	req := message.Request{Command: control.HandlerStatus, Parameters: datatype.New()}
	reply := test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	status, err := reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.SocketReady, status)

	// Testing the external connection
	s.Require().Empty(test.cmd1Result)
	req = message.Request{
		Command:    "command_1",
		Parameters: datatype.New().Set("id", cmd1Id),
	}
	err = test.submit(test.externalClient, req)
	s.Require().NoError(err)

	// Wait a bit for the processing
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(cmd1Id, test.cmd1Result)

	// Close the handler
	req.Command = control.HandlerClose
	reply = test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	// clean out
	test.cleanOut()
}

func (test *TestWorkerSuite) Test_11_ControlLifecycle() {
	s := &test.Suite

	cmd1Id := "before_close"
	cmd2Id := "after_restart"

	err := test.worker.Start()
	s.Require().NoError(err)
	s.Require().Equal(base.SocketReady, test.worker.Control.Status())

	req := message.Request{
		Command:    "command_1",
		Parameters: datatype.New().Set("id", cmd1Id),
	}
	err = test.submit(test.externalClient, req)
	s.Require().NoError(err)
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(cmd1Id, test.cmd1Result)

	controlReq := message.Request{Command: control.HandlerClose, Parameters: datatype.New()}
	controlReply := test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())
	time.Sleep(time.Millisecond * 150)
	s.Require().Equal(base.SocketNil, test.worker.Control.Status())

	controlReq = message.Request{Command: control.HandlerStart, Parameters: datatype.New()}
	controlReply = test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())
	status, err := controlReply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.SocketReady, status)

	s.Require().NoError(test.externalClient.Close())
	test.externalClient, err = test.newExternalClient()
	s.Require().NoError(err)

	req = message.Request{
		Command:    "command_2",
		Parameters: datatype.New().Set("id", cmd2Id),
	}
	err = test.submit(test.externalClient, req)
	s.Require().NoError(err)
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(cmd2Id, test.cmd2Result)

	controlReq = message.Request{Command: control.HandlerClose, Parameters: datatype.New()}
	controlReply = test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())

	test.cleanOut()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestWorker(t *testing.T) {
	suite.Run(t, new(TestWorkerSuite))
}
