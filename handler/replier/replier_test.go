package replier

import (
	"strings"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestReplierSuite struct {
	suite.Suite
	replier        *Replier
	handlerConfig  *config.Handler
	managerClient  *zmq.Socket
	externalClient *zmq.Socket
	logger         *log.Logger
	routes         map[string]base.HandleFunc
}

func (test *TestReplierSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("replier", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.replier = New()

	// Socket to talk to clients
	test.routes = make(map[string]base.HandleFunc, 2)
	test.routes["command_1"] = func(request message.RequestInterface) message.ReplyInterface {
		time.Sleep(time.Millisecond * 100)
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	test.routes["command_2"] = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}

	err = test.replier.Route("command_1", test.routes["command_1"])
	s.Require().NoError(err)
	err = test.replier.Route("command_2", test.routes["command_2"])
	s.Require().NoError(err)

	testID := strings.ReplaceAll(test.T().Name(), "/", "_")
	test.handlerConfig = config.New(config.ReplierType, testID, "test", 0)

	// Setting a logger should fail since we don't have a configuration set
	s.Require().Error(test.replier.SetLogger(test.logger))

	// Setting the configuration
	// Setting the logger should be successful
	test.replier.SetConfig(test.handlerConfig)
	s.Require().NoError(test.replier.SetLogger(test.logger))

	test.managerClient, err = zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	managerConfig := control.CreateInternalConfig(test.handlerConfig)
	managerUrl := managerConfig.ClientUrl()
	err = test.managerClient.Connect(managerUrl)
	s.Require().NoError(err)

	externalClient, err := test.newExternalClient()
	s.Require().NoError(err)
	test.externalClient = externalClient
}

func (test *TestReplierSuite) newExternalClient() (*zmq.Socket, error) {
	externalClient, err := zmq.NewSocket(zmq.REQ)
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

func (test *TestReplierSuite) req(client *zmq.Socket, request message.Request) message.ReplyInterface {
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

func (test *TestReplierSuite) externalReq(client *zmq.Socket, request message.Request) (message.ReplyInterface, error) {
	reqStr, err := request.ZmqEnvelope()
	if err != nil {
		return nil, err
	}
	if _, err := client.SendMessage("", message.JoinMessages(reqStr)); err != nil {
		return nil, err
	}

	raw, err := client.RecvMessage(0)
	if err != nil {
		return nil, err
	}

	return message.NewRep(raw)
}

func (test *TestReplierSuite) handlerStatus() string {
	s := &test.Suite

	req := message.Request{Command: control.HandlerStatus, Parameters: datatype.New()}
	reply := test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	status, err := reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)

	return status
}

func (test *TestReplierSuite) cleanOut() {
	s := &test.Suite

	if test.externalClient != nil {
		s.Require().NoError(test.externalClient.Close())
	}

	err := test.managerClient.Close()
	s.Require().NoError(err)

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 100)
}

func (test *TestReplierSuite) Test_10_StartHandlesRequests() {
	s := &test.Suite

	err := test.replier.Start()
	s.Require().NoError(err)

	s.Require().Equal(base.SocketReady, test.replier.Status())
	s.Require().NotNil(test.replier.Socket())

	req := message.Request{Command: "command_1", Parameters: datatype.New()}
	reply, err := test.externalReq(test.externalClient, req)
	s.Require().NoError(err)
	s.Require().True(reply.IsOK())

	id, err := reply.ReplyParameters().StringValue("id")
	s.Require().NoError(err)
	s.Require().Equal("command_1", id)

	// Close the handler
	req.Command = control.HandlerClose
	reply = test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	test.cleanOut()
}

func (test *TestReplierSuite) Test_11_ControlLifecycle() {
	s := &test.Suite

	err := test.replier.Start()
	s.Require().NoError(err)
	s.Require().Equal(base.SocketReady, test.handlerStatus())

	req := message.Request{Command: "command_1", Parameters: datatype.New()}
	reply, err := test.externalReq(test.externalClient, req)
	s.Require().NoError(err)
	s.Require().True(reply.IsOK())

	controlReq := message.Request{Command: control.HandlerClose, Parameters: datatype.New()}
	controlReply := test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())
	time.Sleep(time.Millisecond * 150) // run loop processes close asynchronously

	s.Require().NoError(test.externalClient.SetRcvtimeo(time.Second))
	reply, err = test.externalReq(test.externalClient, req)
	s.Require().Error(err)
	s.Require().Nil(reply)
	s.Require().Equal(base.SocketNil, test.handlerStatus())

	controlReq = message.Request{Command: control.HandlerStart, Parameters: datatype.New()}
	controlReply = test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())
	status, err := controlReply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.SocketReady, status)
	s.Require().Equal(base.SocketReady, test.handlerStatus())

	s.Require().NoError(test.externalClient.Close())
	test.externalClient, err = test.newExternalClient()
	s.Require().NoError(err)

	reply, err = test.externalReq(test.externalClient, req)
	s.Require().NoError(err)
	s.Require().True(reply.IsOK())

	controlReq = message.Request{Command: control.HandlerClose, Parameters: datatype.New()}
	controlReply = test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())

	test.cleanOut()
}

func (test *TestReplierSuite) Test_12_HandlesMultipleRouterClients() {
	s := &test.Suite

	err := test.replier.Start()
	s.Require().NoError(err)

	secondClient, err := test.newExternalClient()
	s.Require().NoError(err)
	defer func() { _ = secondClient.Close() }()

	type requestResult struct {
		command string
		reply   message.ReplyInterface
		err     error
	}

	start := make(chan struct{})
	results := make(chan requestResult, 2)
	request := func(externalClient *zmq.Socket, command string) {
		<-start
		req := message.Request{Command: command, Parameters: datatype.New()}
		reply, err := test.externalReq(externalClient, req)
		results <- requestResult{command: command, reply: reply, err: err}
	}

	go request(test.externalClient, "command_1")
	go request(secondClient, "command_2")

	close(start)

	for i := 0; i < 2; i++ {
		result := <-results
		s.Require().NoError(result.err)
		s.Require().True(result.reply.IsOK())
		id, err := result.reply.ReplyParameters().StringValue("id")
		s.Require().NoError(err)
		s.Require().Equal(result.command, id)
	}

	controlReq := message.Request{Command: control.HandlerClose, Parameters: datatype.New()}
	controlReply := test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())

	test.cleanOut()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestReplier(t *testing.T) {
	suite.Run(t, new(TestReplierSuite))
}
