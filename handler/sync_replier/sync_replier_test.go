package sync_replier

import (
	"strings"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestSyncReplierSuite struct {
	suite.Suite
	syncReplier    *SyncReplier
	handlerConfig  message.Endpoint
	managerClient  *zmq.Socket
	externalClient *zmq.Socket
	logger         *log.Logger
	routes         map[string]handler.HandleFunc
}

// Make sure that Account is set to five
// before each test
func (test *TestSyncReplierSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("sync-replier", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.syncReplier = New()

	// Socket to talk to clients
	test.routes = make(map[string]handler.HandleFunc, 2)
	test.routes["command_1"] = func(request message.RequestInterface) message.ReplyInterface {
		time.Sleep(time.Millisecond * 100)
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	test.routes["command_2"] = func(request message.RequestInterface) message.ReplyInterface {
		time.Sleep(time.Millisecond * 200)
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}

	err = test.syncReplier.Route("command_1", test.routes["command_1"])
	s.Require().NoError(err)
	err = test.syncReplier.Route("command_2", test.routes["command_2"])
	s.Require().NoError(err)

	testID := strings.ReplaceAll(test.T().Name(), "/", "_")
	test.handlerConfig = message.NewEndpoint(testID, 0)

	s.Require().NoError(test.syncReplier.SetLogger(test.logger))

	// Setting the configuration
	// Setting the logger should be successful
	test.syncReplier.SetEndpoint(test.handlerConfig)
	s.Require().NoError(test.syncReplier.SetLogger(test.logger))

	test.managerClient, err = zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	managerConfig := handler.NewInternalControlEndpoint(test.handlerConfig)
	managerUrl := managerConfig.ClientUrl()
	err = test.managerClient.Connect(managerUrl)
	s.Require().NoError(err)

	externalClient, err := test.newExternalClient()
	s.Require().NoError(err)
	test.externalClient = externalClient
}

func (test *TestSyncReplierSuite) newExternalClient() (*zmq.Socket, error) {
	s := &test.Suite

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

	s.Require().NotNil(externalClient)
	return externalClient, nil
}

func (test *TestSyncReplierSuite) req(client *zmq.Socket, request message.Request) message.ReplyInterface {
	s := &test.Suite

	reqStr, err := test.syncReplier.Packer().SerializeRequest(&request)
	s.Require().NoError(err)

	_, err = client.SendMessage(reqStr)
	s.Require().NoError(err)

	raw, err := client.RecvMessage(0)
	s.Require().NoError(err)

	reply, _, err := test.syncReplier.Packer().DeserializeReply(raw)
	s.Require().NoError(err)

	return reply
}

func (test *TestSyncReplierSuite) externalReq(client *zmq.Socket, request message.Request) (message.ReplyInterface, error) {
	reqStr, err := test.syncReplier.Packer().SerializeRequest(&request)
	if err != nil {
		return nil, err
	}
	if _, err := client.SendMessage(reqStr); err != nil {
		return nil, err
	}

	raw, err := client.RecvMessage(0)
	if err != nil {
		return nil, err
	}

	reply, _, err := test.syncReplier.Packer().DeserializeReply(raw)
	return reply, err
}

func (test *TestSyncReplierSuite) handlerStatus() string {
	s := &test.Suite

	req := message.Request{Command: handler.HandlerStatus, Parameters: datatype.New()}
	reply := test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	status, err := reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)

	return status
}

func (test *TestSyncReplierSuite) cleanOut() {
	s := &test.Suite

	if test.externalClient != nil {
		s.Require().NoError(test.externalClient.Close())
	}

	err := test.managerClient.Close()
	s.Require().NoError(err)

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 100)
}

func (test *TestSyncReplierSuite) Test_10_StartHandlesOneRequestAtATime() {
	s := &test.Suite

	err := test.syncReplier.Start()
	s.Require().NoError(err)

	s.Require().Equal(handler.SocketReady, test.syncReplier.Control.Status())
	s.Require().NotNil(test.syncReplier.socket)

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

	startedAt := time.Now()
	close(start)

	for i := 0; i < 2; i++ {
		result := <-results
		s.Require().NoError(result.err)
		s.Require().True(result.reply.IsOK())
		id, err := result.reply.ReplyParameters().StringValue("id")
		s.Require().NoError(err)
		s.Require().Equal(result.command, id)
	}

	s.Require().GreaterOrEqual(time.Since(startedAt), time.Millisecond*280)

	// Close the handler
	req := message.Request{Command: handler.HandlerClose, Parameters: datatype.New()}
	reply := test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	// clean out
	test.cleanOut()
}

func (test *TestSyncReplierSuite) Test_11_ControlLifecycle() {
	s := &test.Suite

	err := test.syncReplier.Start()
	s.Require().NoError(err)
	s.Require().Equal(handler.SocketReady, test.handlerStatus())

	req := message.Request{Command: "command_1", Parameters: datatype.New()}
	reply, err := test.externalReq(test.externalClient, req)
	s.Require().NoError(err)
	s.Require().True(reply.IsOK())

	controlReq := message.Request{Command: handler.HandlerClose, Parameters: datatype.New()}
	controlReply := test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())
	time.Sleep(time.Millisecond * 150) // run loop processes close asynchronously

	s.Require().NoError(test.externalClient.SetSndtimeo(time.Second))
	s.Require().NoError(test.externalClient.SetRcvtimeo(time.Second))
	reply, err = test.externalReq(test.externalClient, req)
	s.Require().Error(err)
	s.Require().Nil(reply)
	s.Require().Equal(handler.SocketNil, test.handlerStatus())

	controlReq = message.Request{Command: handler.HandlerStart, Parameters: datatype.New()}
	controlReply = test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())
	status, err := controlReply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(handler.SocketReady, status)
	s.Require().Equal(handler.SocketReady, test.handlerStatus())

	s.Require().NoError(test.externalClient.Close())
	test.externalClient, err = test.newExternalClient()
	s.Require().NoError(err)

	reply, err = test.externalReq(test.externalClient, req)
	s.Require().NoError(err)
	s.Require().True(reply.IsOK())

	controlReq = message.Request{Command: handler.HandlerClose, Parameters: datatype.New()}
	controlReply = test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())

	test.cleanOut()
}

func (test *TestSyncReplierSuite) Test_13_WhitelistHMAC() {
	s := &test.Suite
	defer test.cleanOut()

	const secret = "sync-replier-secret"
	s.Require().NoError(test.syncReplier.Whitelist("command_1", secret))
	s.Require().NoError(test.syncReplier.Start())
	time.Sleep(time.Millisecond * 100)

	unsigned := message.Request{
		Command:    "command_1",
		Parameters: datatype.New().Set("id", "unsigned"),
	}
	unsignedEnvelope, err := test.syncReplier.Packer().SerializeRequest(&unsigned)
	s.Require().NoError(err)
	_, err = test.externalClient.SendMessage(unsignedEnvelope)
	s.Require().NoError(err)
	raw, err := test.externalClient.RecvMessage(0)
	s.Require().NoError(err)
	unsignedReply, _, err := test.syncReplier.Packer().DeserializeReply(raw)
	s.Require().NoError(err)
	s.Require().False(unsignedReply.IsOK())
	s.Require().Equal(message.ErrAccessDenied.Error(), unsignedReply.ErrorMessage())

	signed := message.Request{
		Command:    "command_1",
		Parameters: datatype.New().Set("id", "signed"),
	}
	hmacHash := message.ComputeHMAC(signed.String(), secret)
	signedEnvelope, err := test.syncReplier.Packer().SerializeRequest(&signed, hmacHash)
	s.Require().NoError(err)
	_, err = test.externalClient.SendMessage(signedEnvelope)
	s.Require().NoError(err)
	raw, err = test.externalClient.RecvMessage(0)
	s.Require().NoError(err)
	signedReply, replyHmac, err := test.syncReplier.Packer().DeserializeReply(raw)
	s.Require().NoError(err)
	s.Require().True(signedReply.IsOK())
	s.Require().NotEmpty(replyHmac)
	s.Require().True(message.VerifyHMAC(signedReply.String(), secret, replyHmac))

	controlReq := message.Request{Command: handler.HandlerClose, Parameters: datatype.New()}
	controlReply := test.req(test.managerClient, controlReq)
	s.Require().True(controlReply.IsOK())
}

func (test *TestSyncReplierSuite) Test_12_StartWithoutLogger() {
	s := &test.Suite
	defer test.cleanOut()

	svc := New()
	testID := strings.ReplaceAll(test.T().Name(), "/", "_")
	handlerConfig := message.NewEndpoint(testID, 0)
	svc.SetEndpoint(handlerConfig)
	s.Require().NoError(svc.Route("command_1", test.routes["command_1"]))

	s.Require().NoError(svc.Start())
	s.Require().Equal(handler.SocketReady, svc.Control.Status())

	managerClient, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	defer func() { _ = managerClient.Close() }()

	managerConfig := handler.NewInternalControlEndpoint(handlerConfig)
	s.Require().NoError(managerClient.Connect(managerConfig.ClientUrl()))

	controlReq := message.Request{Command: handler.HandlerClose, Parameters: datatype.New()}
	controlReply := test.req(managerClient, controlReq)
	s.Require().True(controlReply.IsOK())
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestSyncReplier(t *testing.T) {
	suite.Run(t, new(TestSyncReplierSuite))
}
