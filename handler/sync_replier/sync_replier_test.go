package sync_replier

import (
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
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
	handlerConfig  *config.Handler
	managerClient  *zmq.Socket
	externalClient *client.Socket
	logger         *log.Logger
	routes         map[string]interface{}
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
	test.routes = make(map[string]interface{}, 2)
	test.routes["command_1"] = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	test.routes["command_2"] = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}

	err = test.syncReplier.Route("command_1", test.routes["command_1"])
	s.Require().NoError(err)
	err = test.syncReplier.Route("command_2", test.routes["command_2"])
	s.Require().NoError(err)

	test.handlerConfig = config.NewInternalHandler(config.SyncReplierType, "test", "test")

	// Setting a logger should fail since we don't have a configuration set
	s.Require().Error(test.syncReplier.SetLogger(test.logger))

	// Setting the configuration
	// Setting the logger should be successful
	test.syncReplier.SetConfig(test.handlerConfig)
	s.Require().NoError(test.syncReplier.SetLogger(test.logger))

	test.managerClient, err = zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	managerUrl := test.handlerConfig.ManagerConnectUrl()
	err = test.managerClient.Connect(managerUrl)
	s.Require().NoError(err)

	externalUrl := config.ExternalUrl(test.handlerConfig.Id, test.handlerConfig.Port)
	externalClient, err := client.NewRaw(zmq.REP, externalUrl)
	s.Require().NoError(err)
	test.externalClient = externalClient
}

func (test *TestSyncReplierSuite) req(client *zmq.Socket, request message.Request) message.ReplyInterface {
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

func (test *TestSyncReplierSuite) cleanOut() {
	s := &test.Suite

	err := test.managerClient.Close()
	s.Require().NoError(err)

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 100)
}

// Test_10_Start starts the sync replier and makes sure that it can not have more than 1 instance.
func (test *TestSyncReplierSuite) Test_10_Start() {
	s := &test.Suite

	err := test.syncReplier.Start()
	s.Require().NoError(err)

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything works
	req := message.Request{Command: config.HandlerStatus, Parameters: datatype.New()}
	reply := test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	status, err := reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(control.Ready, status)

	// By default, the handler creates a socket.
	// Trying to add a new socket, it will throw an error
	s.Require().NotNil(test.syncReplier.instance)

	// Adding a new instance must fail
	req.Command = config.AddInstance
	reply = test.req(test.managerClient, req)
	s.Require().False(reply.IsOK())

	// Testing the external connection
	req = message.Request{Command: "command_1", Parameters: datatype.New()}
	reply, err = test.externalClient.Request(&req)
	s.Require().NoError(err)
	s.Require().True(reply.IsOK())

	// Close the handler
	req.Command = config.HandlerClose
	reply = test.req(test.managerClient, req)
	s.Require().True(reply.IsOK())

	// clean out
	test.cleanOut()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestSyncReplier(t *testing.T) {
	suite.Run(t, new(TestSyncReplierSuite))
}
