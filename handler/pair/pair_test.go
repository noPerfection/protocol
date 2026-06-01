package pair

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
type TestPairSuite struct {
	suite.Suite

	externalConfig *config.Handler
	pairConfig     *config.Handler
	pair           *Pair
	managerClient  *zmq.Socket
	externalClient *zmq.Socket
	logger         *log.Logger
}

func (test *TestPairSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("pair", false)
	s.Require().NoError(err, "failed to create logger")
	test.logger = logger

	testID := strings.ReplaceAll(test.T().Name(), "/", "_")
	test.externalConfig = config.New(config.ReplierType, testID, "external_main", 0)
	test.pairConfig = config.New(config.PairType, testID+"_pair", "external_main_pair", 0)
	test.pair = New()

	s.Require().NoError(test.pair.SetLogger(test.logger))
	test.pair.SetConfig(test.pairConfig)
	s.Require().NoError(test.pair.SetLogger(test.logger))
	s.Require().NoError(test.pair.Route("client-message", func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("handled", request.CommandName()))
	}))

	test.managerClient, err = zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	managerConfig := control.CreateInternalConfig(test.pairConfig)
	s.Require().NoError(test.managerClient.Connect(managerConfig.ClientUrl()))
}

func (test *TestPairSuite) TearDownTest() {
	if test.managerClient != nil {
		if test.pair != nil && test.pair.Status() == base.SocketReady {
			reply := test.req(message.Request{Command: control.HandlerClose, Parameters: datatype.New()})
			test.Require().True(reply.IsOK())
		}
		test.Require().NoError(test.managerClient.Close())
	}

	if test.externalClient != nil {
		test.Require().NoError(test.externalClient.Close())
	}

	time.Sleep(time.Millisecond * 100)
}

func (test *TestPairSuite) newExternalClient() *zmq.Socket {
	s := &test.Suite

	externalClient, err := zmq.NewSocket(zmq.PAIR)
	s.Require().NoError(err)
	s.Require().NoError(externalClient.SetLinger(0))
	s.Require().NoError(externalClient.Connect(test.pairConfig.ClientUrl()))
	test.externalClient = externalClient

	return externalClient
}

func (test *TestPairSuite) req(request message.Request) message.ReplyInterface {
	s := &test.Suite

	reqStr, err := test.pair.Packer().SerializeRequest(&request)
	s.Require().NoError(err)

	_, err = test.managerClient.SendMessage(reqStr)
	s.Require().NoError(err)

	raw, err := test.managerClient.RecvMessage(0)
	s.Require().NoError(err)

	reply, err := test.pair.Packer().DeserializeReply(raw)
	s.Require().NoError(err)

	return reply
}

func (test *TestPairSuite) messageAmount() uint64 {
	s := &test.Suite

	reply := test.req(message.Request{Command: MessageAmount, Parameters: datatype.New()})
	s.Require().True(reply.IsOK())

	amount, err := reply.ReplyParameters().Uint64Value("broadcasting_length")
	s.Require().NoError(err)

	return amount
}

func (test *TestPairSuite) requireMessageAmount(expected uint64) {
	s := &test.Suite

	deadline := time.After(time.Second * 2)
	tick := time.Tick(time.Millisecond * 5)
	for {
		amount := test.messageAmount()
		if amount == expected {
			return
		}

		select {
		case <-deadline:
			s.Require().Equal(expected, amount)
			return
		case <-tick:
		}
	}
}

func (test *TestPairSuite) handlerStatus() string {
	s := &test.Suite

	reply := test.req(message.Request{Command: control.HandlerStatus, Parameters: datatype.New()})
	s.Require().True(reply.IsOK())

	status, err := reply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)

	return status
}

func (test *TestPairSuite) receiveRequest() message.RequestInterface {
	s := &test.Suite

	poller := zmq.NewPoller()
	poller.Add(test.externalClient, zmq.POLLIN)

	polled, err := poller.Poll(time.Second * 2)
	s.Require().NoError(err)
	s.Require().NotEmpty(polled)

	raw, err := test.externalClient.RecvMessage(0)
	s.Require().NoError(err)

	req, err := test.pair.Packer().DeserializeRequest(raw)
	s.Require().NoError(err)

	return req
}

func (test *TestPairSuite) receiveBroadcast() message.ReplyInterface {
	s := &test.Suite

	poller := zmq.NewPoller()
	poller.Add(test.externalClient, zmq.POLLIN)

	polled, err := poller.Poll(time.Second * 2)
	s.Require().NoError(err)
	s.Require().NotEmpty(polled)

	raw, err := test.externalClient.RecvMessage(0)
	s.Require().NoError(err)

	reply, err := test.pair.Packer().DeserializeReply(raw)
	s.Require().NoError(err)

	return reply
}

func (test *TestPairSuite) pairRequest(request message.Request) message.ReplyInterface {
	s := &test.Suite

	reqStr, err := test.pair.Packer().SerializeRequest(&request)
	s.Require().NoError(err)

	_, err = test.externalClient.SendMessage(reqStr)
	s.Require().NoError(err)

	poller := zmq.NewPoller()
	poller.Add(test.externalClient, zmq.POLLIN)

	polled, err := poller.Poll(time.Second * 2)
	s.Require().NoError(err)
	s.Require().NotEmpty(polled)

	raw, err := test.externalClient.RecvMessage(0)
	s.Require().NoError(err)

	reply, err := test.pair.Packer().DeserializeReply(raw)
	s.Require().NoError(err)

	return reply
}

func (test *TestPairSuite) Test_10_StartReceivesAndBroadcasts() {
	s := &test.Suite

	err := test.pair.Start()
	s.Require().NoError(err)
	s.Require().Equal(base.SocketReady, test.pair.Status())

	test.newExternalClient()
	time.Sleep(time.Millisecond * 50)

	clientReq := message.Request{
		Command:    "client-message",
		Parameters: datatype.New().Set("number", uint64(11)),
	}
	clientReply := test.pairRequest(clientReq)
	s.Require().True(clientReply.IsOK())
	number, err := clientReply.ReplyParameters().Uint64Value("number")
	s.Require().NoError(err)
	s.Require().Equal(uint64(11), number)
	handled, err := clientReply.ReplyParameters().StringValue("handled")
	s.Require().NoError(err)
	s.Require().Equal("client-message", handled)

	test.requireMessageAmount(0)

	controlReply := test.req(message.Request{
		Command: Broadcast,
		Parameters: datatype.New().Set(BroadcastParameter, message.Reply{
			Status:     message.OK,
			Parameters: datatype.New().Set("number", uint64(22)),
		}),
	})
	s.Require().True(controlReply.IsOK())

	broadcastReply := test.receiveBroadcast()
	s.Require().True(broadcastReply.IsOK())
	number, err = broadcastReply.ReplyParameters().Uint64Value("number")
	s.Require().NoError(err)
	s.Require().Equal(uint64(22), number)
}

func (test *TestPairSuite) Test_11_ControlLifecycle() {
	s := &test.Suite

	err := test.pair.Start()
	s.Require().NoError(err)
	s.Require().Equal(base.SocketReady, test.handlerStatus())

	closeReply := test.req(message.Request{Command: control.HandlerClose, Parameters: datatype.New()})
	s.Require().True(closeReply.IsOK())
	time.Sleep(time.Millisecond * 150)
	s.Require().Equal(base.SocketNil, test.handlerStatus())

	startReply := test.req(message.Request{Command: control.HandlerStart, Parameters: datatype.New()})
	s.Require().True(startReply.IsOK())
	status, err := startReply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.SocketReady, status)
	s.Require().Equal(base.SocketReady, test.handlerStatus())

	test.newExternalClient()
	time.Sleep(time.Millisecond * 50)

	broadcastReply := test.req(message.Request{
		Command: Broadcast,
		Parameters: datatype.New().Set(BroadcastParameter, message.Reply{
			Status:     message.OK,
			Parameters: datatype.New().Set("number", uint64(33)),
		}),
	})
	s.Require().True(broadcastReply.IsOK())

	received := test.receiveBroadcast()
	s.Require().True(received.IsOK())
	number, err := received.ReplyParameters().Uint64Value("number")
	s.Require().NoError(err)
	s.Require().Equal(uint64(33), number)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestPair(t *testing.T) {
	suite.Run(t, new(TestPairSuite))
}
