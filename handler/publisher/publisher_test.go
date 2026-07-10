package publisher

import (
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	base "github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

type TestPublisherSuite struct {
	suite.Suite
	pub           *Publisher
	config        message.Endpoint
	managerClient *zmq.Socket
	sub           *zmq.Socket
	logger        *log.Logger
	subscribed    chan []string
	closeClient   bool
	poller        *zmq.Poller
}

func (test *TestPublisherSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("publisher", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.pub = New()

	test.config = message.NewEndpoint("test", 0)

	s.Require().NoError(test.pub.SetLogger(test.logger))

	test.pub.SetEndpoint(test.config)
	s.Require().Equal(base.PublisherType, test.pub.Type())
	s.Require().NoError(test.pub.SetLogger(test.logger))

	test.managerClient, err = zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	managerConfig := control.NewInternalControlEndpoint(test.config)
	s.Require().NoError(test.managerClient.Connect(managerConfig.ClientUrl()))

	go test.subscribe()
	time.Sleep(time.Millisecond * 50)
}

func (test *TestPublisherSuite) subscribe() {
	s := &test.Suite

	sub, err := zmq.NewSocket(zmq.SUB)
	s.Require().NoError(err)
	s.Require().NoError(sub.SetSubscribe(""))
	url := test.config.ClientUrl()
	err = sub.Connect(url)

	s.Require().NoError(err)
	test.sub = sub
	test.subscribed = make(chan []string)
	test.closeClient = false

	test.poller = zmq.NewPoller()
	test.poller.Add(test.sub, zmq.POLLIN)

	for {
		if test.closeClient {
			break
		}

		polled, err := test.poller.Poll(time.Millisecond)
		s.Require().NoError(err)
		if len(polled) == 0 {
			continue
		}

		reply, err := test.sub.RecvMessage(0)
		s.Require().NoError(err)

		test.subscribed <- reply
	}
}

func (test *TestPublisherSuite) TearDownTest() {
	s := &test.Suite

	test.closeClient = true
	time.Sleep(time.Millisecond * 20)

	s.Require().NoError(test.sub.Close())
	s.Require().NoError(test.managerClient.Close())
	time.Sleep(time.Millisecond * 100)
}

func (test *TestPublisherSuite) restartSubscriber() {
	s := &test.Suite

	test.closeClient = true
	time.Sleep(time.Millisecond * 20)

	s.Require().NoError(test.sub.Close())

	go test.subscribe()
	time.Sleep(time.Millisecond * 50)
}

func (test *TestPublisherSuite) req(request message.Request) message.ReplyInterface {
	s := &test.Suite

	reqStr, err := test.pub.Packer().SerializeRequest(&request)
	s.Require().NoError(err)

	_, err = test.managerClient.SendMessage(reqStr)
	s.Require().NoError(err)

	raw, err := test.managerClient.RecvMessage(0)
	s.Require().NoError(err)

	reply, _, err := test.pub.Packer().DeserializeReply(raw)
	s.Require().NoError(err)

	return reply
}

func (test *TestPublisherSuite) sendNumberedBroadcasts(start uint64, amount uint64) {
	s := &test.Suite

	for number := start; number < start+amount; number++ {
		reply := test.req(message.Request{
			Command: Broadcast,
			Parameters: datatype.New().Set(BroadcastParameter, message.Reply{
				Status:     message.OK,
				Parameters: datatype.New().Set("number", number),
			}),
		})
		s.Require().True(reply.IsOK())
		time.Sleep(time.Millisecond * 2)
	}
}

func (test *TestPublisherSuite) receiveNumbers(start uint64, amount uint64) {
	s := &test.Suite

	for number := start; number < start+amount; number++ {
		select {
		case raw := <-test.subscribed:
			reply, _, err := test.pub.Packer().DeserializeReply(raw)
			s.Require().NoError(err)
			s.Require().True(reply.IsOK())

			received, err := reply.ReplyParameters().Uint64Value("number")
			s.Require().NoError(err)
			s.Require().Equal(number, received)
		case <-time.After(time.Second * 2):
			s.Require().Failf("timeout for subscribing", "expected number %d", number)
		}
	}
}

func (test *TestPublisherSuite) messageAmount() uint64 {
	s := &test.Suite

	reply := test.req(message.Request{Command: MessageAmount, Parameters: datatype.New()})
	s.Require().True(reply.IsOK())

	amount, err := reply.ReplyParameters().Uint64Value("broadcasting_length")
	s.Require().NoError(err)

	return amount
}

func (test *TestPublisherSuite) requireMessageAmount(expected uint64) {
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

func (test *TestPublisherSuite) Test_10_Start() {
	s := &test.Suite

	err := test.pub.Start()
	s.Require().NoError(err)

	time.Sleep(time.Millisecond * 100)

	statusReply := test.req(message.Request{Command: control.HandlerStatus, Parameters: datatype.New()})
	s.Require().True(statusReply.IsOK())
	status, err := statusReply.ReplyParameters().StringValue("status")
	s.Require().NoError(err)
	s.Require().Equal(base.SocketReady, status)

	test.sendNumberedBroadcasts(0, 10)
	test.receiveNumbers(0, 10)
	test.requireMessageAmount(0)

	closeReply := test.req(message.Request{Command: control.HandlerClose, Parameters: datatype.New()})
	s.Require().True(closeReply.IsOK())

	test.sendNumberedBroadcasts(10, 10)
	test.requireMessageAmount(10)

	test.restartSubscriber()

	startReply := test.req(message.Request{Command: control.HandlerStart, Parameters: datatype.New()})
	s.Require().True(startReply.IsOK())

	test.receiveNumbers(10, 10)
	test.requireMessageAmount(0)

	closeReply = test.req(message.Request{Command: control.HandlerClose, Parameters: datatype.New()})
	s.Require().True(closeReply.IsOK())
}

func TestPublisher(t *testing.T) {
	suite.Run(t, new(TestPublisherSuite))
}
