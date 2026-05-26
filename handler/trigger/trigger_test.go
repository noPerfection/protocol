package trigger

import (
	"sync"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/frontend"
	"github.com/noPerfection/protocol/handler/instance_manager"
	"github.com/noPerfection/protocol/handler/manager_client"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestTriggerSuite struct {
	suite.Suite
	handler     *Trigger
	config      *config.Trigger
	client      *zmq.Socket
	closeClient bool
	trigger     *client.Socket
	poller      *zmq.Poller
	logger      *log.Logger
	subscribed  chan []string
}

// Todo test in-process and external types of handlers
// todo test the business of the handler.
// Make sure that Account is set to five
// before each test
func (test *TestTriggerSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("trigger", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.handler = New()

	// Socket to talk to clients
	handlerConfig := config.NewInternalHandler(config.SyncReplierType, "test", "test")
	triggerConfig, err := config.InternalTriggerAble(handlerConfig, config.PublisherType)
	s.Require().NoError(err)
	test.config = triggerConfig

	// Setting the configuration
	// Setting the logger should be successful
	test.handler.SetConfig(test.config)
	s.Require().NoError(test.handler.SetLogger(test.logger))

	// Initiate the trigger
	socket, err := client.New(test.handler.TriggerClient())
	s.Require().NoError(err)
	test.trigger = socket

	// Initiate the subscriber
	go test.subscribe()
	time.Sleep(time.Millisecond * 50)
}

// subscribe to the handler.
func (test *TestTriggerSuite) subscribe() {
	s := &test.Suite

	sub, err := zmq.NewSocket(zmq.SUB)
	s.Require().NoError(err)
	s.Require().NoError(sub.SetSubscribe(""))
	// It won't work if the trigger is using a tcp protocol.
	// For tcp protocol, use clientConfig.Url()
	url := config.ExternalUrl(test.config.BroadcastId, test.config.BroadcastPort)
	err = sub.Connect(url)

	s.Require().NoError(err)
	test.client = sub
	test.subscribed = make(chan []string)
	test.closeClient = false

	test.poller = zmq.NewPoller()
	test.poller.Add(test.client, zmq.POLLIN)

	for {
		if test.closeClient {
			break
		}

		polled, err := test.poller.Poll(time.Millisecond)
		s.Require().NoError(err)
		if len(polled) == 0 {
			continue
		}

		reply, err := test.client.RecvMessage(0)
		s.Require().NoError(err)

		test.subscribed <- reply
	}
}

// TearDownTest cleans out the client sockets.
// The subscriber client socket must be closed after
func (test *TestTriggerSuite) TearDownTest() {
	s := &test.Suite

	// unsubscribe
	test.closeClient = true
	// wait a bit for unsubscribing
	time.Sleep(time.Millisecond * 50)

	s.Require().NoError(test.client.Close())
	s.Require().NoError(test.trigger.Close())
}

// Test_10_Config makes sure that configuration methods started without any error.
//
// It doesn't validate the internal parameters.
// So it must validate them.
func (test *TestTriggerSuite) Test_10_Config() {
	s := &test.Suite

	// Testing config trigger-able
	_, err := config.TriggerAble(config.SyncReplierType, "localhost", "test", 6000, config.PublisherType, "localhost", 6001)
	s.Require().NoError(err)
}

// Test_14_Start start the trigger.
//
// The trigger creates a client that will subscribe for the messages to subscribe.
func (test *TestTriggerSuite) Test_14_Start() {
	s := &test.Suite

	err := test.handler.Start()
	s.Require().NoError(err)

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	// make sure that instance is created
	s.Require().Len(test.handler.Handler.InstanceManager.Instances(), 1)

	// trigger a message
	req := message.Request{Command: "hello", Parameters: datatype.New().Set("number", 1)}

	err = test.trigger.Submit(&req)
	s.Require().NoError(err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		isTimeout := false
		select {
		case _ = <-test.subscribed:
			isTimeout = false
		case <-time.After(time.Second * 2):
			// call timed out
			isTimeout = true
		}
		wg.Done()
		s.Require().False(isTimeout, "timeout for subscribing")
	}()
	wg.Wait()

	// Make sure that everything works
	s.Require().Equal(test.handler.InstanceManager.Status(), instance_manager.Running)
	s.Require().Equal(test.handler.Frontend.Status(), frontend.RUNNING)
	s.Require().NotNil(test.handler.socket)

	// Now let's close it
	triggerClient, err := manager_client.New(test.config.Handler)
	s.Require().NoError(err)
	s.Require().NoError(triggerClient.Close())

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything is closed
	s.Require().Equal(test.handler.InstanceManager.Status(), instance_manager.Idle)
	s.Require().Equal(test.handler.Frontend.Status(), frontend.CREATED)
	s.Require().Nil(test.handler.socket)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestTrigger(t *testing.T) {
	suite.Run(t, new(TestTriggerSuite))
}
