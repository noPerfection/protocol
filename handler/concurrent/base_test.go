package concurrent

import (
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestBaseHandlerSuite struct {
	suite.Suite
	tcpHandler    *Concurrent
	inprocHandler *Concurrent
	tcpConfig     *Config
	inprocConfig  *Config
	tcpClient     *client.Socket
	inprocClient  *client.Socket
	logger        *log.Logger
	routes        map[string]interface{}
}

// todo test in-process and external types of the handlers
// todo test the business of the handler
// Make sure that Account is set to five
// before each test
func (test *TestBaseHandlerSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("handler", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.tcpHandler = NewConcurrent()
	test.inprocHandler = NewConcurrent()

	// Socket to talk to clients
	test.routes = make(map[string]interface{}, 2)
	test.routes["command_1"] = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	test.routes["command_2"] = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}

	err = test.inprocHandler.Route("command_1", test.routes["command_1"])
	s.Require().NoError(err)
	err = test.inprocHandler.Route("command_2", test.routes["command_2"])
	s.Require().NoError(err)

	test.inprocConfig = NewInternalConfig(config.SyncReplierType, "test", "test")
	test.tcpConfig = NewConfig(config.SyncReplierType, "localhost", "test", 6000)

	// Setting a logger should fail since we don't have a configuration set
	s.Require().Error(test.inprocHandler.SetLogger(test.logger))

	// Setting the configuration
	// Setting the logger should be successful
	test.inprocHandler.SetConfig(test.inprocConfig)
	s.Require().NoError(test.inprocHandler.SetLogger(test.logger))

	// Setting the parameters of the Tcp Handler
	test.tcpHandler.SetConfig(test.tcpConfig)
	s.Require().NoError(test.tcpHandler.SetLogger(test.logger))
}

func (test *TestBaseHandlerSuite) Test_11_Route() {
	s := &test.Suite

	test.routes["command_3"] = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}

	err := test.inprocHandler.Route("command_3", test.routes["command_3"])
	s.Require().NoError(err)
	err = test.tcpHandler.Route("command_3", test.routes["command_3"])
	s.Require().NoError(err)

	err = test.tcpHandler.Route("command_4", test.routes["command_3"])
	s.Require().NoError(err)

	err = test.tcpHandler.Route("command_5", test.routes["command_2"])
	s.Require().NoError(err)

	test.routes["command_4"] = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	err = test.inprocHandler.Route("command_4", test.routes["command_4"])
	s.Require().NoError(err)
}

// Test_13_InstanceManager tests setting of the instance Manager and then listening to it.
func (test *TestBaseHandlerSuite) Test_13_InstanceManager() {
	s := &test.Suite

	// the instance Manager requires
	s.Require().NotNil(test.inprocHandler.InstanceManager)

	// It should be idle
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), Idle)
	s.Require().False(test.inprocHandler.instanceManagerStarted)
	s.Require().Empty(test.inprocHandler.InstanceManager.Instances())

	// Starting instance Manager
	s.Require().NoError(test.inprocHandler.StartInstanceManager())

	// Waiting a bit for instance Manager initialization
	time.Sleep(time.Millisecond * 2000)

	// Instance Manager should be running
	s.Require().Equal(Running, test.inprocHandler.InstanceManager.Status())
	s.Require().True(test.inprocHandler.instanceManagerStarted)
	s.Require().Len(test.inprocHandler.InstanceManager.Instances(), 1)

	// Let's send the close signal to the instance manager
	test.inprocHandler.InstanceManager.Close()

	// Waiting a bit for instance Manager closing
	time.Sleep(time.Millisecond * 100)

	// Check that Instance Manager is not running
	s.Require().Equal(Idle, test.inprocHandler.InstanceManager.Status())
	s.Require().False(test.inprocHandler.instanceManagerStarted)
	s.Require().Empty(test.inprocHandler.InstanceManager.Instances())
}

// Test_14_Start starts the handler.
func (test *TestBaseHandlerSuite) Test_14_Start() {
	s := &test.Suite

	err := test.inprocHandler.Start()
	s.Require().NoError(err)

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything works
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), Running)
	s.Require().Equal(test.inprocHandler.Frontend.Status(), RUNNING)

	// Now let's close it
	managerConfig := control.CreateInternalConfig(test.inprocConfig.Handler)
	inprocClient, err := client.NewRaw(config.SocketType(test.inprocConfig.Type), config.ConnectUrl(managerConfig.Id, managerConfig.Port))
	s.Require().NoError(err)
	reply, err := inprocClient.Request(&message.Request{
		Command:    control.HandlerClose,
		Parameters: datatype.New(),
	})
	s.Require().NoError(err)
	s.Require().True(reply.IsOK())
	s.Require().NoError(inprocClient.Close())

	// Wait a bit for closing handler threads
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything is closed
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), Idle)
	s.Require().Equal(test.inprocHandler.Frontend.Status(), CREATED)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestBaseHandler(t *testing.T) {
	suite.Run(t, new(TestBaseHandlerSuite))
}
