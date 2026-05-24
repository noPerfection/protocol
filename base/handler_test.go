package base

import (
	"github.com/sds-framework/client-lib"
	clientConfig "github.com/sds-framework/client-lib/config"
	"github.com/sds-framework/datatype-lib/message"
	"github.com/sds-framework/handler-lib/config"
	"github.com/sds-framework/handler-lib/frontend"
	"github.com/sds-framework/handler-lib/instance_manager"
	"github.com/sds-framework/handler-lib/manager_client"
	"github.com/sds-framework/log-lib"
	"github.com/stretchr/testify/suite"
	"slices"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestBaseHandlerSuite struct {
	suite.Suite
	tcpHandler    *Handler
	inprocHandler *Handler
	tcpConfig     *config.Handler
	inprocConfig  *config.Handler
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

	test.tcpHandler = New()
	test.inprocHandler = New()

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

	test.inprocConfig = config.NewInternalHandler(config.SyncReplierType, "test", "test")
	test.tcpConfig = config.NewHandler(config.SyncReplierType, "localhost", "test", 6000)

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

// Test_11_Deps tests setting of the route dependencies
func (test *TestBaseHandlerSuite) Test_11_Deps() {
	s := &test.Suite

	// Handler must not have dependencies yet
	s.Require().Empty(test.inprocHandler.DepIds())
	s.Require().Empty(test.tcpHandler.DepIds())

	test.routes["command_3"] = func(request message.RequestInterface, _ *client.Socket, _ *client.Socket) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}

	// Adding a new route with the dependencies
	err := test.inprocHandler.Route("command_3", test.routes["command_3"], "dep_1", "dep_2")
	s.Require().NoError(err)
	err = test.tcpHandler.Route("command_3", test.routes["command_3"], "dep_1", "dep_2")
	s.Require().NoError(err)

	s.Require().Len(test.inprocHandler.DepIds(), 2)
	s.Require().Len(test.tcpHandler.DepIds(), 2)

	// Trying to route the handler with inconsistent dependencies must fail
	err = test.tcpHandler.Route("command_4", test.routes["command_3"]) // command_3 handler requires two dependencies
	s.Require().Error(err)

	err = test.tcpHandler.Route("command_5", test.routes["command_2"], "dep_1", "dep_2") // command_2 handler not requires any dependencies
	s.Require().Error(err)

	// Adding a new command with already added dependency should be fine
	test.routes["command_4"] = func(request message.RequestInterface, _ *client.Socket, _ *client.Socket) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	err = test.inprocHandler.Route("command_4", test.routes["command_4"], "dep_1", "dep_3") // command_3 handler requires two dependencies
	s.Require().NoError(err)

	depIds := test.inprocHandler.DepIds()
	s.Require().Len(depIds, 3)
	s.Require().EqualValues([]string{"dep_1", "dep_2", "dep_3"}, depIds)

}

// Test_12_DepConfig tests setting of the dependency configurations
func (test *TestBaseHandlerSuite) Test_12_DepConfig() {
	s := &test.Suite

	s.Require().NotNil(test.inprocHandler.logger)

	test.routes = make(map[string]interface{}, 2)
	test.routes["command_1"] = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	test.routes["command_2"] = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	test.routes["command_3"] = func(request message.RequestInterface, _ *client.Socket, _ *client.Socket) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}
	test.routes["command_4"] = func(request message.RequestInterface, _ *client.Socket, _ *client.Socket) message.ReplyInterface {
		return request.Ok(request.RouteParameters().Set("id", request.CommandName()))
	}

	err := test.inprocHandler.Route("command_1", test.routes["command_1"])
	s.Require().NoError(err)
	err = test.inprocHandler.Route("command_2", test.routes["command_2"])
	s.Require().NoError(err)
	err = test.inprocHandler.Route("command_3", test.routes["command_3"], "dep_1", "dep_2")
	s.Require().NoError(err)
	err = test.inprocHandler.Route("command_4", test.routes["command_4"], "dep_1", "dep_3") // command_3 handler requires two dependencies
	s.Require().NoError(err)

	// No dependency configurations were added yet
	s.Require().Error(test.inprocHandler.depConfigsAdded())

	// No dependency config should be given
	depIds := test.inprocHandler.DepIds()
	//AddDepByService
	for _, id := range depIds {
		s.Require().False(test.inprocHandler.AddedDepByService(id))
	}

	// Adding the dependencies
	for _, id := range depIds {
		depConfig := &clientConfig.Client{
			Id:         id,
			ServiceUrl: "github.com/sds-framework/" + id,
			Port:       0,
		}

		s.Require().NoError(test.inprocHandler.AddDepByService(depConfig))
	}

	// There should be dependency configurations now
	for _, id := range depIds {
		s.Require().True(test.inprocHandler.AddedDepByService(id))
	}

	// All dependency configurations were added
	s.Require().NoError(test.inprocHandler.depConfigsAdded())

	// trying to add the configuration for the dependency that doesn't exist should fail
	depId := "not_exist"
	s.Require().False(slices.Contains(depIds, depId))
	depConfig := &clientConfig.Client{
		Id:         depId,
		ServiceUrl: "github.com/sds-framework/" + depId,
		Port:       0,
	}
	s.Require().Error(test.inprocHandler.AddDepByService(depConfig))

	// Trying to add the configuration that was already added should fail
	depId = depIds[0]
	depConfig = &clientConfig.Client{
		Id:         depId,
		ServiceUrl: "github.com/sds-framework/" + depId,
		Port:       0,
	}
	s.Require().Error(test.inprocHandler.AddDepByService(depConfig))
}

// Test_13_InstanceManager tests setting of the instance Manager and then listening to it.
func (test *TestBaseHandlerSuite) Test_13_InstanceManager() {
	s := &test.Suite

	// the instance Manager requires
	s.Require().NotNil(test.inprocHandler.InstanceManager)

	// It should be idle
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), instance_manager.Idle)
	s.Require().False(test.inprocHandler.instanceManagerStarted)
	s.Require().Empty(test.inprocHandler.InstanceManager.Instances())

	// Starting instance Manager
	s.Require().NoError(test.inprocHandler.StartInstanceManager())

	// Waiting a bit for instance Manager initialization
	time.Sleep(time.Millisecond * 2000)

	// Instance Manager should be running
	s.Require().Equal(instance_manager.Running, test.inprocHandler.InstanceManager.Status())
	s.Require().True(test.inprocHandler.instanceManagerStarted)
	s.Require().Len(test.inprocHandler.InstanceManager.Instances(), 1)

	// Let's send the close signal to the instance manager
	test.inprocHandler.InstanceManager.Close()

	// Waiting a bit for instance Manager closing
	time.Sleep(time.Millisecond * 100)

	// Check that Instance Manager is not running
	s.Require().Equal(instance_manager.Idle, test.inprocHandler.InstanceManager.Status())
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
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), instance_manager.Running)
	s.Require().Equal(test.inprocHandler.Frontend.Status(), frontend.RUNNING)

	// Now let's close it
	inprocClient, err := manager_client.New(test.inprocConfig)
	s.Require().NoError(err)
	s.Require().NoError(inprocClient.Close())

	// Wait a bit for closing handler threads
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything is closed
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), instance_manager.Idle)
	s.Require().Equal(test.inprocHandler.Frontend.Status(), frontend.CREATED)
}

// Test_15_Misc tests requiredMetadata, AnyRoute functions.
func (test *TestBaseHandlerSuite) Test_15_Misc() {
	s := &test.Suite

	s.Require().Len(requiredMetadata(), 2)

	handler := New()
	s.Require().Empty(handler.Routes)

	s.Require().NoError(AnyRoute(handler))

	s.Require().NotEmpty(handler.Routes)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestBaseHandler(t *testing.T) {
	suite.Run(t, new(TestBaseHandlerSuite))
}
