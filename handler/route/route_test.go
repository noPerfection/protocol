package route

import (
	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	"testing"

	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestRouteSuite struct {
	suite.Suite
	handler *zmq.Socket
}

// Make sure that Account is set to five
// before each test
func (test *TestRouteSuite) SetupTest() {}

func (test *TestRouteSuite) Test_1_Route() {
	s := &test.Suite
	handlers := datatype.New()
	var anyHandle = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(datatype.New())
	}
	var emptyHandle = func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(datatype.New())
	}
	cmd := "cmd"

	// Trying to route unregistered command should fail
	_, err := Route(cmd, handlers)
	s.Require().Error(err)

	// Trying to route unregistered command, when any command is supported should return any
	handlers.Set(Any, anyHandle)

	handleInterface, err := Route(cmd, handlers)
	s.Require().NoError(err)
	_, ok := handleInterface.(HandleFunc)
	s.Require().True(ok)

	// Routing to the existing function should be successful
	handlers.Set(cmd, emptyHandle)
	handleInterface, err = Route(cmd, handlers)
	s.Require().NoError(err)
	_, ok = handleInterface.(HandleFunc)
	s.Require().True(ok)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestRoute(t *testing.T) {
	suite.Run(t, new(TestRouteSuite))
}
