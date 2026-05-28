package route

import (
	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	"testing"

	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestHandleFuncSuite struct {
	suite.Suite

	handleX interface{}
	handle  interface{}
}

// Make sure that Account is set to five
// before each test
func (test *TestHandleFuncSuite) SetupTest() {
	// the second argument is invalid
	handleX := func(request message.RequestInterface, param string) message.ReplyInterface {
		return request.Ok(datatype.New())
	}
	handle := func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(datatype.New())
	}

	test.handleX = handleX
	test.handle = handle
}

func (test *TestHandleFuncSuite) Test_0_IsHandleFunc() {
	s := &test.Suite

	s.Require().False(IsHandleFunc(test.handleX))
	s.Require().True(IsHandleFunc(test.handle))
}

func (test *TestHandleFuncSuite) Test_1_Handle() {
	req := &message.Request{
		Command:    "ping",
		Parameters: datatype.New(),
	}

	// Trying to pass invalid HandleFunc should fail
	reply := Handle(req, test.handleX)
	test.Suite.Require().False(reply.IsOK())

	reply = Handle(req, test.handle)
	test.Suite.Require().True(reply.IsOK())
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestHandleFunc(t *testing.T) {
	suite.Run(t, new(TestHandleFuncSuite))
}
