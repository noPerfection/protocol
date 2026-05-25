package route

import (
	"github.com/sds-framework/client-lib"
	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/datatype-lib/message"
	"testing"

	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestHandleFuncSuite struct {
	suite.Suite

	handleX interface{}
	handle0 interface{}
	handle1 interface{}
	handle2 interface{}
	handle3 interface{}
	handleN interface{}
}

// Make sure that Account is set to five
// before each test
func (test *TestHandleFuncSuite) SetupTest() {
	// the second argument is invalid
	handleX := func(request message.RequestInterface, param string) message.ReplyInterface {
		return request.Ok(key_value.New())
	}
	handle0 := func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(key_value.New())
	}
	handle1 := func(request message.RequestInterface, _ *client.Socket) message.ReplyInterface {
		return request.Ok(key_value.New())
	}
	handle2 := func(request message.RequestInterface, _ *client.Socket, _ *client.Socket) message.ReplyInterface {
		return request.Ok(key_value.New())
	}
	handle3 := func(request message.RequestInterface, _ *client.Socket, _ *client.Socket, _ *client.Socket) message.ReplyInterface {
		return request.Ok(key_value.New())
	}
	handleN := func(request message.RequestInterface, _ ...*client.Socket) message.ReplyInterface {
		return request.Ok(key_value.New())
	}

	test.handleX = handleX
	test.handle0 = handle0
	test.handle1 = handle1
	test.handle2 = handle2
	test.handle3 = handle3
	test.handleN = handleN
}

func (test *TestHandleFuncSuite) Test_0_DepAmount() {
	s := &test.Suite

	s.Require().Equal(-1, DepAmount(test.handleX))
	s.Require().Equal(0, DepAmount(test.handle0))
	s.Require().Equal(1, DepAmount(test.handle1))
	s.Require().Equal(2, DepAmount(test.handle2))
	s.Require().Equal(3, DepAmount(test.handle3))
	s.Require().Equal(4, DepAmount(test.handleN))
}

func (test *TestHandleFuncSuite) Test_1_Handle() {
	req := &message.Request{
		Command:    "ping",
		Parameters: key_value.New(),
	}

	deps4 := []*client.Socket{nil, nil, nil, nil}
	deps3 := []*client.Socket{nil, nil, nil}
	deps2 := []*client.Socket{nil, nil}
	deps1 := []*client.Socket{nil}
	var deps0 []*client.Socket

	// Trying to pass invalid HandleFunc should fail
	reply := Handle(req, test.handleX, deps4)
	test.Suite.Require().False(reply.IsOK())

	// Trying to pass fewer deps to the HandleFuncN should fail
	reply = Handle(req, test.handleN, deps3)
	test.Suite.Require().False(reply.IsOK())

	// More than 3 deps passed, it should be successful
	reply = Handle(req, test.handleN, deps4)
	test.Suite.Require().True(reply.IsOK())

	// Trying to pass more deps to the HandleFunc<number> should fail
	reply = Handle(req, test.handle3, deps4)
	test.Suite.Require().False(reply.IsOK())

	// Trying to pass fewer deps to the HandleFunc<number> should fail
	reply = Handle(req, test.handle3, deps2)
	test.Suite.Require().False(reply.IsOK())

	// Passing the exact number of deps to HandleFunc<number> should be successful
	reply = Handle(req, test.handle3, deps3)
	test.Suite.Require().True(reply.IsOK())

	reply = Handle(req, test.handle2, deps2)
	test.Suite.Require().True(reply.IsOK())

	reply = Handle(req, test.handle1, deps1)
	test.Suite.Require().True(reply.IsOK())

	reply = Handle(req, test.handle0, deps0)
	test.Suite.Require().True(reply.IsOK())
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestHandleFunc(t *testing.T) {
	suite.Run(t, new(TestHandleFuncSuite))
}
