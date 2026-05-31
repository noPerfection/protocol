package message

import (
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestRequestSuite struct {
	suite.Suite
	ok *Request
}

// Make sure that Account is set to five
// before each test
func (suite *TestRequestSuite) SetupTest() {
	request := &Request{
		Command:    "some_command",
		Parameters: datatype.New(),
	}

	suite.ok = request
}

func (suite *TestRequestSuite) TestToString() {
	okString := `{"command":"some_command","parameters":{}}`

	suite.EqualValues(okString, suite.ok.String())

	// The Parameters as a nil should fail
	request := Request{}
	suite.Empty(request.String())

	// Requests without parameters can not be serialized.
	request = Request{
		Command: "command",
	}
	suite.Empty(request.String())

	// Empty commands are allowed; routing validation happens outside the message type.
	request = Request{
		Parameters: datatype.New(),
	}
	suite.EqualValues(`{"command":"","parameters":{}}`, request.String())
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestRequest(t *testing.T) {
	suite.Run(t, new(TestRequestSuite))
}
