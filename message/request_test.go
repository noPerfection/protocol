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
	_, err := request.ZmqEnvelope()
	suite.Error(err)

	// The Failure request can not have an empty message
	request = Request{
		Command: "command",
	}
	_, err = request.ZmqEnvelope()
	suite.Error(err)

	// The Failure request can not have an empty message
	request = Request{
		Parameters: datatype.New(),
	}
	_, err = request.ZmqEnvelope()
	suite.Error(err)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestRequest(t *testing.T) {
	suite.Run(t, new(TestRequestSuite))
}
