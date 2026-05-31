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

func (suite *TestRequestSuite) TestParsing() {
	okString := suite.ok.String()

	ok, err := NewReq(MessageToEnvelope("", okString))
	suite.Require().NoError(err)

	suite.EqualValues(suite.ok, ok)

	// Parsing a request with the nil values should fail
	invalidReply := `{"command":"","parameters":null}`
	_, err = NewReq(MessageToEnvelope("", invalidReply))
	suite.Error(err)

	// Parsing should fail for missing keys
	invalidReply = `{}`
	_, err = NewReq(MessageToEnvelope("", invalidReply))
	suite.Error(err)

	// Parsing the json with additional field should be
	// successful, but skip the other parameters
	invalidReply = `{"command":"is here","parameters":{},"status":"OK", "sig": ""}`
	_, err = NewReq(MessageToEnvelope("", invalidReply))
	suite.NoError(err)

	// Parsing the request with the missing field should fail
	invalidReply = `{"parameters":{}}`
	_, err = NewReq(MessageToEnvelope("", invalidReply))
	suite.Error(err)

	// Parsing the request with the missing field should fail
	invalidReply = `{"command":"command"}`
	_, err = NewReq(MessageToEnvelope("", invalidReply))
	suite.Error(err)

	// Request parameters are case-insensitive
	// Not way to turn off
	// https://golang.org/pkg/encoding/json/#Unmarshal
	invalidReply = `{"Command":"command","parameters":{}}`
	_, err = NewReq(MessageToEnvelope("", invalidReply))
	suite.NoError(err)

	// Request parsing with the right parameters should succeed
	invalidReply = `{"command":"command","parameters":{}}`
	_, err = NewReq(MessageToEnvelope("", invalidReply))
	suite.NoError(err)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestRequest(t *testing.T) {
	suite.Run(t, new(TestRequestSuite))
}
