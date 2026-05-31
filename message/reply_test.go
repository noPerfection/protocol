package message

import (
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestReplySuite struct {
	suite.Suite
	fail *Reply
	ok   *Reply
}

// Make sure that Account is set to five
// before each test
func (suite *TestReplySuite) SetupTest() {
	reply := Reply{
		Status:     OK,
		Message:    "",
		Parameters: datatype.New(),
	}
	failReply := Reply{
		Status:     FAIL,
		Message:    "failed for testing purpose",
		Parameters: datatype.New(),
	}
	suite.ok = &reply
	suite.fail = &failReply
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *TestReplySuite) TestIsOk() {
	suite.True(suite.ok.IsOK())
	suite.False(suite.fail.IsOK())
}

func (suite *TestReplySuite) TestToString() {
	okString := `{"message":"","parameters":{},"status":"OK"}`
	failString := `{"message":"failed for testing purpose","parameters":{},"status":"fail"}`

	suite.EqualValues(okString, suite.ok.String())
	suite.EqualValues(failString, suite.fail.String())

	// The Parameters as a nil should fail
	reply := Reply{
		Status:  FAIL,
		Message: "failed for testing purpose",
	}
	_, err := reply.ZmqEnvelope()
	suite.Error(err)

	// The Failure reply can not have an empty message
	reply = Reply{
		Status:     FAIL,
		Parameters: datatype.New(),
	}
	_, err = reply.ZmqEnvelope()
	suite.Error(err)

	// The Failure reply can not have an empty message
	reply = Reply{
		Message:    "",
		Parameters: datatype.New(),
	}
	_, err = reply.ZmqEnvelope()
	suite.Error(err)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestReply(t *testing.T) {
	suite.Run(t, new(TestReplySuite))
}
