package message

import (
	"encoding/json"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"testing"

	"github.com/stretchr/testify/suite"
)

//
// Testing all functions one by one.
// Testing identical rawReq to the zeromq envelopes
// Testing message length based on the socket
// Testing Request as a RawRequest and Reply as a RawReply.
//

type TestRawSuite struct {
	suite.Suite

	cmdName   string
	reqKey    string
	replyKey  string
	rawReq    []string
	syncReq   []string
	rawReply  []string
	syncReply []string
	req       []string
	reply     []string
	stacks    string
}

// Make sure that Account is set to five
// before each test
func (test *TestRawSuite) SetupTest() {
	s := test.Require

	test.rawReq = []string{"req_id", "", "content"}
	test.rawReply = []string{"reply_id", "", "content"}
	test.cmdName = "hello"
	test.reqKey = "number"
	test.replyKey = "number"
	test.syncReq = []string{"", "content"}
	test.syncReply = []string{"", "content"}

	req := &Request{
		Command:    test.cmdName,
		Parameters: key_value.New().Set(test.reqKey, 12),
	}
	reqStrings, err := req.ZmqEnvelope()
	s().NoError(err)
	test.req = reqStrings

	reply := &Reply{
		Status:     OK,
		Message:    "",
		Parameters: key_value.New().Set(test.replyKey, 53),
	}
	replyStrings, err := reply.ZmqEnvelope()
	s().NoError(err)
	test.reply = replyStrings

	stacks := []*Stack{{}}
	bytes, err := json.Marshal(stacks)
	s().NoError(err)
	test.stacks = string(bytes)
}

// Test_10_RawMessage checks that operations are returned
func (test *TestRawSuite) Test_10_RawMessage() {
	s := test.Require

	messageOps := RawMessage()

	s().Equal("raw", messageOps.Name)

	expected, err := NewRawReq(test.rawReq)
	s().NoError(err)
	actual, err := messageOps.NewReq(test.rawReq)
	s().NoError(err)
	s().EqualValues(expected, actual)

	expectedReply, err := NewRawRep(test.rawReply)
	s().NoError(err)
	actualReply, err := messageOps.NewReply(test.rawReply)
	s().NoError(err)
	s().EqualValues(expectedReply, actualReply)

	s().Empty(messageOps.EmptyReq())
	s().Empty(messageOps.EmptyReply())
}

// Test_11_NewRawReq tests the converting of the zeromq message envelope into RawRequest
func (test *TestRawSuite) Test_11_NewRawReq() {
	s := test.Require

	// non multipart message and non sync replier envelope must fail
	_, err := NewRawReq(test.rawReq[:1])
	s().Error(err)

	_, err = NewRawReq([]string{"", "", ""})
	s().NoError(err)

	// without a message, the stack is counted as a message
	rawInterface, err := NewRawReq([]string{"", "", "", test.stacks})
	s().NoError(err)
	s().True(rawInterface.IsFirst())

	// if the envelope contains trace delimiter, then stacks must not be empty.
	stackedReq := append(test.rawReq, "not a json", "")
	_, err = NewRawReq(stackedReq)

	stackedReq = append(stackedReq, test.stacks)
	_, err = NewRawReq(stackedReq)
	s().NoError(err)

	// if the envelope has more than 4 messages,
	// then messages from delimiter and last part must be put in the stack.
	multiPart := append(test.rawReq, "second content", "", test.stacks)
	rawInterface, err = NewRawReq(multiPart)
	rawReq := rawInterface.(*RawRequest)
	s().NoError(err)
	s().Len(rawReq.messages, 2)
	s().EqualValues(multiPart[2:4], rawReq.messages)
	s().False(rawReq.IsFirst())

	// Testing for sync replier envelope
	_, err = NewRawReq(test.syncReq)
	s().NoError(err)

	stackedReq = append(test.syncReq, "", test.stacks)
	_, err = NewRawReq(stackedReq)
	s().NoError(err)

	// if the sync replier envelope has more than 4 messages,
	// then messages from delimiter and last part must be put in the stack.
	multiPart = append(test.syncReq, "second content", "", test.stacks)
	rawInterface, err = NewRawReq(multiPart)
	rawReq = rawInterface.(*RawRequest)
	s().NoError(err)
	s().Len(rawReq.messages, 2)
	s().EqualValues(multiPart[1:3], rawReq.messages)
}

// Test_12_NewRawRep tests converting zeromq envelope into the RawReply
func (test *TestRawSuite) Test_12_NewRawRep() {
	s := test.Require

	// non multipart message and non sync replier envelope must fail
	_, err := NewRawRep(test.rawReply[:1])
	s().Error(err)

	// if the envelope contains three parts, then the last part must be stacked.
	stackedReply := append(test.rawReply, "not a json", "")
	_, err = NewRawRep(stackedReply)

	stackedReply = append(stackedReply, test.stacks)
	_, err = NewRawRep(stackedReply)
	s().NoError(err)

	// if the envelope has more than 4 messages,
	// then messages from delimiter and last part must be put in the stack.
	multiPart := append(test.rawReply, "second content", "", test.stacks)
	rawInterface, err := NewRawRep(multiPart)
	rawReply := rawInterface.(*RawReply)
	s().NoError(err)
	s().Len(rawReply.messages, 2)
	s().EqualValues(multiPart[2:4], rawReply.messages)

	// Testing for sync replier envelope
	_, err = NewRawRep(test.syncReply)
	s().NoError(err)

	stackedReply = append(test.syncReply, "", test.stacks)
	_, err = NewRawRep(stackedReply)
	s().NoError(err)

	// if the sync replier envelope has more than 4 messages,
	// then messages from delimiter and last part must be put in the stack.
	multiPart = append(test.syncReply, "second content", "", test.stacks)
	rawInterface, err = NewRawRep(multiPart)
	rawReply = rawInterface.(*RawReply)
	s().NoError(err)
	s().Len(rawReply.messages, 2)
	s().EqualValues(multiPart[1:3], rawReply.messages)
}

// Test_13_CommandName gets the command name if the RawRequest is the wrapper around Request
func (test *TestRawSuite) Test_13_CommandName() {
	s := test.Require

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)

	// The test.rawReq is not a Request.
	// It must return empty command name
	s().Empty(rawReq.CommandName())

	rawReq, err = NewRawReq(test.req)
	s().NoError(err)
	s().Equal(test.cmdName, rawReq.CommandName())
}

// Test_14_RouteParameters gets the request parameters if the RawRequest is the wrapper around Request
func (test *TestRawSuite) Test_14_RouteParameters() {
	s := test.Require

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)

	// The test.rawReq is not a Request.
	// It must return empty command name
	s().Empty(rawReq.RouteParameters())

	rawReq, err = NewRawReq(test.req)
	s().NoError(err)
	reqParams := rawReq.RouteParameters()
	s().NotEmpty(reqParams)

	value, err := reqParams.Uint64Value(test.reqKey)
	s().NoError(err)
	s().NotZero(value)
}

// Test_15_ReqConId tests the connection id
func (test *TestRawSuite) Test_15_ReqConId() {
	s := test.Require

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)
	s().Equal(test.rawReq[0], rawReq.ConId())
}

// Test_16_ReqTraces tests the traces fetching.
// It tests Traces and IsFirst functions.
func (test *TestRawSuite) Test_16_ReqTraces() {
	s := test.Require

	// It doesn't have any traces
	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)
	s().NotNil(rawReq.Traces())
	s().Empty(rawReq.Traces())
	s().True(rawReq.IsFirst())

	stackReq := append(test.rawReq, "", test.stacks)
	rawReq, err = NewRawReq(stackReq)
	s().NoError(err)
	s().NotEmpty(rawReq.Traces())
	s().False(rawReq.IsFirst())
}

// Test_17_ReqSyncTrace test copying reply traces to the request trace
func (test *TestRawSuite) Test_17_ReqSyncTrace() {
	s := test.Require

	stackedReply := append(test.rawReply, "", test.stacks)
	rawReply, err := NewRawRep(stackedReply)
	s().NoError(err)

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)
	s().True(rawReq.IsFirst())
	s().Len(rawReq.Traces(), 0)

	// imagine that new request was created from rawReq.
	// the request result is rawReply.
	rawReq.SyncTrace(rawReply)
	s().False(rawReq.IsFirst())
	s().Len(rawReq.Traces(), 1)
}

// Test_18_AddRequestStack test appending a new stack
func (test *TestRawSuite) Test_18_AddRequestStack() {
	s := test.Require

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)
	s().True(rawReq.IsFirst())
	s().Len(rawReq.Traces(), 0)

	rawReq.AddRequestStack("url", "name", "instance")
	s().False(rawReq.IsFirst())
	s().Len(rawReq.Traces(), 1)

	rawReq.AddRequestStack("url", "name", "instance")
	s().False(rawReq.IsFirst())
	s().Len(rawReq.Traces(), 2)
}

// Test_19_ZmqEnvelope tests converting RawRequest to the zmq envelope.
// Since RawRequest is the zmq envelope wrapper, the ZmqEnvelope() function must return message identical NewRawReq.
func (test *TestRawSuite) Test_19_ZmqEnvelope() {
	s := test.Require

	// Test the multi part message (id, delimiter, content)
	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)

	zmqEnvelope, err := rawReq.ZmqEnvelope()
	s().NoError(err)
	s().Equal(test.rawReq, zmqEnvelope)
	s().Len(zmqEnvelope, 3)

	// If the message has many parts, then the size of the envelope must fit them
	req := rawReq.(*RawRequest)
	req.messages = []string{test.rawReq[2], "another content", "included content"}
	zmqEnvelope, err = rawReq.ZmqEnvelope()
	s().NoError(err)
	s().Equal(append(test.rawReq, "another content", "included content"), zmqEnvelope)
	s().Len(zmqEnvelope, 5)

	// Test the sync replier envelope (delimiter, content)
	rawReq, err = NewRawReq(test.syncReq)
	s().NoError(err)

	zmqEnvelope, err = rawReq.ZmqEnvelope()
	s().NoError(err)
	s().Equal(test.syncReq, zmqEnvelope)
	s().Len(zmqEnvelope, 2)

	// Multipart message consisting a stack must return zmq envelope of 4
	stackedMessage := append(test.rawReq, "", test.stacks)
	s().Len(stackedMessage, 5)
	stackedReq, err := NewRawReq(stackedMessage)
	s().NoError(err)
	zmqEnvelope, err = stackedReq.ZmqEnvelope()
	s().NoError(err)
	s().Len(zmqEnvelope, 5)
	// zmq envelope returned by NewRawReq must return Multipart message consisting message stack, conId
	s().Equal(stackedMessage, zmqEnvelope)
	stackedRaw := stackedReq.(*RawRequest)
	// con id must be equal to the zmq envelope index 0
	s().Equal(stackedMessage[0], stackedRaw.conId)
	s().Len(stackedRaw.messages, 1)

	// Sync replier envelope message consisting a stack must return zmq envelope of 3
	stackedMessage = append(test.syncReq, "", test.stacks)
	s().Len(stackedMessage, 4)
	stackedReq, err = NewRawReq(stackedMessage)
	s().NoError(err)
	zmqEnvelope, err = stackedReq.ZmqEnvelope()
	s().NoError(err)
	s().Len(zmqEnvelope, 4)
	// zmq envelope returned by NewRawReq must return Sync replier envelope consisting message and stack
	s().Equal(stackedMessage, zmqEnvelope)
	// con id must be empty
	stackedRaw = stackedReq.(*RawRequest)
	s().Empty(stackedRaw.conId)
	s().Len(stackedRaw.messages, 1)

	// Multipart consisting 5 message parts must return zmq envelope of 5
	stackedMessage = append(test.rawReq, "another message", "", test.stacks)
	s().Len(stackedMessage, 6)
	stackedReq, err = NewRawReq(stackedMessage)
	s().NoError(err)
	s().False(stackedReq.IsFirst())
	zmqEnvelope, err = stackedReq.ZmqEnvelope()
	s().NoError(err)
	s().Len(zmqEnvelope, 6)
	// zmq envelope returned by NewRawReq must return Multipart message consisting message stack, conId
	s().Equal(stackedMessage, zmqEnvelope)
	stackedRaw = stackedReq.(*RawRequest)
	// con id must be equal to the zmq envelope index 0
	s().Equal(stackedMessage[0], stackedRaw.conId)
	s().Len(stackedRaw.messages, 2)

	// Sync replier envelope consisting 4 message parts must return zmq envelope of 4
	stackedMessage = append(test.syncReq, "another message", "", test.stacks)
	s().Len(stackedMessage, 5)
	stackedReq, err = NewRawReq(stackedMessage)
	s().NoError(err)
	s().False(stackedReq.IsFirst())
	zmqEnvelope, err = stackedReq.ZmqEnvelope()
	s().NoError(err)
	s().Len(zmqEnvelope, 5)
	// zmq envelope returned by NewRawReq must return Multipart message consisting message stack, conId
	s().Equal(stackedMessage, zmqEnvelope)
	stackedRaw = stackedReq.(*RawRequest)
	// con id must be equal to the zmq envelope index 0
	s().Empty(stackedRaw.conId)
	s().Len(stackedRaw.messages, 2)
}

// Test_20_ReqString tests the string representation of the request.
func (test *TestRawSuite) Test_20_ReqString() {
	s := test.Require

	// String must match to the concatenated string
	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)
	raw := rawReq.(*RawRequest)
	s().Equal(test.rawReq[2], rawReq.String())
	s().Equal(JoinMessages(raw.messages), rawReq.String())

	// the multiple messages are concatenated
	rawMessage := append(test.rawReq, "another message")
	rawReq, err = NewRawReq(rawMessage)
	s().NoError(err)
	raw = rawReq.(*RawRequest)
	s().Equal(JoinMessages(rawMessage[2:]), rawReq.String())
	s().Equal(JoinMessages(raw.messages), rawReq.String())

	// the stack is not included
	rawMessage = append(test.rawReq, "another message", "", test.stacks)
	rawReq, err = NewRawReq(rawMessage)
	s().NoError(err)
	raw = rawReq.(*RawRequest)
	s().Equal(JoinMessages(rawMessage[2:4]), rawReq.String())
	s().Equal(JoinMessages(raw.messages), rawReq.String())
}

// Test_21_ReqBytes tests the string is valid bytes
func (test *TestRawSuite) Test_21_ReqBytes() {
	s := test.Require

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)
	bytes, err := rawReq.Bytes()
	s().NoError(err)
	s().NotEmpty(bytes)

	// trying to get the empty message
	emptyMessage := []string{"req_id", "", ""}
	rawReq, err = NewRawReq(emptyMessage)
	s().NoError(err)
	s().True(rawReq.IsFirst())
	s().Empty(rawReq.String())
	_, err = rawReq.Bytes()
	s().Error(err)

	// empty message must work even with the stack
	emptyMessage = append(emptyMessage, "", test.stacks)
	rawReq, err = NewRawReq(emptyMessage)
	s().NoError(err)
	s().False(rawReq.IsFirst())
	s().Empty(rawReq.String())
	_, err = rawReq.Bytes()
	s().Error(err)
}

// Test_22_ReqPublicKey tests setting and retrieving the public key.
// Tests SetPublicKey, PublicKey and SetMeta
func (test *TestRawSuite) Test_22_ReqPublicKey() {
	s := test.Require

	publicKey := "public_key"
	meta := map[string]string{}
	meta["pub_key"] = publicKey

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)

	raw := rawReq.(*RawRequest)
	s().Empty(raw.publicKey)

	rawReq.SetPublicKey(publicKey)
	s().Equal(publicKey, rawReq.PublicKey())

	// testing setup with the SetMeta
	rawReq, err = NewRawReq(test.rawReq)
	s().NoError(err)
	s().Empty(rawReq.PublicKey())
	rawReq.SetMeta(meta)
	s().Equal(publicKey, rawReq.PublicKey())
}

// Test_23_ReqUuid tests the setting of the request
func (test *TestRawSuite) Test_23_ReqUuid() {
	s := test.Require

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)

	raw := rawReq.(*RawRequest)
	s().Empty(raw.Uuid)

	rawReq.SetUuid()
	s().NotEmpty(raw.Uuid)
}

// Test_24_NextReq tests creating of the next Request to send.
func (test *TestRawSuite) Test_24_NextReq() {
	s := test.Require

	parameters := key_value.New()

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)

	// the test.rawReq is not a valid Request. so it must fail
	raw := rawReq.(*RawRequest)
	_, err = NewReq(raw.messages)
	s().Error(err)

	rawReq.Next(test.cmdName, parameters)
	req, err := NewReq(raw.messages)
	s().NoError(err)
	s().Equal(test.cmdName, req.CommandName())
}

// Test_25_ReqToReply tests creation of the successful and failed reply
func (test *TestRawSuite) Test_25_ReqToReply() {
	s := test.Require

	failMessage := "error"
	value := uint64(123)
	parameters := key_value.New().Set(test.replyKey, value)

	rawReq, err := NewRawReq(test.rawReq)
	s().NoError(err)

	rawReply := rawReq.Fail(failMessage)
	s().False(rawReply.IsOK())
	s().Equal(failMessage, rawReply.ErrorMessage())

	// the RawRequest.Fail() must return RawReply
	_, ok := rawReply.(*RawReply)
	s().True(ok)

	// testing the successful message
	rawReply = rawReq.Ok(parameters)
	s().True(rawReply.IsOK())
	s().Empty(rawReply.ErrorMessage())
	replyParameters := rawReply.ReplyParameters()
	replyValue, err := replyParameters.Uint64Value(test.replyKey)
	s().NoError(err)
	s().Equal(value, replyValue)
}

// TestRaw tests the RawRequest and RawReply methods.
//
// The RawReply is identical to the RawRequest.
// The RawReply methods are not tested, since RawRequest functions will test them.
func TestRaw(t *testing.T) {
	suite.Run(t, new(TestRawSuite))
}
