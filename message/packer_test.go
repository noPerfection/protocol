package message

import (
	"fmt"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/require"
)

func TestMessagePackerDeserializeRequest(t *testing.T) {
	packer := &MessagePacker{}
	okRequest := &Request{
		Command:    "some_command",
		Parameters: datatype.New(),
	}

	ok, _, err := packer.DeserializeRequest(MessageToEnvelope("", okRequest.String()))
	require.NoError(t, err)
	require.EqualValues(t, okRequest, ok)

	_, _, err = packer.DeserializeRequest(MessageToEnvelope("", `{"command":"","parameters":null}`))
	require.Error(t, err)

	_, _, err = packer.DeserializeRequest(MessageToEnvelope("", `{}`))
	require.Error(t, err)

	_, _, err = packer.DeserializeRequest(MessageToEnvelope("", `{"command":"is here","parameters":{},"status":"OK", "sig": ""}`))
	require.NoError(t, err)

	_, _, err = packer.DeserializeRequest(MessageToEnvelope("", `{"parameters":{}}`))
	require.NoError(t, err)

	_, _, err = packer.DeserializeRequest(MessageToEnvelope("", `{"command":"command"}`))
	require.Error(t, err)

	// Request parameters are case-insensitive.
	// No way to turn off: https://golang.org/pkg/encoding/json/#Unmarshal
	_, _, err = packer.DeserializeRequest(MessageToEnvelope("", `{"Command":"command","parameters":{}}`))
	require.NoError(t, err)

	_, _, err = packer.DeserializeRequest(MessageToEnvelope("", `{"command":"command","parameters":{}}`))
	require.NoError(t, err)
}

func TestMessagePackerDeserializeReply(t *testing.T) {
	packer := &MessagePacker{}
	okReply := &Reply{
		Status:     OK,
		Message:    "",
		Parameters: datatype.New(),
	}
	failReply := &Reply{
		Status:     FAIL,
		Message:    "failed for testing purpose",
		Parameters: datatype.New(),
	}

	ok, _, err := packer.DeserializeReply(MessageToEnvelope("", okReply.String()))
	require.NoError(t, err)
	fail, _, err := packer.DeserializeReply(MessageToEnvelope("", failReply.String()))
	require.NoError(t, err)

	require.EqualValues(t, okReply, ok)
	require.EqualValues(t, failReply, fail)

	_, _, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":null,"status":"OK"}`))
	require.Error(t, err)

	_, _, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":{},"status":""}`))
	require.Error(t, err)

	_, _, err = packer.DeserializeReply(MessageToEnvelope("", `{}`))
	require.Error(t, err)

	_, _, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":{},"status":"OK", "sig": ""}`))
	require.NoError(t, err)

	_, _, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":{},"status":"fail", "sig": ""}`))
	require.Error(t, err)

	_, _, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":{}}`))
	require.Error(t, err)

	_, _, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","status":"OK"}`))
	require.Error(t, err)

	_, _, err = packer.DeserializeReply(MessageToEnvelope("", `{"parameters":{}, "status":"OK"}`))
	require.NoError(t, err)
}

func TestMessagePackerSerializeRequest(t *testing.T) {
	packer := &MessagePacker{}
	request := &Request{
		Command:    "some_command",
		Parameters: datatype.New(),
	}

	envelope, err := packer.SerializeRequest(request)
	require.NoError(t, err)
	require.Equal(t, MessageToEnvelope("", request.String()), envelope)
}

func TestMessagePackerSerializeRequestValidation(t *testing.T) {
	packer := &MessagePacker{}

	_, err := packer.SerializeRequest(&Request{})
	require.Error(t, err)

	_, err = packer.SerializeRequest(&Request{Command: "command"})
	require.Error(t, err)

	envelope, err := packer.SerializeRequest(&Request{Parameters: datatype.New()})
	require.NoError(t, err)
	require.Equal(t, MessageToEnvelope("", `{"command":"","parameters":{}}`), envelope)
}

func TestMessagePackerSerializeReply(t *testing.T) {
	packer := &MessagePacker{}
	reply := &Reply{
		Status:     OK,
		Message:    "",
		Parameters: datatype.New(),
	}

	envelope, err := packer.SerializeReply(reply)
	require.NoError(t, err)
	require.Equal(t, MessageToEnvelope("", reply.String()), envelope)
}

func TestMessagePackerSerializeReplyRejectsInvalidReply(t *testing.T) {
	packer := &MessagePacker{}

	_, err := packer.SerializeReply(&Reply{
		Status:  FAIL,
		Message: "failed for testing purpose",
	})
	require.Error(t, err)

	_, err = packer.SerializeReply(&Reply{
		Status:     FAIL,
		Parameters: datatype.New(),
	})
	require.Error(t, err)

	_, err = packer.SerializeReply(&Reply{
		Message:    "",
		Parameters: datatype.New(),
	})
	require.Error(t, err)
}

func TestMessagePackerEmptyMessages(t *testing.T) {
	packer := &MessagePacker{}

	require.IsType(t, &Request{}, packer.EmptyRequest())
	require.IsType(t, &Reply{}, packer.EmptyReply())
}

func TestMessagePackerWithReqRepSockets(t *testing.T) {
	packer := &MessagePacker{}
	endpoint := fmt.Sprintf("inproc://message-packer-req-rep-%d", time.Now().UnixNano())

	handler, err := zmq.NewSocket(zmq.REP)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()
	require.NoError(t, handler.SetLinger(0))
	require.NoError(t, handler.SetRcvtimeo(time.Second))
	require.NoError(t, handler.SetSndtimeo(time.Second))
	require.NoError(t, handler.Bind(endpoint))

	client, err := zmq.NewSocket(zmq.REQ)
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	require.NoError(t, client.SetLinger(0))
	require.NoError(t, client.SetRcvtimeo(time.Second))
	require.NoError(t, client.SetSndtimeo(time.Second))
	require.NoError(t, client.Connect(endpoint))

	currentTime := time.Now().UTC().Format(time.RFC3339Nano)
	request := &Request{
		Command:    "current_time",
		Parameters: datatype.New().Set("current_time", currentTime),
	}

	requestEnvelope, err := packer.SerializeRequest(request)
	require.NoError(t, err)
	_, err = client.SendMessage(requestEnvelope)
	require.NoError(t, err)

	rawRequest, err := handler.RecvMessage(0)
	require.NoError(t, err)
	receivedRequest, _, err := packer.DeserializeRequest(rawRequest)
	require.NoError(t, err)
	require.Equal(t, request.CommandName(), receivedRequest.CommandName())

	receivedTime, err := receivedRequest.RouteParameters().StringValue("current_time")
	require.NoError(t, err)
	require.Equal(t, currentTime, receivedTime)

	replyEnvelope, err := packer.SerializeReply(receivedRequest.Ok(datatype.New().Set("current_time", receivedTime)))
	require.NoError(t, err)
	_, err = handler.SendMessage(replyEnvelope)
	require.NoError(t, err)

	rawReply, err := client.RecvMessage(0)
	require.NoError(t, err)
	receivedReply, _, err := packer.DeserializeReply(rawReply)
	require.NoError(t, err)
	require.True(t, receivedReply.IsOK())

	replyTime, err := receivedReply.ReplyParameters().StringValue("current_time")
	require.NoError(t, err)
	require.Equal(t, currentTime, replyTime)
	t.Logf("req/rep reply: status=%t current_time=%s", receivedReply.IsOK(), replyTime)
}

func TestMessagePackerWithDealerRouterSockets(t *testing.T) {
	packer := &MessagePacker{}
	endpoint := fmt.Sprintf("inproc://message-packer-dealer-router-%d", time.Now().UnixNano())

	router, err := zmq.NewSocket(zmq.ROUTER)
	require.NoError(t, err)
	defer func() { require.NoError(t, router.Close()) }()
	require.NoError(t, router.SetLinger(0))
	require.NoError(t, router.SetRcvtimeo(time.Second))
	require.NoError(t, router.SetSndtimeo(time.Second))
	require.NoError(t, router.Bind(endpoint))

	dealer, err := zmq.NewSocket(zmq.DEALER)
	require.NoError(t, err)
	defer func() { require.NoError(t, dealer.Close()) }()
	require.NoError(t, dealer.SetLinger(0))
	require.NoError(t, dealer.SetRcvtimeo(time.Second))
	require.NoError(t, dealer.SetSndtimeo(time.Second))
	require.NoError(t, dealer.SetIdentity("message-packer-dealer"))
	require.NoError(t, dealer.Connect(endpoint))

	currentTime := time.Now().UTC().Format(time.RFC3339Nano)
	request := &Request{
		Command:    "current_time",
		Parameters: datatype.New().Set("current_time", currentTime),
	}

	requestEnvelope, err := packer.SerializeRequest(request)
	require.NoError(t, err)
	_, err = dealer.SendMessage(requestEnvelope)
	require.NoError(t, err)

	rawRequest, err := router.RecvMessage(0)
	require.NoError(t, err)
	receivedRequest, _, err := packer.DeserializeRequest(rawRequest)
	require.NoError(t, err)
	require.Equal(t, "message-packer-dealer", receivedRequest.ConId())
	require.Equal(t, request.CommandName(), receivedRequest.CommandName())

	receivedTime, err := receivedRequest.RouteParameters().StringValue("current_time")
	require.NoError(t, err)
	require.Equal(t, currentTime, receivedTime)

	replyEnvelope, err := packer.SerializeReply(receivedRequest.Ok(datatype.New().Set("current_time", receivedTime)))
	require.NoError(t, err)
	_, err = router.SendMessage(replyEnvelope)
	require.NoError(t, err)

	rawReply, err := dealer.RecvMessage(0)
	require.NoError(t, err)
	receivedReply, _, err := packer.DeserializeReply(rawReply)
	require.NoError(t, err)
	require.True(t, receivedReply.IsOK())

	replyTime, err := receivedReply.ReplyParameters().StringValue("current_time")
	require.NoError(t, err)
	require.Equal(t, currentTime, replyTime)
	t.Logf("dealer/router reply: status=%t current_time=%s", receivedReply.IsOK(), replyTime)
}

func TestMessagePackerWithRequestRouterSockets(t *testing.T) {
	packer := &MessagePacker{}
	endpoint := fmt.Sprintf("inproc://message-packer-request-router-%d", time.Now().UnixNano())

	router, err := zmq.NewSocket(zmq.ROUTER)
	require.NoError(t, err)
	defer func() { require.NoError(t, router.Close()) }()
	require.NoError(t, router.SetLinger(0))
	require.NoError(t, router.SetRcvtimeo(time.Second))
	require.NoError(t, router.SetSndtimeo(time.Second))
	require.NoError(t, router.Bind(endpoint))

	req, err := zmq.NewSocket(zmq.REQ)
	require.NoError(t, err)
	defer func() { require.NoError(t, req.Close()) }()
	require.NoError(t, req.SetLinger(0))
	require.NoError(t, req.SetRcvtimeo(time.Second))
	require.NoError(t, req.SetSndtimeo(time.Second))
	require.NoError(t, req.Connect(endpoint))

	currentTime := time.Now().UTC().Format(time.RFC3339Nano)
	request := &Request{
		Command:    "current_time",
		Parameters: datatype.New().Set("current_time", currentTime),
	}

	requestEnvelope, err := packer.SerializeRequest(request)
	require.NoError(t, err)
	_, err = req.SendMessage(requestEnvelope)
	require.NoError(t, err)

	rawRequest, err := router.RecvMessage(0)
	require.NoError(t, err)
	receivedRequest, _, err := packer.DeserializeRequest(rawRequest)
	require.NoError(t, err)
	require.NotEmpty(t, receivedRequest.ConId())
	require.Equal(t, request.CommandName(), receivedRequest.CommandName())

	receivedTime, err := receivedRequest.RouteParameters().StringValue("current_time")
	require.NoError(t, err)
	require.Equal(t, currentTime, receivedTime)

	replyEnvelope, err := packer.SerializeReply(receivedRequest.Ok(datatype.New().Set("current_time", receivedTime)))
	require.NoError(t, err)
	_, err = router.SendMessage(replyEnvelope)
	require.NoError(t, err)

	rawReply, err := req.RecvMessage(0)
	require.NoError(t, err)
	receivedReply, _, err := packer.DeserializeReply(rawReply)
	require.NoError(t, err)
	require.True(t, receivedReply.IsOK())

	replyTime, err := receivedReply.ReplyParameters().StringValue("current_time")
	require.NoError(t, err)
	require.Equal(t, currentTime, replyTime)
	t.Logf("req/router reply: status=%t current_time=%s", receivedReply.IsOK(), replyTime)
}
