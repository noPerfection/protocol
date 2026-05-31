package message

import (
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/stretchr/testify/require"
)

func TestMessagePackerDeserializeRequest(t *testing.T) {
	packer := &MessagePacker{}
	okRequest := &Request{
		Command:    "some_command",
		Parameters: datatype.New(),
	}

	ok, err := packer.DeserializeRequest(MessageToEnvelope("", okRequest.String()))
	require.NoError(t, err)
	require.EqualValues(t, okRequest, ok)

	_, err = packer.DeserializeRequest(MessageToEnvelope("", `{"command":"","parameters":null}`))
	require.Error(t, err)

	_, err = packer.DeserializeRequest(MessageToEnvelope("", `{}`))
	require.Error(t, err)

	_, err = packer.DeserializeRequest(MessageToEnvelope("", `{"command":"is here","parameters":{},"status":"OK", "sig": ""}`))
	require.NoError(t, err)

	_, err = packer.DeserializeRequest(MessageToEnvelope("", `{"parameters":{}}`))
	require.NoError(t, err)

	_, err = packer.DeserializeRequest(MessageToEnvelope("", `{"command":"command"}`))
	require.Error(t, err)

	// Request parameters are case-insensitive.
	// No way to turn off: https://golang.org/pkg/encoding/json/#Unmarshal
	_, err = packer.DeserializeRequest(MessageToEnvelope("", `{"Command":"command","parameters":{}}`))
	require.NoError(t, err)

	_, err = packer.DeserializeRequest(MessageToEnvelope("", `{"command":"command","parameters":{}}`))
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

	ok, err := packer.DeserializeReply(MessageToEnvelope("", okReply.String()))
	require.NoError(t, err)
	fail, err := packer.DeserializeReply(MessageToEnvelope("", failReply.String()))
	require.NoError(t, err)

	require.EqualValues(t, okReply, ok)
	require.EqualValues(t, failReply, fail)

	_, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":null,"status":"OK"}`))
	require.Error(t, err)

	_, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":{},"status":""}`))
	require.Error(t, err)

	_, err = packer.DeserializeReply(MessageToEnvelope("", `{}`))
	require.Error(t, err)

	_, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":{},"status":"OK", "sig": ""}`))
	require.NoError(t, err)

	_, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":{},"status":"fail", "sig": ""}`))
	require.Error(t, err)

	_, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","parameters":{}}`))
	require.Error(t, err)

	_, err = packer.DeserializeReply(MessageToEnvelope("", `{"message":"","status":"OK"}`))
	require.Error(t, err)

	_, err = packer.DeserializeReply(MessageToEnvelope("", `{"parameters":{}, "status":"OK"}`))
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
