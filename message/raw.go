package message

import (
	"fmt"
	"strings"

	"github.com/noPerfection/datatype"
)

// Raw is the wrapper around zeromq message envelope.
// Its just handles the empty delimeters and the connection ids.
type Raw struct {
	conId    string
	messages []string
}
type RawPacker struct{}

var _ RequestInterface = (*Raw)(nil)
var _ ReplyInterface = (*Raw)(nil)
var _ Packer = (*RawPacker)(nil)

// RawMessage returns a message for parsing request and parsing reply.
func RawMessage() Packer {
	return &RawPacker{}
}

func (packer *RawPacker) DeserializeRequest(envelope []string) (RequestInterface, string, error) {
	if err := ValidEnvelope(envelope); err != nil {
		return nil, "", err
	}

	conId, message, tail := EnvelopeToMessage(envelope)
	hmacHash, rest := hmacFromTail(tail)

	request := &Raw{
		conId:    conId,
		messages: []string{message},
	}
	request.messages = append(request.messages, rest...)

	return request, hmacHash, nil
}

func (packer *RawPacker) DeserializeReply(envelope []string) (ReplyInterface, string, error) {
	if err := ValidEnvelope(envelope); err != nil {
		return nil, "", err
	}

	conId, message, tail := EnvelopeToMessage(envelope)
	hmacHash, rest := hmacFromTail(tail)

	reply := &Raw{
		conId:    conId,
		messages: []string{message},
	}
	reply.messages = append(reply.messages, rest...)

	return reply, hmacHash, nil
}

func (packer *RawPacker) SerializeRequest(generic RequestInterface, hmac ...string) ([]string, error) {
	request, ok := generic.(*Raw)
	if !ok {
		return nil, fmt.Errorf("generic is not a *Raw")
	}
	if len(request.messages) == 0 {
		return nil, fmt.Errorf("raw request has no message body")
	}

	tail := envelopeHMACTail(hmac...)
	if len(request.messages) > 1 {
		tail = append(tail, request.messages[1:]...)
	}
	if len(tail) > 0 {
		return MessageToEnvelope(request.ConId(), request.messages[0], tail...), nil
	}
	return MessageToEnvelope(request.conId, request.messages[0]), nil
}

func (packer *RawPacker) SerializeReply(generic ReplyInterface, hmac ...string) ([]string, error) {
	reply, ok := generic.(*Raw)
	if !ok {
		return nil, fmt.Errorf("generic is not a *Raw")
	}
	if len(reply.messages) == 0 {
		return nil, fmt.Errorf("raw reply has no message body")
	}

	tail := envelopeHMACTail(hmac...)
	if len(reply.messages) > 1 {
		tail = append(tail, reply.messages[1:]...)
	}
	if len(tail) > 0 {
		return MessageToEnvelope(reply.conId, reply.messages[0], tail...), nil
	}
	return MessageToEnvelope(reply.conId, reply.messages[0]), nil
}

func (packer *RawPacker) EmptyRequest() RequestInterface {
	return &Raw{}
}

func (packer *RawPacker) EmptyReply() ReplyInterface {
	return &Raw{}
}

// CommandName returns the command name if it was a Request
func (request *Raw) CommandName() string {
	return ""
}

// RouteParameters returns the parameters if it was a Request
func (request *Raw) RouteParameters() datatype.KeyValue {
	return datatype.New()
}

// ConId returns a connection id for each sending session.
func (request *Raw) ConId() string {
	return request.conId
}

func (request *Raw) SetConId(conId string) {
	request.conId = conId
}

// String the message
func (request *Raw) String() string {
	return strings.Join(request.messages, "")
}

// Fail creates a new Reply as a failure
// It accepts the error message that explains the reason of the failure.
func (request *Raw) Fail(message string) ReplyInterface {
	request.messages = append(request.messages, message)
	return request
}

func (request *Raw) Ok(parameters ...any) ReplyInterface {
	kv, err := replyParameters(parameters...)
	if err != nil {
		request.messages = append(request.messages, err.Error())
		return request
	}
	request.messages = append(request.messages, kv.String())
	return request
}

// IsOK is unsupported
func (reply *Raw) IsOK() bool {
	return len(reply.messages) > 0
}

// ReplyParameters returns the parameters if it was a Reply
func (reply *Raw) ReplyParameters() datatype.KeyValue {
	return datatype.New()
}

// ErrorMessage if it was a Reply
func (reply *Raw) ErrorMessage() string {
	if len(reply.messages) == 0 {
		return ""
	}
	return reply.messages[0]
}
