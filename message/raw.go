package message

import (
	"fmt"

	"github.com/noPerfection/datatype"
)

//
// RawRequest and RawReply are not shared as a json or yaml.
//

// RawRequest is the wrapper around zeromq message envelope.
type RawRequest struct {
	conId    string
	messages []string
	trace    []*Stack
}

type RawReply struct {
	conId    string
	messages []string
	trace    []*Stack
}

var _ RequestInterface = (*RawRequest)(nil)
var _ ReplyInterface = (*RawReply)(nil)

// RawMessage returns a message for parsing request and parsing reply.
func RawMessage() *Operations {
	return &Operations{
		Name:       "raw",
		NewReq:     NewRawReq,
		NewReply:   NewRawRep,
		EmptyReq:   NewEmptyRawReq,
		EmptyReply: NewEmptyRawReply,
	}
}

func NewEmptyRawReq() RequestInterface {
	return &RawRequest{}
}

func NewEmptyRawReply() ReplyInterface {
	return &RawReply{}
}

// NewRawReq from the zeromq rawReq.
func NewRawReq(messages []string) (RequestInterface, error) {
	if !MultiPart(messages) && !SyncReplierEnvelope(messages) {
		return nil, fmt.Errorf("not multipart or sync replier envelope")
	}

	contentOffset := 1
	contentEnd := len(messages)

	request := &RawRequest{
		trace: make([]*Stack, 0),
	}
	if MultiPart(messages) {
		request.conId = messages[0]
		contentOffset = 2
	}

	contentEnd = rawContentEnd(messages, contentEnd)

	request.messages = messages[contentOffset:contentEnd]
	if err := request.setTraceFromEnvelope(messages); err != nil {
		return nil, err
	}

	return request, nil
}

func NewRawRep(messages []string) (ReplyInterface, error) {
	if !MultiPart(messages) && !SyncReplierEnvelope(messages) {
		return nil, fmt.Errorf("not multipart or sync replier envelope")
	}

	contentOffset := 1
	contentEnd := len(messages) - 1

	reply := &RawReply{
		trace: make([]*Stack, 0),
	}
	if MultiPart(messages) {
		reply.conId = messages[0]
		contentOffset = 2
	}

	contentEnd = rawContentEnd(messages, contentEnd)

	reply.messages = messages[contentOffset:contentEnd]
	if err := reply.setTraceFromEnvelope(messages); err != nil {
		return nil, err
	}

	return reply, nil
}

// CommandName returns the command name if it was a Request
func (request *RawRequest) CommandName() string {
	defReq, err := NewReq(request.messages)
	if err != nil {
		return ""
	}

	return defReq.CommandName()
}

// RouteParameters returns the parameters if it was a Request
func (request *RawRequest) RouteParameters() datatype.KeyValue {
	defReq, err := NewReq(request.messages)
	if err != nil {
		return datatype.New()
	}

	return defReq.RouteParameters()
}

// ConId returns a connection id for each sending session.
func (request *RawRequest) ConId() string {
	return request.conId
}

func (request *RawRequest) SetConId(conId string) {
	request.conId = conId
}

// ZmqEnvelope the message
func (request *RawRequest) ZmqEnvelope() ([]string, error) {
	preOffset := 1
	if len(request.conId) > 0 {
		preOffset = 2
	}
	postOffset := rawTracePostOffset(request.trace)

	msgLen := len(request.messages)
	if msgLen == 0 {
		msgLen = 1
	}
	messages := make([]string, preOffset+msgLen+postOffset)

	if len(request.conId) > 0 {
		messages[0] = request.conId
		messages[1] = ""
	} else {
		messages[0] = ""
	}

	if len(request.messages) > 0 {
		for i := 0; i < msgLen; i++ {
			messages[i+preOffset] = request.messages[i]
		}
	} else {
		messages[preOffset] = "" // no message
	}

	if err := setRawTraceEnvelope(messages, request.trace, preOffset+msgLen); err != nil {
		return nil, err
	}

	return messages, nil
}

// String the message
func (request *RawRequest) String() string {
	messages, err := request.ZmqEnvelope()
	if err != nil {
		return ""
	}

	contentOffset := 0
	contentEnd := len(messages)

	if len(messages) == 1 {
		return messages[0]
	} else if SyncReplierEnvelope(messages) {
		contentOffset = 1
	} else if MultiPart(messages) {
		contentOffset = 2
	}

	contentEnd = rawContentEnd(messages, contentEnd)

	return JoinMessages(messages[contentOffset:contentEnd])
}

// Fail creates a new Reply as a failure
// It accepts the error message that explains the reason of the failure.
func (request *RawRequest) Fail(message string) ReplyInterface {
	defaultReply, _ := (&Reply{Status: FAIL, Message: message, Parameters: datatype.New()}).ZmqEnvelope()

	reply := &RawReply{
		conId:    request.conId,
		messages: defaultReply,
		trace:    request.trace,
	}

	return reply
}

func (request *RawRequest) Ok(parameters datatype.KeyValue) ReplyInterface {
	defaultReply, _ := (&Reply{Status: OK, Message: "", Parameters: parameters}).ZmqEnvelope()

	reply := &RawReply{
		conId:    request.conId,
		messages: defaultReply,
		trace:    request.trace,
	}

	return reply
}

//
// RawRequest methods
//

func (reply *RawReply) ConId() string {
	return reply.conId
}

func (reply *RawReply) SetConId(conId string) {
	reply.conId = conId
}

// IsOK is unsupported
func (reply *RawReply) IsOK() bool {
	defRep, err := NewRep(reply.messages)
	if err != nil {
		return false
	}

	return defRep.IsOK()
}

// ReplyParameters returns the parameters if it was a Reply
func (reply *RawReply) ReplyParameters() datatype.KeyValue {
	defRep, err := NewRep(reply.messages)
	if err != nil {
		return nil
	}

	return defRep.ReplyParameters()
}

// ErrorMessage if it was a Reply
func (reply *RawReply) ErrorMessage() string {
	defRep, err := NewRep(reply.messages)
	if err != nil {
		return ""
	}

	return defRep.ErrorMessage()
}

// String the message
func (reply *RawReply) String() string {
	messages, err := reply.ZmqEnvelope()
	if err != nil {
		return ""
	}

	contentOffset := 0
	contentEnd := len(messages)

	if len(messages) == 1 {
		return messages[0]
	} else if SyncReplierEnvelope(messages) {
		contentOffset = 1
	} else if MultiPart(messages) {
		contentOffset = 2
	}

	contentEnd = rawContentEnd(messages, contentEnd)

	return JoinMessages(messages[contentOffset:contentEnd])
}

// ZmqEnvelope the message
func (reply *RawReply) ZmqEnvelope() ([]string, error) {
	preOffset := 1
	if len(reply.conId) > 0 {
		preOffset = 2
	}
	postOffset := rawTracePostOffset(reply.trace)

	msgLen := len(reply.messages)
	if msgLen == 0 {
		msgLen = 1
	}
	messages := make([]string, preOffset+msgLen+postOffset)

	if len(reply.conId) > 0 {
		messages[0] = reply.conId
		messages[1] = ""
	} else {
		messages[0] = ""
	}

	if len(reply.messages) > 0 {
		for i := 0; i < msgLen; i++ {
			messages[i+preOffset] = reply.messages[i]
		}
	} else {
		messages[preOffset] = "" // no message
	}

	if err := setRawTraceEnvelope(messages, reply.trace, preOffset+msgLen); err != nil {
		return nil, err
	}

	return messages, nil
}
