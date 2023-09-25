package message

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/google/uuid"
)

type RawRequest struct {
	Uuid      string
	conId     string
	messages  []string
	trace     []*Stack
	publicKey string
}

type RawReply struct {
	Uuid     string
	conId    string
	messages []string
	trace    []*Stack
}

// RawMessage returns a message for parsing request and parsing reply.
func RawMessage() (ReqFunc, ReplyFunc) {
	return NewRawReq, NewRawRep
}

// NewRawReq from the zeromq messages.
func NewRawReq(messages []string) (RequestInterface, error) {
	if !MultiPart(messages) {
		return nil, fmt.Errorf("message is not multipart")
	}

	request := &RawRequest{
		conId: messages[0],
	}
	if len(messages) == 3 {
		request.messages = messages[2:]
		request.trace = make([]*Stack, 0)
	} else {
		request.messages = messages[2 : len(messages)-1]

		var traces []*Stack
		err := json.Unmarshal([]byte(messages[len(messages)-1]), &traces)
		if err != nil {
			return nil, fmt.Errorf("json.Unmarshal('last_message_part'): %w", err)
		}
		request.trace = traces
	}

	return request, nil
}

func NewRawRep(messages []string) (ReplyInterface, error) {
	if !MultiPart(messages) {
		return nil, fmt.Errorf("message is not multipart")
	}

	reply := &RawReply{
		conId: messages[0],
	}
	if len(messages) == 3 {
		reply.messages = messages[2:]
		reply.trace = make([]*Stack, 0)
	} else {
		reply.messages = messages[2 : len(messages)-1]

		var traces []*Stack
		err := json.Unmarshal([]byte(messages[len(messages)-1]), &traces)
		if err != nil {
			return nil, fmt.Errorf("json.Unmarshal('last_message_part'): %w", err)
		}
		reply.trace = traces
	}

	return reply, nil
}

// ConId returns a connection id for each sending session.
func (request *RawRequest) ConId() string {
	return request.conId
}

func (request *RawRequest) Traces() []*Stack {
	return request.trace
}

// IsFirst returns true if the request has no trace,
//
// For example, if the proxy inserts it.
func (request *RawRequest) IsFirst() bool {
	return len(request.trace) == 0
}

// SyncTrace is if the reply has more stacks, the request is updated with it.
func (request *RawRequest) SyncTrace(reply ReplyInterface) {
	repTraceLen := len(reply.Traces())
	reqTraceLen := len(request.Traces())

	if repTraceLen > reqTraceLen {
		request.trace = append(request.trace, reply.Traces()[reqTraceLen:]...)
	}
}

func (request *RawRequest) AddRequestStack(serviceUrl string, serverName string, serverInstance string) {
	stack := &Stack{
		RequestTime:    uint64(time.Now().UnixMicro()),
		ReplyTime:      0,
		Command:        fmt.Sprintf("%d", len(request.trace)),
		ServiceUrl:     serviceUrl,
		ServerName:     serverName,
		ServerInstance: serverInstance,
	}

	request.trace = append(request.trace, stack)
}

// Bytes convert the message to the sequence of bytes
func (request *RawRequest) Bytes() ([]byte, error) {
	str, err := request.String()
	if err != nil {
		return nil, fmt.Errorf("request.String: %w", err)
	}

	return []byte(str), nil
}

// SetPublicKey For security; Work in Progress.
func (request *RawRequest) SetPublicKey(publicKey string) {
	request.publicKey = publicKey
}

// PublicKey For security; Work in Progress.
func (request *RawRequest) PublicKey() string {
	return request.publicKey
}

// String the message
func (request *RawRequest) String() (string, error) {
	messages, err := request.Strings()
	if err != nil {
		return "", fmt.Errorf("request.Strings: %w", err)
	}
	return JoinMessages(messages), nil
}

// Strings the message
func (request *RawRequest) Strings() ([]string, error) {
	msgLen := len(request.messages)
	messages := make([]string, 3+msgLen)

	messages[0] = request.conId
	messages[1] = ""
	for i := 0; i < msgLen; i++ {
		messages[i+2] = request.messages[i]
	}

	if len(request.trace) > 0 {
		kv, err := key_value.NewFromInterface(request.trace)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize Request to key-value %v: %v", request, err)
		}

		str, err := kv.String()
		if err != nil {
			return nil, fmt.Errorf("kv.Bytes: %w", err)
		}
		messages[2+msgLen] = str
	}

	return messages, nil
}

func (request *RawRequest) SetUuid() {
	id := uuid.New()
	request.Uuid = id.String()
}

// Next creates a new request based on the previous one. It uses the Request.
func (request *RawRequest) Next(command string, parameters key_value.KeyValue) {
	nextReq, err := (&Request{Command: command, Parameters: parameters}).String()
	if err != nil {
		return
	}
	request.messages = []string{nextReq}
}

// Fail creates a new Reply as a failure
// It accepts the error message that explains the reason of the failure.
func (request *RawRequest) Fail(message string) ReplyInterface {
	defaultReply, _ := (&Reply{Status: FAIL, Message: message, Parameters: key_value.Empty()}).Strings()

	reply := &RawReply{
		Uuid:     request.Uuid,
		conId:    request.conId,
		messages: defaultReply,
		trace:    request.trace,
	}

	return reply
}

func (request *RawRequest) Ok(parameters key_value.KeyValue) ReplyInterface {
	defaultReply, _ := (&Reply{Status: OK, Message: "", Parameters: parameters}).Strings()

	reply := &RawReply{
		Uuid:     request.Uuid,
		conId:    request.conId,
		messages: defaultReply,
		trace:    request.trace,
	}

	return reply
}

func (request *RawRequest) SetMeta(meta map[string]string) {
	pubKey, ok := meta["pub_key"]
	if ok {
		request.SetPublicKey(pubKey)
	}
}

func (reply *RawReply) ConId() string {
	return reply.conId
}

func (reply *RawReply) Traces() []*Stack {
	return reply.trace
}

// SetStack adds the current service's server into the reply
func (reply *RawReply) SetStack(serviceUrl string, serverName string, serverInstance string) error {
	for i, stack := range reply.trace {
		if strings.Compare(stack.ServiceUrl, serviceUrl) == 0 &&
			strings.Compare(stack.ServerName, serverName) == 0 &&
			strings.Compare(stack.ServerInstance, serverInstance) == 0 {
			reply.trace[i].ReplyTime = uint64(time.Now().UnixMicro())
			return nil
		}
	}

	return fmt.Errorf("no trace stack for service %s server %s:%s", serviceUrl, serverName, serverInstance)
}

// IsOK is unsupported
func (reply *RawReply) IsOK() bool {
	defRep, err := NewRep(reply.messages)
	if err != nil {
		return false
	}

	return defRep.IsOK()
}

// String the message
func (reply *RawReply) String() (string, error) {
	messages, err := reply.Strings()
	if err != nil {
		return "", fmt.Errorf("request.Strings: %w", err)
	}
	return JoinMessages(messages), nil
}

// Strings the message
func (reply *RawReply) Strings() ([]string, error) {
	msgLen := len(reply.messages)
	messages := make([]string, 3+msgLen)

	messages[0] = reply.conId
	messages[1] = ""
	for i := 0; i < msgLen; i++ {
		messages[i+2] = reply.messages[i]
	}

	if len(reply.trace) > 0 {
		kv, err := key_value.NewFromInterface(reply.trace)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize Request to key-value %v: %v", reply, err)
		}

		str, err := kv.String()
		if err != nil {
			return nil, fmt.Errorf("kv.Bytes: %w", err)
		}
		messages[2+msgLen] = str
	}

	return messages, nil
}

// Bytes convert the message to the sequence of bytes
func (reply *RawReply) Bytes() ([]byte, error) {
	str, err := reply.String()
	if err != nil {
		return nil, fmt.Errorf("request.String: %w", err)
	}

	return []byte(str), nil
}
