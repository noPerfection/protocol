package message

import (
	"fmt"
	"strings"
	"time"

	"github.com/noPerfection/datatype"
)

// Reply is returned by a noPerfection service. Anyone who sends a request to the service gets this message.
type Reply struct {
	Uuid       string            `json:"uuid,omitempty"`
	Trace      []*Stack          `json:"traces,omitempty"`
	Status     ReplyStatus       `json:"status"`     // message.OK or message.FAIL
	Message    string            `json:"message"`    // If Status is fail, then the field will contain an error message.
	Parameters datatype.KeyValue `json:"parameters"` // If the Status is OK, then the field will contain the parameters.
	conId      string
}

func NewEmptyReply() ReplyInterface {
	return &Reply{}
}

// NewRep decodes Zeromq messages into Reply.
func NewRep(messages []string) (ReplyInterface, error) {
	msg := JoinMessages(messages)
	data, err := datatype.NewFromString(msg)
	if err != nil {
		return nil, fmt.Errorf("datatype.NewFromString: %w", err)
	}

	var reply Reply
	err = data.Interface(&reply)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize key-value to msg.Reply: %v", err)
	}

	// It will call valid_fail(), valid_status()
	_, err = reply.Bytes()
	if err != nil {
		return nil, fmt.Errorf("validation: %w", err)
	}

	return &reply, nil
}

func (reply *Reply) ErrorMessage() string {
	return reply.Message
}

func (reply *Reply) ReplyParameters() datatype.KeyValue {
	return reply.Parameters
}

func (reply *Reply) ConId() string {
	return reply.conId
}

func (reply *Reply) SetConId(conId string) {
	reply.conId = conId
}

func (reply *Reply) Traces() []*Stack {
	return reply.Trace
}

// SetStack adds the current service's server into the reply
func (reply *Reply) SetStack(serviceUrl string, serverName string, serverInstance string) error {
	for i, stack := range reply.Trace {
		if strings.Compare(stack.ServiceUrl, serviceUrl) == 0 &&
			strings.Compare(stack.ServerName, serverName) == 0 &&
			strings.Compare(stack.ServerInstance, serverInstance) == 0 {
			reply.Trace[i].ReplyTime = uint64(time.Now().UnixMicro())
			return nil
		}
	}

	return fmt.Errorf("no trace stack for service %s server %s:%s", serviceUrl, serverName, serverInstance)
}

// IsOK returns the Status of the message.
func (reply *Reply) IsOK() bool {
	return reply.Status == OK
}

// String converts the Reply to the string format
func (reply *Reply) String() string {
	bytes, err := reply.Bytes()
	if err != nil {
		return ""
	}

	return string(bytes)
}

func (reply *Reply) ZmqEnvelope() ([]string, error) {
	bytes, err := reply.Bytes()
	if err != nil {
		return nil, fmt.Errorf("request.ZmqEnvelope: %w", err)
	}

	str := string(bytes)

	if len(reply.conId) > 0 {
		return []string{reply.conId, "", str}, nil
	}

	return []string{"", str}, nil
}

// Bytes converts Reply to the sequence of bytes
func (reply *Reply) Bytes() ([]byte, error) {
	err := ValidFail(reply.Status, reply.Message)
	if err != nil {
		return nil, fmt.Errorf("failure validation: %w", err)
	}
	err = ValidStatus(reply.Status)
	if err != nil {
		return nil, fmt.Errorf("status validation: %w", err)
	}

	kv, err := datatype.NewFromInterface(reply)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize Reply to key-value: %v", err)
	}

	bytes, err := kv.Bytes()
	if err != nil {
		return nil, fmt.Errorf("serialized key-value.Bytes: %w", err)
	}

	return bytes, nil
}
