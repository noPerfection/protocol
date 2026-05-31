package message

import (
	"fmt"

	"github.com/noPerfection/datatype"
)

// Reply is returned by a noPerfection service. Anyone who sends a request to the service gets this message.
type Reply struct {
	Trace      []*Stack          `json:"traces,omitempty"`
	Status     ReplyStatus       `json:"status"`     // message.OK or message.FAIL
	Message    string            `json:"message"`    // If Status is fail, then the field will contain an error message.
	Parameters datatype.KeyValue `json:"parameters"` // If the Status is OK, then the field will contain the parameters.
	conId      string
}

var _ ReplyInterface = (*Reply)(nil)

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
	if reply.String() == "" {
		return nil, fmt.Errorf("validation failed")
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

// IsOK returns the Status of the message.
func (reply *Reply) IsOK() bool {
	return reply.Status == OK
}

// String converts the Reply to the string format
func (reply *Reply) String() string {
	err := ValidFail(reply.Status, reply.Message)
	if err != nil {
		return ""
	}
	err = ValidStatus(reply.Status)
	if err != nil {
		return ""
	}

	kv, err := datatype.NewFromInterface(reply)
	if err != nil {
		return ""
	}

	bytes, err := kv.Bytes()
	if err != nil {
		return ""
	}

	return string(bytes)
}

func (reply *Reply) ZmqEnvelope() ([]string, error) {
	str := reply.String()
	if len(str) == 0 {
		return nil, fmt.Errorf("reply.String returned an empty string")
	}

	if len(reply.conId) > 0 {
		return []string{reply.conId, "", str}, nil
	}

	return []string{"", str}, nil
}
