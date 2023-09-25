package message

import (
	"fmt"
	"strings"
	"time"

	"github.com/ahmetson/common-lib/data_type/key_value"
)

// ReplyStatus can be only as "OK" or "fail"
// It indicates whether the reply message is correct or not.
type ReplyStatus string

const (
	OK   ReplyStatus = "OK"
	FAIL ReplyStatus = "fail"
)

// Reply SDS Service returns the reply. Anyone who sends a request to the SDS Service gets this message.
type Reply struct {
	Uuid       string             `json:"uuid,omitempty"`
	Trace      []*Stack           `json:"Trace,omitempty"`
	Status     ReplyStatus        `json:"status"`     // message.OK or message.FAIL
	Message    string             `json:"message"`    // If Status is fail, then the field will contain an error message.
	Parameters key_value.KeyValue `json:"parameters"` // If the Status is OK, then the field will contain the parameters.
	conId      string
}

func (reply *Reply) ConId() string {
	return reply.conId
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
func (reply *Reply) String() (string, error) {
	bytes, err := reply.Bytes()
	if err != nil {
		return "", fmt.Errorf("reply.Bytes: %w", err)
	}

	return string(bytes), nil
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

	kv, err := key_value.NewFromInterface(reply)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize Reply to key-value %v: %v", reply, err)
	}

	bytes, err := kv.Bytes()
	if err != nil {
		return nil, fmt.Errorf("serialized key-value.Bytes: %w", err)
	}

	return bytes, nil
}

// ParseReply decodes the Zeromq messages into the Reply.
func ParseReply(messages []string) (Reply, error) {
	msg := JoinMessages(messages)
	data, err := key_value.NewFromString(msg)
	if err != nil {
		return Reply{}, fmt.Errorf("key_value.NewFromString: %w", err)
	}

	reply, err := ParseJsonReply(data)
	if err != nil {
		return Reply{}, fmt.Errorf("ParseJsonReply: %w", err)
	}

	return reply, nil
}

// ParseJsonReply creates the 'Reply' message from a key value
func ParseJsonReply(dat key_value.KeyValue) (Reply, error) {
	var reply Reply
	err := dat.Interface(&reply)
	if err != nil {
		return Reply{}, fmt.Errorf("failed to serialize key-value to msg.Reply: %v", err)
	}

	// It will call valid_fail(), valid_status()
	_, err = reply.Bytes()
	if err != nil {
		return Reply{}, fmt.Errorf("validation: %w", err)
	}

	return reply, nil
}

// ValidStatus validates the status of the reply.
// It should be either OK or fail.
func ValidStatus(status ReplyStatus) error {
	if status != FAIL && status != OK {
		return fmt.Errorf("status is either '%s' or '%s', but given: '%s'", OK, FAIL, status)
	}

	return nil
}

// ValidFail checks if the reply type is failure, then
// THe message should be given too
func ValidFail(status ReplyStatus, msg string) error {
	if status == FAIL && len(msg) == 0 {
		return fmt.Errorf("failure should not have an empty message")
	}

	return nil
}
