package message

import (
	"fmt"

	"github.com/noPerfection/datatype"
)

type ReplyInterface interface {
	ConId() string
	SetConId(string)
	// IsOK returns the Status of the message.
	IsOK() bool
	// String converts the Reply to the string format. Empty if occurred an error.
	// It implements Stringer interface from a standard library
	String() string
	ErrorMessage() string
	ReplyParameters() datatype.KeyValue
}

// ReplyStatus can be only as "OK" or "fail"
// It indicates whether the reply message is correct or not.
type ReplyStatus string

const (
	OK   ReplyStatus = "OK"
	FAIL ReplyStatus = "fail"
)

// Reply is returned by a noPerfection service. Anyone who sends a request to the service gets this message.
type Reply struct {
	Status     ReplyStatus       `json:"status"`     // message.OK or message.FAIL
	Message    string            `json:"message"`    // If Status is fail, then the field will contain an error message.
	Parameters datatype.KeyValue `json:"parameters"` // If the Status is OK, then the field will contain the parameters.
	conId      string
}

var _ ReplyInterface = (*Reply)(nil)

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
