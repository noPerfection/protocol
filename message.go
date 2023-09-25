package message

import (
	"fmt"
	"strings"
)

// ReplyStatus can be only as "OK" or "fail"
// It indicates whether the reply message is correct or not.
type ReplyStatus string

const (
	OK   ReplyStatus = "OK"
	FAIL ReplyStatus = "fail"
)

// ValidCommand checks if the reply type is failure, then
// THe message should be given too
func ValidCommand(cmd string) error {
	if len(cmd) == 0 {
		return fmt.Errorf("command is missing")
	}

	return nil
}

// MultiPart returns true if the message has id, delimiter, and content
func MultiPart(messages []string) bool {
	return len(messages) >= 3 && messages[1] == ""
}

// JoinMessages into the single string the array of zeromq messages
func JoinMessages(messages []string) string {
	body := messages[:]
	if MultiPart(messages) {
		body = messages[2:]
	}
	return strings.Join(body, "")
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
