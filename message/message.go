package message

import "fmt"

func ValidEnvelope(messages []string) error {
	if len(messages) == 0 {
		return fmt.Errorf("envelope is empty")
	}

	if (len(messages) >= 2) && messages[0] == "" {
		return fmt.Errorf("first delimiter is missing")
	}
	if (len(messages) >= 3) && messages[1] == "" {
		return fmt.Errorf("conid delimiter is missing")
	}

	return nil
}

// EnvelopeToMessage splits an envelope into connection id, first message body, and tail frames.
func EnvelopeToMessage(messages []string) (conId string, message string, tail []string) {
	if err := ValidEnvelope(messages); err != nil {
		return "", "", []string{}
	}

	if len(messages) >= 3 {
		conId = messages[0]
		message = messages[1]
		if len(messages) > 3 {
			tail = messages[3:]
		} else {
			tail = []string{}
		}
	} else if len(messages) >= 2 {
		conId = ""
		message = messages[1]
		if len(messages) > 2 {
			tail = messages[2:]
		} else {
			tail = []string{}
		}
	}

	return conId, message, tail
}

// MessageToEnvelope builds an envelope from connection id, first message body, and tail frames.
func MessageToEnvelope(conId string, message string, tail ...string) []string {
	if len(conId) == 0 {
		envelope := []string{"", message}
		return append(envelope, tail...)
	}

	envelope := []string{conId, "", message}
	return append(envelope, tail...)
}
