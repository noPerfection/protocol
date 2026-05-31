package message

import "fmt"

func ValidEnvelope(messages []string) error {
	if len(messages) == 0 {
		return fmt.Errorf("empty envelope, msg len is 0")
	}

	// With ConID as the first frame according to zmq protocol
	if messages[0] != "" {
		if len(messages) < 3 {
			return fmt.Errorf("envelope with conId is too short")
		}
		// ConID delimiter is the second frame according to zmq protocol
		if messages[1] != "" {
			return fmt.Errorf("conId delimiter is missing")
		}

		return nil
	}
	// Without ConID, then is it even have a message or only delimiter, or perhaps its empty?
	if len(messages) == 1 || messages[1] == "" {
		return fmt.Errorf("empty message without conId")
	}

	return nil
}

// EnvelopeToMessage splits an envelope into connection id, first message body, and tail frames.
func EnvelopeToMessage(messages []string) (conId string, message string, tail []string) {
	if err := ValidEnvelope(messages); err != nil {
		return "", "", []string{}
	}

	// With ConID
	if messages[0] != "" {
		conId = messages[0]
		message = messages[1]
		return conId, messages[2], messages[3:]
	}

	return "", messages[1], messages[2:]
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
