package message

import "fmt"

func ValidEnvelope(messages []string) error {
	if len(messages) == 0 {
		return fmt.Errorf("empty envelope, msg len is 0")
	}

	// With ConID as the first frame according to zmq protocol
	if messages[0] != "" {
		if len(messages) >= 3 {
			// ConID delimiter is the second frame according to zmq protocol
			if messages[1] != "" {
				return fmt.Errorf("conId delimiter is missing")
			}
		}
		// could be just list of messages without conId
		return nil
	}
	// first is empty delimeter for zmq.REP? Then it must have non empty message.
	if len(messages) < 2 || messages[1] == "" {
		return fmt.Errorf("empty message without conId")
	}

	return nil
}

// EnvelopeToMessage splits an envelope into connection id, first message body, and tail frames.
// SIlently fails if envelope is invalid.
// Connection ID depends on the handler type, and might be empty.
func EnvelopeToMessage(messages []string) (conId string, message string, tail []string) {
	if err := ValidEnvelope(messages); err != nil {
		return "", "", []string{}
	}

	// Not starts with empty delimiter? Its not coming from zmq.REP
	if messages[0] != "" {
		// It has empty delimeter as second parameter?
		// Its dealer, router
		if len(messages) >= 3 && messages[1] == "" {
			conId = messages[0]
			// sometimes its req to router will put own empty delimiter as third parameter
			if len(messages) >= 4 && messages[2] == "" {
				message = messages[3]
				tail = messages[4:]
			} else {
				message = messages[2]
				tail = messages[3:]
			}
			return conId, message, tail
		}
		// Its zmq.REQ from zmq.ROUTER or zmq.DEALER
		return "", messages[0], messages[1:]
	}

	// Its coming from zmq.REP to zmq.REQ
	return "", messages[1], messages[2:]
}

// MessageToEnvelope builds an envelope from connection id, first message body, and tail frames.
func MessageToEnvelope(conId string, message string, tail ...string) []string {
	if len(conId) == 0 {
		envelope := []string{"", message}
		return append(envelope, tail...)
	}

	envelope := []string{conId, "", message}
	full := append(envelope, tail...)
	return full
}
