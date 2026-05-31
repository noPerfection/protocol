package message

import "fmt"

// ReplyStatus can be only as "OK" or "fail"
// It indicates whether the reply message is correct or not.
type ReplyStatus string

type MessagePacker struct{}

var _ Packer = (*MessagePacker)(nil)

func (packer *MessagePacker) DeserializeRequest(zmqEnvelope []string) (RequestInterface, error) {
	return NewReq(zmqEnvelope)
}

func (packer *MessagePacker) DeseralizeReply(zmqEnvelope []string) (ReplyInterface, error) {
	return NewRep(zmqEnvelope)
}

func (packer *MessagePacker) SerializeRequest(request RequestInterface) ([]string, error) {
	str := request.String()
	if str == "" {
		return nil, fmt.Errorf("request.String returned an empty string")
	}

	return MessageToEnvelope(request.ConId(), str), nil
}

func (packer *MessagePacker) SerializeReply(reply ReplyInterface) ([]string, error) {
	str := reply.String()
	if str == "" {
		return nil, fmt.Errorf("request.String returned an empty string")
	}

	return MessageToEnvelope(reply.ConId(), str), nil
}

func (packer *MessagePacker) EmptyRequest() RequestInterface {
	return NewEmptyReq()
}

func (packer *MessagePacker) EmptyReply() ReplyInterface {
	return NewEmptyReply()
}

const (
	OK   ReplyStatus = "OK"
	FAIL ReplyStatus = "fail"
)

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
