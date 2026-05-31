package message

import (
	"fmt"

	"github.com/noPerfection/datatype"
)

type Packer interface {
	DeserializeRequest(zmqEnvelope []string) (RequestInterface, error)
	DeserializeReply(zmqEnvelope []string) (ReplyInterface, error)
	SerializeRequest(request RequestInterface) ([]string, error)
	SerializeReply(reply ReplyInterface) ([]string, error)
	EmptyRequest() RequestInterface
	EmptyReply() ReplyInterface
}

type MessagePacker struct{}

var _ Packer = (*MessagePacker)(nil)

func (packer *MessagePacker) DeserializeRequest(zmqEnvelope []string) (RequestInterface, error) {
	if err := ValidEnvelope(zmqEnvelope); err != nil {
		return nil, err
	}

	conId, msg, _ := EnvelopeToMessage(zmqEnvelope)

	data, err := datatype.NewFromString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to convert message string %s to key-value: %v", msg, err)
	}

	var request Request
	err = data.Interface(&request)
	if err != nil {
		return nil, fmt.Errorf("failed to convert key-value %v to intermediate interface: %v", data, err)
	}

	// verify that data is not nil
	if request.String() == "" {
		return nil, fmt.Errorf("failed to validate")
	}

	request.conId = conId

	return &request, nil
}

func (packer *MessagePacker) DeserializeReply(zmqEnvelope []string) (ReplyInterface, error) {
	if err := ValidEnvelope(zmqEnvelope); err != nil {
		return nil, err
	}

	conId, msg, _ := EnvelopeToMessage(zmqEnvelope)
	data, err := datatype.NewFromString(msg)
	if err != nil {
		return nil, fmt.Errorf("datatype.NewFromString: %w", err)
	}

	var reply Reply
	err = data.Interface(&reply)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize key-value to msg.Reply: %v", err)
	}
	reply.conId = conId

	// It will call valid_fail(), valid_status()
	if reply.String() == "" {
		return nil, fmt.Errorf("validation failed")
	}

	return &reply, nil
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
	return &Request{}
}

func (packer *MessagePacker) EmptyReply() ReplyInterface {
	return &Reply{}
}
