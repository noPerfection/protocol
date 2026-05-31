package message

import "fmt"

type Packer interface {
	DeserializeRequest(zmqEnvelope []string) (RequestInterface, error)
	DeseralizeReply(zmqEnvelope []string) (ReplyInterface, error)
	SerializeRequest(request RequestInterface) ([]string, error)
	SerializeReply(reply ReplyInterface) ([]string, error)
	EmptyRequest() RequestInterface
	EmptyReply() ReplyInterface
}

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
	return &Request{}
}

func (packer *MessagePacker) EmptyReply() ReplyInterface {
	return &Reply{}
}
