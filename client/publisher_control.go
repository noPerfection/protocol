package client

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
)

type PublisherControl struct {
	*Control
}

func NewPublisherControl(id string, port uint64) (*PublisherControl, error) {
	control, err := NewControl(id, port)
	if err != nil {
		return nil, err
	}
	return &PublisherControl{Control: control}, nil
}

func (c *PublisherControl) Broadcast(reply message.Reply) error {
	broadcastReq := &message.Request{
		Command:    "broadcast",
		Parameters: datatype.New().Set("reply", reply),
	}

	controlReply, err := c.SyncReplierClient.Request(broadcastReq)
	if err != nil {
		return err
	}
	if !controlReply.IsOK() {
		return fmt.Errorf("reply.Message: %s", controlReply.ErrorMessage())
	}
	return nil
}

func (c *PublisherControl) MessageAmount() (uint, error) {
	req := &message.Request{
		Command:    "message-amount",
		Parameters: datatype.New(),
	}

	reply, err := c.Request(req)
	if err != nil {
		return 0, fmt.Errorf("control.Request('%s'): %w", messageAmountCommand, err)
	}
	if !reply.IsOK() {
		return 0, fmt.Errorf("reply.Message: %s", reply.ErrorMessage())
	}

	amount, err := reply.ReplyParameters().Uint64Value("broadcasting_length")
	if err != nil {
		return 0, fmt.Errorf("reply.ReplyParameters().Uint64Value('broadcasting_length'): %w", err)
	}
	return uint(amount), nil
}
