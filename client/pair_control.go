package client

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
)

const (
	broadcastCommand     = "broadcast"
	broadcastParameter   = "reply"
	messageAmountCommand = "message-amount"
)

type PairControl struct {
	*Control
}

func NewPairControl(id string, port uint64) (*PairControl, error) {
	control, err := NewControl(id, port)
	if err != nil {
		return nil, err
	}

	return &PairControl{Control: control}, nil
}

func (c *PairControl) Broadcast(reply message.Reply) error {
	broadcastReq := &message.Request{
		Command:    broadcastCommand,
		Parameters: datatype.New().Set(broadcastParameter, reply),
	}

	controlReply, err := c.Request(broadcastReq)
	if err != nil {
		return err
	}
	if !controlReply.IsOK() {
		return fmt.Errorf("reply.Message: %s", controlReply.ErrorMessage())
	}
	return nil
}

func (c *PairControl) MessageAmount() (uint, error) {
	req := &message.Request{
		Command:    messageAmountCommand,
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
