package publisher

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client/sync_replier"
	"github.com/noPerfection/protocol/message"
)

const (
	broadcastCommand     = "broadcast"
	messageAmountCommand = "message-amount"
)

type Control struct {
	*sync_replier.BaseControl
}

func NewControl(id string, port uint64) (*Control, error) {
	control, err := sync_replier.NewBaseControl(id, port)
	if err != nil {
		return nil, err
	}
	return &Control{BaseControl: control}, nil
}

func (c *Control) Broadcast(req message.RequestInterface, packers ...message.Packer) error {
	if req == nil {
		return fmt.Errorf("request is nil")
	}
	if packer := firstPacker(packers); packer != nil {
		c.Packer(packer)
	}

	broadcastReq := &message.Request{
		Command:    broadcastCommand,
		Parameters: req.RouteParameters(),
	}

	reply, err := c.Request(broadcastReq)
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf("reply.Message: %s", reply.ErrorMessage())
	}
	return nil
}

func (c *Control) MessageAmount() (uint, error) {
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

func firstPacker(packers []message.Packer) message.Packer {
	if len(packers) == 0 {
		return nil
	}
	return packers[0]
}
