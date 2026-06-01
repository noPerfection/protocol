package replier

import "github.com/noPerfection/protocol/client/sync_replier"

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
