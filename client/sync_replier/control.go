package sync_replier

type Control struct {
	*BaseControl
}

func NewControl(id string, port uint64) (*Control, error) {
	control, err := NewBaseControl(id, port)
	if err != nil {
		return nil, err
	}
	return &Control{BaseControl: control}, nil
}
