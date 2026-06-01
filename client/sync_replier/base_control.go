package sync_replier

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/message"
)

const (
	handlerStatusCommand = "status"
	handlerStartCommand  = "start"
	handlerCloseCommand  = "close"
	handlerConfigCommand = "config"
)

// BaseControl talks to a handler control endpoint over a SyncReplier client.
type BaseControl struct {
	*Client
}

// NewBaseControl connects to a handler control endpoint.
func NewBaseControl(id string, port uint64) (*BaseControl, error) {
	syncClient, err := NewClient(id, port)
	if err != nil {
		return nil, err
	}
	return &BaseControl{Client: syncClient}, nil
}

func (c *BaseControl) StartHandler() (string, error) {
	reply, err := c.requestCommand(handlerStartCommand)
	if err != nil {
		return "", err
	}
	return statusFromReply(reply)
}

func (c *BaseControl) HandlerStatus() (string, error) {
	reply, err := c.requestCommand(handlerStatusCommand)
	if err != nil {
		return "", err
	}
	return statusFromReply(reply)
}

func (c *BaseControl) HandlerConfig() (client.HandlerConfig, error) {
	reply, err := c.requestCommand(handlerConfigCommand)
	if err != nil {
		return client.HandlerConfig{}, err
	}

	kv, err := reply.ReplyParameters().NestedValue("config")
	if err != nil {
		return client.HandlerConfig{}, fmt.Errorf("reply.ReplyParameters().NestedValue('config'): %w", err)
	}

	var config client.HandlerConfig
	if err := kv.Interface(&config); err != nil {
		return client.HandlerConfig{}, fmt.Errorf("kv.Interface('client.HandlerConfig'): %w", err)
	}
	return config, nil
}

func (c *BaseControl) HandlerClose() error {
	_, err := c.requestCommand(handlerCloseCommand)
	return err
}

func (c *BaseControl) requestCommand(command string) (message.ReplyInterface, error) {
	req := message.Request{
		Command:    command,
		Parameters: datatype.New(),
	}

	reply, err := c.Request(&req)
	if err != nil {
		return nil, fmt.Errorf("client.Request('%s'): %w", command, err)
	}
	if !reply.IsOK() {
		return nil, fmt.Errorf("reply.Message: %s", reply.ErrorMessage())
	}
	return reply, nil
}

func statusFromReply(reply message.ReplyInterface) (string, error) {
	status, err := reply.ReplyParameters().StringValue("status")
	if err != nil {
		return "", fmt.Errorf("reply.ReplyParameters().StringValue('status'): %w", err)
	}
	return status, nil
}
