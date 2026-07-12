package client

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
)

// Control talks to a handler control endpoint over a SyncReplier client.
type Control struct {
	*SyncReplierClient
}

// NewControl connects to a handler control endpoint.
func NewControl(id string, port uint64) (*Control, error) {
	syncClient, err := NewSyncReplier(id, port)
	if err != nil {
		return nil, err
	}
	return &Control{SyncReplierClient: syncClient}, nil
}

// RequestAsContext is what clients call to request the message within a context of a handler.
//
// Arguments:
//   - endpoint: the outbound handler's endpoint.
//   - client-type: the type of the client.
//   - public-key: the public key of that handler, can be empty if the handler is not secure.
//   - envelope: the array of messages after serializing the message.RequestInterface.
//   - command: request.CommandName()
//   - attempt: the number of attempts to send the message.
//   - timeout: the timeout for the message.
//   - hmac: the hmac of the message, can be empty if the handler is not secure.
func (c *Control) RequestAsContext(endpoint message.Endpoint, clientType HandlerType, publicKey string, envelope []string, command string, attempt uint8, timeout time.Duration, hmac ...string) (message.ReplyInterface, error) {
	req := message.Request{
		Command: "request-as-context",
		Parameters: datatype.New().
			Set("endpoint", endpoint).
			Set("client-type", clientType).
			Set("public-key", publicKey).
			Set("envelope", envelope).
			Set("command", command).
			Set("attempt", attempt).
			Set("timeout", timeout),
	}
	if len(hmac) > 0 {
		req.Parameters.Set("hmac", hmac[0])
	}

	reply, err := c.Request(&req)
	if err != nil {
		return nil, fmt.Errorf("client.Request('request-as-context'): %w", err)
	}
	return reply, nil
}

func (c *Control) StartHandler() (string, error) {
	reply, err := c.requestCommand("start")
	if err != nil {
		return "", err
	}
	return statusFromReply(reply)
}

func (c *Control) HandlerStatus() (string, error) {
	reply, err := c.requestCommand("status")
	if err != nil {
		return "", err
	}
	return statusFromReply(reply)
}

func (c *Control) HandlerConfig() (HandlerConfig, error) {
	reply, err := c.requestCommand("config")
	if err != nil {
		return HandlerConfig{}, err
	}

	kv, err := reply.ReplyParameters().NestedValue("config")
	if err != nil {
		return HandlerConfig{}, fmt.Errorf("reply.ReplyParameters().NestedValue('config'): %w", err)
	}

	var config HandlerConfig
	if err := kv.Interface(&config); err != nil {
		return HandlerConfig{}, fmt.Errorf("kv.Interface('HandlerConfig'): %w", err)
	}
	return config, nil
}

func (c *Control) HandlerClose() error {
	_, err := c.requestCommand("close")
	return err
}

func (c *Control) requestCommand(command string) (message.ReplyInterface, error) {
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
