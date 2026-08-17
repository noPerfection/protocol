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
// HMAC is computed on the handler control side from register-outbounds secrets.
//
// Arguments:
//   - endpoint: the outbound handler's endpoint.
//   - client-type: the type of the client.
//   - public-key: the public key of that handler, can be empty if the handler is not secure.
//   - envelope: the array of messages after serializing the message.RequestInterface.
//   - command: request.CommandName()
//   - attempt: the number of attempts to send the message.
//   - timeout: the timeout for the message.
func (c *Control) RequestAsContext(endpoint message.Endpoint, clientType HandlerType, publicKey string, envelope []string, command string, attempt uint8, timeout time.Duration) (message.ReplyInterface, error) {
	req := message.Request{
		Command: "request-as-context",
		Parameters: datatype.New().
			Set("endpoint", endpoint).
			Set("client-type", clientType).
			Set("public-key", publicKey).
			Set("envelope", envelope).
			Set("command", command).
			Set("attempt", uint64(attempt)).
			Set("timeout", uint64(timeout)),
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

func (c *Control) Commands() ([]string, error) {
	reply, err := c.requestCommand("commands")
	if err != nil {
		return nil, err
	}

	commands, err := reply.ReplyParameters().StringsValue("commands")
	if err != nil {
		return nil, fmt.Errorf("reply.ReplyParameters().StringsValue('commands'): %w", err)
	}
	return commands, nil
}

func (c *Control) HandlerClose() error {
	_, err := c.requestCommand("close")
	return err
}

// SecureOutbound ensures the control outbound identity is secure and returns its CURVE public key.
func (c *Control) SecureOutbound() (string, error) {
	reply, err := c.requestCommand("secure-outbound")
	if err != nil {
		return "", err
	}

	pubKey, err := reply.ReplyParameters().StringValue("public-key")
	if err != nil {
		return "", fmt.Errorf("reply.ReplyParameters().StringValue('public-key'): %w", err)
	}
	if pubKey == "" {
		return "", fmt.Errorf("reply.ReplyParameters().StringValue('public-key'): empty public key")
	}
	return pubKey, nil
}

// RequireSecure ensures the handler socket is secure and returns its CURVE public key.
func (c *Control) RequireSecure() (string, error) {
	reply, err := c.requestCommand("require-secure")
	if err != nil {
		return "", err
	}

	pubKey, err := reply.ReplyParameters().StringValue("public-key")
	if err != nil {
		return "", fmt.Errorf("reply.ReplyParameters().StringValue('public-key'): %w", err)
	}
	if pubKey == "" {
		return "", fmt.Errorf("reply.ReplyParameters().StringValue('public-key'): empty public key")
	}
	return pubKey, nil
}

// RegisterOutbounds registers outbound endpoints and command secrets on the handler.
// publicKey is stored for request-as-context when the outbound handler is secure.
// outboundURL and localCmd are passed to npac when the handler control has autocontext wired.
func (c *Control) RegisterOutbounds(endpoint message.Endpoint, publicKey string, commands map[string]string, outboundURL, localCmd string) error {
	endpointKV, err := datatype.NewFromInterface(endpoint)
	if err != nil {
		return fmt.Errorf("datatype.NewFromInterface(endpoint): %w", err)
	}
	commandsKV, err := datatype.NewFromInterface(commands)
	if err != nil {
		return fmt.Errorf("datatype.NewFromInterface(commands): %w", err)
	}

	params := datatype.New().
		Set("endpoint", endpointKV).
		Set("commands", commandsKV)
	if publicKey != "" {
		params.Set("public-key", publicKey)
	}
	if outboundURL != "" {
		params.Set("outbound-url", outboundURL)
	}
	if localCmd != "" {
		params.Set("local-command", localCmd)
	}

	reply, err := c.Request(&message.Request{
		Command:    "register-outbounds",
		Parameters: params,
	})
	if err != nil {
		return fmt.Errorf("client.Request('register-outbounds'): %w", err)
	}
	if !reply.IsOK() {
		return fmt.Errorf("reply.Message: %s", reply.ErrorMessage())
	}
	return nil
}

func (c *Control) RequireWhitelist(cmd string, secret ...string) error {
	params := datatype.New().Set("command", cmd)
	if len(secret) > 0 && secret[0] != "" {
		params.Set("secret", secret[0])
	}

	req := message.Request{
		Command:    "require-whitelist",
		Parameters: params,
	}

	reply, err := c.Request(&req)
	if err != nil {
		return fmt.Errorf("client.Request('require-whitelist'): %w", err)
	}
	if !reply.IsOK() {
		return fmt.Errorf("reply.Message: %s", reply.ErrorMessage())
	}
	return nil
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
