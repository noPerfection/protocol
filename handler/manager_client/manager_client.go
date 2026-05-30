// Package manager_client creates a client that interacts with the handler manager.
// Useful for calling it from the service.
package manager_client

import (
	"fmt"
	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client"
	handlerConfig "github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"time"
)

type Client struct {
	socket *client.Socket
	config *handlerConfig.Handler
}

type Interface interface {
	// Close the handler
	Close() error
	Timeout(duration time.Duration)
	Attempt(uint8)

	// HandlerStatus returns the handler status.
	HandlerStatus() (string, error)

	// Id of the handler
	Id() string

	// Config returns the handler configuration
	Config() (*handlerConfig.Handler, error)
}

// New client that's connected to the handler
func New(configHandler *handlerConfig.Handler) (Interface, error) {
	managerConfig := control.CreateInternalConfig(configHandler)
	socket, err := client.NewRaw(zmq.REQ, managerConfig.ClientUrl())
	if err != nil {
		return nil, fmt.Errorf("client.New: %w", err)
	}

	return &Client{socket: socket, config: configHandler}, nil
}

// Timeout of the client socket
func (c *Client) Timeout(duration time.Duration) {
	c.socket.Timeout(duration)
}

// Attempt amount for requests
func (c *Client) Attempt(attempt uint8) {
	c.socket.Attempt(attempt)
}

// Close sends a close signal to the Handler
func (c *Client) Close() error {
	req := message.Request{
		Command:    control.HandlerClose,
		Parameters: datatype.New(),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return fmt.Errorf("socket.Request(cmd='%s'): %w", control.HandlerClose, err)
	}
	if !reply.IsOK() {
		return fmt.Errorf("reply.Message: %s", reply.ErrorMessage())
	}

	err = c.socket.Close()
	if err != nil {
		return fmt.Errorf("client.socket.Close: %w", err)
	}
	return nil
}

// Config returns the handler configuration
func (c *Client) Config() (*handlerConfig.Handler, error) {
	req := message.Request{
		Command:    control.HandlerConfig,
		Parameters: datatype.New(),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return nil, fmt.Errorf("socket.Request('%s'): %w", control.HandlerConfig, err)
	}
	if !reply.IsOK() {
		return nil, fmt.Errorf("reply.Message: %s", reply.ErrorMessage())
	}

	kv, err := reply.ReplyParameters().NestedValue("config")
	if err != nil {
		return nil, fmt.Errorf("reply.ReplyParmaters().NestedValue('config'): %w", err)
	}
	var returnedConfig handlerConfig.Handler
	err = kv.Interface(&returnedConfig)
	if err != nil {
		return nil, fmt.Errorf("kv.Interface('handlerConfig.Handler'): %w", err)
	}

	return &returnedConfig, nil
}

// Id of the handler that this Client connected to the manager.
func (c *Client) Id() string {
	return c.config.Id
}

// HandlerStatus returns the handler status.
func (c *Client) HandlerStatus() (string, error) {
	req := message.Request{
		Command:    control.HandlerStatus,
		Parameters: datatype.New(),
	}

	reply, err := c.socket.Request(&req)
	if err != nil {
		return "", fmt.Errorf("socket.Request('%s'): %w", control.HandlerStatus, err)
	}
	if !reply.IsOK() {
		return "", fmt.Errorf("reply.Message: %s", reply.ErrorMessage())
	}

	status, err := reply.ReplyParameters().StringValue("status")
	if err != nil {
		return "", fmt.Errorf("reply.Parameters.GetString('status'): %w", err)
	}

	return status, nil
}
