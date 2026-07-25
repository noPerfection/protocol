// Package autocontext is the client-side half of the noPerfection AutoContext (npac).
// It looks up CURVE public keys and HMAC secrets from the in-process npac handler
// so that the client can recover from ErrNoCurveKey and access-denied replies automatically.
package client

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
)

const (
	npacHandlerContext   = "handler-context"
	npacRegisterOutbound = "register-outbound"
	npacRemoveOutbound   = "remove-outbound"
)

type Autocontext struct {
	client *Socket
}

func NewAutocontext() *Autocontext {
	c, _ := New("npac", 0, SyncReplierType)
	if c == nil {
		return nil
	}
	c.Timeout(50 * time.Millisecond)
	return &Autocontext{
		client: c,
	}
}

func (c *Autocontext) Close() error {
	return c.client.Close()
}

// Returns the validated context if any to access the endpoint and request cmd.
//
// Returns:
//   - unregistered (if endpoint is not registered by any handler), true if unregistered, false if registered
//   - public-key of the endpoint to pass to control to call it. Can be empty.
//   - control-endpoint the message.Endpoint of the handler that can call it.
//
// HandlerContext resolves whether endpoint/cmd is reachable from the active
// handler context on npac's stack.
func (c *Autocontext) HandlerContext(endpoint message.Endpoint, cmd string) (bool, string, message.Endpoint, error) {
	params := datatype.New().
		Set("endpoint", endpoint).
		Set("command", cmd)

	callerStack := callerStackEntries()
	if len(callerStack) == 0 {
		return false, "", message.Endpoint{}, fmt.Errorf("no caller stack")
	}
	params.Set("caller-stack", callerStack)

	reply, err := c.client.Request(&message.Request{
		Command:    npacHandlerContext,
		Parameters: params,
	})

	if err != nil {
		return false, "", message.Endpoint{}, err
	}
	if !reply.IsOK() {
		return false, "", message.Endpoint{}, fmt.Errorf(reply.ErrorMessage())
	}

	unregistered, err := reply.ReplyParameters().BoolValue("unregistered")
	if err != nil {
		return false, "", message.Endpoint{}, fmt.Errorf("unregistered: %w", err)
	}
	if unregistered {
		return true, "", message.Endpoint{}, nil
	}
	publicKey, err := reply.ReplyParameters().StringValue("public-key")
	if err != nil {
		publicKey = ""
	}
	controlKV, err := reply.ReplyParameters().NestedValue("control-endpoint")
	if err != nil {
		return false, "", message.Endpoint{}, fmt.Errorf("control-endpoint: %w", err)
	}
	var control message.Endpoint
	if err := controlKV.Interface(&control); err != nil {
		return false, "", message.Endpoint{}, fmt.Errorf("control-endpoint: %w", err)
	}
	return false, publicKey, control, nil
}

// Created a service to register handlers called within handlers. Parameters are:
// - endpoint: the endpoint of the handler
func (c *Autocontext) RegisterOutbound(endpoint message.Endpoint, mushroomURL string, publicKey string) error {
	reply, err := c.client.Request(&message.Request{
		Command: npacRegisterOutbound,
		Parameters: datatype.New().
			Set("endpoint", endpoint).
			Set("mushroom-url", mushroomURL).
			Set("public-key", publicKey),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// callerStackEntries returns normalized function names from the current stack,
// innermost frame first, stopping at main.main.
func callerStackEntries() []string {
	const depth = 64
	var pcs [depth]uintptr
	n := runtime.Callers(0, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])

	var entries []string
	for {
		frame, more := frames.Next()
		entries = append(entries, strings.TrimSuffix(frame.Function, "-fm"))
		if frame.Function == "main.main" || !more {
			break
		}
	}
	return entries
}

func (c *Autocontext) RemoveOutbound(entrypoint message.Endpoint) error {
	reply, err := c.client.Request(&message.Request{
		Command: npacRemoveOutbound,
		Parameters: datatype.New().
			Set("entrypoint", entrypoint),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}
