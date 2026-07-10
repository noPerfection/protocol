package handler

import (
	"fmt"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler/npac"
	"github.com/noPerfection/protocol/message"
)

const (
	// NpacEndpointId is the inproc endpoint id of the npac handler (inproc://npac).
	NpacEndpointId = "npac"
	// NpacTimeout is the maximum time to wait for a reply from npac.
	NpacTimeout = 50 * time.Millisecond
)

type Autocontext struct {
	npacSecret string
	client     *client.Socket
}

func NewAutocontext() *Autocontext {
	secret := GenerateSecret()
	c, _ := client.New(NpacEndpointId, 0, client.SyncReplierType)
	if c != nil {
		c.Timeout(NpacTimeout)
		_ = c.Whitelist(npac.RemoveHandlerCmd, secret)
		_ = c.Whitelist(npac.PushHandlerContextCmd, secret)
		_ = c.Whitelist(npac.PopHandlerContextCmd, secret)
	}
	h.routes[AddOutboundCmd] = h.onAddOutbound
	h.routes[RemOutboundCmd] = h.onRemoveOutbound
	h.routes[AddHandlerCmd] = h.onAddHandler                 // HMAC protected
	h.routes[RemoveHandlerCmd] = h.onRemoveHandler           // HMAC protected
	h.routes[SecureEdgeCaseCmd] = h.onSecureEdgeCase         // HMAC protected
	h.routes[PushHandlerContextCmd] = h.onPushHandlerContext // HMAC protected
	h.routes[PopHandlerContextCmd] = h.onPopHandlerContext   // HMAC protected
	return &Autocontext{
		npacSecret: secret,
		client:     c,
	}
}

// npacRegisterHandler registers a handler's URL and CURVE public key with npac.
// The handler's npacSecret is passed as a plain parameter; npac stores it and
// uses it to authenticate future add-route / remove-route calls from this handler.
// If the URL is already registered with a different secret, npac rejects the call.
func (c *Autocontext) npacRegisterHandler(url, pubKey string) error {
	reply, err := c.client.Request(&message.Request{
		Command: npac.AddHandlerCmd,
		Parameters: datatype.New().
			Set("url", url).
			Set("public-key", pubKey).
			Set("secret", c.npacSecret),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// npacPushHandleContext registers an HMAC secret for a handler URL and command with npac.
// The request is HMAC-signed automatically via the client whitelist.
func (c *Autocontext) npacPushHandleContext(url, cmd, routeSecret string) error {
	reply, err := c.client.Request(&message.Request{
		Command: npac.PushHandlerContextCmd,
		Parameters: datatype.New().
			Set("url", url).
			Set("command", cmd).
			Set("secret", routeSecret),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// npacRemoveHandler removes a handler's registration from npac.
// The request is HMAC-signed automatically via the client whitelist.
func (c *Autocontext) npacRemoveHandler(url string) error {
	reply, err := c.client.Request(&message.Request{
		Command:    npac.RemoveHandlerCmd,
		Parameters: datatype.New().Set("url", url),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// popHandleContext removes an HMAC secret registration for a handler URL and command.
// The request is HMAC-signed automatically via the client whitelist.
func (c *Autocontext) popHandleContext(url, cmd string) error {
	reply, err := c.client.Request(&message.Request{
		Command: npac.PopHandlerContextCmd,
		Parameters: datatype.New().
			Set("url", url).
			Set("command", cmd),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}
