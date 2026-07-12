package handler

import (
	"errors"
	"fmt"
	"time"

	"github.com/ahmetson/mushroom"
	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/handler/npac"
	"github.com/noPerfection/protocol/message"
)

type Autocontext struct {
	npacSecret  string
	mushroomURL string
	started     bool
	client      *client.Socket
}

func NewAutocontext() *Autocontext {
	secret := message.GenerateSecret()
	c, _ := client.New("npac", 0, client.SyncReplierType)
	if c != nil {
		c.Timeout(50 * time.Millisecond)
		_ = c.Whitelist(npac.RemoveHandler, secret)
		_ = c.Whitelist(npac.SecureEdgeCase, secret)
		_ = c.Whitelist(npac.PushHandlerContext, secret)
		_ = c.Whitelist(npac.PopHandlerContext, secret)
	}
	return &Autocontext{
		npacSecret: secret,
		client:     c,
	}
}

func (c *Autocontext) SetMushroomURL(mushroomURL string) {
	c.mushroomURL = mushroomURL
}

// routeURL returns the handler's mushroom URL with cmd embedded as the
// "command" additional property, producing a route URL that npac can parse.
func (c *Autocontext) routeURL(cmd string) (string, error) {
	hypha, err := (&mushroom.Soil{}).Hypha(c.mushroomURL)
	if err != nil {
		return "", fmt.Errorf("routeURL: parse %q: %w", c.mushroomURL, err)
	}
	if hypha.AdditionalProps == nil {
		hypha.AdditionalProps = make(map[string]string)
	}
	hypha.AdditionalProps["command"] = cmd
	return hypha.String(), nil
}

// npacRegisterHandler registers a handler's mushroom URL, CURVE public key, and
// control endpoint with npac, and caches the mushroom URL for subsequent calls.
func (c *Autocontext) npacRegisterHandler(controlEndpoint message.Endpoint) error {
	if c.mushroomURL == "" {
		return fmt.Errorf("mushroom URL not set, call SetMushroomURL first")
	}
	req := &message.Request{
		Command: npac.RegisterHandler,
		Parameters: datatype.New().
			Set("mushroom-url", c.mushroomURL).
			Set("control-endpoint", controlEndpoint).
			Set("npac-secret", c.npacSecret),
	}
	hmac := message.ComputeHMAC(req.String(), c.npacSecret)
	reply, err := c.client.Request(req, hmac)
	if err != nil {
		if errors.Is(err, message.RequestTimeoutError) {
			c.started = false
			return nil
		}
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	c.started = true
	return nil
}

// npacPushHandleContext pushes a route URL (mushroomURL + cmd as additional prop)
// onto npac's context stack. HMAC-signed automatically via the client whitelist.
func (c *Autocontext) npacPushHandleContext(cmd string) error {
	if !c.started {
		return nil
	}
	url, err := c.routeURL(cmd)
	if err != nil {
		return err
	}
	reply, err := c.client.Request(&message.Request{
		Command:    npac.PushHandlerContext,
		Parameters: datatype.New().Set("mushroom-url", url),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// NpacSecureEdgeCase authorizes the cmd's context to call the outbound.
func (c *Autocontext) NpacSecureEdgeCase(outbound, cmd string) error {
	mushroomURL, err := c.routeURL(cmd)
	if err != nil {
		return err
	}
	reply, err := c.client.Request(&message.Request{
		Command: npac.SecureEdgeCase,
		Parameters: datatype.New().
			Set("outbound", outbound).
			Set("mushroom-url", mushroomURL),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// npacRemoveHandler removes this handler's registration from npac.
// HMAC-signed automatically via the client whitelist.
func (c *Autocontext) npacRemoveHandler() error {
	if !c.started {
		return nil
	}
	reply, err := c.client.Request(&message.Request{
		Command:    npac.RemoveHandler,
		Parameters: datatype.New().Set("mushroom-url", c.mushroomURL),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}

// popHandleContext pops the route URL (mushroomURL + cmd) from npac's context stack.
// HMAC-signed automatically via the client whitelist.
func (c *Autocontext) popHandleContext(cmd string) error {
	if !c.started {
		return nil
	}
	url, err := c.routeURL(cmd)
	if err != nil {
		return err
	}
	reply, err := c.client.Request(&message.Request{
		Command:    npac.PopHandlerContext,
		Parameters: datatype.New().Set("mushroom-url", url),
	})
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	return nil
}
