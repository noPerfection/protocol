// Package pair adds a layer that forwards incoming messages through an in-process pair socket.
package pair

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/autocontext"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const Broadcast = "broadcast"
const MessageAmount = "message-amount"
const BroadcastParameter = "reply"

type Pair struct {
	*handler.Handler
	socket               *zmq.Socket
	pairW                sync.WaitGroup
	broadcasting         *datatype.Queue
	Control              *handler.Control
	curveSecretKey       string
	allowedClientPubKeys []string
	npacSecret           string
}

var _ handler.Interface = (*Pair)(nil)

// New Pair returned.
func New() *Pair {
	return &Pair{
		Handler:      handler.New(),
		broadcasting: datatype.NewQueue(),
		Control:      handler.NewControl(),
		npacSecret:   handler.GenerateSecret(),
	}
}

// SetEndpoint adds the parameters of the handler from the config.
func (c *Pair) SetEndpoint(endpoint message.Endpoint) {
	c.Handler.SetEndpoint(endpoint)
	c.Control.SetEndpoint(endpoint)
}

func (c *Pair) SetLogger(parent *log.Logger) error {
	if err := c.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return c.Control.SetLogger(nil)
	}
	return c.Control.SetLogger(parent.Child(handler.ControlCategory))
}

// Secure stores the CURVE server secret key. An empty key keeps the handler non-secure.
func (c *Pair) Secure(secretKey string) {
	c.curveSecretKey = secretKey
}

// Allow registers a client CURVE public key permitted to connect when ZAP is active (zmq.AuthStart).
func (c *Pair) Allow(clientPubKey string) {
	if clientPubKey == "" {
		return
	}
	for _, key := range c.allowedClientPubKeys {
		if key == clientPubKey {
			return
		}
	}
	c.allowedClientPubKeys = append(c.allowedClientPubKeys, clientPubKey)
}

// Type returns the handler type.
func (c *Pair) Type() handler.HandlerType {
	return handler.PairType
}

// Start the pair directly, not by goroutine.
func (c *Pair) Start() error {
	if c.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("configuration not set")
	}
	if c.Control == nil {
		return fmt.Errorf("control not set")
	}

	c.setControlRoutes()

	if c.Control.Status() != handler.SocketReady {
		if err := c.Control.Start(); err != nil {
			return fmt.Errorf("control.Start: %w", err)
		}
	}

	if err := c.startPair(); err != nil {
		return fmt.Errorf("pair.startPair: %w", err)
	}

	return nil
}

func (c *Pair) setControlRoutes() {
	c.Control.Route(handler.HandlerConfig, c.onControlConfig)
	c.Control.Route(handler.HandlerStart, c.onControlStart)
	c.Control.Route(handler.HandlerClose, c.onControlClose)
	c.Control.Route(Broadcast, c.onBroadcast)
	c.Control.Route(MessageAmount, c.onMessageAmount)
}

func (c *Pair) onControlClose(req message.RequestInterface) message.ReplyInterface {
	c.stopPair()
	_ = autocontext.RemoveHandler(c.Endpoint().HandlerUrl(), c.npacSecret)
	return req.Ok(datatype.New())
}

func (c *Pair) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", c.Endpoint()))
}

func (c *Pair) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if c.Control.Status() == handler.SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", c.Control.Status()))
	}
	if err := c.startPair(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", c.Control.Status()))
}

func (c *Pair) startPair() error {
	if c.socket != nil {
		return fmt.Errorf("pair already running")
	}

	c.pairW.Add(1)

	ready := make(chan error)

	go func(ready chan error) {
		defer c.pairW.Done()

		socket, err := zmq.NewSocket(zmq.PAIR)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket(PAIR): %w", err)
			return
		}

		pubKey := ""
		if c.curveSecretKey != "" {
			domain := c.Endpoint().ZapDomain()
			if err := socket.ServerAuthCurve(domain, c.curveSecretKey); err != nil {
				_ = socket.Close()
				ready <- fmt.Errorf("socket.ServerAuthCurve: %w", err)
				return
			}
			if len(c.allowedClientPubKeys) > 0 {
				zmq.AuthCurveAdd(domain, c.allowedClientPubKeys...)
			}
			if derivedKey, deriveErr := zmq.AuthCurvePublic(c.curveSecretKey); deriveErr == nil {
				pubKey = derivedKey
			}
		}

		pairUrl := c.Endpoint().HandlerUrl()
		if err := socket.Bind(pairUrl); err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("socket.Bind('%s'): %w", pairUrl, err)
			return
		}

		c.socket = socket
		c.Control.SetSocketReady()

		_ = autocontext.AddHandler(pairUrl, pubKey, c.npacSecret)
		for cmd, secrets := range c.WhitelistSnapshot() {
			for _, secret := range secrets {
				_ = autocontext.AddRoute(pairUrl, cmd, secret, c.npacSecret)
			}
		}

		ready <- nil

		poller := zmq.NewPoller()
		poller.Add(socket, zmq.POLLIN)

		for c.Control.Running() {
			c.flushBroadcast(socket)

			polled, err := poller.Poll(time.Millisecond)
			if err != nil {
				c.LogError("poller.Poll", "error", err)
				break
			}

			if len(polled) == 0 {
				continue
			}

			if err := c.handleRequest(socket); err != nil {
				c.LogError("pair.handleRequest", "error", err)
				break
			}
		}

		if err := poller.RemoveBySocket(socket); err != nil {
			c.LogError("poller.RemoveBySocket", "error", err)
		}
		if err := socket.Close(); err != nil {
			c.LogError("socket.Close", "error", err)
		}
		c.socket = nil
		c.Control.SetSocketNil()
	}(ready)

	return <-ready
}

func (c *Pair) flushBroadcast(socket *zmq.Socket) {
	for !c.broadcasting.IsEmpty() {
		reply := c.broadcasting.Pop().(message.ReplyInterface)
		envelope, err := c.Packer().SerializeReply(reply)
		if err != nil {
			c.LogError("messageOps.SerializeReply", "error", err)
			continue
		}
		if _, err := socket.SendMessageDontwait(envelope); err != nil {
			c.LogError("socket.SendMessageDontwait", "error", err)
			return
		}
	}
}

func (c *Pair) handleRequest(socket *zmq.Socket) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, hmacHash, err := c.Packer().DeserializeRequest(raw)
	if err != nil {
		reply := c.Packer().EmptyRequest().Fail(fmt.Sprintf("messageOps.DeserializeRequest: %v", err))
		return c.sendReply(socket, reply, "", "")
	}

	cmd := req.CommandName()
	matchedSecret := ""
	if c.RequiresWhitelist(cmd) {
		var ok bool
		matchedSecret, ok = c.MatchRequestSecret(req, hmacHash)
		if !ok {
			return c.sendReply(socket, c.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()), cmd, matchedSecret)
		}
	}

	handleFunc, err := c.GetHandleFunc(cmd)
	if err != nil {
		return c.sendReply(socket, req.Fail(fmt.Sprintf("handler.GetHandleFunc(%s): %v", cmd, err)), cmd, matchedSecret)
	}

	handlerUrl := c.Endpoint().HandlerUrl()
	if matchedSecret != "" {
		if err := autocontext.AddRoute(handlerUrl, cmd, matchedSecret, c.npacSecret); err != nil {
			c.LogError("autocontext.AddRoute", "error", err)
		}
	}

	reply := handleFunc(req)

	if matchedSecret != "" {
		if err := autocontext.RemoveRoute(handlerUrl, cmd, c.npacSecret); err != nil {
			c.LogError("autocontext.RemoveRoute", "error", err)
		}
	}

	return c.sendReply(socket, reply, cmd, matchedSecret)
}

func (c *Pair) sendReply(socket *zmq.Socket, reply message.ReplyInterface, cmd, matchedSecret string) error {
	var hmac string
	if c.RequiresWhitelist(cmd) && matchedSecret != "" {
		hmac = message.ComputeHMAC(reply.String(), matchedSecret)
	}
	envelope, err := c.Packer().SerializeReply(reply, hmac)
	if err != nil {
		return fmt.Errorf("messageOps.SerializeReply: %w", err)
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		return fmt.Errorf("socket.SendMessage: %w", err)
	}
	return nil
}

func (c *Pair) stopPair() {
	if c.socket == nil && !c.Control.Running() {
		return
	}

	c.Control.SetSocketNil()
	c.pairW.Wait()
}

func (c *Pair) onBroadcast(req message.RequestInterface) message.ReplyInterface {
	if c.broadcasting.IsFull() {
		return req.Fail("broadcasting queue full")
	}

	replyKV, err := req.RouteParameters().NestedValue(BroadcastParameter)
	if err != nil {
		return req.Fail(fmt.Sprintf("req.RouteParameters().NestedValue('%s'): %v", BroadcastParameter, err))
	}

	var broadcastReply message.Reply
	if err := replyKV.Interface(&broadcastReply); err != nil {
		return req.Fail(fmt.Sprintf("replyKV.Interface('message.Reply'): %v", err))
	}

	c.broadcasting.Push(&broadcastReply)

	return req.Ok(datatype.New())
}

func (c *Pair) onMessageAmount(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("broadcasting_length", c.broadcasting.Len()))
}
