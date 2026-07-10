// Package pair adds a layer that forwards incoming messages through an in-process pair socket.
package handler

import (
	"fmt"
	"sync"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type Pair struct {
	*Handler
	*Autocontext
	*Security
	socket       *zmq.Socket
	pairW        sync.WaitGroup
	broadcasting *datatype.Queue
	Control      *Control
}

var _ Interface = (*Pair)(nil)

// NewPair Pair returned.
func NewPair() *Pair {
	return &Pair{
		Handler:      New(),
		broadcasting: datatype.NewQueue(),
		Control:      NewControl(),
		Autocontext:  NewAutocontext(),
		Security:     NewSecurity(),
	}
}

// SetEndpoint adds the parameters of the handler from the config.
func (pair *Pair) SetEndpoint(endpoint message.Endpoint) {
	pair.Handler.SetEndpoint(endpoint)
	pair.Control.SetEndpoint(endpoint)
}

func (pair *Pair) SetLogger(parent *log.Logger) error {
	if err := pair.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return pair.Control.SetLogger(nil)
	}
	return pair.Control.SetLogger(parent.Child(ControlCategory))
}

// Type returns the handler type.
func (pair *Pair) Type() HandlerType {
	return PairType
}

// Start the pair directly, not by goroutine.
func (pair *Pair) Start() error {
	if pair.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("configuration not set")
	}
	if pair.Control == nil {
		return fmt.Errorf("control not set")
	}

	if pair.mushroomURL == "" {
		return fmt.Errorf("mushroom URL not set, call SetMushroomURL first")
	}

	pair.setControlRoutes()

	if pair.Control.Status() != SocketReady {
		if err := pair.Control.Start(); err != nil {
			return fmt.Errorf("control.Start: %w", err)
		}
	}

	if err := pair.startPair(); err != nil {
		return fmt.Errorf("pair.startPair: %w", err)
	}

	return nil
}

func (pair *Pair) setControlRoutes() {
	pair.Control.Route(HandlerConfig, pair.onControlConfig)
	pair.Control.Route(HandlerStart, pair.onControlStart)
	pair.Control.Route(HandlerClose, pair.onControlClose)
	pair.Control.Route(Broadcast, pair.onBroadcast)
	pair.Control.Route(MessageAmount, pair.onMessageAmount)
}

func (pair *Pair) onControlClose(req message.RequestInterface) message.ReplyInterface {
	pair.stopPair()
	_ = pair.npacRemoveHandler()
	return req.Ok(datatype.New())
}

func (pair *Pair) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", pair.Endpoint()))
}

func (pair *Pair) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if pair.Control.Status() == SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", pair.Control.Status()))
	}
	if err := pair.startPair(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", pair.Control.Status()))
}

func (pair *Pair) startPair() error {
	if pair.socket != nil {
		return fmt.Errorf("pair already running")
	}

	pair.pairW.Add(1)

	ready := make(chan error)

	go func(ready chan error) {
		defer pair.pairW.Done()

		socket, err := zmq.NewSocket(zmq.PAIR)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket(PAIR): %w", err)
			return
		}

		err = pair.register(socket, pair.Endpoint())
		if err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("register: %w", err)
			return
		}

		pairUrl := pair.Endpoint().HandlerUrl()
		if err := socket.Bind(pairUrl); err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("socket.Bind('%s'): %w", pairUrl, err)
			return
		}

		pair.socket = socket
		pair.Control.SetSocketReady()

		err = pair.npacRegisterHandler(pair.Control.Endpoint())
		if err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("npacRegisterHandler: %w", err)
			return
		}

		ready <- nil

		poller := zmq.NewPoller()
		poller.Add(socket, zmq.POLLIN)

		for pair.Control.Running() {
			pair.flushBroadcast(socket)

			polled, err := poller.Poll(time.Millisecond)
			if err != nil {
				pair.LogError("poller.Poll", "error", err)
				break
			}

			if len(polled) == 0 {
				continue
			}

			if err := pair.handleRequest(socket); err != nil {
				pair.LogError("pair.handleRequest", "error", err)
				break
			}
		}

		if err := poller.RemoveBySocket(socket); err != nil {
			pair.LogError("poller.RemoveBySocket", "error", err)
		}
		if err := socket.Close(); err != nil {
			pair.LogError("socket.Close", "error", err)
		}
		pair.socket = nil
		pair.Control.SetSocketNil()
	}(ready)

	return <-ready
}

func (pair *Pair) flushBroadcast(socket *zmq.Socket) {
	for !pair.broadcasting.IsEmpty() {
		reply := pair.broadcasting.Pop().(message.ReplyInterface)
		envelope, err := pair.Packer().SerializeReply(reply)
		if err != nil {
			pair.LogError("messageOps.SerializeReply", "error", err)
			continue
		}
		if _, err := socket.SendMessageDontwait(envelope); err != nil {
			pair.LogError("socket.SendMessageDontwait", "error", err)
			return
		}
	}
}

func (pair *Pair) handleRequest(socket *zmq.Socket) error {
	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	req, hmacHash, err := pair.Packer().DeserializeRequest(raw)
	if err != nil {
		reply := pair.Packer().EmptyRequest().Fail(fmt.Sprintf("messageOps.DeserializeRequest: %v", err))
		return pair.sendReply(socket, reply, "", "")
	}

	cmd := req.CommandName()
	matchedSecret := ""
	if pair.IsWhitelistExist(cmd) {
		ok := pair.ValidateRequestHmac(req, hmacHash)
		if !ok {
			return pair.sendReply(socket, pair.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()), cmd, matchedSecret)
		}
	}

	handleFunc, err := pair.GetHandleFunc(cmd)
	if err != nil {
		return pair.sendReply(socket, req.Fail(fmt.Sprintf("handler.GetHandleFunc(%s): %v", cmd, err)), cmd, matchedSecret)
	}

	if err := pair.npacPushHandleContext(cmd); err != nil {
		pair.LogError("AddRoute", "error", err)
	}

	reply := handleFunc(req)

	if err := pair.popHandleContext(cmd); err != nil {
		pair.LogError("RemoveRoute", "error", err)
	}

	return pair.sendReply(socket, reply, cmd, matchedSecret)
}

func (pair *Pair) sendReply(socket *zmq.Socket, reply message.ReplyInterface, cmd, matchedSecret string) error {
	var hmac string
	if pair.IsWhitelistExist(cmd) && matchedSecret != "" {
		hmac = message.ComputeHMAC(reply.String(), matchedSecret)
	}
	envelope, err := pair.Packer().SerializeReply(reply, hmac)
	if err != nil {
		return fmt.Errorf("messageOps.SerializeReply: %w", err)
	}
	if _, err := socket.SendMessage(envelope); err != nil {
		return fmt.Errorf("socket.SendMessage: %w", err)
	}
	return nil
}

func (pair *Pair) stopPair() {
	if pair.socket == nil && !pair.Control.Running() {
		return
	}

	pair.Control.SetSocketNil()
	pair.pairW.Wait()
}

func (pair *Pair) onBroadcast(req message.RequestInterface) message.ReplyInterface {
	if pair.broadcasting.IsFull() {
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

	pair.broadcasting.Push(&broadcastReply)

	return req.Ok(datatype.New())
}

func (pair *Pair) onMessageAmount(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("broadcasting_length", pair.broadcasting.Len()))
}
