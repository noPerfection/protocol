// Package pair adds a layer that forwards incoming messages through an in-process pair socket.
package handler

import (
	"fmt"
	"sync"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

type Pair struct {
	*Handler
	*Security
	socket           *zmq.Socket
	wake             *wakePipe
	pairW            sync.WaitGroup
	broadcasting     *datatype.Queue
	PublisherControl *PublisherControl
}

var _ Interface = (*Pair)(nil)

// NewPair Pair returned.
func NewPair() *Pair {
	return &Pair{
		Handler:          New(),
		broadcasting:     datatype.NewQueue(),
		PublisherControl: NewPublisherControl(),
		Security:         NewSecurity(),
	}
}

// SetMushroomURL configures the npac mushroom URL on the pair control.
func (pair *Pair) SetMushroomURL(mushroomURL string) {
	pair.PublisherControl.Autocontext.SetMushroomURL(mushroomURL)
}

func (pair *Pair) Secure(secretKey string) {
	pair.Security.Secure(secretKey)
	pair.PublisherControl.setSecretKey(secretKey)
}

// SetEndpoint adds the parameters of the handler from the config.
func (pair *Pair) SetEndpoint(endpoint message.Endpoint) {
	pair.Handler.SetEndpoint(endpoint)
	pair.PublisherControl.SetEndpoint(endpoint)
}

func (pair *Pair) SetLogger(parent *log.Logger) error {
	if err := pair.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		return pair.PublisherControl.SetLogger(nil)
	}
	return pair.PublisherControl.SetLogger(parent.Child(ControlCategory))
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
	if pair.PublisherControl == nil {
		return fmt.Errorf("control not set")
	}

	if pair.PublisherControl.mushroomURL == "" {
		return fmt.Errorf("mushroom URL not set, call SetMushroomURL first")
	}

	pair.setControlRoutes()

	if pair.PublisherControl.Status() != SocketReady {
		if err := pair.PublisherControl.Start(); err != nil {
			return fmt.Errorf("control.Start: %w", err)
		}
	}

	if err := pair.startPair(); err != nil {
		return fmt.Errorf("pair.startPair: %w", err)
	}

	return nil
}

func (pair *Pair) setControlRoutes() {
	pair.PublisherControl.Route(HandlerConfig, pair.onControlConfig)
	pair.PublisherControl.Route(HandlerStart, pair.onControlStart)
	pair.PublisherControl.Route(HandlerClose, pair.onControlClose)
	pair.PublisherControl.Route(Broadcast, pair.onBroadcast)
	pair.PublisherControl.Route(MessageAmount, pair.onMessageAmount)
}

func (pair *Pair) onControlClose(req message.RequestInterface) message.ReplyInterface {
	pair.stopPair()
	_ = pair.PublisherControl.npacRemoveHandler()
	return req.Ok(datatype.New())
}

func (pair *Pair) onControlConfig(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("config", pair.Endpoint()))
}

func (pair *Pair) onControlStart(req message.RequestInterface) message.ReplyInterface {
	if pair.PublisherControl.Status() == SocketReady {
		return req.Fail(fmt.Sprintf("handler already running with status %s", pair.PublisherControl.Status()))
	}
	if err := pair.startPair(); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New().Set("status", pair.PublisherControl.Status()))
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
		pair.PublisherControl.SetSocketReady()

		err = pair.PublisherControl.npacRegisterHandler(pair.PublisherControl.Endpoint())
		if err != nil {
			_ = socket.Close()
			ready <- fmt.Errorf("npacRegisterHandler: %w", err)
			return
		}

		ready <- nil

		wake, err := newWakePipe()
		if err != nil {
			_ = socket.Close()
			ready <- err
			return
		}
		pair.wake = wake
		defer wake.close()

		poller := zmq.NewPoller()
		poller.Add(socket, zmq.POLLIN)
		wake.addToPoller(poller)

		for pair.PublisherControl.Running() {
			pair.flushBroadcast(socket)

			polled, err := poller.Poll(blockForever)
			if err != nil {
				pair.LogError("poller.Poll", "error", err)
				break
			}

			for _, item := range polled {
				if isWakePoll(wake, item) {
					wake.drain()
					continue
				}
				if item.Socket != socket {
					continue
				}
				if err := pair.handleRequest(socket); err != nil {
					pair.LogError("pair.handleRequest", "error", err)
					break
				}
			}
		}

		if err := poller.RemoveBySocket(socket); err != nil {
			pair.LogError("poller.RemoveBySocket", "error", err)
		}
		takeAndCloseSocket(&pair.socket)
		pair.wake = nil
		pair.PublisherControl.SetSocketNil()
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
		var ok bool
		matchedSecret, ok = pair.getRequestSecret(req, hmacHash)
		if !ok {
			return pair.sendReply(socket, pair.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()), cmd, matchedSecret)
		}
	} else if pair.IsWhitelistRequired(cmd) {
		return pair.sendReply(socket, pair.Packer().EmptyRequest().Fail(message.ErrAccessDenied.Error()+", whitelist required"), cmd, matchedSecret)
	}

	handleFunc, err := pair.GetHandleFunc(cmd)
	if err != nil {
		return pair.sendReply(socket, req.Fail(fmt.Sprintf("handler.GetHandleFunc(%s): %v", cmd, err)), cmd, matchedSecret)
	}

	if err := pair.PublisherControl.npacPushHandleContext(cmd, handleFunc); err != nil {
		pair.LogError("npacPushHandleContext", "error", err)
	}

	reply := handleFunc(req)

	if err := pair.PublisherControl.popHandleContext(cmd, handleFunc); err != nil {
		pair.LogError("popHandleContext", "error", err)
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
	if pair.socket == nil && !pair.PublisherControl.Running() {
		return
	}

	pair.PublisherControl.SetSocketNil()
	if pair.wake != nil {
		pair.wake.signal()
	}
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
	if pair.wake != nil {
		pair.wake.signal()
	}

	return req.Ok(datatype.New())
}

func (pair *Pair) onMessageAmount(req message.RequestInterface) message.ReplyInterface {
	return req.Ok(datatype.New().Set("broadcasting_length", pair.broadcasting.Len()))
}
