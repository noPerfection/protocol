package client

import "github.com/noPerfection/protocol/message"

// SendInterface is a one-way transmit to a handler.
type SendInterface interface {
	Send(req message.RequestInterface) error
}

// RequestInterface is a request–reply transmit that waits for a handler reply.
type RequestInterface interface {
	Request(req message.RequestInterface) (message.ReplyInterface, error)
}

// ReceiveInterface delivers inbound handler replies on a channel.
// Only reply messages are surfaced, not generic handler events.
type ReceiveInterface interface {
	Receive() <-chan message.ReplyInterface
}
