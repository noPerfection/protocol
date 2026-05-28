package route

import (
	"fmt"

	"github.com/noPerfection/protocol/message"
)

// HandleFunc is the function type that manipulates commands.
// It always accepts a request and returns a reply.
type HandleFunc = func(message.RequestInterface) message.ReplyInterface

// Handle calls the handle func for the req.
func Handle(req message.RequestInterface, handleInterface interface{}) message.ReplyInterface {
	var reply message.ReplyInterface

	if !IsHandleFunc(handleInterface) {
		reply = req.Fail(fmt.Sprintf("the '%s' command handler is not a valid handle function", req.CommandName()))
		return reply
	}

	handleFunc := handleInterface.(HandleFunc)
	reply = handleFunc(req)

	return reply
}

// IsHandleFunc returns true if the given interface is convertible into HandleFunc
func IsHandleFunc(handleInterface interface{}) bool {
	_, ok := handleInterface.(HandleFunc)
	return ok
}
