package route

import (
	"fmt"

	"github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/message"
)

type depSock = *client.Socket

// HandleFunc0 is the function type that manipulates the commands.
// It accepts at least message.RequestInterface and log.Logger then returns a message.ReplyInterface.
//
// Optionally, the handler can pass the shared states in the additional parameters.
// The most use case for optional request is to pass the link to the Database.
type HandleFunc0 = func(message.RequestInterface) message.ReplyInterface
type HandleFunc1 = func(message.RequestInterface, depSock) message.ReplyInterface
type HandleFunc2 = func(message.RequestInterface, depSock, depSock) message.ReplyInterface
type HandleFunc3 = func(message.RequestInterface, depSock, depSock, depSock) message.ReplyInterface
type HandleFuncN = func(message.RequestInterface, ...depSock) message.ReplyInterface

// DepAmount returns -1 if the interface is not a valid HandleFunc.
// If the interface has more than 3 arguments, it returns 4.
// Otherwise, it returns 0..3
func DepAmount(handleInterface interface{}) int {
	_, ok := handleInterface.(HandleFunc0)
	if ok {
		return 0
	}
	_, ok = handleInterface.(HandleFunc1)
	if ok {
		return 1
	}
	_, ok = handleInterface.(HandleFunc2)
	if ok {
		return 2
	}
	_, ok = handleInterface.(HandleFunc3)
	if ok {
		return 3
	}
	_, ok = handleInterface.(HandleFuncN)
	if ok {
		return 4
	}

	return -1
}

// Handle calls the handle func for the req.
// Optionally, if the handler requires the extensions, it will pass the socket clients to the handle func.
func Handle(req message.RequestInterface, handleInterface interface{}, depClients []*client.Socket) message.ReplyInterface {
	var reply message.ReplyInterface

	depAmount := DepAmount(handleInterface)
	if !IsHandleFuncWithDeps(handleInterface, len(depClients)) {
		reply = req.Fail(fmt.Sprintf("the '%s' command handler requires %d dependencies, but route has %d dependencies",
			req.CommandName(), depAmount, len(depClients)))
		return reply
	}

	if len(depClients) == 0 {
		handleFunc := handleInterface.(HandleFunc0)
		reply = handleFunc(req)
	} else if len(depClients) == 1 {
		handleFunc := handleInterface.(HandleFunc1)
		reply = handleFunc(req, depClients[0])
	} else if len(depClients) == 2 {
		handleFunc := handleInterface.(HandleFunc2)
		reply = handleFunc(req, depClients[0], depClients[1])
	} else if len(depClients) == 3 {
		handleFunc := handleInterface.(HandleFunc3)
		reply = handleFunc(req, depClients[0], depClients[1], depClients[2])
	} else {
		handleFunc := handleInterface.(HandleFuncN)
		reply = handleFunc(req, depClients...)
	}

	return reply
}

// IsHandleFunc returns true if the given interface is convertible into HandleFunc
func IsHandleFunc(handleInterface interface{}) bool {
	return DepAmount(handleInterface) > -1
}

// IsHandleFuncWithDeps returns true if the handle function can pass the given dependencies
func IsHandleFuncWithDeps(handleInterface interface{}, actualAmount int) bool {
	depAmount := DepAmount(handleInterface)
	if depAmount == -1 {
		return false
	}
	if depAmount > 3 && actualAmount < 4 {
		return false
	}
	if depAmount != actualAmount {
		return false
	}

	return true
}
