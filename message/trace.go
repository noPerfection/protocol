package message

import (
	"fmt"
	"strings"
	"time"

	"github.com/noPerfection/datatype"
)

// Stack keeps the parameters of the message in the service.
type Stack struct {
	RequestTime    uint64 `json:"request_time"`
	ReplyTime      uint64 `json:"reply_time,omitempty"`
	Command        string `json:"command"`
	ServiceUrl     string `json:"service_url"`
	ServerName     string `json:"server_name"`
	ServerInstance string `json:"server_instance"`
}

type RequestTraceInterface interface {
	IsFirst() bool
	SyncTrace(ReplyTraceInterface)
	AddRequestStack(serviceUrl string, serverName string, serverInstance string)
	Next(command string, parameters datatype.KeyValue)
	Traces() []*Stack
}

type ReplyTraceInterface interface {
	SetStack(serviceUrl string, serverName string, serverInstance string) error
	Traces() []*Stack
}

var _ RequestTraceInterface = (*Request)(nil)
var _ ReplyTraceInterface = (*Reply)(nil)

func (request *Request) Traces() []*Stack {
	return request.Trace
}

// IsFirst returns true if the request has no trace.
func (request *Request) IsFirst() bool {
	return len(request.Trace) == 0
}

// SyncTrace updates the request when the reply has more trace stacks.
func (request *Request) SyncTrace(reply ReplyTraceInterface) {
	repTraceLen := len(reply.Traces())
	reqTraceLen := len(request.Traces())

	if repTraceLen > reqTraceLen {
		request.Trace = append(request.Trace, reply.Traces()[reqTraceLen:]...)
	}
}

func (request *Request) AddRequestStack(serviceUrl string, serverName string, serverInstance string) {
	stack := &Stack{
		RequestTime:    uint64(time.Now().UnixMicro()),
		ReplyTime:      0,
		Command:        request.Command,
		ServiceUrl:     serviceUrl,
		ServerName:     serverName,
		ServerInstance: serverInstance,
	}

	request.Trace = append(request.Trace, stack)
}

// Next creates a new request based on the previous one.
func (request *Request) Next(command string, parameters datatype.KeyValue) {
	request.Command = command
	request.Parameters = parameters
}

func (reply *Reply) Traces() []*Stack {
	return reply.Trace
}

// SetStack adds the current service's server into the reply.
func (reply *Reply) SetStack(serviceUrl string, serverName string, serverInstance string) error {
	for i, stack := range reply.Trace {
		if strings.Compare(stack.ServiceUrl, serviceUrl) == 0 &&
			strings.Compare(stack.ServerName, serverName) == 0 &&
			strings.Compare(stack.ServerInstance, serverInstance) == 0 {
			reply.Trace[i].ReplyTime = uint64(time.Now().UnixMicro())
			return nil
		}
	}

	return fmt.Errorf("no trace stack for service %s server %s:%s", serviceUrl, serverName, serverInstance)
}
