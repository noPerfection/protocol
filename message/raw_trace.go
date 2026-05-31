package message

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/noPerfection/datatype"
)

var _ RequestTraceInterface = (*RawRequest)(nil)
var _ ReplyTraceInterface = (*RawReply)(nil)

func RawTraceIndex(messages []string) int {
	// contentOffset skips the first delimiter
	contentOffset := 0
	if MultiPart(messages) {
		contentOffset = 2
	} else if SyncReplierEnvelope(messages) {
		contentOffset = 1
	}

	for i := len(messages) - 1; i > contentOffset; i-- {
		if messages[i] == "" {
			return i
		}
	}

	return -1
}

func rawContentEnd(messages []string, defaultEnd int) int {
	traceDelimiter := RawTraceIndex(messages)
	if traceDelimiter > -1 {
		return traceDelimiter
	}

	return defaultEnd
}

func rawTracePostOffset(trace []*Stack) int {
	if len(trace) > 0 {
		return 2
	}

	return 0
}

func parseRawTrace(messages []string) ([]*Stack, bool, error) {
	traceDelimiter := RawTraceIndex(messages)
	if traceDelimiter == -1 {
		return nil, false, nil
	}

	if len(messages[traceDelimiter+1:]) == 0 {
		return nil, false, fmt.Errorf("trace delimiter given but trace is empty")
	}

	var traces []*Stack
	err := json.Unmarshal([]byte(messages[len(messages)-1]), &traces)
	if err != nil {
		return nil, false, fmt.Errorf("json.Unmarshal('last_message_part'): %w", err)
	}

	return traces, true, nil
}

func setRawTraceEnvelope(messages []string, trace []*Stack, offset int) error {
	if len(trace) == 0 {
		return nil
	}

	bytes, err := json.Marshal(trace)
	if err != nil {
		return fmt.Errorf("failed to serialize Request to key-value: %v", err)
	}

	str := string(bytes)
	if len(str) == 0 {
		return fmt.Errorf("kv.String is nil")
	}

	messages[offset] = ""
	messages[offset+1] = str

	return nil
}

func (request *RawRequest) setTraceFromEnvelope(messages []string) error {
	traces, ok, err := parseRawTrace(messages)
	if err != nil {
		return err
	}
	if ok {
		request.trace = traces
	}

	return nil
}

func (reply *RawReply) setTraceFromEnvelope(messages []string) error {
	traces, ok, err := parseRawTrace(messages)
	if err != nil {
		return err
	}
	if ok {
		reply.trace = traces
	}

	return nil
}

func (request *RawRequest) Traces() []*Stack {
	return request.trace
}

// IsFirst returns true if the request has no trace.
func (request *RawRequest) IsFirst() bool {
	return len(request.trace) == 0
}

// SyncTrace updates the request when the reply has more trace stacks.
func (request *RawRequest) SyncTrace(reply ReplyTraceInterface) {
	repTraceLen := len(reply.Traces())
	reqTraceLen := len(request.Traces())

	if repTraceLen > reqTraceLen {
		request.trace = append(request.trace, reply.Traces()[reqTraceLen:]...)
	}
}

// AddRequestStack adds the new trace into the request.
func (request *RawRequest) AddRequestStack(serviceUrl string, serverName string, serverInstance string) {
	stack := &Stack{
		RequestTime:    uint64(time.Now().UnixMicro()),
		ReplyTime:      0,
		Command:        fmt.Sprintf("%d", len(request.trace)+1),
		ServiceUrl:     serviceUrl,
		ServerName:     serverName,
		ServerInstance: serverInstance,
	}

	request.trace = append(request.trace, stack)
}

// Next creates a new request based on the previous one. It uses the Request.
func (request *RawRequest) Next(command string, parameters datatype.KeyValue) {
	nextReq := (&Request{Command: command, Parameters: parameters}).String()

	if len(nextReq) > 0 {
		request.messages = []string{nextReq}
	}
}

func (reply *RawReply) Traces() []*Stack {
	return reply.trace
}

// SetStack adds the current service's server into the reply.
func (reply *RawReply) SetStack(serviceUrl string, serverName string, serverInstance string) error {
	for i, stack := range reply.trace {
		if strings.Compare(stack.ServiceUrl, serviceUrl) == 0 &&
			strings.Compare(stack.ServerName, serverName) == 0 &&
			strings.Compare(stack.ServerInstance, serverInstance) == 0 {
			reply.trace[i].ReplyTime = uint64(time.Now().UnixMicro())
			return nil
		}
	}

	return fmt.Errorf("no trace stack for service %s server %s:%s", serviceUrl, serverName, serverInstance)
}
