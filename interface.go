package message

import (
	"github.com/ahmetson/common-lib/data_type/key_value"
)

// RequestInterface generic requests
type RequestInterface interface {
	// ConId returns a connection id for each sending session.
	ConId() string
	// IsFirst returns true if the request has no trace request or id,
	IsFirst() bool
	SyncTrace(ReplyInterface)
	AddRequestStack(serviceUrl string, serverName string, serverInstance string)
	// Bytes convert the message to the sequence of bytes
	Bytes() ([]byte, error)
	// PublicKey For security; Work in Progress.
	PublicKey() string
	// SetPublicKey For security; Work in Progress.
	SetPublicKey(publicKey string)
	String() (string, error)
	Strings() ([]string, error)
	SetUuid()
	// Next creates a new request based on the previous one.
	Next(command string, parameters key_value.KeyValue)
	// Fail creates a new Reply as a failure
	// It accepts the error message that explains the reason of the failure.
	Fail(message string) ReplyInterface
	Ok(parameters key_value.KeyValue) ReplyInterface
	Traces() []*Stack
	SetMeta(map[string]string)
	CommandName() string
	RouteParameters() key_value.KeyValue
}

type ReplyInterface interface {
	ConId() string
	// SetStack adds the current service's server into the reply
	SetStack(serviceUrl string, serverName string, serverInstance string) error
	// IsOK returns the Status of the message.
	IsOK() bool
	// String converts the Reply to the string format
	String() (string, error)
	Strings() ([]string, error)
	// Bytes converts Reply to the sequence of bytes
	Bytes() ([]byte, error)
	Traces() []*Stack
	ErrorMessage() string
	ReplyParameters() key_value.KeyValue
}

type ReqFunc = func(messages []string) (RequestInterface, error)
type ReplyFunc = func(messages []string) (ReplyInterface, error)
