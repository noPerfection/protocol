package message

import (
	"github.com/noPerfection/datatype"
)

type Packer interface {
	DeserializeRequest(zmqEnvelope []string) (RequestInterface, error)
	DeseralizeReply(zmqEnvelope []string) (ReplyInterface, error)
	SerializeRequest(request RequestInterface) ([]string, error)
	SerializeReply(reply ReplyInterface) ([]string, error)
	EmptyRequest() RequestInterface
	EmptyReply() ReplyInterface
}

// RequestInterface generic requests
type RequestInterface interface {
	// ConId returns a connection id for each sending session.
	ConId() string
	SetConId(string)
	// String implements the Stringer interface from a standard library
	String() string
	// ZmqEnvelope converts the message to the zeromq envelope
	ZmqEnvelope() ([]string, error)
	// Fail creates a new Reply as a failure
	// It accepts the error message that explains the reason of the failure.
	Fail(message string) ReplyInterface
	Ok(parameters datatype.KeyValue) ReplyInterface
	CommandName() string
	RouteParameters() datatype.KeyValue
}

type ReplyInterface interface {
	ConId() string
	SetConId(string)
	// IsOK returns the Status of the message.
	IsOK() bool
	// String converts the Reply to the string format. Empty if occurred an error.
	// It implements Stringer interface from a standard library
	String() string
	// ZmqEnvelope converts the message to the zeromq envelope
	ZmqEnvelope() ([]string, error)
	ErrorMessage() string
	ReplyParameters() datatype.KeyValue
}
