# Protocol/message

`Protocol/message` is part of the noPerfection framework. It defines the message transport used between noPerfection services, carried by `protocol/client` and `protocol/handler` using zeromq library.

It has two built-in message families:

- `Message` &ndash; the default messages noPerfection uses in production. `protocol/client` and `protocol/handler` work with these through `MessagePacker`. A request carries a command and parameters; a reply carries status, message, and parameters.
- `Raw`: an example of a custom message. It's just a thin wrapper over the transport zeromq, but handles the message id used by asynchronous requests. So if you want to manage message ids, without handling string manipulations, use this.

There are two message types: requests and replies. Requests used by clients, and replies used by handlers.

## Architecture

The module has three layers:

1. ZeroMQ utilities
2. Packer
3. Message types

. Its defined by the protocol and noPerfection follows it. The utility functions that  passed to and from sockets. A packer deserializes those envelope parts into a `RequestInterface` or `ReplyInterface`, and serializes message interfaces back into envelope parts.

noPerfection wires in `MessagePacker` with `Request` / `Reply`. `RawPacker` with `RawRequest` / `RawReply` is the reference extension: same `Packer` interface, different message types and envelope rules.

## Endpoints

`Endpoint` describes where a ZeroMQ socket should bind or connect. It has two fields:

```go
type Endpoint struct {
	Id   string
	Port uint64
}
```

Endpoints are used in two ways:

- As a handler endpoint with `HandlerUrl()`. This is the URL a handler binds.
- As a client endpoint with `ClientUrl()`. This is the URL a client connects to.

The endpoint chooses the ZeroMQ transport from `Id` and `Port`:

- `Port == 0` and `Id` starts with `tmp/`: use Unix IPC with `ipc:///<id>`. This is for processes communicating through a filesystem IPC path.
- `Port == 0` and any other `Id`: use in-process transport with `inproc://<id>`. This is for internal goroutines or threads.
- `Port != 0`: use TCP. In this case `Id` is the domain or IP address and `Port` is the TCP port.

For TCP endpoints, local handlers and clients use different URLs. If `Id` is empty, `localhost`, or starts with `127.0.0.`, `HandlerUrl()` binds to `tcp://*:<port>`, while `ClientUrl()` connects to `tcp://localhost:<port>`. For any other `Id`, both handler and client URLs use `tcp://<id>:<port>`.

## ZeroMQ Envelopes
ZeroMQ envelopes are defined by the zeromq library as a series of strings: `[]string`. Message module is based on it. To interact with them, you may need to check its official protocol specification.
Otherwise if you don't want to handle with it simply use the `message.go` package.

Most users do not need to dig into the zmq protocl. Use the helper API in `message.go` when working with envelopes:

- `ValidateEnvelope([]string) error` detects envelopes is following zmq protocol or not.
- `MessageToEnvelope(conId string, message string, tail ...string) []string` returns zmq envelope.
- `EnvelopeToMessage([]string) (conId string, message string, tail []string)` returns `conId`, the first message body frame, and tail frames.

## Packer API

A packer implements:

```go
type Packer interface {
	DeserializeRequest(zmqEnvelope []string) (RequestInterface, error)
	DeserializeReply(zmqEnvelope []string) (ReplyInterface, error)
	SerializeRequest(request RequestInterface) ([]string, error)
	SerializeReply(reply ReplyInterface) ([]string, error)
	EmptyRequest() RequestInterface
	EmptyReply() ReplyInterface
}
```

To extend the protocol, implement your own packer. It receives ZeroMQ envelopes and must return your request/reply message types. Your custom messages only need to implement `message.RequestInterface` and `message.ReplyInterface`.

## Custom Messages

Custom data such as tracing, authentication, signatures, or routing metadata can live outside the core request/reply interfaces. Add it to your own message types or keep it in a separate file, as tracing does with `trace.go` and `raw_trace.go`.

After that, define your own ZeroMQ envelope layout, implement a packer that understands it, fetch the messages, and handle them however your service needs.
