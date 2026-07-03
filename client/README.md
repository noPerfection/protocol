# protocol/client

Thread-safe ZeroMQ clients for connecting to [github.com/noPerfection/protocol/handler](https://github.com/noPerfection/protocol/tree/main/handler) handlers.

## Requirements

- Go 1.19 or newer.
- ZeroMQ/libzmq available for [github.com/pebbe/zmq4](https://github.com/pebbe/zmq4).

## Installation

```sh
go get github.com/noPerfection/protocol/client
```

## Short Tutorial

This tutorial assumes you have familiriaty with the `noPerfection` framework and its terminology.
Such as client and handler.

Connect to a `SyncReplier` handler with `client/sync_replier`:

```go
import "github.com/noPerfection/protocol/client/sync_replier"

reqSync, err := sync_replier.NewClient("localhost", 3000)
if err != nil {
	return err
}
defer reqSync.Close()

reply, err := reqSync.Request(request)
```

## Client Types

Clients are divided per package by handler name. Depending on the handler type, a client exposes only the supported interfaces: `SendInterface` for fire-and-forget messages, `RequestInterface` for request-reply messages, and `ReceiveInterface` for receiving handler replies.

Handler types are defined in the README for [github.com/noPerfection/protocol/handler](https://github.com/noPerfection/protocol/tree/main/handler).

| Handler's Client | Client's ZMQ socket | Send Interface | Request Interface | Receive Interface |
|------------------|---------------------|----------------|-------------------|-------------------|
| `sync_replier.Client` | `zmq.REQ` | | ✓ | |
| `replier.Client` | `zmq.DEALER` | ✓ | | ✓ |
| `worker.Client` | `zmq.PUSH` | ✓ | | |
| `publisher.Client` | `zmq.SUB` | | | ✓ |
| `pair.Client` | `zmq.PAIR` | ✓ | | ✓ |

## Client Cycle

First instantiate a new client with `client/<handler_name>.NewClient(id, port)`. The `id` and `port` values are based on [protocol/message](https://github.com/noPerfection/protocol/tree/main/message) endpoint rules. Socket endpoint setup is described in the README for [github.com/noPerfection/protocol/message](https://github.com/noPerfection/protocol/tree/main/message).

```go
import (
	"github.com/noPerfection/protocol/client/pair"
	"github.com/noPerfection/protocol/client/publisher"
	"github.com/noPerfection/protocol/client/replier"
	"github.com/noPerfection/protocol/client/sync_replier"
	"github.com/noPerfection/protocol/client/worker"
)

id := "localhost"
port := uint64(3000)

p, err := pair.NewClient(id, port)
subscriber, err := publisher.NewClient(id, port)
req, err := replier.NewClient(id, port)
reqSync, err := sync_replier.NewClient(id, port)
pusher, err := worker.NewClient(id, port)
```

Before transmitting, set options if needed:

- `Packer` changes message serialization. If the handler uses a custom packer, use the same packer here.
- `Whitelist` registers a shared secret for a command on the target handler. When set, the client signs outbound requests and verifies signed replies.
- `Timeout` changes how long one attempt can wait. The minimum timeout is 2ms.
- `Attempt` changes how many retries are allowed. `0` retries forever.

```go
subscriber.Packer(yamlFormat)
subscriber.Timeout(time.Second)
subscriber.Attempt(0)

reqSync.Timeout(time.Second * 30)
reqSync.Whitelist("charge", "billing-secret")
reqSync.Whitelist(client.Any, "global-secret") // all commands when no command-specific entry exists
```

Then check the table above and call the available interface:

```go
// SendInterface
err := pusher.Send(request)

// RequestInterface
reply, err := reqSync.Request(request)

// ReceiveInterface
for reply := range subscriber.Receive() {
	_ = reply
}
```

`Send` is intentionally fire-and-forget. A nil error means the client accepted or wrote the message, not that the handler processed it.

## HMAC Whitelisting

When the target handler whitelists a command, configure the same secret on the client before `Send` or `Request`:

```go
import "github.com/noPerfection/protocol/client"

reqSync, _ := sync_replier.NewClient("billing", 0)
reqSync.Whitelist("charge", "billing-secret")

reply, err := reqSync.Request(&message.Request{
	Command:    "charge",
	Parameters: datatype.New().Set("amount", 100),
})
```

Behavior:

- If the command is whitelisted on the client, `Send` / `Request` sign the JSON body with HMAC-SHA256 and attach the hash as the first envelope tail frame.
- If the command is not whitelisted on the client, messages are sent without an HMAC tail (unchanged behavior).
- On `Request`, when the command is whitelisted and the reply includes an HMAC tail, the client verifies it with the same secret.
- On `Receive` (replier, pair, publisher), reply HMAC is verified when `client.Any` (`"*"`) is whitelisted and the reply carries an HMAC frame.

When a handler calls another handler internally, sign with the **downstream** handler's secret for the command you send. Do not forward the inbound HMAC from the external caller.

See [protocol/test/hmac_test.go](../test/hmac_test.go) for client/handler integration tests.

## CURVE Encryption

When the target handler is secured with CURVE (see [protocol/handler](../handler)), configure the client with the handler's Z85 CURVE server public key using `Secure`. An empty key keeps the client non-secure:

```go
import "github.com/noPerfection/protocol/handler/base"

serverPublic, serverSecret, _ := base.GenerateCurveKey()

reqSync, _ := sync_replier.NewClient("billing", 6000)
reqSync.Secure(serverPublic)

// Optional: pass a fixed client secret key instead of generating one per reconnect.
clientPublic, clientSecret, _ := base.GenerateCurveKey()
reqSync.Secure(serverPublic, clientSecret)
```

Behavior:

- CURVE is applied only when the endpoint is not `inproc`; `inproc` endpoints skip it.
- On secure connections, the client connects with `SetCurveServerkey` using the configured server public key.
- When a client secret key is provided, its public key is derived with `AuthCurvePublic` and reused on every reconnect.
- When no client secret key is provided, an ephemeral client keypair is generated on each reconnect.
- CURVE is independent of HMAC whitelisting; you can use either, both, or neither.

Clients are thread-safe, so one client can be called from multiple goroutines:

```go
for i := 0; i < 5; i++ {
	go func(loopIndex int) {
		_ = pusher.Send(requestFrom(loopIndex))
	}(i)
}
```

## Controls

Each handler package also exposes a control client. Use it when you need to inspect or manage a running handler:

```go
control, err := client/<handler_name>.NewControl(id, port)
```

For example:

```go
control, err := sync_replier.NewControl(id, port)
status, err := control.HandlerStatus()
config, err := control.HandlerConfig()
status, err = control.StartHandler()
err = control.HandlerClose()
```

The control endpoint uses the handler control address, not the handler's message endpoint. Derive it with `control.NewInternalControlEndpoint(message.NewEndpoint(handlerId, handlerPort))` and pass the returned `Id` to `NewControl` with port `0` for inproc (for example `localhost0_control` for `localhost` on port `0`). Check the control type in each `client/<handler_name>` package to see the exact interface. `pair.Control` and `publisher.Control` also expose `Broadcast(message.Reply)`.

## Limitations

- Client operations are queued internally. The current dispatcher queue is small, so bursts of many simultaneous `Send` or `Request` calls can return `queue is full, try again later`.
- `Receive` clients close their receive channel automatically when nothing arrives for `Attempt` consecutive idle periods, each lasting `Timeout`. Set `Attempt(0)` to retry forever. Receiving a message resets the idle counter. Call `Receive()` again on a new client after the channel closes.
- `replier.Client` and `pair.Client` receive paths are not polished under stress yet. Stress tests show receive channels can close after only a few accepted messages during heavy concurrent send pressure.
- `pair.Client` is sensitive to PAIR socket timing. Start `Receive()` and allow the connection to settle before sending, especially in tests.
## Maintenance Memo

This is a memo for myself if I need to change the code after a few months of pause.

The main file is `client.go` at the root. It sets the `Socket` struct, but it is not intended to be used directly by users. Users should use the handler packages that derive from the client. Each handler package ensures its interface and adjusts the client config for that handler.

The client itself is advanced. It depends on two ZeroMQ algorithms for thread-safety:

- The reactor is used for the event-based queue. All messages are queued, and the dispatcher consumes the queue on a timer.
- The `zmq.Poller` is used for timeout handling and queue/socket readiness.

Receiving is exposed separately because a receiving client is the exception rather than the norm. That is why receiving is not part of the main send/request file.
