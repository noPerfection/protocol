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
- `Timeout` changes how long one attempt can wait. The minimum timeout is 2ms.
- `Attempt` changes how many retries are allowed. `0` retries forever.

```go
subscriber.Packer(yamlFormat)
subscriber.Timeout(time.Second)
subscriber.Attempt(0)

reqSync.Timeout(time.Second * 30)
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

Clients are thread-safe, so one client can be called from multiple goroutines:

```go
for i := 0; i < 5; i++ {
	go func(loopIndex int) {
		_ = pusher.Send(requestFrom(loopIndex))
	}(i)
}
```

## Maintenance Memo

This is a memo for myself if I need to change the code after a few months of pause.

The main file is `client.go` at the root. It sets the `Socket` struct, but it is not intended to be used directly by users. Users should use the handler packages that derive from the client. Each handler package ensures its interface and adjusts the client config for that handler.

The client itself is advanced. It depends on two ZeroMQ algorithms for thread-safety:

- The reactor is used for the event-based queue. All messages are queued, and the dispatcher consumes the queue on a timer.
- The `zmq.Poller` is used for timeout handling and queue/socket readiness.

Receiving is exposed separately because a receiving client is the exception rather than the norm. That is why receiving is not part of the main send/request file.
