# protocol/handler

Route ZeroMQ messages to Go functions. Handlers expose a command-based API: a client sends a command name plus parameters, and the handler dispatches to the matching route.

## Requirements

- Go 1.19+
- ZeroMQ 4.x (`libzmq3-dev` on Debian/Ubuntu)

## Installation

```bash
go get github.com/noPerfection/protocol/handler@latest
```

## Handler Types

| Package | Type | Use when |
|---------|------|----------|
| [`sync_replier`](sync_replier) | `SyncReplier` | One request at a time per handler; extra requests wait in queue |
| [`replier`](replier) | `Replier` | Many concurrent request/reply clients; handlers run asynchronously behind a ROUTER socket |
| [`publisher`](publisher) | `Publisher` | Broadcast to subscribers |
| [`worker`](worker) | `Worker` | Consume fire-and-forget messages without replying to the caller |
| [`pair`](pair) | `Pair` | Bridge another protocol or in-process component through a PAIR socket |

All handlers implement [`base.Interface`](base/interface.go): `SetConfig`, `SetLogger`, `Route`, `Start`, and related lifecycle methods.

## Quick Start

Minimal `SyncReplier` using an in-process endpoint:

```go
package main

import (
	"log"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/sync_replier"
	"github.com/noPerfection/protocol/message"
)

func main() {
	handler := sync_replier.New()
	handler.SetConfig(config.New(config.SyncReplierType, "my_service", "my_service", 0))

	err := handler.Route("hello", func(req message.RequestInterface) message.ReplyInterface {
		name, _ := req.RouteParameters().StringValue("name")
		return req.Ok(datatype.New().Set("greeting", "Hello, "+name))
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := handler.Start(); err != nil {
		log.Fatal(err)
	}

	select {}
}
```

### Optional Logs

Handlers run without a logger. To enable logs, create one and pass it with `SetLogger` before or after `SetConfig`:

```go
import loglib "github.com/noPerfection/log"

logger, err := loglib.New("my_service", true)
if err != nil {
	return err
}
if err := handler.SetLogger(logger); err != nil {
	return err
}
```

When no logger is configured, internal log messages are omitted.

### Message Packers

Handlers use a default `message.MessagePacker` to serialize requests and replies. Use `Packer()` to inspect the current packer and `SetPacker(...)` to install a custom implementation; see [github.com/noPerfection/protocol/message](https://github.com/noPerfection/protocol/tree/main/message) for the `message.Packer` interface and examples.

## Handler Lifecycle

The lifecycle starts with choosing the handler behavior, then preparing it before sockets are bound:

```go
myHandler := handlerType.New()
myConfig := config.New(...)

myHandler.SetConfig(myConfig) -> myHandler.SetLogger(...) -> myHandler.Route(...) -> myHandler.Start()
```

`handlerType.New()` creates the handler implementation you want to run. For example, `sync_replier.New()` creates a serial request/reply handler, while `replier.New()` creates an asynchronous request/reply handler.

`config.New()` creates the handler config: type, endpoint id, category, and port. The config decides where the handler binds and which ZeroMQ socket type it uses.

`SetConfig` attaches that config to the handler. This should happen before `Start`, because the handler needs the endpoint and type before it can bind sockets.

`SetLogger` is optional. Pass a logger from [noPerfection/log](https://github.com/noPerfection/log) when you want internal logs; if you skip it, the handler simply omits log messages.

`Route` registers command handlers. A route maps a command string of your choice to a `base.HandleFunc`, which receives a request and returns a reply. `Publisher` is the exception: it broadcasts data and does not dispatch client commands through routes.

`Start` binds the handler socket, starts its controller, and begins routing incoming messages to the registered functions.

## Controllers

Each handler exposes its controller through the exported `Control` field. The controller is itself a handler and exposes routes such as `status`, `config`, `start`, and `close`.

All controls use the special category `control` (`control.ControlCategory`). This lets the [noPerfection/service](https://github.com/noPerfection/service) module find another service's control handler and manage its handlers through it. In normal handler code, you do not have to use the control handler directly.

By default, `SetConfig` configures `Control` with `control.CreateInternalConfig(yourHandlerConfig)`. If you want the controller somewhere else, call `yourHandler.Control.SetConfig(...)` after `SetConfig` and before `Start`.

## Configuration

Create handler config with `config.New(type, id, category, port)`.

```go
cfg := config.New(config.ReplierType, "localhost", "api", 5555)

bindURL := cfg.HandlerUrl()
clientURL := cfg.ClientUrl()
```

`port = 0` creates a local endpoint: `inproc://{Id}` by default, or `ipc:///{Id}` when `Id` starts with `tmp`. Non-zero ports use TCP.

Control endpoints are created with `control.CreateInternalConfig(handlerCfg)`. Common manager routes are `status`, `config`, `start`, and `close`.

## Stress Tests

The `replier` and `worker` packages include stress tests:

```bash
go test ./handler/replier -run TestStressTest -v
go test ./handler/worker -run TestStressTest -v
```

Large replier scenarios are opt-in:

```bash
REPLIER_STRESS_LARGE=1 go test ./handler/replier -run TestStressTest -v -timeout 25m
```

Example benchmark results with the default `50ms` handler delay:

| Clients | Requests per client | Total requests | Throughput | Duration | Result |
|---------|---------------------|----------------|------------|----------|--------|
| `1,000` | `5` | `5,000` | `8,221.33 req/s` | `608ms` | Pass |
| `10,000` | `100` | `1,000,000` | `8,347.28 req/s` | `1m59.8s` | Pass |
| `100,000` | `5` | `500,000` | `9,010.06 req/s` | `55.493s` | Pass |

Example benchmark results with `REPLIER_HANDLE_TIME=1`:

| Clients | Requests per client | Total requests | Throughput | Duration | Result |
|---------|---------------------|----------------|------------|----------|--------|
| `1,000` | `5` | `5,000` | `11,527.30 req/s` | `434ms` | Pass |
| `10,000` | `100` | `1,000,000` | `12,589.71 req/s` | `1m19.43s` | Pass |
| `100,000` | `5` | `500,000` | `12,511.81 req/s` | `39.962s` | Pass |

Large client counts may require a higher file descriptor limit, for example `ulimit -n 200000`.

## License

See [LICENSE](LICENSE).
