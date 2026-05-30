# handler-lib

Route ZeroMQ messages to your Go functions. Part of the [SDS Framework](https://github.com/sds-framework).

Handlers expose a **command-based API**: clients send a command name and parameters; handler-lib dispatches to the matching route and returns a reply.

For architecture, socket layout, and contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Requirements

- Go 1.19+
- ZeroMQ 4.x (`libzmq3-dev` on Debian/Ubuntu)
- SDS modules: [client-lib](https://github.com/noPerfection/protocol/client), [datatype-lib](https://github.com/noPerfection/datatype), [log-lib](https://github.com/noPerfection/log)

## Installation

```bash
go get github.com/noPerfection/protocol/handler@latest
```

## Handler types

Pick the handler that matches your concurrency model:

| Package | Type | Use when |
|---------|------|----------|
| [`sync_replier`](sync_replier) | `SyncReplier` | One request at a time per handler; extra requests wait in queue |
| [`replier`](replier) | `Replier` | Many concurrent request/reply clients; handlers run asynchronously behind a ROUTER socket |
| [`publisher`](publisher) | `Publisher` | Broadcast to subscribers; separate **trigger** endpoint to publish |
| [`worker`](worker) | `Worker` | Consume fire-and-forget messages without replying to the caller (PULL) |

All handlers implement [`base.Interface`](base/interface.go): `SetConfig`, `SetLogger`, `Route`, `Start`, etc.

## Quick start

Minimal **SyncReplier** — one handler, in-process endpoint:

```go
package main

import (
	"log"

	"github.com/noPerfection/datatype/data_type/key_value"
	"github.com/noPerfection/protocol/message"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/sync_replier"
	loglib "github.com/noPerfection/log"
)

func main() {
	handler := sync_replier.New()

	handlerConfig := config.New(config.SyncReplierType, "my_service", "my_service", 0)
	handler.SetConfig(handlerConfig)

	logger, err := loglib.New("my_service", true)
	if err != nil {
		log.Fatal(err)
	}
	if err := handler.SetLogger(logger); err != nil {
		log.Fatal(err)
	}

	err = handler.Route("hello", func(req message.RequestInterface) message.ReplyInterface {
		name, _ := req.RouteParameters().StringValue("name")
		return req.Ok(key_value.New().Set("greeting", "Hello, "+name))
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := handler.Start(); err != nil {
		log.Fatal(err)
	}

	// Handler runs until closed via manager_client or manager socket
	select {}
}
```

Send requests with [client-lib](https://github.com/noPerfection/protocol/client) (see [Tutorial: Call a handler](#tutorial-call-a-handler-from-a-client)).

---

## Tutorials

### Tutorial: Your first route

1. Create a handler (`sync_replier.New()` or `replier.New()`).
2. Build config with `config.New` (`port = 0` for in-process, non-zero for TCP).
3. `SetConfig` → `SetLogger` → register routes → `Start()`.

**Route handler signature** (`base.HandleFunc`):

```go
func(req message.RequestInterface) message.ReplyInterface
```

**Success / failure replies:**

```go
return req.Ok(key_value.New().Set("key", "value"))
return req.Fail("something went wrong")
```

**Register a route:**

```go
err := handler.Route("my_command", myHandler)
```

---

### Tutorial: Configuration and endpoints

`config.Handler` fields:

| Field | Description |
|-------|-------------|
| `Endpoint.Id` | Endpoint identity (used in ZMQ URL) |
| `Endpoint.Port` | `0` = local (inproc or ipc); non-zero = TCP |
| `Type` | Set automatically by `sync_replier`, `replier`, etc. |
| `Category` | Logical grouping |

**Helpers:**

```go
// In-process (same process), id "my_service" -> inproc://my_service
cfg := config.New(config.SyncReplierType, "my_service", "my_service", 0)

// TCP on an explicit port. Local IDs bind on all interfaces.
cfg := config.New(config.ReplierType, "localhost", "api", 5555)

bindURL := cfg.HandlerUrl()
clientURL := cfg.ClientUrl()
```

**Handler URL** (`Endpoint.HandlerUrl`) — where the handler **binds**:

| Port | Id | Bind URL |
|------|-----|----------|
| non-zero | `localhost` or `127.0.0.*` | `tcp://*:{Port}` |
| non-zero | anything else | `tcp://{Id}:{Port}` |
| 0 | starts with `tmp` | `ipc:///{Id}` (filesystem socket) |
| 0 | otherwise | `inproc://{Id}` |

Clients use the same `Id` and `Port` with `Endpoint.ClientUrl` (`tcp://{Id}:{Port}` for TCP).

Control endpoints are created by `control.CreateInternalConfig(handlerCfg)`. Internal control ids use the `{Id}_control` suffix with category `"control"`.

**IPC example** — use an id under `/tmp/`:

```go
cfg := config.New(config.SyncReplierType, "tmp/my-worker.sock", "worker", 0)
```

Remove the socket file when the handler stops.

---

### Tutorial: Call a handler from a client

After `handler.Start()`, connect with **client-lib**:

```go
import (
	"fmt"

	"github.com/pebbe/zmq4"
	"github.com/noPerfection/protocol/client"
	clientConfig "github.com/noPerfection/protocol/client/config"
	"github.com/noPerfection/datatype/data_type/key_value"
	"github.com/noPerfection/protocol/message"
	"github.com/noPerfection/protocol/handler/config"
)

func callHello(handlerCfg *config.Handler) error {
	cfg := clientConfig.New(
		"github.com/sds-framework/my-service",
		handlerCfg.Id,
		handlerCfg.Port,
		zmq.REQ, // client side of REQ/REP (SyncReplier, Replier)
	)
	cfg.UrlFunc(clientConfig.Url)

	sock, err := client.New(cfg)
	if err != nil {
		return err
	}

	req := message.Request{
		Command:    "hello",
		Parameters: key_value.New().Set("name", "world"),
	}
	reply, err := sock.Request(&req)
	if err != nil {
		return err
	}
	if !reply.IsOK() {
		return fmt.Errorf(reply.ErrorMessage())
	}
	greeting, _ := reply.ReplyParameters().StringValue("greeting")
	fmt.Println(greeting)
	return nil
}
```

Use the same `Id` and `Port` as the handler config so bind and connect URLs match.

---

### Tutorial: Replier (concurrent requests)

`replier` binds a ZMQ ROUTER socket and accepts many REQ clients. Incoming requests are read by the socket loop, then route handlers run asynchronously so slow work in one request does not block the handler from receiving later requests.

Replies are routed back to the original client by the ZMQ ROUTER identity frame. `message.NewReq` stores that connection id, and `req.Ok(...)` / `req.Fail(...)` copy it into the reply envelope.

```go
handler := replier.New()

cfg := config.New(config.ReplierType, "localhost", "api", 5555)
handler.SetConfig(cfg)
// SetLogger, Route, Start — same lifecycle as SyncReplier

if err := handler.Start(); err != nil {
	log.Fatal(err)
}
```

Use `replier` when every request needs a response:

```go
err := handler.Route("profile", func(req message.RequestInterface) message.ReplyInterface {
	userID, _ := req.RouteParameters().StringValue("user_id")
	profile := loadProfile(userID)
	return req.Ok(key_value.New().Set("profile", profile))
})
```

---

### Tutorial: Worker (fire-and-forget requests)

`worker` binds a ZMQ PULL socket. Clients send messages with PUSH and do not wait for a reply. The worker reads messages from the socket loop and runs route handlers asynchronously, so processing time does not depend on the number of connected clients.

```go
handler := worker.New()

cfg := config.New(config.WorkerType, "localhost", "jobs", 5556)
handler.SetConfig(cfg)
// SetLogger, Route, Start — same lifecycle as other handlers

err := handler.Route("index_document", func(req message.RequestInterface) message.ReplyInterface {
	documentID, _ := req.RouteParameters().StringValue("document_id")
	indexDocument(documentID)
	return req.Ok(key_value.New())
})
```

Use `worker` when the caller only needs to submit work and does not need a response.

---

### Stress tests

The replier and worker packages include stress tests that create many client sockets and simulate internal work per message. The replier stress test uses `50ms` of handler work by default and keeps each client active by sending five request/reply transactions as fast as possible.

Run the default stress tests:

```bash
go test ./handler/replier -run TestStressTest -v
go test ./handler/worker -run TestStressTest -v
```

The replier stress test runs 1,000 clients with 5 requests each by default. It also has opt-in larger scenarios:

- `10,000` clients with `100` requests each (`1,000,000` total requests)
- `100,000` clients with `5` requests each (`500,000` total requests)

```bash
REPLIER_STRESS_LARGE=1 go test ./handler/replier -run TestStressTest -v -timeout 25m
```

For more than 1,000 clients, the test uses a **512-worker pool** so only that many goroutines block in ZMQ at once (Go’s default ~10k thread limit). All client sockets are still created; workers drain a job queue.

Set `REPLIER_HANDLE_TIME` to change the simulated handler delay in milliseconds. Accepted values are `1` through `2000`; the default is `50`.

```bash
REPLIER_HANDLE_TIME=250 go test ./handler/replier -run TestStressTest -v
```

Use `-v` to print timing output such as client count, requests per client, total request count, connect time, total request time, max reply time, and throughput.

Example benchmark results with the default handler delay. In this variant, each handler call simulates `50ms` of work per request.

```bash
ulimit -n 200000
REPLIER_STRESS_LARGE=1 go test ./handler/replier -run TestStressTest -v -timeout 25m
```

| Clients | Requests per client | Total requests | Throughput | Duration | Result |
|---------|---------------------|----------------|------------|----------|--------|
| `1,000` | `5` | `5,000` | `8,221.33 req/s` | `608ms` | Pass |
| `10,000` | `100` | `1,000,000` | `8,347.28 req/s` | `1m59.8s` | Pass |
| `100,000` | `5` | `500,000` | `9,010.06 req/s` | `55.493s` | Pass |

Example benchmark results with the handler delay reduced to the minimum `1ms` per request:

```bash
ulimit -n 200000
REPLIER_STRESS_LARGE=1 REPLIER_HANDLE_TIME=1 go test ./handler/replier -run TestStressTest -v -timeout 25m
```

| Clients | Requests per client | Total requests | Throughput | Duration | Result |
|---------|---------------------|----------------|------------|----------|--------|
| `1,000` | `5` | `5,000` | `11,527.30 req/s` | `434ms` | Pass |
| `10,000` | `100` | `1,000,000` | `12,589.71 req/s` | `1m19.43s` | Pass |
| `100,000` | `5` | `500,000` | `12,511.81 req/s` | `39.962s` | Pass |

Large client counts require one ZMQ socket per simulated client. If you start handling a large amount of clients or requests, increase the process file descriptor limit first; otherwise the OS can reject new sockets with `too many open files`. Two limits apply:

1. **ZeroMQ** — the default context allows **1024 sockets** (`ZMQ_MAX_SOCKETS`). The stress test raises this in `TestMain`; production code can call `zmq.SetMaxSockets(n)` before creating sockets.
2. **OS** — each socket also uses file descriptors. The default `ulimit -n` is often 1024 or 4096, so 10k/100k subtests **skip** with a hint if the soft limit is too low.

Raise the OS limit before running large cases:

```bash
ulimit -n 200000
REPLIER_STRESS_LARGE=1 go test ./handler/replier -run TestStressTest -v -timeout 25m
```

To attempt large runs without the pre-check (you may still hit `too many open files`):

```bash
REPLIER_STRESS_IGNORE_FDLIMIT=1 REPLIER_STRESS_LARGE=1 go test ./handler/replier -run TestStressTest -v
```

---

### Tutorial: Publisher (broadcast + manager)

A **Publisher** has:

- **Broadcast socket** — subscribers connect (SUB)
- **Manager socket** — control and broadcast via the `broadcast` command

```go
import (
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/handler/publisher"
)

pub := publisher.New()

pubCfg := config.New(config.PublisherType, "events", "events", 0)

pub.SetConfig(pubCfg)
pub.SetLogger(logger)
pub.Start()

managerCfg := control.CreateInternalConfig(pubCfg)
manager, _ := client.NewRaw(zmq.REQ, managerCfg.ClientUrl())

req := message.Request{Command: publisher.Broadcast, Parameters: key_value.New().Set("event", "updated")}
reply, err := manager.Request(&req)
```

Manager commands: `start` and `close` control the broadcaster; `message-amount` returns `broadcasting_length`; `status`, `config`, and `broadcast` behave like other handlers.

Subscribers connect to `pubCfg.ClientUrl()` with a ZMQ SUB socket (or use `client-lib` with SUB).

---

### Tutorial: Manage a running handler

Use [`manager_client`](manager_client) from your service process:

```go
mc, err := manager_client.New(handlerConfig)
if err != nil {
	log.Fatal(err)
}

status, err := mc.HandlerStatus()
err = mc.Close()
```

Common manager routes are `status`, `config`, `start`, and `close`.

---

### Tutorial: HTTP or custom protocols

To expose a handler over HTTP or another protocol, add a **pair** handler layer. See [CONTRIBUTING.md — Pair layer](CONTRIBUTING.md#pair-layer) and [web-lib](https://github.com/sds-framework/web-lib).

---

## Typical lifecycle

```
New() → SetConfig() → SetLogger() → Route(...) → Start()
```

| Step | Notes |
|------|--------|
| `SetConfig` | Required before `SetLogger` |
| `SetLogger` | Child logger per handler id |
| `Route` | Command name must be unique |
| `Start` | Binds the handler socket, starts the control manager, and returns when sockets are ready |

Check health: `handler.Status()` or `manager_client.HandlerStatus()` returns values like `ready`, `idle`, or `nil`.

## Project layout

| Path | Purpose |
|------|---------|
| `sync_replier/`, `replier/`, `publisher/`, `worker/` | Handler implementations |
| `config/` | Handler config and URL helpers |
| `route/` | Route types and dispatch |
| `manager_client/` | Service-side control client |
| `pair/` | External protocol pairing |

## License

See [LICENSE](LICENSE).
