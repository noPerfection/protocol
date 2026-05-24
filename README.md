# handler-lib

Route ZeroMQ messages to your Go functions. Part of the [SDS Framework](https://github.com/sds-framework).

Handlers expose a **command-based API**: clients send a command name and parameters; handler-lib dispatches to the matching route and returns a reply.

For architecture, socket layout, and contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Requirements

- Go 1.19+
- ZeroMQ 4.x (`libzmq3-dev` on Debian/Ubuntu)
- SDS modules: [client-lib](https://github.com/sds-framework/client-lib), [datatype-lib](https://github.com/sds-framework/datatype-lib), [log-lib](https://github.com/sds-framework/log-lib)

## Installation

```bash
go get github.com/sds-framework/handler-lib@latest
```

## Handler types

Pick the handler that matches your concurrency model:

| Package | Type | Use when |
|---------|------|----------|
| [`sync_replier`](sync_replier) | `SyncReplier` | One request at a time per handler; extra requests wait in queue |
| [`replier`](replier) | `Replier` | Many concurrent clients; scales instances up to CPU count |
| [`publisher`](publisher) | `Publisher` | Broadcast to subscribers; separate **trigger** endpoint to publish |
| [`sdsin`](sdsin) | `Publisher` | Send `io.Writer` output into SDS |
| [`sdsout`](sdsout) | Subscriber | Receive SDSIn output into an `io.Writer` |
| [`worker`](worker) | `Worker` | Consume messages without replying to the caller (PULL) |

All handlers implement [`base.Interface`](base/interface.go): `SetConfig`, `SetLogger`, `Route`, `Start`, etc.

## Quick start

Minimal **SyncReplier** — one instance, in-process endpoint:

```go
package main

import (
	"log"

	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/datatype-lib/message"
	"github.com/sds-framework/handler-lib/config"
	"github.com/sds-framework/handler-lib/sync_replier"
	loglib "github.com/sds-framework/log-lib"
)

func main() {
	handler := sync_replier.New()

	handlerConfig := config.NewInternalHandler(config.SyncReplierType, "my_service", "my_service")
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

Send requests with [client-lib](https://github.com/sds-framework/client-lib) (see [Tutorial: Call a handler](#tutorial-call-a-handler-from-a-client)).

---

## Tutorials

### Tutorial: Your first route

1. Create a handler (`sync_replier.New()` or `replier.New()`).
2. Build config (`config.NewInternalHandler` for in-process, or `config.NewHandler` for TCP).
3. `SetConfig` → `SetLogger` → register routes → `Start()`.

**Route handler signature** (`route` package):

```go
// No dependencies
func(req message.RequestInterface) message.ReplyInterface

// With one or more service dependencies (see Tutorial: Dependencies)
func(req message.RequestInterface, dep *client.Socket) message.ReplyInterface
```

**Success / failure replies:**

```go
return req.Ok(key_value.New().Set("key", "value"))
return req.Fail("something went wrong")
```

**Register a route:**

```go
err := handler.Route("my_command", myHandler)
// With dependencies:
err := handler.Route("other_command", myHandler, "dep_service_id")
```

---

### Tutorial: Configuration and endpoints

`config.Handler` fields:

| Field | Description |
|-------|-------------|
| `Id` | Endpoint identity (used in ZMQ URL) |
| `Port` | `0` = local (inproc or ipc); non-zero = TCP |
| `Type` | Set automatically by `sync_replier`, `replier`, etc. |
| `Category` | Logical grouping |
| `InstanceAmount` | Hint for instance count (service may manage instances) |
| `ManagerId` | Manager endpoint identity |
| `ManagerPort` | `0` = local manager; non-zero = TCP manager |

**Helpers:**

```go
// In-process (same process), id "my_service" -> inproc://my_service
cfg := config.NewInternalHandler(config.SyncReplierType, "my_service", "my_service")

// TCP on an explicit port. Local IDs bind on all interfaces.
cfg := config.NewHandler(config.ReplierType, "localhost", "api", 5555)

// Manual config
cfg := &config.Handler{
	Type:     config.ReplierType,
	Category: "api",
	Id:       "api_1",
	Port:     5555,
}
```

**External URL** (`config.ExternalUrl`) — where the handler **binds**:

| Port | Id | Bind URL |
|------|-----|----------|
| non-zero | `localhost` or `127.0.0.*` | `tcp://*:{Port}` |
| non-zero | anything else | `tcp://{Id}:{Port}` |
| 0 | starts with `tmp` | `ipc:///{Id}` (filesystem socket) |
| 0 | otherwise | `inproc://{Id}` |

Clients use the same `Id` and `Port` with `config.ConnectUrl` (`tcp://{Id}:{Port}` for TCP).

Manager endpoints use the same URL rules through `handlerCfg.ManagerExternalUrl()` and `handlerCfg.ManagerConnectUrl()`. By default constructors set the manager to `inproc://manager_{Id}` with category `config.ManagerCategory` (`"handler_manager"`); set `ManagerId` and `ManagerPort` when the manager must be reachable outside the process.

**IPC example** — use an id under `/tmp/`:

```go
cfg := &config.Handler{
	Type:     config.SyncReplierType,
	Category: "worker",
	Id:       "tmp/my-worker.sock", // binds ipc:///tmp/my-worker.sock
	Port:     0,
}
```

Remove the socket file when the handler stops.

---

### Tutorial: Call a handler from a client

After `handler.Start()`, connect with **client-lib**:

```go
import (
	"fmt"

	"github.com/pebbe/zmq4"
	"github.com/sds-framework/client-lib"
	clientConfig "github.com/sds-framework/client-lib/config"
	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/datatype-lib/message"
	"github.com/sds-framework/handler-lib/config"
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

`replier` allows multiple instances (up to `runtime.NumCPU()`). The service or manager can add instances with the `add-instance` command.

```go
handler := replier.New()

cfg := config.NewHandler(config.ReplierType, "localhost", "api", 5555)
handler.SetConfig(cfg)
// SetLogger, Route, Start — same as SyncReplier

if err := handler.Start(); err != nil {
	log.Fatal(err)
}
```

Scale instances via **manager_client**:

```go
import "github.com/sds-framework/handler-lib/manager_client"

mc, err := manager_client.New(cfg)
if err != nil {
	log.Fatal(err)
}
instanceId, err := mc.AddInstance()
```

---

### Tutorial: Dependencies (call other services)

Routes can depend on other SDS services. Declare dependency IDs on the route; the service calls `AddDepByService` before `Start()`.

```go
import "github.com/sds-framework/client-lib"

// Route: needs two dependency sockets
err := handler.Route("aggregate", aggregateHandler, "users", "orders")

func aggregateHandler(
	req message.RequestInterface,
	users *client.Socket,
	orders *client.Socket,
) message.ReplyInterface {
	// use users.Request(...), orders.Request(...)
	return req.Ok(key_value.New())
}
```

Register configs (typically from your service bootstrap):

```go
usersCfg := clientConfig.New("github.com/sds-framework/users", "users", 0, zmq.REQ)
usersCfg.UrlFunc(clientConfig.Url)
handler.AddDepByService(usersCfg)

ordersCfg := clientConfig.New("github.com/sds-framework/orders", "orders", 0, zmq.REQ)
ordersCfg.UrlFunc(clientConfig.Url)
handler.AddDepByService(ordersCfg)
```

`Start()` fails if a route references a dependency that was not registered.

---

### Tutorial: Publisher (broadcast + trigger)

A **Publisher** has:

- **Broadcast socket** — subscribers connect (SUB)
- **Trigger socket** — send a message to publish (via `TriggerClient()`)

```go
pub := publisher.New()

baseCfg := config.NewInternalHandler(config.SyncReplierType, "events", "events")
triggerCfg, err := config.InternalTriggerAble(baseCfg, config.PublisherType)
if err != nil {
	log.Fatal(err)
}

pub.SetConfig(triggerCfg)
pub.SetLogger(logger)
pub.Start()

// Client that triggers a broadcast
triggerClientCfg := pub.TriggerClient()
trigger, _ := client.New(triggerClientCfg)

req := message.Request{Command: "publish", Parameters: key_value.New().Set("event", "updated")}
reply, err := trigger.Request(&req)
```

Subscribers connect to `config.ExternalUrl(triggerCfg.BroadcastId, triggerCfg.BroadcastPort)` with a ZMQ SUB socket (or use `client-lib` with SUB).

---

### Tutorial: SDSIn (io.Writer input)

Use **SDSIn** when some Go code wants an `io.Writer`, but you want the output to go to another SDS place instead of stdout. For example, write terminal logs into `sdsin`, then read them in another terminal with `client-lib/sdsout`.

Each `Write(p)` broadcasts an SDS `message.Request`:

- `Command`: `"io"`
- `Parameters["row"]`: `string(p)`

```go
import (
	"fmt"
	"log"

	zmq "github.com/pebbe/zmq4"
	"github.com/sds-framework/datatype-lib/message"
	"github.com/sds-framework/handler-lib/config"
	"github.com/sds-framework/handler-lib/sdsin"
	"github.com/sds-framework/handler-lib/sdsout"
	loglib "github.com/sds-framework/log-lib"
)

logger, err := loglib.New("events", false)
if err != nil {
	log.Fatal(err)
}

in := sdsin.New()
cfg := config.NewInternalHandler(config.PublisherType, "events", "events")
cfg.Id = "tmp/events_sdsin" // Port 0 + "tmp" prefix binds ipc:///tmp/events_sdsin.

// TCP version, for subscribers in another process or host:
// cfg := config.NewHandler(config.PublisherType, "events", "events", 5555)
//
// In-process version, for subscribers in the same process:
// cfg := config.NewInternalHandler(config.PublisherType, "events", "events")
// This binds inproc://events when the ID does not start with "tmp".

in.SetConfig(cfg)
if err := in.SetLogger(logger); err != nil {
	log.Fatal(err)
}

// StartInBg returns only after the PUB socket and manager are ready.
if err := in.StartInBg(); err != nil {
	log.Fatal(err)
}

fmt.Fprintln(in, "terminal log line")
```

Use **SDSOut** to receive those rows and write them somewhere else. If no writer is passed, it writes to `os.Stdout`.

```go
out := sdsout.New()
out.SetConfig(cfg) // optional second arg: any io.Writer; defaults to os.Stdout
if err := out.StartInBg(); err != nil {
	log.Fatal(err)
}
defer out.Close()
```

Under the hood, subscribers connect to the publisher URL and parse the received message as an SDS request:

```go
sub, err := zmq.NewSocket(zmq.SUB)
if err != nil {
	log.Fatal(err)
}
defer sub.Close()

if err := sub.SetSubscribe(""); err != nil {
	log.Fatal(err)
}

subscriberURL := config.ConnectUrl(cfg.Id, cfg.Port)
// IPC example from above: ipc:///tmp/events_sdsin
// TCP subscriber URL: tcp://{cfg.Id}:{cfg.Port}
// In-process subscriber URL: inproc://{cfg.Id}, and it must be in the same process.
if err := sub.Connect(subscriberURL); err != nil {
	log.Fatal(err)
}

raw, err := sub.RecvMessage(0)
if err != nil {
	log.Fatal(err)
}

req, err := message.NewReq(raw)
if err != nil {
	log.Fatal(err)
}

row, err := req.RouteParameters().StringValue("row")
if err != nil {
	log.Fatal(err)
}
fmt.Println(row)
```

For TCP, publishers bind with `config.ExternalUrl(cfg.Id, cfg.Port)`. Local IDs (`localhost` or `127.0.0.*`) bind to `tcp://*:{port}`; other IDs bind to `tcp://{id}:{port}`. Subscribers connect with `config.ConnectUrl(cfg.Id, cfg.Port)` (`tcp://{id}:{port}`). For in-process subscribers, keep `Port` as `0` and use an ID without the `tmp` prefix so the URL is `inproc://{id}`.

---

### Tutorial: Manage a running handler

Use [`manager_client`](manager_client) from your service process:

```go
mc, err := manager_client.New(handlerConfig)
if err != nil {
	log.Fatal(err)
}

status, parts, err := mc.HandlerStatus()
instances, err := mc.InstanceAmount()
err = mc.Close()
```

Manager routes are defined in `config` (`status`, `close`, `add-instance`, `delete-instance`, `instance-amount`, etc.).

---

### Tutorial: HTTP or custom protocols

To expose a handler over HTTP or another protocol, add a **pair** layer on the frontend. See [CONTRIBUTING.md — Frontend (pair layer)](CONTRIBUTING.md#frontend-pair-layer) and [web-lib](https://github.com/sds-framework/web-lib).

---

## Typical lifecycle

```
New() → SetConfig() → SetLogger() → Route(...) → [AddDepByService(...)] → Start()
```

| Step | Notes |
|------|--------|
| `SetConfig` | Required before `SetLogger` |
| `SetLogger` | Child logger per handler id |
| `Route` | Command name must be unique |
| `AddDepByService` | Called by service, not inside handler-lib |
| `Start` | Starts frontend, instance manager, handler manager; blocks until parts are ready |

Check health: `handler.Status()` (empty string = running) or `manager_client.HandlerStatus()`.

## Project layout

| Path | Purpose |
|------|---------|
| `sync_replier/`, `replier/`, `publisher/`, `worker/` | Handler implementations |
| `config/` | Handler config and URL helpers |
| `route/` | Route types and dispatch |
| `manager_client/` | Service-side control client |
| `pair/` | External protocol pairing |

## Related projects

| Repo | Role |
|------|------|
| [client-lib](https://github.com/sds-framework/client-lib) | Connect and send requests |
| [datatype-lib](https://github.com/sds-framework/datatype-lib) | `message.Request` / `message.Reply` |
| [log-lib](https://github.com/sds-framework/log-lib) | Logging |
| [service-lib](https://github.com/sds-framework/service-lib) | Run handlers inside a service |
| [web-lib](https://github.com/sds-framework/web-lib) | HTTP frontend example |

## License

See [LICENSE](LICENSE).
