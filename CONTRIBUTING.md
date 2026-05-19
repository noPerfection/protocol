# Contributing to handler-lib

Thank you for contributing. This document covers architecture and internals. For usage and tutorials, see [README.md](README.md).

## Overview

Handler-lib routes ZeroMQ messages to user-defined Go functions. A handler is split into cooperating parts, each running on its own goroutine and owning its sockets (ZeroMQ sockets are not thread-safe).

![User and Handler diagram](_assets/Handler.jpg "Handler diagram")

*Diagram: [Source](https://drive.google.com/file/d/1B0JOWbrbby9yUy66pMwWnlf8ic18XOs-/view?usp=sharing)*

The `base` package defines `Handler` and `base.Interface`. Do not use `base.Handler` directly in application code; use a derived handler (`sync_replier`, `replier`, `publisher`, `worker`).

## Glossary

| Term | Meaning |
|------|---------|
| **Send** | Handler–user interaction (generic) |
| **Request** | Sender expects a reply |
| **Submit** | Message sent without expecting a reply (fast, no delivery guarantee) |

## Route

A route has three parts:

1. **Command** — name the client sends in the request
2. **Handle function** — `route.HandleFunc0` … `HandleFuncN` (see `route/handle_func.go`)
3. **Dependencies** (optional) — service IDs whose `client-lib` sockets are injected into the handler

## Config package

Handler configuration lives in `config`:

| File / type | Role |
|-------------|------|
| `Handler` | Id, Port, Type, Category, InstanceAmount |
| `Trigger` | Publisher broadcast settings |
| `controller.go` | `ExternalUrl`, `NewHandler`, handler types |
| `internal.go` | Internal `inproc://` URLs (manager, instance manager, instances) |

### Handler URLs (external)

`config.ExternalUrl` is for the **external** socket only. Internal parts always use `inproc://` helpers in `internal.go`.

| Port | Id | Bind URL |
|------|-----|----------|
| non-zero | any | `tcp://*:{Port}` |
| 0 | starts with `tmp` | `ipc:///{Id}` |
| 0 | otherwise | `inproc://{Id}` |

Clients use the same `Id` and `Port` with [`client-lib/config.Url`](https://github.com/sds-framework/client-lib).

## Internal parts

### Instance manager

Manages worker instances (`AddInstance`, `DeleteInstance`). Operations are **asynchronous** — a successful method call does not guarantee the instance is ready yet. Subscribe to instance-manager events or poll status.

- **Pull socket** — receives status pushes from instances
- **Publisher socket** — broadcasts events to subscribers

### Instance

Runs routes and handle functions. Two sockets:

- **Handler socket** — user requests
- **Manager socket** — close/control (e.g. `CLOSE` command)

Instances push status to the instance manager’s pull socket. The instance manager connects to each instance’s manager and handler endpoints.

### Frontend

Accepts messages on the **external** socket, queues them, and forwards them to ready instances via a consumer loop. Depends on `instance_manager`.

### Handler manager

Control plane for handler parts (frontend, instance manager, instances). External management goes through `handler_manager` routes (`status`, `close`, `add-instance`, etc.) or [`manager_client`](manager_client/manager_client.go).

### Recap

```
instance manager  ←→  instances
frontend          →   ready instances
handler manager   →   all parts
```

Socket closing: parts do not close another thread’s sockets; they signal the owning thread to close them.

## Overwriting behavior

Handlers can override aspects of parts (custom handlers or SDS services). This does not replace entire packages—only selected hooks.

### Handler manager

You may override management routes (not add new route names).

### Frontend (pair layer)

Overwrite the external socket by adding a protocol layer paired to the frontend.

![Pair external diagram](_assets/PairExternal.jpg "Add another layer over external")

*Diagram: [Source](https://drive.google.com/file/d/1B0JOWbrbby9yUy66pMwWnlf8ic18XOs-/view?usp=sharing)*

Flow:

1. Run your protocol server (e.g. HTTP) on the handler port
2. Call `handler.Frontend.PairExternal()` after `SetConfig`
3. Use `handler.Frontend.PairClient()` to forward requests into the handler

See [`pair/pair.go`](pair/pair.go). HTTP example: [web-lib](https://github.com/sds-framework/web-lib).

### Instance manager

Override message types via `SetMessageOperations`. Custom messages must implement `message.RequestInterface` and `message.ReplyInterface`. Defaults: `message.Request`, `message.Reply`.

## Development

```bash
go test ./...
```

Requires ZeroMQ 4.x (`libzmq3-dev` on Debian/Ubuntu).

## Related repositories

- [client-lib](https://github.com/sds-framework/client-lib) — connect to handlers
- [datatype-lib](https://github.com/sds-framework/datatype-lib) — request/reply types
- [service-lib](https://github.com/sds-framework/service-lib) — service orchestration
- [web-lib](https://github.com/sds-framework/web-lib) — HTTP over handler pair
