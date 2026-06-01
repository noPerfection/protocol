# protocol/test

This module exists only for testing the entire protocol across module boundaries.

Keep tests here when they need to run `protocol/client` against `protocol/handler` through the public APIs of both modules.

Use the module for:

- Client/handler integration tests for each handler type.
- Handler control tests that start, stop, inspect, and restart handlers through their matching client control package.

Keep unit tests that only need one module inside that module instead.

Run it from the repository root with:

```sh
go test ./test/...
```

Stress tests are intentionally separate because they push queue and timeout edges and may fail while exposing limits:

```sh
go test -tags stress ./test/...
```

## Current Findings

- Client queue pressure starts around the internal queue limit. In stress tests, only part of a 25-operation simultaneous burst is accepted; the rest can return `queue is full` or timeout.
- `SyncReplier` requests recover after queue pressure in the current tests.
- `Worker` send pressure accepts some messages and cleanly rejects the rest when the queue is full.
- `Replier` and `Pair` receive paths currently break under stress. Accepted sends may not all arrive on `Receive()`, and the receive channel can close early.
- `Pair` can also time out during send or control close after pressure.
- `Publisher` receive stress currently passes, including many control broadcasts to a subscriber.

These findings are intentionally documented instead of hidden. The `stress` tag is for breaking the protocol and discovering what needs polish next.
