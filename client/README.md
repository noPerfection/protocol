# protocol/client

The `client` module exchanges messages with [protocol/handler](https://github.com/noPerfection/protocol/handler/).

## Terminology
*Transmit* &ndash; any message transfers between a client and handler.

*Request* &ndash; is the two **transmits** with the handler. Sending and receiving. It guarantees a delivery.

*Send* &ndash; a one-way **transmit** with the handler.
The client sends the message.
Client doesn't wait for a reply. 

*Target* &ndash; a handler to which a message is **transmitted**.

## Rules
* Client has options
* - *timeout* &ndash; option that halts sending after this period of time. Minimum value is 2 milliseconds.
* - *attempt* &ndash; option that repeats the message **transmitting** after *timeout*. Zero means retry indefinitely.
* Client must set correct message parts for asynchronous handlers for internal zeromq socket.

## Implementation

### Clients
Handler-specific client packages expose only the message operations that make sense for the target handler.

| Package | Target handler | Client socket | Supported messages |
|---------|----------------|---------------|--------------------|
| `client/sync_replier` | `SyncReplier` | `REQ` | `Request` |
| `client/replier` | `Replier` | `DEALER` | `Send`, channel-based `Receive` |
| `client/worker` | `Worker` | `PUSH` | `Send` |
| `client/publisher` | `Publisher` | `SUB` | channel-based `Receive` |
| `client/pair` | `Pair` | `PAIR` | `Send`, channel-based `Receive` |

The generic `client.New(id, port, handlerType)` constructor is the shared base used by those packages.
Prefer handler-specific packages in application code:

```go
import "github.com/noPerfection/protocol/client/replier"

c, err := replier.NewClient("my_handler", 0)
if err != nil { /* ... */ }
defer c.Close()

err = c.Send(request)
for reply := range c.Receive() {
    _ = reply
}
```

The base `client` package also defines `SendInterface`, `RequestInterface`, and `ReceiveInterface` for the supported operations.

### Options
The default *timeout* is **10 Seconds**.
The default *attempt* is **5**.

The `Client.Timeout(time.Duration)` method over-writes the timeout. 
The `minimumTimeout` is **2 milliseconds**. 

The `Client.Attempt(uint8)` method sets the attempt. 
Zero means retry indefinitely. 

### Type
The type of the client is the opposite of the target type.
Thus, when a client is defined, it's defined against the target to whom it will interact with.

The handlers use the clients for creating a managers.
To avoid import cycling the clients are using the target's internal socket type.

For intercommunication, noPerfection uses ZeroMQ sockets.

### URL
`client.New(id, port, handlerType)` builds the ZeroMQ endpoint from the given id and port:

| Condition | Endpoint |
|-----------|----------|
| `Port` > 0 | `tcp://localhost:{Port}` |
| `Port` == 0 and `Id` starts with `tmp` | `ipc:///{Id}` (filesystem IPC socket) |
| `Port` == 0 otherwise | `inproc://{Id}` (in-process) |

`client.New` connects to the generated address and selects the socket type from the handler type.

### Concurrent
The client is a [thread safe](https://en.wikipedia.org/wiki/Thread_safety) ZeroMQ wrapper for interacting with noPerfection handlers.
It is intended for creating a few long-lived clients and sharing them across goroutines over time.

One client can accept messages from multiple goroutines. The client queues the messages and serializes ZeroMQ socket access internally.

```go
// Thread 1
client1.Request(message)
// Thread 2
client1.Request(message)
```

> **Todo**
> 
> Optimize the client passing to the handle functions as one child is passed to multiple handle func.
> We need to avoid passing from parent to the nested child tree.

> Test the limits of the clients, and number of the threads.
> Maybe create a pool of client sockets and get one when it's available?

> **Todo**
> 
> Create a library of the pool for available sockets. 
> Then design the handle and client based on that.

