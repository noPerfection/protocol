# NoPerfection framework's protocol

`protocol` is the communication layer between noPerfection services. NoPerfection services communicate using [ZeroMQ](https://zeromq.org/) sockets, so treat this repo as the zeromq services on steroids.

NoPerfection adds to them a security, circuit break, and other additional utilities for a production grade.

It is split into three Go modules:

- [client](./client/README.md) is the client side zeromq sockets:
    `zmq.REQ` as client.SyncReplier, 
    `zmq.PAIR` as ClientPair, 
    `zmq.DEALER` as client.Replier, 
    `zmq.SUB` as client.Publisher,
    *etc*
- [handler](./handler/README.md) is the server side zeromq sockets: 
    `zmq.REP` as handler.SyncReplier, 
    `zmq.PAIR` as handler.Pair, 
    `zmq.ROUTER` as handler.Replier,
    `zmq.PUB` as handler.Publisher,
    *etc*
- [message](./message/README.md) defines the message format that sockets use: 
    request, 
    reply, 
    endpoint,
    etc.

Additionally it has the [test](./test/README.md) that tests the protocol workflow from client to handlers.

## Endpoints

Socket addresses (ip address or domain names etc) are called endpoints. Endpoints consists of two properties: `id` and `port`.
The network protocol are identified dynamically. NoPerfection supports three types of network protocols: tcp, ipc and inproc:

### Network protocol identification rules

1. TCP is when `port` is not 0:

```js
// tcp is internet's foundational protocol, I assume my readers knows it already.
const endpoint1 = {id: "localhost", port: 3000}     // tcp://localhost:3000/
const endpoint2 = {id: "example.com", port: 3000}   // tcp://example.com:3000/
```

1. IPC is when `port` is 0, id has the `tmp/` prefix:

```js
const endpoint1 = {id: "tmp/localhost", port: 0}    // ipc://tmp/localhost
const endpoint2 = {id: "tmp/my-sample-app", port: 0}// ipc://tmp/my-sample-app
```

`ipc` stands for inter-process-communication and used in operating systems for app to app communications.

1. Inproc is when `port` is 0, id has no `tmp/` prefix:

```js
const endpoint1 = {id: "localhost", port: 0}        // inproc://localhost
const endpoint2 = {id: "tmp-app", port: 0}    // inproc://tmp-app
```

`inproc` is the zeromq's own protocol for multithreaded communication.

---

In noPerfection, you can compile your app as a single binary but multi threaded. All you need to do to convert it into multi process application is by changing the endpoints.
If your app turns into a cloud based app, across multiple servers, then simply add the port.

Source code of the endpoints are in the `protocol/message/endpoint.go`.

# Security

Socket authentication and message encryption is done using Curve public/secret keys. Curves are used for both authentication and encryption. Its built into the zeromq. So under the hood it uses `zap` protocol.

With noPerfection, you can be manage access control per remote function calls:

Assume your service has a one handler socket with five route commands. You can make access control that one route can be called by certain sockets only. It's called `whitelisting` which uses the HMAC (hashed message authentication) .

## NPAP

If you want to connect to the handler within a handler, then enable the `npap`:

```go
import "github.com/noPerfection/protocol/handler/npap"

func main() {
    ap := npap.New()
    ap.Start()
}
```

The `noPerfection/service` automatically start the npap so you won't need to start them.

Npap stands for *noPerfection authentication protocol*. Its actually wraps the ZAP (zeromq authentication protocol). And all handlers automatically interact with this socket to tell about context. Whenever you connect to a socket within a handler function, then the client uses the `npap` to authenticate within the handler.

- `**protocol/handler/autocontext`** — handler-side registration (`AddHandler`, `AddRoute`, `RemoveHandler`, `RemoveRoute`).
- `**protocol/client/autocontext*`* — client-side lookup (`GetPublicKey`, `GetHmacSecret`).

> For advanced users, you can think of NPAP as the event system for the application, so later you can extend it if you want to have events. In that case, just make sure you start it before you call any `service.Start()`.

### How it works

**Handlers** register their security information with npac at runtime:

1. When a handler **starts**, it calls `autocontext.AddHandler(endpoint, url, publicKey)` to register its CURVE public key.
2. **Before** calling a user handle function, the handler calls `autocontext.AddRoute(endpoint, url, command, secret)` to publish the HMAC secret for the current request.
3. **After** the handle function returns, the handler calls `autocontext.RemoveRoute(url, command)` to unpublish the secret.
4. When a handler **closes**, it calls `autocontext.RemoveHandler(url)` to remove its registration.

**Clients** use npac to recover from security errors automatically:

- On `ErrNoCurveKey` — the client looks up the server's CURVE public key via `autocontext.GetPublicKey(url)`, configures the socket with `Secure`, and retries once.
- On an `"access-denied"` reply — the client looks up the HMAC secret via `autocontext.GetHmacSecret(url, command)`, signs the request, and retries once.

All autocontext calls use a **50 ms timeout with 1 attempt**. Errors are silently ignored by handlers (npac may not be running in all environments).