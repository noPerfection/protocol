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
// 
const endpoint1 = {id: "localhost", port: 3000}     // tcp://localhost:3000/
const endpoint2 = {id: "example.com", port: 3000}   // tcp://example.com:3000/
```

`tcp` is internet's foundational protocol, I assume my readers knows it already.

1. IPC is when `port` is 0, id has the `tmp/` prefix:

```js
const endpoint1 = {id: "tmp/localhost", port: 0}    // ipc://tmp/localhost
const endpoint2 = {id: "tmp/my-sample-app", port: 0}// ipc://tmp/my-sample-app
```

`ipc` protocol is the stands for *inter-process communication*. Its an operating systems protocol for app to app communications. Check on wikipedia [Inter-process communication](https://en.wikipedia.org/wiki/Inter-process_communication)

1. Inproc is when `port` is 0, id has no `tmp/` prefix:

```js
const endpoint1 = {id: "localhost", port: 0}        // inproc://localhost
const endpoint2 = {id: "tmp-app", port: 0}    // inproc://tmp-app
```

`inproc` is the zeromq's own protocol for inter-thread communication. If your app has multiple concurrent threads then use the inproc for it.

---

Source code of the endpoints are in the `protocol/message/endpoint.go`.

# Security

> **If you use the noPerfection/service you won't have to worry about security, its done automatically. You don't have to remember where to whitelist, where to secure as services will do it themselves**

For authentication and encryption noPerfection uses zeromq protocol. Its based on curve public/secret keys. In short it uses *zap* protocol.

Additionally, noPerfection supports access control per route using [HMAC](https://en.wikipedia.org/wiki/HMAC).

In noPerfection terminology the authentication/encryption is called *allowance*, the access control is called *whitelisting*.

```go
import "github.com/noPerfection/protocol/handler"
import "github.com/noPerfection/protocol/message"

func main() {
    clientPublic, _, _ := message.GenerateCurveKey()
    _, handlerSecret, _ := message.GenerateCurveKey()
    h, _ := handler.NewSyncReplier()
    h.Secure(handlerSecret) // Enabling the authentication
    h.Allow(clientPublic) // Authenticate only this.
    
    // Add the handlers for the routes: h.Route("hello-world", onHelloWorld)
    // h.SetMushroomURL()
    // h.SetEndpoint()
    
    hmacSecret := message.GenerateSecret()
    h.Whitelist("hello-world", hmacSecret) // now, hello-world is possible if hmacKey is written

    // h.Start()
    // h.Watch()
}
```

If you set the Secure(), then it will encrypt the messages but anyone can access the socket. 
Client needs to know the public key of the handler.

```go
import "github.com/noPerfection/protocol/client"

func main() {
    c, _ := client.NewSyncReplier()
    c.Secure(serverPublic)
} 
```

The allow means it authenticates and only authenticated clients can access the service.
In this case, client needs to pass its own secret key when setting the security:

```go
c.Secure(serverPublic, clientSecret)
```

In both case if not encrypted or not authenticated, the client will return `message.ErrNoCurveKey`.

The whitelisted commands mean client must pass the hash of the message as well:

```go
c.Whitelist("hello-world", hmacSecret)

helloWorldMsg := message.Request{Command: "hello-world", /*...*/}
fooBarMsg := message.Request{Command: "foo-bar", /*...*/}

c.Request(&helloWorldMsg)   // will apply hmac hash
c.Request(&fooBarMsg)   // will be skipped from hmac
```

## NPAC

If you want to connect to a handler from within another handler, start **npac** (noPerfection AutoContext). It is an in-process registry that tracks handler security context so nested calls can authenticate and sign requests automatically.

```go
import "github.com/noPerfection/protocol/handler/npac"

func main() {
    n := npac.New()
    n.Start()
}
```

`**noPerfection/service` starts npac automatically**.

> For advanced users, npac can be thought of as an in-process event bus for the application. You can extend it later if you need broader events — just make sure it is started before any `service.Start()`.

The purpose of the `Secure()`, `Allow()` and `Whitelist()` is to secure the service-to-service calls.
The noPerfection services does it automatically using the `npac`.

For example if you define a database extension (type of noPerfection service) for the main service, then only that main handler will be accessing to the database extension. Even if the extension and main app are multi threaded, and your main app has other code bases and modules or even third party libraries they won't be able to connect to the database. Because database receives sockets only within the main service.

### For contributors: How it works

Handlers embed `handler.Autocontext` and talk to npac over an inproc socket. Whenever a client connects from inside a handler function, it uses npac to resolve CURVE keys and route context.

- `protocol/handler/autocontext.go` — handler-side registration (`register-handler`, `push-handler-context`, `pop-handler-context`, `remove-handler`).
- `protocol/client/autocontext.go` — client-side lookup (`HandlerContext`, `RegisterOutbound`, `RemoveOutbound`).

**Handlers** register their security information with npac at runtime:

1. When a handler **starts**, it registers its mushroom URL and control endpoint with npac (`register-handler`).
2. **Before** calling a user handle function, it pushes the current route onto npac's context stack (`push-handler-context`).
3. **After** the handle function returns, it pops the route from the stack (`pop-handler-context`).
4. When a handler **closes**, it removes its registration (`remove-handler`).

**Clients** use npac to recover from security errors automatically:

- On `ErrNoCurveKey` — the client calls `HandlerContext(endpoint, command)`, gets the target's CURVE public key and the calling handler's control endpoint, then retries once through `Control.RequestAsContext`.
- On an `"access-denied"` reply — the same `HandlerContext` lookup routes the request through the calling handler's control socket, which signs it with the route HMAC secret, and retries once.

Autocontext clients use a **50 ms timeout**. Handler-side npac calls that fail (for example when npac is not running) are silently ignored.