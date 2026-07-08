# protocol

`protocol` is the communication layer between noPerfection services.

It is split into three Go modules:

- [client](./client/README.md) sends requests or fire-and-forget messages to handlers.
- [handler](./handler/README.md) routes incoming messages to Go functions.
- [message](./message/README.md) defines the request, reply, endpoint, and envelope formats shared by clients and handlers.

## Autocontext

`autocontext` is the security channel for clients that needs to connect handlers from within the handlers. It's done by **npac** in-process handler (`protocol/handler/npac`). Start it from the service, and when handlers start, they will register themselves.

- **`protocol/handler/autocontext`** — handler-side registration (`AddHandler`, `AddRoute`, `RemoveHandler`, `RemoveRoute`).
- **`protocol/client/autocontext`** — client-side lookup (`GetPublicKey`, `GetHmacSecret`).

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
