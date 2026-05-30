# protocol

`protocol` is the communication layer between noPerfection services.

It is split into three Go modules:

- [`client`](client) sends requests or fire-and-forget messages to handlers.
- [`handler`](handler) routes incoming messages to Go functions.
- [`message`](message) defines the request, reply, endpoint, and envelope formats shared by clients and handlers.
