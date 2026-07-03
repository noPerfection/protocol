# protocol

`protocol` is the communication layer between noPerfection services.

It is split into three Go modules:

- [client](./client/README.md) sends requests or fire-and-forget messages to handlers.
- [handler](./handler/README.md) routes incoming messages to Go functions.
- [message](./message/README.md) defines the request, reply, endpoint, and envelope formats shared by clients and handlers.
