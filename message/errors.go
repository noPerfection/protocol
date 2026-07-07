package message

import "errors"

// RequestTimeoutError is returned when a client request exceeds its timeout window.
var RequestTimeoutError = errors.New("request_timeout")

// ErrAccessDenied is returned when inbound HMAC or access policy rejects a request.
var ErrAccessDenied = errors.New("access-denied")

// ErrNoCurveKey is returned when the server rejects the client during a CURVE handshake.
// This typically means the client's public key is not in the server's allow-list,
// or the client connected without CURVE credentials to a CURVE-only server.
var ErrNoCurveKey = errors.New("no-curve-key")
