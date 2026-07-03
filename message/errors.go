package message

import "errors"

// RequestTimeoutError is returned when a client request exceeds its timeout window.
var RequestTimeoutError = errors.New("request_timeout")

// ErrAccessDenied is returned when inbound HMAC or access policy rejects a request.
var ErrAccessDenied = errors.New("access-denied")
