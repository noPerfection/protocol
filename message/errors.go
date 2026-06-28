package message

import "errors"

// RequestTimeoutError is returned when a client request exceeds its timeout window.
var RequestTimeoutError = errors.New("request_timeout")
