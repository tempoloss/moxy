package resp

import "errors"

var (
	ErrMalformedRESP = errors.New("malformed RESP value")
	ErrValueTooLarge = errors.New("RESP value too large")
)
