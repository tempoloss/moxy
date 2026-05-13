// Package command provides Moxy's protocol-neutral command layer.
//
// The package accepts typed Go commands such as MOXY.ENQUEUE and MOXY.FETCH.
// Network concerns like TCP, RESP parsing, and Redis protocol compatibility live
// outside this package.
package command
