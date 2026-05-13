// Package core coordinates leases for a single queue backend.
//
// Engine owns lease metadata, expiration scheduling, acknowledgments, and
// requeue recovery. The queue backend owns task storage.
package core
