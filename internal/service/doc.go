// Package service manages multiple named Moxy queues.
//
// The service owns the mapping from queue name to core engine. Each engine still
// coordinates exactly one backend-backed queue.
package service
