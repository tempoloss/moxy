// Package queue defines task storage backends for Moxy.
//
// Backends move tasks between ready and processing storage. MemoryQueue is used
// for deterministic local execution, and RedisQueue provides a Redis-backed
// implementation with Lua-backed complete and requeue operations.
package queue
