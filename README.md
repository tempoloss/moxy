# Moxy

Moxy is a stateful reliability middleware for Redis-style queues.

Moxy currently provides a deterministic in-memory lease engine with at-least-once delivery semantics. Tasks can be delivered more than once, but active work should not silently disappear when a worker fails to acknowledge a lease.

## Current Lifecycle

```text
READY -> PROCESSING -> ACKED
READY -> PROCESSING -> EXPIRED -> REQUEUED -> READY
```

Active leases are metadata owned by the core engine. Task storage is owned by the queue backend, which tracks ready and processing tasks. ACK completes a processing task; expiration requeues a processing task back to ready.

## Internal Layers

`core.Engine` remains a single-queue lease coordinator. `service.Service` manages multiple named queues by lazily creating one engine per queue from a backend factory. `command.Handler` provides a protocol-neutral command layer over the service.

Supported internal commands:

- `MOXY.ENQUEUE queue payload`
- `MOXY.FETCH queue timeout_ms`
- `MOXY.ACK lease_id`
- `MOXY.STATS queue`

These are plain Go command values today. TCP, RESP, and Redis protocol handling are intentionally not implemented yet.

## Current Scope

Implemented:

- In-memory queue backend with ready and processing sets
- Redis queue backend for integration use
- Leased task delivery
- Lease acknowledgments
- Expiration scheduling with `container/heap`
- Lazy invalidation of stale heap entries
- Delayed retry scheduling when expiration requeue fails
- Expiration reaping back into ready storage
- A simple background reaper
- Multi-queue service layer
- Protocol-neutral internal command handler

Not implemented yet:

- Redis protocol support
- RESP parsing
- TCP proxying
- WAL or snapshot crash recovery
- Redis Streams
- Distributed coordination

## Try It

```sh
go test ./...
go run ./cmd/moxy
```

MemoryQueue is the default backend used by the demo and core engine constructor. RedisQueue is available as another `queue.Backend` implementation for integration use:

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
backend := queue.NewRedisQueue(client, "default")
```

Redis integration tests are opt-in:

```sh
MOXY_REDIS_INTEGRATION=1 MOXY_REDIS_ADDR=localhost:6379 go test ./internal/queue -run Redis -count=1 -v
```

RedisQueue uses JSON serialization and Redis lists. `Acquire` uses `LMOVE`; `Complete` and `Requeue` use Lua scripts so finding the processing task and moving/removing it happens atomically inside Redis.

Moxy is still single-node and backend-adapter based. Phase 3.5 does not include TCP proxying, RESP parsing, WAL, snapshots, crash recovery, Redis Streams, distributed behavior, or user-facing protocol commands.

Phase 4 adds only an internal service and command layer. It still does not include TCP, RESP, Redis proxying, WAL, snapshots, crash recovery, Redis Streams, distributed behavior, or user-facing network protocol commands.

Race testing is intentionally deferred until the local Windows development environment has cgo/GCC available.
