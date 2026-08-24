# ADR 0001: Redis Lists vs Streams

## Status

Accepted for the current alpha backend.

## Context

Moxy's first Redis backend uses plain Redis data types plus small Lua scripts.
The backend models tasks moving through explicit lifecycle storage:

```text
READY -> PROCESSING -> ACK/REQUEUE
```

Redis Streams are also a valid Redis-native way to model lease-aware work queues.

## Decision

Keep the current Redis backend on plain data types for now:

- Ready tasks live in a Redis list, taken with `RPOP` and returned with `LPUSH`.
- Processing tasks live in a **hash keyed by task id**, not a list.
- Processing lease fences live in `moxy:{queue}:processing:leases`, another hash
  keyed by task id.
- One Lua script performs the ready-to-processing transition: `RPOP` off the list
  and `HSET` into both processing hashes, so a crash cannot land between the task
  move and its generation fence.
- Further Lua scripts perform bounded atomic transitions for ACK, requeue,
  dead-letter moves, and startup reclamation. They compare the stored lease ID
  before mutating processing state.

Processing is a hash rather than a list because every operation on an in-flight
task addresses it by id: ACK deletes one entry, requeue moves one back, the
reaper reclaims specific ones. The second hash stores which lease generation owns
that task so a stale lease cannot mutate a newer one. On a list each task lookup
is a scan; on a hash each is `HGET`/`HDEL`. `LMOVE` would be the natural primitive
for list-to-list, and it is deliberately not used here for that reason.

## Why Plain Data Types First

A list for what is waiting and hashes for what is claimed are the simplest
explicit baseline. They make `READY -> PROCESSING -> ACK/REQUEUE` easy to reason
about, map cleanly to the current `queue.Backend` abstraction, and keep every
transition short enough to express as one Lua script. Nothing about the lifecycle
is implied by the data type: it is all written down.

## Streams Alternative

Redis Streams provide consumer groups, pending entries, `XACK`, `XAUTOCLAIM`,
and built-in tools for inspecting and reclaiming stuck messages.

Streams are more Redis-native for lease-aware work queues and may be a better
fit for deployments that need Redis-native recovery controls.

## Consequences

The lists backend stays small and understandable, but Moxy must own more queue
semantics in code and Lua. A future `RedisStreamsQueue` backend can be added
without changing `core.Engine` if it satisfies the same backend interface.
