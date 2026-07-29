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
- One Lua script performs the ready-to-processing transition: `RPOP` off the list
  and `HSET` into the hash, so a crash cannot land between them.
- Further Lua scripts perform bounded atomic transitions for ACK, requeue,
  dead-letter moves, and startup reclamation of orphaned processing entries.

Processing is a hash rather than a list because every operation on an in-flight
task addresses it by id: ACK deletes one entry, requeue moves one back, the
reaper reclaims specific ones. On a list each of those is a scan; on a hash each
is `HDEL`/`HGET`. `LMOVE` would be the natural primitive for list-to-list, and it
is deliberately not used here for that reason.

## Why Plain Data Types First

A list for what is waiting and a hash for what is claimed is the simplest explicit
baseline. It makes `READY -> PROCESSING -> ACK/REQUEUE` easy to reason about, maps
cleanly to the current `queue.Backend` abstraction, and keeps every transition
short enough to express as one Lua script. Nothing about the lifecycle is implied
by the data type: it is all written down.

## Streams Alternative

Redis Streams provide consumer groups, pending entries, `XACK`, `XAUTOCLAIM`,
and built-in tools for inspecting and reclaiming stuck messages.

Streams are more Redis-native for lease-aware work queues and may be a better
fit for deployments that need Redis-native recovery controls.

## Consequences

The lists backend stays small and understandable, but Moxy must own more queue
semantics in code and Lua. A future `RedisStreamsQueue` backend can be added
without changing `core.Engine` if it satisfies the same backend interface.
