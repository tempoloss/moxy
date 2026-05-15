# ADR 0001: Redis Lists vs Streams

## Status

Accepted for the current alpha backend.

## Context

Moxy's first Redis backend uses Redis lists plus `LMOVE` and small Lua scripts.
The backend models tasks moving through explicit lifecycle storage:

```text
READY -> PROCESSING -> ACK/REQUEUE
```

Redis Streams are also a valid Redis-native way to model reliable work queues.

## Decision

Keep the current Redis backend on lists for now:

- Ready tasks live in a Redis list.
- Processing tasks live in a Redis list.
- `LMOVE` moves one task from ready to processing.
- Lua scripts perform bounded atomic transitions for ACK, requeue, and
  dead-letter moves.

## Why Lists First

Lists are the simplest explicit baseline. They make `READY -> PROCESSING ->
ACK/REQUEUE` easy to reason about, map cleanly to the current `queue.Backend`
abstraction, and are a good educational first backend for Moxy's lease model.

## Streams Alternative

Redis Streams provide consumer groups, pending entries, `XACK`, `XAUTOCLAIM`,
and built-in tools for inspecting and reclaiming stuck messages.

Streams are more Redis-native for reliable work queues and may be a better fit
for production-oriented deployments.

## Consequences

The lists backend stays small and understandable, but Moxy must own more queue
semantics in code and Lua. A future `RedisStreamsQueue` backend can be added
without changing `core.Engine` if it satisfies the same backend interface.
