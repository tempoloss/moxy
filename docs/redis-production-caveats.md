# Redis Production Caveats

Moxy is currently `v0.1.0-alpha`. It is useful for learning, experimentation,
and validating queue semantics, but it is not production-hardened yet.

## Delivery Semantics

Moxy currently provides at-least-once delivery, not exactly-once delivery. A task
can be delivered more than once after worker crashes, lease expiration,
replication failover, or client retry behavior. Workers should be idempotent.

## Redis Persistence

Moxy cannot provide stronger durability than the configured Redis persistence.

- With AOF `appendfsync everysec`, Redis can lose a small recent write window
  during a crash.
- With AOF `appendfsync always`, Redis fsyncs every write and is safer, but
  slower.
- RDB-only or loosely configured persistence can lose more queue state than a
  durable queue workload usually expects.

Choose Redis persistence settings based on the durability required by the queue.

## Redis Eviction

Queue Redis instances should not use cache-style eviction policies such as
`allkeys-lru` for queue data. Evicting queue keys can silently drop ready,
processing, or dead-lettered work.

Prefer `noeviction` and a persistence-oriented Redis configuration for queue
workloads.

## Replication And Failover

Redis replication and failover can still produce duplicates or replayed work.
Writes acknowledged by a primary may not be present on a promoted replica, and
clients may retry operations around failover boundaries.

Moxy expects workers to treat task handling as idempotent.

## Redis Cluster Key Slots

Redis Cluster requires all keys touched by one atomic operation to hash to the
same slot. Moxy queue keys use a shared hash tag per logical queue:

```text
moxy:{<queueName>}:ready
moxy:{<queueName>}:processing
moxy:{<queueName>}:dead
```

The content inside braces must be identical for all keys belonging to the same
logical queue.

## Lua Scripts

Redis executes Lua scripts atomically and blocks other server activity while a
script runs. Moxy scripts should remain short and bounded to queue transition
work such as ACK, requeue, and dead-letter moves.

## Dead-Letter Queues

Dead-letter queue support is being added so tasks that expire too many times can
move out of the retry loop instead of requeueing forever. This is a baseline
safety feature, not full crash recovery.

## Streams

Redis Streams are a valid alternative for reliable work queues. Consumer groups,
pending entries, `XACK`, and reclaim operations may make Streams a better fit for
some production systems. Moxy may explore a separate Redis Streams backend later.
