# Primitives

This document names the low-level mechanisms Moxy rests on and explains why the code is shaped around them. Each entry is anchored to real code or an existing design document so the claim can be checked instead of trusted.

The line-by-line annotations behind these entries live in [`primitives.code.json`](primitives.code.json): the cited ranges, the note lines, and a fingerprint of the code each one was written against. `python3 scripts/check_primitives_anchors.py` fails when an anchor in this file or in that one no longer points at the code it describes. The Primitives workflow runs it on every push, re-anchors a pure line shift by matching content instead of line numbers, and pushes that repair back, so only a substantive code change needs a person.

## Redis

### Redis list pop (`LPOP`/`RPOP`) deletes

A Redis list is a sequence of strings with a left end and a right end. `LPUSH queue "task-1"` puts one task on the left; `RPOP queue` takes one from the right and returns it. The important detail is the same for `LPOP` and `RPOP`: **the pop deletes the element from Redis.**

**Where:** `internal/queue/redis.go:74` and `internal/queue/redis_scripts.go:31` - enqueue pushes JSON into the ready list, and the acquire script pops one task out of that list before writing the processing task and lease-fence hashes.

**What breaks if it is wrong:** 1) A worker pops a task. 2) The task is no longer in `ready`. 3) The worker or server dies before any other durable state says who has it. 4) No later worker can fetch it because Redis already deleted it from the ready list.

**Caught by:** `TestFetchCrashBoundaryAfterBackendAcquireBeforeJournalRequeuesOrphan`; `TestTornFinalFetchRecordIsDroppedAndOrphanedTaskIsRequeued`; `RunBackendContractTests/AcquireMovesReadyToProcessing`.

### Ready, processing, and dead storage

Moxy does not keep one anonymous queue. It keeps lifecycle storage: ready tasks waiting to be fetched, processing tasks that have been leased, a processing lease-fence index, and dead tasks that have left the retry loop. In Redis, ready and dead are lists, while processing and processing lease IDs are hashes keyed by task id so transitions can address one task and verify its current lease generation.

**Where:** `internal/queue/redis.go:50-53` - Redis keys include `moxy:{queue}:ready`, `moxy:{queue}:processing`, `moxy:{queue}:processing:leases`, and `moxy:{queue}:dead`; `internal/queue/redis_scripts.go:5` explains why processing is a hash.

**What breaks if it is wrong:** 1) A fetched task is not indexed by task id and lease id. 2) `Ack`, `Requeue`, or `DeadLetter` cannot prove it is mutating the current lease generation. 3) A stale lease can move a task that has already been fetched again. 4) A completed task can be retried, or a live task can disappear.

**Caught by:** `TestRedisQueueKeysUseClusterSafeHashTag`; `RunBackendContractTests/StatsReportsReadyAndProcessing`; `RunBackendContractTests/CompleteMismatchedLeaseFails`; `RunBackendContractTests/RequeueMismatchedLeaseFails`; `RunBackendContractTests/DeadLetterMismatchedLeaseFails`.

### Redis Lua scripts / `EVAL`

A Redis Lua script is code Redis runs inside the server, as one command from other clients' point of view. Moxy uses that to turn operations like "pop from ready", "write to processing", and "write the lease fence" into one indivisible transition so no other client can act between them.

**Where:** `internal/queue/redis.go:78` - `Acquire(leaseID)` runs the cached script; `internal/queue/redis_scripts.go:40` and `internal/queue/redis_scripts.go:46-47` - the script does `RPOP` and both `HSET` calls together.

**What breaks if it is wrong:** 1) The code does `RPOP ready`. 2) Before it does both `HSET` writes, the process crashes or another client observes the gap. 3) The task is in neither place, or it has no lease fence. 4) Startup has no Redis state to reconcile or stale leases can mutate current work.

**Caught by:** `TestRedisQueueCachedScriptsRunRepeatedly`; `RunBackendContractTests/AcquireMovesReadyToProcessing`; `RunBackendContractTests/OldLeaseCannotMutateCurrentLease`.

### Redis lists rather than Redis streams

Redis Streams have consumer groups, pending entries, `XACK`, and `XAUTOCLAIM`; they are a Redis-native way to inspect and reclaim leased work. Moxy deliberately starts with plain data types instead - a list for what is waiting, hashes for what is claimed and which lease owns it - because the `READY -> PROCESSING -> ACK/REQUEUE` state machine is then explicit, small, and maps directly to `queue.Backend`; the ADR leaves room for a future Streams backend behind the same interface.

**Where:** `docs/adr/0001-redis-lists-vs-streams.md:20` - the accepted decision keeps the Redis backend on plain data types; `internal/queue/backend.go:42` - the engine only depends on the backend interface.

**What breaks if it is wrong:** 1) The code assumes Redis Streams semantics that lists do not have. 2) No pending-entry table exists for Redis to reclaim from. 3) Moxy's own WAL, reaper, and orphan reconciliation are bypassed or duplicated. 4) Recovery behavior becomes two partial designs instead of one.

**Caught by:** nothing yet; this is documented by `docs/adr/0001-redis-lists-vs-streams.md`, not enforced by a test.

## Queue and WAL ordering

### Fetch moves in the backend before it journals

`Fetch` does **not** journal first. It generates a lease ID, calls `e.ready.Acquire(leaseID)`, then builds and appends the fetch record; the code's own comment says, "The task has already left the ready queue" and the journal happens before the lease becomes visible in memory, not before the backend transition.

**Where:** `internal/core/engine.go:186` - the lease ID is generated; `internal/core/engine.go:187` - backend acquire receives that lease ID; `internal/core/engine.go:203` - the comment says the task has already left ready; `internal/core/engine.go:206` - the WAL record is appended after that.

**What breaks if it is wrong:** 1) `Acquire(leaseID)` moves ready to processing and stores the fence. 2) The process dies before an intact fetch record is durable. 3) Restart sees a processing task but no recovered live task-plus-lease fence. 4) Without reconciliation, the reaper has no heap item and the task sits in processing forever. These are the F1/F2 windows; startup reconciliation now returns those orphaned processing tasks to ready without charging an attempt.

**Caught by:** `TestFetchCrashBoundaryAfterBackendAcquireBeforeJournalRequeuesOrphan`; `TestTornFinalFetchRecordIsDroppedAndOrphanedTaskIsRequeued`; see `docs/failure-matrix.md:53` and `docs/failure-matrix.md:54`.

### Startup reconciliation of orphaned processing tasks

A recovered WAL tells Moxy which task-plus-lease fences should still be live. Anything in backend `processing` without an exact recovered task ID and lease ID match is treated as an acquire that did not become a durable lease or as a stale generation, so startup moves unmatched backend tasks back to ready and closes unmatched WAL leases with `wal.OpStale`.

**Where:** `internal/core/engine.go:98` - restore keeps only leases whose task and generation still match backend processing; `internal/queue/backend.go:30` - `LeaseFence` carries both task ID and lease ID; `internal/queue/redis.go:132` - Redis implements the same transition atomically.

**What breaks if it is wrong:** 1) A crash lands after backend acquire but before a durable fetch record, or an old WAL lease exists after the task was fetched again. 2) The task remains in processing under a fence that does not match recovered WAL. 3) No active current lease exists to expire that old claim, or a stale claim can steal the new one. 4) The service reports an empty ready queue while work is stranded, or a current lease disappears.

**Caught by:** `RunBackendContractTests/RecoverOrphanedProcessingMovesOnlyTasksWithoutActiveLease`; `TestFetchCrashBoundaryAfterBackendAcquireBeforeJournalRequeuesOrphan`; `TestTornFinalFetchRecordIsDroppedAndOrphanedTaskIsRequeued`; `TestRecoveryKeepsOnlyMatchingLeaseGeneration`.

## Disk

### `write` is not `fsync`

A successful file `write` means the kernel accepted the bytes; it does not mean the bytes will survive power loss. `fsync` asks the operating system to push the file's dirty data to storage before the call returns, which is why the default WAL syncs every append.

**Where:** `internal/wal/wal.go:62` - `Options.Sync` controls durability; `internal/wal/wal.go:131` and `internal/wal/wal.go:134` - `Append` writes the frame and calls `Sync` when configured.

**What breaks if it is wrong:** 1) Fetch writes a complete record into the kernel page cache. 2) Power fails before `fsync` completes. 3) Restart may see the record, a torn record, or no record. 4) Recovery must not pretend an unsynced write promised a lease deadline.

**Caught by:** `TestFetchCrashBoundaryAfterAppendBeforeFsyncRestoresIfRecordSurvives`; `TestFetchCrashBoundaryAfterFsyncBeforeMemoryPublishRestoresOriginalDeadline`; nothing yet simulates a real power loss.

### WAL length and CRC frames

The WAL is not a stream of naked JSON. Each record is `[uint32 payload length][uint32 CRC32 of payload][payload]`, so replay knows exactly how many bytes belong to the next record and whether those bytes still match what was written.

**Where:** `internal/wal/wal.go:10` - the package comment defines the frame; `internal/wal/wal.go:254` and `internal/wal/wal.go:254` - `encode` writes the length and CRC.

**What breaks if it is wrong:** 1) A crash leaves stray bytes at the end. 2) Replay cannot tell record data from garbage. 3) It may decode a partial JSON value or allocate from a bogus length. 4) Recovery rebuilds leases from corrupted state.

**Caught by:** `TestCorruptChecksumStopsReplay`; `TestImplausibleLengthStopsReplay`; `TestRecordsSurviveReopen`.

### Torn tail replay

A torn tail is the last WAL record cut off or corrupted by a crash. Replay reads intact frames in order, stops at the first short, invalid, or checksum-failing frame, and truncates the file back to the last good boundary.

**Where:** `internal/wal/wal.go:97` - `Open` replays the file; `internal/wal/wal.go:102` - it truncates to the last good offset; `internal/wal/wal.go:259` - `replay` defines that offset.

**What breaks if it is wrong:** 1) Two old records are intact and the third is half-written. 2) Replay tries to keep reading past the tear. 3) It creates a lease from incomplete bytes or leaves the file positioned in garbage. 4) The next append lands after garbage and recovery gets worse on every restart.

**Caught by:** `TestTornTailIsDroppedAndTruncated`; `TestTornFinalFetchRecordIsDroppedAndOrphanedTaskIsRequeued`; `TestTornFinalAckRecordIsDroppedAndReaperDoesNotResurrectCompletedTask`.

### WAL compaction through temp file and rename

Compaction keeps only fetch records for leases still open; closed history can be dropped because `wal.Live` folds fetches and closes into the current live set. The rewrite goes to `path.compact`, fsyncs that file, closes the old handle, and uses `rename` as an atomic name switch so readers see the old journal or the replacement, not an in-place half-edit.

**Where:** `internal/wal/wal.go:148` - compaction keeps still-open leases; `internal/wal/wal.go:165` - it writes a temporary file; `internal/wal/wal.go:202` - it replaces the journal with `os.Rename`.

**What breaks if it is wrong:** 1) Compaction edits the live WAL in place. 2) The process dies after deleting old bytes but before writing all live leases. 3) Restart opens a syntactically valid but semantically incomplete journal. 4) Active leases disappear from recovery.

**Caught by:** `TestCompactKeepsOnlyLiveLeases` and `TestCompactedRecordsCarryTaskDetail` cover compaction contents; nothing yet simulates a crash during compaction or directory-entry durability after rename.

## Concurrency

### Engine mutex

The engine mutex serializes access to the in-memory lease map, expiration heap, recovery error, and backend calls made through the engine. Without it, two goroutines can mutate Go maps and heap slices at the same time, which is a data race and can also corrupt the ordering the reaper depends on.

**Where:** `internal/core/engine.go:49` - `Engine` owns mutable state behind `mu`; `internal/core/engine.go:176` - `Fetch` locks before touching backend, WAL, leases, and heap.

**What breaks if it is wrong:** 1) Two clients fetch or ack concurrently. 2) One goroutine mutates `leases` while another reads or deletes from it. 3) The heap gets an entry with no matching lease, or a lease loses its heap entry. 4) Expiry can panic, miss work, or requeue the wrong deadline.

**Caught by:** nothing yet directly for data races; `TestServerTwoSimultaneousClients` covers concurrent clients reaching the server, not `go test -race` on engine state.

### Expiration heap

A heap is a slice arranged so the earliest deadline is always at the front. Moxy pushes every lease deadline into the heap and the reaper only peeks at the earliest item; it does not scan every active lease on every tick.

**Where:** `internal/core/heap.go:10` - `expirationHeap` orders by `ExpiresAt`; `internal/core/engine.go:220` - `Fetch` pushes a deadline; `internal/core/engine.go:270` - `ReapExpired` peeks and pops due items.

**What breaks if it is wrong:** 1) One lease expires now and another expires in an hour. 2) The reaper scans or orders them incorrectly. 3) It requeues the later lease too early or fails to requeue the due one. 4) A worker's lease deadline stops meaning anything.

**Caught by:** `TestAckedLeaseHeapEntryIsIgnored`; `TestReapExpiredSkipsStaleHeapEntries`; `TestReapExpiredRetriesFailedBackendRequeueLater`.

### Reaper ticker

The reaper is a small loop around a timer. Every tick passes the current time to `ReapExpired`, which is the only thing that turns expired processing tasks back into ready tasks or dead letters.

**Where:** `internal/reaper/reaper.go:16` - `Run` starts a `time.Ticker`; `internal/reaper/reaper.go:23` - each tick calls `target.ReapExpired(now)`.

**What breaks if it is wrong:** 1) A worker fetches a task and never acks. 2) The deadline passes. 3) No tick calls the engine. 4) The task stays in processing even though its lease expired.

**Caught by:** `TestRunEventuallyRequeuesExpiredLeases`; `TestRunStopsOnContextCancellation`.

## Protocol

### RESP2 framing

RESP is the Redis Serialization Protocol: a byte format where the first byte says the type, such as `*` for an array, `$` for a bulk string, `+` for a simple string, `-` for an error, and `:` for an integer. Redis clients can talk to Moxy because the server reads RESP arrays of bulk strings as commands and writes RESP values back on the same TCP connection.

**Where:** `internal/resp/value.go:3` - value types are the RESP2 prefix bytes; `internal/server/server.go:133` - each connection is read with `resp.Reader`, handled, and written with `resp.Writer`.

**What breaks if it is wrong:** 1) A Redis client sends `*3\r\n$10\r\nMOXY.FETCH...`. 2) The reader treats it as plain text or loses the CRLF framing. 3) The command adapter never receives the bulk-string array it expects. 4) The client sees a protocol error or hangs waiting for a valid Redis-shaped reply.

**Caught by:** `TestReadArrayCommandWithBulkStrings`; `TestReadMoxyFetchCommand`; `TestWriteArrayResponse`; `TestServerMoxyFetchReturnsLeaseTaskAndPayload`; `TestServerMalformedRESPReturnsErrorAndClosesCleanly`.

## Time and correctness

### Lease as a claim with a deadline

A lease is a temporary claim: this worker may work on this task until `ExpiresAt`. The task remains in backend processing while the lease is active; if no ack arrives before the deadline, the reaper moves it back to ready or to dead storage.

**Where:** `internal/core/lease.go:5` - `Lease` stores task, creation time, and expiry; `internal/core/engine.go:200` - `Fetch` creates the lease deadline from the requested timeout.

**What breaks if it is wrong:** 1) A worker fetches a task and dies. 2) There is no deadline, or the deadline is not recorded. 3) The reaper cannot know when the claim ended. 4) The task remains processing forever.

**Caught by:** `TestFetchCreatesLease`; `TestExpiredLeaseRequeuesTask`; `TestFetchInvalidTimeoutFails`.

### Recovery keeps the original deadline

Restart recovery must restore a lease with the deadline it originally had, not grant a fresh timeout. A fresh timeout lets a task be held for up to two visibility windows: one before the crash and one after restart, which breaks the consumer's assumption about when unacked work becomes visible again.

**Where:** `internal/core/engine.go:98` - the restore comment says open leases are reinstated only when their task and generation still match backend processing; `internal/core/engine.go:134` - restored leases use `record.ExpiresAt`.

**What breaks if it is wrong:** 1) A worker gets a 30-second lease. 2) The process crashes at second 29. 3) Restart grants a new 30 seconds. 4) Other workers cannot see the task until almost 60 seconds after the original fetch.

**Caught by:** `TestRestoredLeaseKeepsItsOriginalDeadline`; `TestRecoveryReconciliationKeepsRecoveredLeaseUntilOriginalDeadline`; `TestFetchCrashBoundaryAfterFsyncBeforeMemoryPublishRestoresOriginalDeadline`.

### At-least-once, not exactly-once

At-least-once means Moxy tries not to lose an accepted task: if a lease expires without ack, the task can be delivered again. It does not mean exactly-once; a worker can keep running after its deadline while another worker later receives the requeued task.

**Where:** `docs/failure-matrix.md:6` - the failure matrix explicitly says it does not claim exactly-once delivery; `internal/core/engine.go:300` - expiry requeues or dead-letters the task and records a closing operation.

**What breaks if it is wrong:** 1) Worker A fetches a task. 2) A is slow and misses the deadline. 3) The reaper requeues the task. 4) Worker B fetches it while A may still finish. 5) If the task is not idempotent, its side effect can happen twice.

**Caught by:** `TestReapExpiredBeforeAckRequeuesTaskAndAckFails`; `TestFetchCrashBoundaryAfterLeasePublishBeforeReplyUsesOriginalDeadline`; the F4-F6 rows in `docs/failure-matrix.md:56-58` cover duplicate-delivery risk after durable fetch.

### Attempts and dead-lettering

An attempt is the number of times a task has been returned from processing to be tried again. Expiry requeue increments it, and once `MaxAttempts` would be reached the engine moves the task to dead storage instead of ready.

**Where:** `internal/task/task.go:3` - tasks carry `Attempts`; `internal/core/engine.go:324` - the engine compares attempts to `MaxAttempts`; `internal/queue/redis_scripts.go:88` - Redis requeue increments attempts before `LPUSH`.

**What breaks if it is wrong:** 1) A poison task fails every worker. 2) Attempts never increment. 3) The task returns to ready forever. 4) Good tasks behind it keep competing with the same broken work, and the dead queue never records why it stopped.

**Caught by:** `RunBackendContractTests/RequeueIncrementsAttempts`; `TestDefaultMaxAttemptsAllowsRepeatedRequeue`; `TestMaxAttemptsOneMovesExpiredTaskToDeadLetter`; `TestMaxAttemptsTwoRequeuesOnceThenDeadLetters`; `TestRedisQueueDeadLetterMovesTaskToDeadKey`.
