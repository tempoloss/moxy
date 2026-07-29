# Failure Matrix

This matrix enumerates the crash boundaries in the core fetch and ack paths as
implemented in `internal/core/engine.go`, the queue backends in
`internal/queue/`, and the WAL in `internal/wal/`. It does not claim
exactly-once delivery. The durable queue backend is authoritative for task
placement; the WAL is authoritative for rebuilding in-memory leases after a
restart.

The queue backends expose these atomic transitions to the engine:

- `Acquire`: ready -> processing. Redis performs this with one Lua script;
  `MemoryQueue` does it under one mutex for tests.
- `Complete`: processing -> removed/completed.
- `Requeue`: processing -> ready, incrementing `attempts`.
- `DeadLetter`: processing -> dead, incrementing `attempts`.
- `RecoverOrphanedProcessing`: startup-only processing -> ready for processing
  entries that are not covered by recovered WAL leases. It does not increment
  `attempts` because no durable lease reached a worker.

The WAL stores length-prefixed CRC records. `Open` replays intact records,
discards the first torn or corrupt tail record, and truncates the file to the
last good offset. With the default WAL (`wal.Open`), `Append` writes and fsyncs
before returning. Rows that mention an unsynced append describe the real disk
states possible if the process dies inside `Append`, or if the WAL is opened with
`Options{Sync:false}`.

## Fetch path

Ordered steps:

1. Client sends `Fetch`.
2. Engine validates timeout.
3. Backend `Acquire` moves one task from ready to processing.
4. Engine builds a lease ID and original deadline.
5. WAL append begins for `wal.OpFetch`.
6. WAL frame bytes may be partially or fully written.
7. WAL fsync succeeds.
8. Engine publishes the lease in memory and pushes the expiration heap item.
9. Engine replies to the client with the lease.

| Boundary | Where the task is after restart | Can it be lost? | Can it be delivered twice? | Reconciler | Guarantee | Test coverage |
| --- | --- | --- | --- | --- | --- | --- |
| F0: before backend `Acquire` starts | Still ready in the backend; no WAL record; no active lease. | No. | No. | Client retry fetches it. | At-least-once. | `TestFetchCrashBoundaryBeforeBackendAcquireLeavesTaskReady` |
| F1: after backend `Acquire`, before any fetch WAL bytes are durable | Returned to ready by startup processing reconciliation; no recovered active lease is created because no fetch record survived. | No. | No from the crashed fetch because no lease was published; a later fetch can deliver it once. | Startup `RecoverOrphanedProcessing` reconciles backend processing entries not covered by recovered leases. | At-least-once for the task; no retry attempt is charged during reconciliation. | `TestFetchCrashBoundaryAfterBackendAcquireBeforeJournalRequeuesOrphan` |
| F2: crash during fetch WAL write, leaving a torn final fetch record | Earlier intact WAL records recover with their original deadlines. The torn fetch record is discarded and truncated; its processing task has no recovered lease and is returned to ready. | No. | No for the torn task from the crashed fetch. Earlier recovered leases can be redelivered after their original deadline. | WAL truncates the torn tail; startup `RecoverOrphanedProcessing` reconciles the processing task whose fetch record was torn. | At-least-once for the torn task; earlier intact leases remain at-least-once with original deadlines. | `TestTornFinalFetchRecordIsDroppedAndOrphanedTaskIsRequeued` |
| F3: after a complete fetch record is written, before fsync returns | If the complete record survives on disk, recovery restores the active lease with the original deadline while the backend still has the task in processing. If the record is absent or torn, startup reconciliation returns the orphaned processing task to ready. | No. | Yes in the surviving-record branch after the original deadline if work continued elsewhere; no in the absent/torn branch from the crashed fetch. | Recovery restores surviving records; startup `RecoverOrphanedProcessing` handles absent/torn branches. | At-least-once. No fsync means no promise about whether recovery preserves the original deadline or immediately makes the task ready. | `TestFetchCrashBoundaryAfterAppendBeforeFsyncRestoresIfRecordSurvives`; absent/torn branches covered by F1/F2 tests. |
| F4: after fsync, before in-memory lease publication | Backend processing plus durable fetch WAL record. Restart restores one active lease and one expiration heap entry with the original deadline. | No. | Yes after the original deadline if work is retried or the original worker resumes. | Recovery restores; reaper redelivers only after the preserved deadline. | At-least-once, not at-most-once. | `TestFetchCrashBoundaryAfterFsyncBeforeMemoryPublishRestoresOriginalDeadline` |
| F5: after in-memory lease publication, before the fetch reply reaches the client | Same as F4 after restart: backend processing plus recovered active lease. If the client never saw the reply, the task waits until the original deadline and is then requeued. | No. | Possible after the deadline if a worker did receive the lease but the response/connection failed ambiguously. | Recovery plus reaper; client retry may fetch after requeue. | At-least-once, not at-most-once. | `TestFetchCrashBoundaryAfterLeasePublishBeforeReplyUsesOriginalDeadline` |
| F6: after the fetch reply reaches the client | Same durable state as F5. The client may ack before expiry after restart; otherwise the reaper requeues after the original deadline. | No, assuming the fetch record was fsynced. | Yes if the original client continues past the lease deadline and the task is requeued. | Client ack or reaper. | At-least-once, not at-most-once. | Covered by the same durable-state tests as F5 and existing recovery tests. |

## Ack path

Ordered steps:

1. Client sends `Ack(leaseID)`.
2. Engine finds the active lease in memory.
3. Backend `Complete` removes the task from processing.
4. WAL append begins for `wal.OpAck`.
5. WAL frame bytes may be partially or fully written.
6. WAL fsync succeeds.
7. Engine deletes the lease from memory.
8. Engine replies to the client.

| Boundary | Where the task is after restart | Can it be lost? | Can it be delivered twice? | Reconciler | Guarantee | Test coverage |
| --- | --- | --- | --- | --- | --- | --- |
| A0: before backend `Complete` starts | Backend processing plus recovered active fetch lease. | No. | Yes if the lease expires before the client retries ack; otherwise retrying ack completes it. | Client retry can ack; reaper requeues after the original deadline. | At-least-once until completion, not at-most-once. | `TestAckCrashBoundaryBeforeBackendCompleteKeepsLeaseRetryable` |
| A1: after backend `Complete`, before any ack WAL bytes are durable | Backend has already removed the task. WAL still shows the fetch lease as open, so restart restores an active lease that points at no processing task. A client retry before reaping gets `queue.ErrTaskNotProcessing`. On expiry, the reaper treats that backend error as evidence the task was already resolved, appends a closing record, and drops the lease. | No: completion already happened. The temporary restored lease is stale, not a resurrected task. | No; the backend no longer has the task and the reaper does not put it back. | Reaper reconciles. Client retry surfaces the backend error but does not currently clear the stale lease. | At-most-once after backend completion. Named weakness: ack retry can report an error for an already-completed task until the reaper runs. | `TestAckCrashBoundaryAfterBackendCompleteBeforeJournalIsReconciledByReaper` |
| A2: crash during ack WAL write, leaving a torn final ack record | WAL drops/truncates the torn ack. Restart is the same logical state as A1: completed in backend, stale active lease from the fetch record. | No. | No; reaper must not resurrect the completed task. | WAL truncates the torn tail; reaper closes the stale lease on `ErrTaskNotProcessing`. | At-most-once after backend completion. | `TestTornFinalAckRecordIsDroppedAndReaperDoesNotResurrectCompletedTask` |
| A3: after a complete ack record is written, before fsync returns | If the complete ack record survives on disk, replay folds the lease closed: no ready task, no processing task, no active lease. If the record is absent or torn, this collapses to A1/A2. | No: backend completion already happened. | No. | Surviving ack record is reconciled by recovery; absent/torn record by the reaper path in A1/A2. | Conditional at-most-once; no fsync means no promise that recovery will see the ack record. | `TestAckCrashBoundaryAfterAppendBeforeFsyncClosesIfRecordSurvives`; absent/torn branches covered by A1/A2 tests. |
| A4: after ack fsync, before in-memory lease deletion | Backend completed and WAL durably closed the lease. Restart has no task in ready or processing and no active lease. | No. | No. | Recovery folds the fetch+ack record stream closed. | At-most-once after completion. | `TestAckCrashBoundaryAfterFsyncBeforeMemoryDeleteKeepsTaskCompleted` |
| A5: after in-memory lease deletion, before the ack reply reaches the client | Same as A4 after restart. If the client retries because it missed the reply, it receives `ErrLeaseNotFound`; the task remains completed. | No. | No. | Recovery; client retry observes that the lease is gone. | At-most-once after completion. | `TestAckCrashBoundaryAfterMemoryDeleteBeforeReplyRemainsCompleted` |
| A6: after the ack reply reaches the client | Same durable state as A5. | No. | No. | None needed. | At-most-once after completion. | Covered by A5 and existing ack recovery tests. |

## Findings

- Startup recovery now reconciles backend `processing` against the WAL's
  recovered live leases. Processing tasks with no recovered lease are returned to
  `ready` without incrementing `attempts`, closing F1 and F2 as at-least-once
  windows instead of stranded-task windows.
- Unsynced append windows are explicitly conditional. A complete fetch record
  that happens to survive can be recovered with its original deadline; an absent
  or torn fetch record is not treated as durable and falls back to startup
  processing reconciliation.
- The ack path favors not resurrecting completed work. If backend completion
  lands but the ack record does not, the restored lease is stale. The client may
  see an error when retrying ack before the reaper runs, but the reaper closes
  the stale lease without requeueing the completed task. If the ack did become
  durable, recovery folds the lease closed and there is no backend processing
  entry for startup reconciliation to return to ready.
