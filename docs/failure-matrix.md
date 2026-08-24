# Failure Matrix

This matrix enumerates the crash boundaries in the core fetch and ack paths as
implemented in `internal/core/engine.go`, the queue backends in
`internal/queue/`, and the WAL in `internal/wal/`. It does not claim
exactly-once delivery. The durable queue backend is authoritative for task
placement; the WAL is authoritative for rebuilding in-memory leases after a
restart.

The queue backends expose these atomic transitions to the engine:

- `Acquire(leaseID)`: ready -> processing. The engine generates the lease ID
  first and passes it in as the backend generation fence. Redis performs the
  ready pop and both processing hash writes with one Lua script; `MemoryQueue`
  does it under one mutex for tests.
- `Complete(taskID, leaseID)`: processing -> removed/completed, only if the
  stored fence still matches.
- `Requeue(taskID, leaseID)`: processing -> ready, incrementing `attempts`, only
  if the stored fence still matches.
- `DeadLetter(taskID, leaseID, reason)`: processing -> dead, incrementing
  `attempts`, only if the stored fence still matches.
- `RecoverOrphanedProcessing(activeLeases)`: startup-only reconciliation using
  exact task ID plus lease ID fences. Entries without an exact recovered fence
  return to `ready` without incrementing `attempts`; matched lease IDs are
  returned so the engine can keep only those WAL leases live.

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
3. Engine generates the lease ID.
4. Backend `Acquire(leaseID)` moves one task from ready to processing and stores
   that lease ID as the generation fence.
5. Engine builds the lease deadline around the acquired task and lease ID.
6. WAL append begins for `wal.OpFetch`.
7. WAL frame bytes may be partially or fully written.
8. WAL fsync succeeds.
9. Engine publishes the lease in memory and pushes the expiration heap item.
10. Engine replies to the client with the lease.

| Boundary | Where the task is after restart | Can it be lost? | Can it be delivered twice? | Reconciler | Guarantee | Test coverage |
| --- | --- | --- | --- | --- | --- | --- |
| F0: before backend `Acquire` starts | Still ready in the backend; no WAL record; no active lease. | No. | No. | Client retry fetches it. | At-least-once. | `TestFetchCrashBoundaryBeforeBackendAcquireLeavesTaskReady` |
| F1: after backend `Acquire(leaseID)`, before any fetch WAL bytes are durable | Returned to ready by startup processing reconciliation; no recovered active lease is created because no fetch record survived. | No. | No from the crashed fetch because no lease was published; a later fetch can deliver it once. | Startup `RecoverOrphanedProcessing` reconciles backend processing entries not covered by exact task-plus-lease fences. | At-least-once for the task; no retry attempt is charged during reconciliation. | `TestFetchCrashBoundaryAfterBackendAcquireBeforeJournalRequeuesOrphan` |
| F2: crash during fetch WAL write, leaving a torn final fetch record | Earlier intact WAL records recover with their original deadlines. The torn fetch record is discarded and truncated; its processing task has no recovered task-plus-lease fence and is returned to ready. | No. | No for the torn task from the crashed fetch. Earlier recovered leases can be redelivered after their original deadline. | WAL truncates the torn tail; startup `RecoverOrphanedProcessing` reconciles the processing task whose fetch record was torn. | At-least-once for the torn task; earlier intact leases remain at-least-once with original deadlines. | `TestTornFinalFetchRecordIsDroppedAndOrphanedTaskIsRequeued` |
| F3: after a complete fetch record is written, before fsync returns | If the complete record survives on disk and its task-plus-lease fence still matches backend processing, recovery restores the active lease with the original deadline. If the record is absent, torn, or no longer matches backend processing, startup reconciliation returns the unmatched processing task to ready and writes `wal.OpStale` for the unmatched WAL lease. | No. | Yes in the surviving matched-record branch after the original deadline if work continued elsewhere; no in the absent/torn branch from the crashed fetch. | Recovery restores surviving exact fences; startup `RecoverOrphanedProcessing` handles absent, torn, and mismatched branches. | At-least-once. No fsync means no promise about whether recovery preserves the original deadline or immediately makes the task ready. | `TestFetchCrashBoundaryAfterAppendBeforeFsyncRestoresIfRecordSurvives`; absent/torn branches covered by F1/F2 tests; mismatched generation covered by `TestRecoveryKeepsOnlyMatchingLeaseGeneration`. |
| F4: after fsync, before in-memory lease publication | Backend processing plus durable fetch WAL record with a matching task-plus-lease fence. Restart restores one active lease and one expiration heap entry with the original deadline. | No. | Yes after the original deadline if work is retried or the original worker resumes. | Recovery keeps only the matching backend fence; reaper redelivers only after the preserved deadline. | At-least-once, not at-most-once. | `TestFetchCrashBoundaryAfterFsyncBeforeMemoryPublishRestoresOriginalDeadline` |
| F5: after in-memory lease publication, before the fetch reply reaches the client | Same as F4 after restart when the fence still matches: backend processing plus recovered active lease. If the client never saw the reply, the task waits until the original deadline and is then requeued or dead-lettered. | No. | Possible after the deadline if a worker did receive the lease but the response or connection failed ambiguously. | Recovery plus reaper; client retry may fetch after requeue. | At-least-once, not at-most-once. | `TestFetchCrashBoundaryAfterLeasePublishBeforeReplyUsesOriginalDeadline` |
| F6: after the fetch reply reaches the client | Same durable state as F5 while the fence matches. The client may ack before expiry after restart; otherwise the reaper transitions the lease after the original deadline. | No, assuming the fetch record was fsynced. | Yes if the original client continues past the lease deadline and the task is requeued. | Client ack or reaper. | At-least-once, not at-most-once. | Covered by the same durable-state tests as F5 and existing recovery tests. |

## Ack path

Ordered steps:

1. Client sends `Ack(leaseID)`.
2. Engine finds the active lease in memory.
3. Backend `Complete(taskID, leaseID)` removes the task and its lease fence from processing.
4. WAL append begins for `wal.OpAck`.
5. WAL frame bytes may be partially or fully written.
6. WAL fsync succeeds.
7. Engine deletes the lease from memory.
8. Engine replies to the client.

| Boundary | Where the task is after restart | Can it be lost? | Can it be delivered twice? | Reconciler | Guarantee | Test coverage |
| --- | --- | --- | --- | --- | --- | --- |
| A0: before backend `Complete(taskID, leaseID)` starts | Backend processing plus recovered active fetch lease with a matching fence. | No. | Yes if the lease expires before the client retries ack; otherwise retrying ack completes it. | Client retry can ack; reaper requeues after the original deadline. | At-least-once until completion, not at-most-once. | `TestAckCrashBoundaryBeforeBackendCompleteKeepsLeaseRetryable` |
| A1: after backend `Complete(taskID, leaseID)`, before any ack WAL bytes are durable | Backend has already removed the task and its fence. WAL still shows the fetch lease as open, so restart sees no matching processing fence, appends `wal.OpStale`, and does not restore the lease. If the same process retries ACK before restart, `Complete` reports the stale lease; the engine appends `wal.OpStale`, deletes only that old lease after the append succeeds, and returns `ErrLeaseNotFound`. | No: completion already happened. The stale lease is durably closed, not resurrected. | No; the backend no longer has the task and neither recovery nor the reaper puts it back. | Startup recovery or ACK retry writes `wal.OpStale`; reaper uses the same stale close path for expired stale leases. | At-most-once after backend completion. | `TestAckCrashBoundaryAfterBackendCompleteBeforeJournalIsReconciledAtStartup`; `TestAckKeepsLeaseWhenCloseRecordFailsThenRetryMarksStale` |
| A2: crash during ack WAL write, leaving a torn final ack record | WAL drops/truncates the torn ack. Restart is the same logical state as A1: completed in backend, stale open lease from the fetch record, no matching processing fence. | No. | No; recovery marks the unmatched lease stale and must not resurrect the completed task. | WAL truncates the torn tail; startup recovery appends `wal.OpStale` for the unmatched lease. | At-most-once after backend completion. | `TestTornFinalAckRecordIsDroppedAndReaperDoesNotResurrectCompletedTask` |
| A3: after a complete ack record is written, before fsync returns | If the complete ack record survives on disk, replay folds the lease closed: no ready task, no processing task, no active lease. If the record is absent or torn, this collapses to A1/A2. | No: backend completion already happened. | No. | Surviving ack record is reconciled by recovery; absent/torn record by the stale close path in A1/A2. | Conditional at-most-once; no fsync means no promise that recovery will see the ack record. | `TestAckCrashBoundaryAfterAppendBeforeFsyncClosesIfRecordSurvives`; absent/torn branches covered by A1/A2 tests. |
| A4: after ack fsync, before in-memory lease deletion | Backend completed and WAL durably closed the lease. Restart has no task in ready or processing and no active lease. | No. | No. | Recovery folds the fetch+ack record stream closed. | At-most-once after completion. | `TestAckCrashBoundaryAfterFsyncBeforeMemoryDeleteKeepsTaskCompleted` |
| A5: after in-memory lease deletion, before the ack reply reaches the client | Same as A4 after restart. If the client retries because it missed the reply, it receives `ErrLeaseNotFound`; the task remains completed. | No. | No. | Recovery; client retry observes that the lease is gone. | At-most-once after completion. | `TestAckCrashBoundaryAfterMemoryDeleteBeforeReplyRemainsCompleted` |
| A6: after the ack reply reaches the client | Same durable state as A5. | No. | No. | None needed. | At-most-once after completion. | Covered by A5 and existing ack recovery tests. |

## Expiry and reaper path

Ordered steps:

1. Reaper calls `ReapExpired(now)`.
2. Engine pops an expired heap item and verifies the lease map still has that
   lease ID.
3. Engine chooses `Requeue(taskID, leaseID)` or
   `DeadLetter(taskID, leaseID, reason)` based on `MaxAttempts`.
4. Backend applies the transition only if the stored task-plus-lease fence still
   matches.
5. WAL append begins for `wal.OpExpire`, `wal.OpDeadLetter`, or `wal.OpStale`.
6. WAL frame bytes may be partially or fully written.
7. WAL fsync succeeds.
8. Engine removes only that lease ID from memory.

| Boundary | Where the task is after restart or retry | Can it be lost? | Can it be delivered twice? | Reconciler | Guarantee | Test coverage |
| --- | --- | --- | --- | --- | --- | --- |
| E0: before backend mutation | Backend processing plus recovered active lease with its original deadline. | No. | Yes if the worker continues past the deadline and the reaper later requeues or dead-letters according to attempts. | Reaper retry with the same lease generation. | At-least-once until the expiry transition lands. | `TestReapExpiredDoesNotDeleteLeaseIfBackendRequeueFails`; `TestReapExpiredDoesNotDeleteLeaseIfBackendDeadLetterFails` |
| E1: backend requeue or dead-letter applied before close WAL append | Backend has moved the task to ready or dead and removed the old fence, but WAL still shows the old fetch lease open. A retry with the same old lease observes missing or mismatched processing and must write `wal.OpStale` before dropping that old lease. | No. | Requeue branch can redeliver; dead-letter branch cannot. A stale retry cannot mutate a newer lease. | Retry sees `ErrTaskNotProcessing` or `ErrLeaseFenceMismatch` and appends `wal.OpStale`. | At-least-once for requeue, at-most-once after dead-letter. | Covered by the named generation regression `TestAmbiguousPostApplyExpiryRetryDoesNotStealNewLease`. |
| E2: torn close record after backend mutation | WAL truncates the torn `expire`, `dead_letter`, or `stale` record. Restart sees an open old fetch lease, reconciles it against backend processing, writes `wal.OpStale` if no exact fence matches, and keeps only matching leases live. | No. | Requeue branch can redeliver; the stale close must not touch any newer matching lease. | WAL truncation plus startup task-plus-lease reconciliation. | Same as E1, with no durability promised for the torn close record. | Covered by the named generation regression `TestAmbiguousPostApplyExpiryRetryDoesNotStealNewLease`. |
| E3: surviving close record after backend mutation | WAL replay folds the lease closed. Requeued tasks are in ready with incremented attempts; dead-lettered tasks are in dead storage; stale records leave newer leases alone. | No. | Requeue branch can redeliver; dead-letter and stale branches do not. | WAL replay. | Durable close. | `TestExpiredLeaseRequeuesTask`; `TestMaxAttemptsOneMovesExpiredTaskToDeadLetter`; stale branch covered by the named generation regression `TestAmbiguousPostApplyExpiryRetryDoesNotStealNewLease`. |
| E4: stale retry after newer lease | Old retry uses the old lease ID against a task now processing under a newer lease ID. Backend returns `ErrLeaseFenceMismatch`; the engine writes `wal.OpStale`, removes only the old lease after that append succeeds, and leaves the current lease processing and ackable. | No. | The current lease remains valid; no old transition can steal it. | Backend lease fence plus `wal.OpStale`. | Exact generation fencing. | `TestAmbiguousPostApplyExpiryRetryDoesNotStealNewLease` |

## Findings

- Startup recovery now reconciles backend `processing` against the WAL's
  recovered live task-plus-lease fences. Processing tasks without an exact
  recovered fence are returned to `ready` without incrementing `attempts`,
  closing F1 and F2 as at-least-once windows instead of stranded-task windows.
  Recovered WAL leases whose fences no longer match are closed with
  `wal.OpStale`.
- Unsynced append windows are explicitly conditional. A complete fetch record
  that happens to survive can be recovered with its original deadline only while
  the backend still has the same task under the same lease ID. An absent, torn,
  or mismatched fetch record is not treated as a live lease.
- The ack path favors not resurrecting completed work. If backend completion
  lands but the ack record does not, the restored lease is stale. ACK retry,
  startup recovery, and the reaper all close stale leases by appending
  `wal.OpStale` before dropping the old in-memory lease. Stale ACK returns
  `ErrLeaseNotFound` after that durable close. If the ack did become durable,
  recovery folds the lease closed and there is no backend processing entry for
  startup reconciliation to return to ready.
