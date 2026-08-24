# ADR 0002: Reconcile orphaned processing at startup, do not reorder the fetch writes

## Status

Accepted.

Decided in `67a4ab4` (2026-07-29), with the crash coverage that motivated it in
`e2fda24`. Written down as an ADR on 2026-07-30, when the decisions that until
then lived only in commit messages were filed here.

## Context

`Fetch` performs two durable actions in order: it generates a lease ID, moves a
task out of the ready queue in the backend with that lease ID
(`e.ready.Acquire(leaseID)`), then appends a fetch record to the WAL. A crash in
between leaves the task in backend `processing` with a lease fence but no WAL
record that covers that task-plus-lease pair. The WAL has nothing to recover, so
the reaper has no heap item and no deadline ever expires. The task is invisible
to every worker while the queue reports itself empty.

`docs/failure-matrix.md` calls these windows F1 (crash after
`Acquire(leaseID)`, before any fetch bytes are durable) and F2 (crash during the
fetch record write, leaving a torn tail).

## Decision

Leave the write order alone and reconcile at startup.

After the WAL is replayed and the recovered leases are known, the engine asks the
backend which `processing` entries are covered by an exact task ID plus lease ID
fence and returns the rest to `ready`:

```go
RecoverOrphanedProcessing(activeLeases []queue.LeaseFence) (queue.RecoveryResult, error)
```

The memory backend and the Redis backend both implement it as one atomic
transition; in Redis it is a Lua script over the processing task hash,
`moxy:{queue}:processing:leases`, and the ready list. Returning a task this way
does **not** charge a retry attempt: nothing was ever delivered, so nothing was
retried. Recovered WAL leases without a matching backend fence are closed with
`wal.OpStale`.

It runs once, from recovery, and nowhere else.

## Alternatives rejected

**Journal the intent before acquiring.** The obvious symmetry -- write first, act
second -- does not work here, because the engine does not know *which* task it is
about to get. The backend chooses it inside `Acquire(leaseID)`. A pre-acquire
record could therefore only say "someone is about to fetch something", which
recovery cannot act on, and binding the id would need a second record: two fsyncs
per fetch to close a window that reconciliation closes with zero.

**A periodic sweep instead of a startup one.** A sweep that runs while workers are
alive cannot tell an orphan from a task that was legitimately acquired one
millisecond ago and whose fetch record is still in flight. It would steal live
work. Startup is the only moment when "no recovered lease" reliably means "no
owner".

**Accepting the loss and documenting it.** This was the state before `67a4ab4`,
and it is not a durability property anyone would choose: a task in `processing`
with no lease is lost until an operator finds it by hand.

## Consequences

F1 and F2 are closed as at-least-once, with no attempt charged, and the failure
matrix records that. The cost is that the backend interface carries a
startup-only method, which is a real wart: `queue.Backend` now has one operation
that is invalid to call during normal service, and the comment on it says so.
The newer lease-fence contract extends the same decision by matching recovered
leases on both task ID and lease ID, then writing `wal.OpStale` for recovered WAL
leases whose backend generation no longer matches.

Covered by `RunBackendContractTests/RecoverOrphanedProcessingMovesOnlyTasksWithoutActiveLease`,
`TestFetchCrashBoundaryAfterBackendAcquireBeforeJournalRequeuesOrphan`,
`TestTornFinalFetchRecordIsDroppedAndOrphanedTaskIsRequeued`, and
`TestRecoveryKeepsOnlyMatchingLeaseGeneration`, plus anti-regression tests that a
durably acked task is not resurrected and a recovered lease is not requeued
early.
