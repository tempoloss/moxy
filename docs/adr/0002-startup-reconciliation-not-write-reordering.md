# ADR 0002: Reconcile orphaned processing at startup, do not reorder the fetch writes

## Status

Accepted.

Decided in `67a4ab4` (2026-07-29), with the crash coverage that motivated it in
`e2fda24`. Written down as an ADR on 2026-07-30, when the decisions that until
then lived only in commit messages were filed here.

## Context

`Fetch` performs two durable actions in order: it moves a task out of the ready
queue in the backend (`e.ready.Acquire()`), then it appends a fetch record to the
WAL. A crash in between leaves the task in backend `processing` with no lease
anywhere: the WAL has nothing to recover, so the reaper has no heap item and no
deadline ever expires. The task is invisible to every worker while the queue
reports itself empty.

`docs/failure-matrix.md` calls these windows F1 (crash after `Acquire`, before any
fetch bytes are durable) and F2 (crash during the fetch record write, leaving a
torn tail).

## Decision

Leave the write order alone and reconcile at startup.

After the WAL is replayed and the recovered leases are known, the engine asks the
backend which `processing` entries are *not* covered by a recovered lease and
returns exactly those to `ready`:

```go
RecoverOrphanedProcessing(activeTaskIDs)   // queue.Backend
```

The memory backend and the Redis backend both implement it as one atomic
transition; in Redis it is a Lua script over the processing hash and the ready
list. Returning a task this way does **not** charge a retry attempt: nothing was
ever delivered, so nothing was retried.

It runs once, from recovery, and nowhere else.

## Alternatives rejected

**Journal the intent before acquiring.** The obvious symmetry -- write first, act
second -- does not work here, because the engine does not know *which* task it is
about to get. The backend chooses it inside `Acquire`. A pre-acquire record could
therefore only say "someone is about to fetch something", which recovery cannot
act on, and binding the id would need a second record: two fsyncs per fetch to
close a window that reconciliation closes with zero.

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
matrix records that. The cost is that the backend interface carries a startup-only
method, which is a real wart: `queue.Backend` now has one operation that is
invalid to call during normal service, and the comment on it says so.

Covered by `RunBackendContractTests/RecoverOrphanedProcessingMovesOnlyTasksWithoutActiveLease`,
`TestFetchCrashBoundaryAfterBackendAcquireBeforeJournalRequeuesOrphan`, and
`TestTornFinalFetchRecordIsDroppedAndOrphanedTaskIsRequeued`, plus anti-regression
tests that a durably acked task is not resurrected and a recovered lease is not
requeued early.
