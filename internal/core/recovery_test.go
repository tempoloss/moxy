package core

import (
	"errors"
	"testing"
	"time"

	"github.com/tempoloss/moxy/internal/queue"
	"github.com/tempoloss/moxy/internal/wal"
)

// recordingJournal stands in for a real WAL: it keeps every record in order and
// can be told to start failing, which is how the rollback path is exercised.
type recordingJournal struct {
	records []wal.Record
	failOn  wal.Op
}

func (j *recordingJournal) Append(record wal.Record) error {
	if j.failOn != "" && record.Op == j.failOn {
		return errors.New("journal is unavailable")
	}
	j.records = append(j.records, record)
	return nil
}

// restart models a process restart: the backend keeps its durable state while
// the engine is rebuilt from nothing but the journal.
func restart(backend queue.Backend, journal *recordingJournal, config EngineConfig) *Engine {
	config.Journal = journal
	config.Recovered = journal.records
	return NewEngineWithBackendAndConfig(backend, config)
}

func TestLeaseSurvivesRestart(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	lease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	recovered := restart(backend, journal, EngineConfig{})

	stats := recovered.Stats()
	if stats.ActiveLeases != 1 {
		t.Fatalf("recovered %d active leases, want 1", stats.ActiveLeases)
	}
	if stats.ExpirationHeap != 1 {
		t.Fatalf("expiration heap holds %d entries, want 1", stats.ExpirationHeap)
	}
	// The rebuilt lease must be the same claim, not a fresh one: acking by the
	// original id has to work.
	if err := recovered.Ack(lease.LeaseID); err != nil {
		t.Fatalf("ack of a recovered lease returned error: %v", err)
	}
}

func TestAckedLeaseIsNotRestored(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	lease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	if err := engine.Ack(lease.LeaseID); err != nil {
		t.Fatalf("ack returned error: %v", err)
	}

	recovered := restart(backend, journal, EngineConfig{})
	if got := recovered.Stats().ActiveLeases; got != 0 {
		t.Fatalf("recovered %d leases after an ack, want 0", got)
	}
}

func TestRestoredLeaseKeepsItsOriginalDeadline(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	if _, err := engine.Fetch(time.Millisecond); err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	// A lease that lapsed while the process was down must be reaped on the
	// first pass, not handed a fresh window it never earned.
	recovered := restart(backend, journal, EngineConfig{})
	requeued, err := recovered.ReapExpired(time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("reap returned error: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("reaped %d leases, want 1", requeued)
	}
	if got := recovered.Stats().Ready; got != 1 {
		t.Fatalf("ready queue holds %d tasks after the reap, want 1", got)
	}
}

func TestFetchReturnsTheTaskWhenTheJournalWriteFails(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{failOn: wal.OpFetch}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}

	if _, err := engine.Fetch(time.Minute); err == nil {
		t.Fatalf("fetch succeeded despite a failing journal")
	}

	// The task left the ready queue before the journal write was attempted, so
	// a failure has to put it back so nobody holds it without a durable record.
	stats := engine.Stats()
	if stats.Ready != 1 {
		t.Fatalf("ready queue holds %d tasks after a failed journal write, want 1", stats.Ready)
	}
	if stats.Processing != 0 {
		t.Fatalf("processing holds %d tasks, want 0", stats.Processing)
	}
	if stats.ActiveLeases != 0 {
		t.Fatalf("engine kept %d leases after a failed journal write, want 0", stats.ActiveLeases)
	}
}

func TestAckKeepsLeaseWhenCloseRecordFailsThenRetryMarksStale(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{failOn: wal.OpAck}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	lease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	if err := engine.Ack(lease.LeaseID); err == nil {
		t.Fatalf("ack succeeded despite a failing journal")
	}
	if stats := engine.Stats(); stats.ActiveLeases != 1 || stats.Processing != 0 {
		t.Fatalf("stats after failed ack record = %+v, want active=1 processing=0", stats)
	}

	journal.failOn = ""
	if err := engine.Ack(lease.LeaseID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("retry ack returned %v, want ErrLeaseNotFound", err)
	}
	if last := journal.records[len(journal.records)-1]; last.Op != wal.OpStale || last.LeaseID != lease.LeaseID {
		t.Fatalf("last journal record = %+v, want stale for %s", last, lease.LeaseID)
	}
	if stats := engine.Stats(); stats.ActiveLeases != 0 || stats.Processing != 0 {
		t.Fatalf("stats after stale retry = %+v, want active=0 processing=0", stats)
	}
}

func TestStaleAckKeepsLeaseWhenStaleRecordFails(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{failOn: wal.OpStale}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	oldLease, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch old lease returned error: %v", err)
	}
	if err := backend.Requeue(oldLease.Task.ID, oldLease.LeaseID); err != nil {
		t.Fatalf("manual requeue returned error: %v", err)
	}
	currentLease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch current lease returned error: %v", err)
	}

	if err := engine.Ack(oldLease.LeaseID); err == nil {
		t.Fatalf("stale ack succeeded despite a failing journal")
	}
	if stats := engine.Stats(); stats.ActiveLeases != 2 || stats.Processing != 1 {
		t.Fatalf("stats after failed stale ack record = %+v, want active=2 processing=1", stats)
	}

	journal.failOn = ""
	if err := engine.Ack(oldLease.LeaseID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("retry stale ack returned %v, want ErrLeaseNotFound", err)
	}
	if err := engine.Ack(currentLease.LeaseID); err != nil {
		t.Fatalf("ack current lease returned error: %v", err)
	}
}

func TestReapReleasesALeaseTheBackendAlreadyResolved(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	lease, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	// Reproduce the crash window between the backend completing a task and its
	// ack reaching the journal: the task is gone from the backend, but replay
	// still shows the lease as open.
	if err := backend.Complete(lease.Task.ID, lease.LeaseID); err != nil {
		t.Fatalf("complete returned error: %v", err)
	}
	recovered := restart(backend, journal, EngineConfig{})

	if got := recovered.Stats().ActiveLeases; got != 0 {
		t.Fatalf("engine recovered %d active leases, want 0", got)
	}
	if got := recovered.Stats().Ready; got != 0 {
		t.Fatalf("ready queue holds %d tasks, want 0", got)
	}
	if last := journal.records[len(journal.records)-1]; last.Op != wal.OpStale || last.LeaseID != lease.LeaseID {
		t.Fatalf("last journal record = %+v, want stale for %s", last, lease.LeaseID)
	}
	requeued, err := recovered.ReapExpired(time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("reap returned error on an already-resolved task: %v", err)
	}
	if requeued != 0 {
		t.Fatalf("reap released %d leases, want 0", requeued)
	}
}

func TestRecoveryKeepsOnlyMatchingLeaseGeneration(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	oldLease, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch old lease returned error: %v", err)
	}
	if err := backend.Requeue(oldLease.Task.ID, oldLease.LeaseID); err != nil {
		t.Fatalf("manual requeue returned error: %v", err)
	}
	currentLease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch current lease returned error: %v", err)
	}

	recovered := restart(backend, journal, EngineConfig{})
	if err := recovered.Ack(oldLease.LeaseID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("ack old lease returned %v, want ErrLeaseNotFound", err)
	}
	if err := recovered.Ack(currentLease.LeaseID); err != nil {
		t.Fatalf("ack current lease returned error: %v", err)
	}

	stats := recovered.Stats()
	if stats.Ready != 0 || stats.Processing != 0 || stats.ActiveLeases != 0 {
		t.Fatalf("stats after recovery ack = %+v, want ready=0 processing=0 active=0", stats)
	}
}

func TestRecoveryStaleRecordFailureBlocksPartialState(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	oldLease, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch old lease returned error: %v", err)
	}
	if err := backend.Requeue(oldLease.Task.ID, oldLease.LeaseID); err != nil {
		t.Fatalf("manual requeue returned error: %v", err)
	}
	currentLease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch current lease returned error: %v", err)
	}

	journal.failOn = wal.OpStale
	recovered := restart(backend, journal, EngineConfig{})
	if stats := recovered.Stats(); stats.ActiveLeases != 0 {
		t.Fatalf("recovered active leases = %d, want 0", stats.ActiveLeases)
	}
	if err := recovered.Ack(currentLease.LeaseID); !errors.Is(err, ErrRecoveryIncomplete) {
		t.Fatalf("ack after incomplete recovery returned %v, want ErrRecoveryIncomplete", err)
	}
}

func TestJournalRecordsEveryTransition(t *testing.T) {
	backend := queue.NewMemoryQueue()
	journal := &recordingJournal{}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journal, MaxAttempts: 1})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	if _, err := engine.Fetch(time.Millisecond); err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	if _, err := engine.ReapExpired(time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("reap returned error: %v", err)
	}

	if len(journal.records) != 2 {
		t.Fatalf("journal holds %d records, want 2", len(journal.records))
	}
	if journal.records[0].Op != wal.OpFetch {
		t.Fatalf("first record op = %q, want %q", journal.records[0].Op, wal.OpFetch)
	}
	// MaxAttempts of 1 makes the first expiry dead-letter before requeueing,
	// and the journal has to say so or recovery would replay the wrong outcome.
	if journal.records[1].Op != wal.OpDeadLetter {
		t.Fatalf("second record op = %q, want %q", journal.records[1].Op, wal.OpDeadLetter)
	}
}
