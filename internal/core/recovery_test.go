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
	// a failure has to put it back rather than leave it held by nobody.
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
	if err := backend.Complete(lease.Task.ID); err != nil {
		t.Fatalf("complete returned error: %v", err)
	}
	recovered := restart(backend, journal, EngineConfig{})

	requeued, err := recovered.ReapExpired(time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("reap returned error on an already-resolved task: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("reap released %d leases, want 1", requeued)
	}
	if got := recovered.Stats().ActiveLeases; got != 0 {
		t.Fatalf("engine still holds %d leases, want 0", got)
	}
	// Nothing may be resurrected: the task was already completed.
	if got := recovered.Stats().Ready; got != 0 {
		t.Fatalf("ready queue holds %d tasks, want 0", got)
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
	// MaxAttempts of 1 means the first expiry dead-letters rather than requeues,
	// and the journal has to say so or recovery would replay the wrong outcome.
	if journal.records[1].Op != wal.OpDeadLetter {
		t.Fatalf("second record op = %q, want %q", journal.records[1].Op, wal.OpDeadLetter)
	}
}
