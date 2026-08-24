package core

import (
	"testing"
	"time"

	"github.com/tempoloss/moxy/internal/queue"
	"github.com/tempoloss/moxy/internal/task"
	"github.com/tempoloss/moxy/internal/wal"
)

// orderSpy records the sequence of durable actions a call performs.
type orderSpy struct {
	queue.Backend
	steps *[]string
}

func (s orderSpy) Complete(taskID, leaseID string) error {
	*s.steps = append(*s.steps, "backend.Complete")
	return s.Backend.Complete(taskID, leaseID)
}

type journalSpy struct {
	steps *[]string
}

func (j journalSpy) Append(record wal.Record) error {
	*j.steps = append(*j.steps, "journal."+string(record.Op))
	return nil
}

// Ack completes in the backend before it journals, and the order is the whole
// design. Reversing it compiles and passes every other test in this package.
func TestAckCompletesBackendBeforeJournalling(t *testing.T) {
	var steps []string
	backend := orderSpy{Backend: queue.NewMemoryQueue(), steps: &steps}
	engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Journal: journalSpy{steps: &steps}})

	if _, err := engine.Enqueue([]byte("work")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	lease, err := engine.Fetch(30 * time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	steps = nil // only the ack sequence is under test

	if err := engine.Ack(lease.LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	want := []string{"backend.Complete", "journal.ack"}
	if len(steps) != len(want) || steps[0] != want[0] || steps[1] != want[1] {
		t.Fatalf("ack must complete then journal, got %v\nsee TestAckJournalFirstWouldRedeliverCompletedWork for why", steps)
	}
}

// Why that order, and not the symmetric one Fetch uses.
//
// Reversing it puts the crash window between a durable ack record and a backend
// task still sitting in processing. This builds that exact recovery state by
// hand, because the current code cannot produce it.
func TestAckJournalFirstWouldRedeliverCompletedWork(t *testing.T) {
	const taskID, leaseID = "t1", "L1"
	fetched := time.Now().Add(-time.Minute)
	fetchRecord := wal.Record{
		Op:        wal.OpFetch,
		LeaseID:   leaseID,
		Task:      task.Task{ID: taskID, Payload: []byte("work")},
		CreatedAt: fetched,
		ExpiresAt: fetched.Add(30 * time.Second),
	}

	// The worker finished the work; only the bookkeeping is in question.
	seed := func(t *testing.T) queue.Backend {
		t.Helper()
		backend := queue.NewMemoryQueue()
		if err := backend.Enqueue(task.Task{ID: taskID, Payload: []byte("work")}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, err := backend.Acquire(leaseID); err != nil {
			t.Fatalf("acquire: %v", err)
		}
		return backend
	}

	t.Run("shipped order marks the already-completed lease stale", func(t *testing.T) {
		backend := seed(t)
		if err := backend.Complete(taskID, leaseID); err != nil {
			t.Fatalf("complete: %v", err)
		}
		engine := NewEngineWithBackendAndConfig(backend, EngineConfig{Recovered: []wal.Record{fetchRecord}})

		if got := engine.Stats(); got.Ready != 0 {
			t.Fatalf("completed work must not return to ready, got %+v", got)
		}
		if _, err := engine.ReapExpired(time.Now()); err != nil {
			t.Fatalf("reap: %v", err)
		}
		if got := engine.Stats(); got.Ready != 0 || got.ActiveLeases != 0 {
			t.Fatalf("stale lease must not resurrect the task, got %+v", got)
		}
	})

	t.Run("reversed order hands completed work out a second time", func(t *testing.T) {
		backend := seed(t)
		engine := NewEngineWithBackendAndConfig(backend, EngineConfig{
			Recovered: []wal.Record{fetchRecord, {Op: wal.OpAck, LeaseID: leaseID}},
		})

		// Not stranded: reconciliation sees processing with no recovered lease.
		if got := engine.Stats(); got.Processing != 0 || got.Ready != 1 {
			t.Fatalf("expected the orphan back in ready, got %+v", got)
		}
		lease, err := engine.Fetch(30 * time.Second)
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		if lease.Task.ID != taskID {
			t.Fatalf("expected %s redelivered, got %s", taskID, lease.Task.ID)
		}
		// Reconciliation charges no attempt, so MaxAttempts cannot bound this.
		if lease.Task.Attempts != 0 {
			t.Fatalf("reconciliation must not charge an attempt, got %d", lease.Task.Attempts)
		}
	})
}
