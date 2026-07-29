package queue

import (
	"errors"
	"time"

	"github.com/tempoloss/moxy/internal/task"
)

var (
	ErrQueueEmpty        = errors.New("ready queue is empty")
	ErrTaskNotProcessing = errors.New("task is not processing")
)

// Stats reports queue-owned task counts.
type Stats struct {
	Ready      int
	Processing int
	Dead       int
}

// DeadTask records a task that has left the retry loop.
type DeadTask struct {
	Task   task.Task `json:"task"`
	Reason string    `json:"reason"`
	DeadAt time.Time `json:"dead_at"`
}

// Backend is the minimal ready-queue storage boundary used by the core engine.
type Backend interface {
	Enqueue(task task.Task) error
	Acquire() (task.Task, error)
	Complete(taskID string) error
	Requeue(taskID string) error
	DeadLetter(taskID string, reason string) error
	// RecoverOrphanedProcessing moves processing tasks that are not covered by
	// recovered active leases back to ready storage without incrementing
	// attempts. It is a startup-only reconciliation step after WAL replay.
	RecoverOrphanedProcessing(activeTaskIDs map[string]struct{}) (int, error)
	Stats() Stats
}
