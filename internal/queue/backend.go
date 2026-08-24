package queue

import (
	"errors"
	"time"

	"github.com/tempoloss/moxy/internal/task"
)

var (
	ErrQueueEmpty         = errors.New("ready queue is empty")
	ErrTaskNotProcessing  = errors.New("task is not processing")
	ErrLeaseFenceMismatch = errors.New("lease generation mismatch")
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

// LeaseFence names the task and backend generation a recovered lease still owns.
type LeaseFence struct {
	TaskID  string
	LeaseID string
}

// RecoveryResult reports startup reconciliation effects.
type RecoveryResult struct {
	Moved           int
	MatchedLeaseIDs map[string]struct{}
}

// Backend is the minimal ready-queue storage boundary used by the core engine.
type Backend interface {
	Enqueue(task task.Task) error
	Acquire(leaseID string) (task.Task, error)
	Complete(taskID, leaseID string) error
	Requeue(taskID, leaseID string) error
	DeadLetter(taskID, leaseID, reason string) error
	// RecoverOrphanedProcessing moves processing tasks that are not covered by
	// recovered active leases back to ready storage without incrementing
	// attempts. It is a startup-only reconciliation step after WAL replay.
	RecoverOrphanedProcessing(activeLeases []LeaseFence) (RecoveryResult, error)
	Stats() Stats
}
