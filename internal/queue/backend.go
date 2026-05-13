package queue

import (
	"errors"

	"github.com/an8kk/moxy/internal/task"
)

var (
	ErrQueueEmpty        = errors.New("ready queue is empty")
	ErrTaskNotProcessing = errors.New("task is not processing")
)

// Stats reports queue-owned task counts.
type Stats struct {
	Ready      int
	Processing int
}

// Backend is the minimal ready-queue storage boundary used by the core engine.
type Backend interface {
	Enqueue(task task.Task) error
	Acquire() (task.Task, error)
	Complete(taskID string) error
	Requeue(taskID string) error
	Stats() Stats
}
