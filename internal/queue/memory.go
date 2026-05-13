package queue

import (
	"sync"

	"github.com/an8kk/moxy/internal/task"
)

// MemoryQueue stores ready tasks in memory using FIFO ordering.
type MemoryQueue struct {
	mu         sync.Mutex
	ready      []task.Task
	processing map[string]task.Task
}

// NewMemoryQueue creates an empty in-memory queue backend.
func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		ready:      make([]task.Task, 0),
		processing: make(map[string]task.Task),
	}
}

// Enqueue appends a task to ready storage.
func (q *MemoryQueue) Enqueue(task task.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ready = append(q.ready, cloneTask(task))
	return nil
}

// Acquire moves one ready task into processing storage.
func (q *MemoryQueue) Acquire() (task.Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.ready) == 0 {
		return task.Task{}, ErrQueueEmpty
	}

	next := q.ready[0]
	copy(q.ready, q.ready[1:])
	last := len(q.ready) - 1
	q.ready[last] = task.Task{}
	q.ready = q.ready[:last]
	q.processing[next.ID] = cloneTask(next)

	return cloneTask(next), nil
}

// Complete removes a processing task.
func (q *MemoryQueue) Complete(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.processing[taskID]; !ok {
		return ErrTaskNotProcessing
	}

	delete(q.processing, taskID)
	return nil
}

// Requeue moves a processing task back to ready storage.
func (q *MemoryQueue) Requeue(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.processing[taskID]
	if !ok {
		return ErrTaskNotProcessing
	}

	delete(q.processing, taskID)
	q.ready = append(q.ready, cloneTask(task))
	return nil
}

// Stats reports ready and processing task counts.
func (q *MemoryQueue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	return Stats{
		Ready:      len(q.ready),
		Processing: len(q.processing),
	}
}

func cloneTask(item task.Task) task.Task {
	return task.Task{
		ID:      item.ID,
		Payload: cloneBytes(item.Payload),
	}
}

func cloneBytes(payload []byte) []byte {
	if payload == nil {
		return nil
	}

	cloned := make([]byte, len(payload))
	copy(cloned, payload)
	return cloned
}
