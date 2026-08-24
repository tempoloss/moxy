package queue

import (
	"sync"
	"time"

	"github.com/tempoloss/moxy/internal/task"
)

// MemoryQueue stores ready tasks in memory using FIFO ordering.
type MemoryQueue struct {
	mu         sync.Mutex
	ready      []task.Task
	processing map[string]memoryProcessingEntry
	dead       []DeadTask
}

type memoryProcessingEntry struct {
	task    task.Task
	leaseID string
}

// NewMemoryQueue creates an empty in-memory queue backend.
func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		ready:      make([]task.Task, 0),
		processing: make(map[string]memoryProcessingEntry),
		dead:       make([]DeadTask, 0),
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
func (q *MemoryQueue) Acquire(leaseID string) (task.Task, error) {
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
	q.processing[next.ID] = memoryProcessingEntry{
		task:    cloneTask(next),
		leaseID: leaseID,
	}

	return cloneTask(next), nil
}

// Complete removes a processing task.
func (q *MemoryQueue) Complete(taskID, leaseID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	entry, ok := q.processing[taskID]
	if !ok {
		return ErrTaskNotProcessing
	}
	if entry.leaseID != leaseID {
		return ErrLeaseFenceMismatch
	}

	delete(q.processing, taskID)
	return nil
}

// Requeue moves a processing task back to ready storage.
func (q *MemoryQueue) Requeue(taskID, leaseID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	entry, ok := q.processing[taskID]
	if !ok {
		return ErrTaskNotProcessing
	}
	if entry.leaseID != leaseID {
		return ErrLeaseFenceMismatch
	}

	delete(q.processing, taskID)
	task := entry.task
	task.Attempts++
	q.ready = append(q.ready, cloneTask(task))
	return nil
}

// RecoverOrphanedProcessing returns processing tasks without a recovered active
// lease to ready storage. These tasks were acquired by the backend but never
// durably leased, so returning them must not burn a retry attempt.
func (q *MemoryQueue) RecoverOrphanedProcessing(activeLeases []LeaseFence) (RecoveryResult, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	active := make(map[LeaseFence]struct{}, len(activeLeases))
	for _, lease := range activeLeases {
		active[lease] = struct{}{}
	}

	result := RecoveryResult{MatchedLeaseIDs: make(map[string]struct{})}
	for taskID, entry := range q.processing {
		if _, ok := active[LeaseFence{TaskID: taskID, LeaseID: entry.leaseID}]; ok {
			result.MatchedLeaseIDs[entry.leaseID] = struct{}{}
			continue
		}
		delete(q.processing, taskID)
		q.ready = append(q.ready, cloneTask(entry.task))
		result.Moved++
	}
	return result, nil
}

// DeadLetter moves a processing task into dead-letter storage.
func (q *MemoryQueue) DeadLetter(taskID, leaseID, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	entry, ok := q.processing[taskID]
	if !ok {
		return ErrTaskNotProcessing
	}
	if entry.leaseID != leaseID {
		return ErrLeaseFenceMismatch
	}

	delete(q.processing, taskID)
	task := entry.task
	task.Attempts++
	q.dead = append(q.dead, DeadTask{
		Task:   cloneTask(task),
		Reason: reason,
		DeadAt: time.Now().UTC(),
	})
	return nil
}

// Stats reports ready, processing, and dead task counts.
func (q *MemoryQueue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	return Stats{
		Ready:      len(q.ready),
		Processing: len(q.processing),
		Dead:       len(q.dead),
	}
}

func cloneTask(item task.Task) task.Task {
	return task.Task{
		ID:       item.ID,
		Payload:  cloneBytes(item.Payload),
		Attempts: item.Attempts,
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
