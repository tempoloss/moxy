package core

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"github.com/tempoloss/moxy/internal/queue"
	"github.com/google/uuid"
)

var (
	ErrQueueEmpty     = queue.ErrQueueEmpty
	ErrLeaseNotFound  = errors.New("lease does not exist")
	ErrInvalidTimeout = errors.New("lease timeout must be positive")
)

const requeueRetryDelay = time.Second

// Stats exposes Engine counts for debugging and tests.
type Stats struct {
	Ready          int
	Processing     int
	Dead           int
	ActiveLeases   int
	ExpirationHeap int
}

// EngineConfig controls lease expiration behavior.
type EngineConfig struct {
	MaxAttempts       int
	RequeueRetryDelay time.Duration
}

// Engine owns all mutable in-memory queue, lease, and expiration state.
type Engine struct {
	mu          sync.Mutex
	ready       queue.Backend
	leases      map[string]*Lease
	expirations expirationHeap
	config      EngineConfig
}

// NewEngine creates an empty in-memory lease engine.
func NewEngine() *Engine {
	return NewEngineWithBackend(queue.NewMemoryQueue())
}

// NewEngineWithBackend creates a single-queue lease engine over the provided backend.
func NewEngineWithBackend(backend queue.Backend) *Engine {
	return newEngine(backend, EngineConfig{})
}

// NewEngineWithBackendAndConfig creates a lease engine with explicit expiration config.
func NewEngineWithBackendAndConfig(backend queue.Backend, config EngineConfig) *Engine {
	return newEngine(backend, config)
}

func newEngine(backend queue.Backend, configs ...EngineConfig) *Engine {
	var config EngineConfig
	if len(configs) > 0 {
		config = configs[0]
	}

	expirations := expirationHeap{}
	heap.Init(&expirations)
	if config.RequeueRetryDelay <= 0 {
		config.RequeueRetryDelay = requeueRetryDelay
	}

	return &Engine{
		ready:       backend,
		leases:      make(map[string]*Lease),
		expirations: expirations,
		config:      config,
	}
}

// Enqueue creates a task and appends it to the ready queue.
func (e *Engine) Enqueue(payload []byte) (Task, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	task := Task{
		ID:      uuid.NewString(),
		Payload: cloneBytes(payload),
	}
	if err := e.ready.Enqueue(task); err != nil {
		return Task{}, err
	}

	return cloneTask(task), nil
}

// Fetch leases one ready task for the provided timeout.
func (e *Engine) Fetch(timeout time.Duration) (*Lease, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if timeout <= 0 {
		return nil, ErrInvalidTimeout
	}

	task, err := e.ready.Acquire()
	if err != nil {
		if errors.Is(err, queue.ErrQueueEmpty) {
			return nil, ErrQueueEmpty
		}
		return nil, err
	}

	now := time.Now()
	lease := &Lease{
		LeaseID:   uuid.NewString(),
		Task:      task,
		CreatedAt: now,
		ExpiresAt: now.Add(timeout),
	}
	e.leases[lease.LeaseID] = lease
	heap.Push(&e.expirations, expirationItem{
		LeaseID:   lease.LeaseID,
		ExpiresAt: lease.ExpiresAt,
	})

	return cloneLease(lease), nil
}

// Ack permanently removes an active lease from the authoritative lease map.
func (e *Engine) Ack(leaseID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.leases[leaseID]; !ok {
		return ErrLeaseNotFound
	}

	if err := e.ready.Complete(e.leases[leaseID].Task.ID); err != nil {
		return err
	}
	delete(e.leases, leaseID)
	return nil
}

// ReapExpired requeues every lease whose expiration is due by now.
func (e *Engine) ReapExpired(now time.Time) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	requeued := 0
	for {
		item, ok := e.expirations.peek()
		if !ok || item.ExpiresAt.After(now) {
			break
		}

		heap.Pop(&e.expirations)
		lease, ok := e.leases[item.LeaseID]
		if !ok {
			continue
		}
		if lease.ExpiresAt.After(now) {
			continue
		}

		err := e.expireLease(lease)
		if err != nil {
			heap.Push(&e.expirations, expirationItem{
				LeaseID:   item.LeaseID,
				ExpiresAt: now.Add(e.config.RequeueRetryDelay),
			})
			return requeued, err
		}
		delete(e.leases, item.LeaseID)
		requeued++
	}

	return requeued, nil
}

func (e *Engine) expireLease(lease *Lease) error {
	if e.shouldDeadLetter(lease) {
		return e.ready.DeadLetter(lease.Task.ID, "max attempts exceeded")
	}

	return e.ready.Requeue(lease.Task.ID)
}

func (e *Engine) shouldDeadLetter(lease *Lease) bool {
	return e.config.MaxAttempts > 0 && lease.Task.Attempts+1 >= e.config.MaxAttempts
}

// Stats returns counts for tests and diagnostics.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()

	queueStats := e.ready.Stats()
	return Stats{
		Ready:          queueStats.Ready,
		Processing:     queueStats.Processing,
		Dead:           queueStats.Dead,
		ActiveLeases:   len(e.leases),
		ExpirationHeap: e.expirations.Len(),
	}
}

func cloneLease(lease *Lease) *Lease {
	if lease == nil {
		return nil
	}

	return &Lease{
		LeaseID:   lease.LeaseID,
		Task:      cloneTask(lease.Task),
		CreatedAt: lease.CreatedAt,
		ExpiresAt: lease.ExpiresAt,
	}
}

func cloneTask(task Task) Task {
	return Task{
		ID:       task.ID,
		Payload:  cloneBytes(task.Payload),
		Attempts: task.Attempts,
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
