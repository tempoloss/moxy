package core

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tempoloss/moxy/internal/queue"
	"github.com/tempoloss/moxy/internal/wal"
)

var (
	ErrQueueEmpty         = queue.ErrQueueEmpty
	ErrLeaseNotFound      = errors.New("lease does not exist")
	ErrInvalidTimeout     = errors.New("lease timeout must be positive")
	ErrRecoveryIncomplete = errors.New("recovery reconciliation failed")
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

// Journal records lease transitions durably so that they survive a restart.
type Journal interface {
	Append(record wal.Record) error
}

// EngineConfig controls lease expiration and durability.
type EngineConfig struct {
	MaxAttempts       int
	RequeueRetryDelay time.Duration
	// Journal, when set, receives every lease transition. Leaving it nil keeps
	// the engine purely in memory, which is the right choice for tests and for
	// workloads that can afford to replay from the source instead.
	Journal Journal
	// Recovered seeds lease state from a journal replay at startup.
	Recovered []wal.Record
}

// Engine owns all mutable in-memory queue, lease, and expiration state.
type Engine struct {
	mu          sync.Mutex
	ready       queue.Backend
	leases      map[string]*Lease
	expirations expirationHeap
	config      EngineConfig
	journal     Journal
	recoveryErr error
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

	engine := &Engine{
		ready:       backend,
		leases:      make(map[string]*Lease),
		expirations: expirations,
		config:      config,
		journal:     config.Journal,
	}
	activeTaskIDs := engine.restore(config.Recovered)
	if err := engine.recoverOrphanedProcessing(activeTaskIDs); err != nil {
		engine.recoveryErr = errors.Join(ErrRecoveryIncomplete, err)
	}
	return engine
}

// restore rebuilds lease state from a journal replay. A lease the journal still
// shows as open is reinstated with its original expiry, so one that lapsed
// while the process was down is reaped on the next pass instead of being handed
// a fresh window it did not earn. It returns the task IDs covered by those
// recovered leases so backend processing entries outside that set can be
// reconciled.
func (e *Engine) restore(records []wal.Record) map[string]struct{} {
	live := wal.Live(records)
	activeTaskIDs := make(map[string]struct{}, len(live))
	for _, record := range live {
		lease := &Lease{
			LeaseID:   record.LeaseID,
			Task:      cloneTask(record.Task),
			CreatedAt: record.CreatedAt,
			ExpiresAt: record.ExpiresAt,
		}
		e.leases[lease.LeaseID] = lease
		activeTaskIDs[lease.Task.ID] = struct{}{}
		heap.Push(&e.expirations, expirationItem{
			LeaseID:   lease.LeaseID,
			ExpiresAt: lease.ExpiresAt,
		})
	}
	return activeTaskIDs
}

func (e *Engine) recoverOrphanedProcessing(activeTaskIDs map[string]struct{}) error {
	_, err := e.ready.RecoverOrphanedProcessing(activeTaskIDs)
	return err
}

func (e *Engine) ensureRecovered() error {
	return e.recoveryErr
}

func (e *Engine) record(entry wal.Record) error {
	if e.journal == nil {
		return nil
	}
	return e.journal.Append(entry)
}

// Enqueue creates a task and appends it to the ready queue.
func (e *Engine) Enqueue(payload []byte) (Task, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.ensureRecovered(); err != nil {
		return Task{}, err
	}

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
	if err := e.ensureRecovered(); err != nil {
		return nil, err
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

	// Journal before the lease becomes visible. The task has already left the
	// ready queue, so a failed write must hand it back rather than leave it in
	// processing with no record that anyone holds it.
	if err := e.record(wal.Record{
		Op:        wal.OpFetch,
		LeaseID:   lease.LeaseID,
		Task:      lease.Task,
		CreatedAt: lease.CreatedAt,
		ExpiresAt: lease.ExpiresAt,
	}); err != nil {
		if requeueErr := e.ready.Requeue(task.ID); requeueErr != nil {
			return nil, errors.Join(err, requeueErr)
		}
		return nil, err
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
	if err := e.ensureRecovered(); err != nil {
		return err
	}

	lease, ok := e.leases[leaseID]
	if !ok {
		return ErrLeaseNotFound
	}

	if err := e.ready.Complete(lease.Task.ID); err != nil {
		return err
	}
	// Journal after the backend, not before. A crash in this gap leaves the
	// journal claiming a lease that is already complete; the next reap tries to
	// requeue it, the backend reports it is no longer processing, and the engine
	// drops it. Writing the journal first would strand the task instead.
	if err := e.record(wal.Record{Op: wal.OpAck, LeaseID: leaseID}); err != nil {
		return err
	}
	delete(e.leases, leaseID)
	return nil
}

// ReapExpired requeues every lease whose expiration is due by now.
func (e *Engine) ReapExpired(now time.Time) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.ensureRecovered(); err != nil {
		return 0, err
	}

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
	op := wal.OpExpire
	var err error
	if e.shouldDeadLetter(lease) {
		op = wal.OpDeadLetter
		err = e.ready.DeadLetter(lease.Task.ID, "max attempts exceeded")
	} else {
		err = e.ready.Requeue(lease.Task.ID)
	}

	// A task the backend no longer holds was already resolved — most often an
	// ack that landed just before its journal write did not. Treat it as closed
	// so the lease is released instead of being retried forever.
	if err != nil && !errors.Is(err, queue.ErrTaskNotProcessing) {
		return err
	}

	return e.record(wal.Record{Op: op, LeaseID: lease.LeaseID})
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
