package core

import (
	"errors"
	"testing"
	"time"

	"github.com/an8kk/moxy/internal/queue"
)

func TestFetchCreatesLease(t *testing.T) {
	engine := NewEngine()

	task := engine.Enqueue([]byte("first"))
	lease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	if lease.Task.ID != task.ID {
		t.Fatalf("lease task ID = %q, want %q", lease.Task.ID, task.ID)
	}
	if lease.LeaseID == "" {
		t.Fatal("lease ID is empty")
	}
	if lease.CreatedAt.IsZero() {
		t.Fatal("lease CreatedAt is zero")
	}
	if !lease.ExpiresAt.After(lease.CreatedAt) {
		t.Fatalf("lease ExpiresAt = %v, want after CreatedAt %v", lease.ExpiresAt, lease.CreatedAt)
	}

	stats := engine.Stats()
	if stats.Ready != 0 {
		t.Fatalf("ready count = %d, want 0", stats.Ready)
	}
	if stats.Processing != 1 {
		t.Fatalf("processing count = %d, want 1", stats.Processing)
	}
	if stats.ActiveLeases != 1 {
		t.Fatalf("active lease count = %d, want 1", stats.ActiveLeases)
	}
	if stats.ExpirationHeap != 1 {
		t.Fatalf("heap size = %d, want 1", stats.ExpirationHeap)
	}
}

func TestAckRemovesLease(t *testing.T) {
	engine := NewEngine()
	engine.Enqueue([]byte("first"))
	lease, err := engine.Fetch(10 * time.Millisecond)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	if err := engine.Ack(lease.LeaseID); err != nil {
		t.Fatalf("ack returned error: %v", err)
	}

	stats := engine.Stats()
	if stats.ActiveLeases != 0 {
		t.Fatalf("active lease count after ack = %d, want 0", stats.ActiveLeases)
	}
	if stats.Processing != 0 {
		t.Fatalf("processing count after ack = %d, want 0", stats.Processing)
	}

	requeued, err := engine.ReapExpired(lease.ExpiresAt.Add(time.Second))
	if err != nil {
		t.Fatalf("reap expired returned error: %v", err)
	}
	if requeued != 0 {
		t.Fatalf("requeued count = %d, want 0", requeued)
	}
	stats = engine.Stats()
	if stats.Ready != 0 {
		t.Fatalf("ready count after reaping acked lease = %d, want 0", stats.Ready)
	}
}

func TestFetchInvalidTimeoutFails(t *testing.T) {
	engine := NewEngine()
	engine.Enqueue([]byte("first"))

	if lease, err := engine.Fetch(0); !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("Fetch(0) returned lease %+v and error %v, want ErrInvalidTimeout", lease, err)
	}
	if lease, err := engine.Fetch(-time.Nanosecond); !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("Fetch(negative) returned lease %+v and error %v, want ErrInvalidTimeout", lease, err)
	}

	stats := engine.Stats()
	if stats.Ready != 1 {
		t.Fatalf("ready count = %d, want 1", stats.Ready)
	}
	if stats.Processing != 0 {
		t.Fatalf("processing count = %d, want 0", stats.Processing)
	}
	if stats.ActiveLeases != 0 {
		t.Fatalf("active lease count = %d, want 0", stats.ActiveLeases)
	}
	if stats.ExpirationHeap != 0 {
		t.Fatalf("heap size = %d, want 0", stats.ExpirationHeap)
	}
}

func TestEnqueueCopiesPayload(t *testing.T) {
	engine := NewEngine()
	payload := []byte("payload")

	task := engine.Enqueue(payload)
	payload[0] = 'P'
	task.Payload[1] = 'A'

	lease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	if string(lease.Task.Payload) != "payload" {
		t.Fatalf("leased payload = %q, want %q", lease.Task.Payload, "payload")
	}
}

func TestReturnedLeasePayloadDoesNotMutateEngine(t *testing.T) {
	engine := NewEngine()
	engine.Enqueue([]byte("payload"))

	lease, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	lease.Task.Payload[0] = 'P'

	requeued, err := engine.ReapExpired(lease.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap expired returned error: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("requeued count = %d, want 1", requeued)
	}

	next, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch requeued task returned error: %v", err)
	}
	if string(next.Task.Payload) != "payload" {
		t.Fatalf("requeued payload = %q, want %q", next.Task.Payload, "payload")
	}
}

func TestAckedLeaseHeapEntryIsIgnored(t *testing.T) {
	engine := NewEngine()
	engine.Enqueue([]byte("first"))
	lease, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	if err := engine.Ack(lease.LeaseID); err != nil {
		t.Fatalf("ack returned error: %v", err)
	}
	requeued, err := engine.ReapExpired(lease.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap expired returned error: %v", err)
	}
	if requeued != 0 {
		t.Fatalf("requeued count = %d, want 0", requeued)
	}

	stats := engine.Stats()
	if stats.Ready != 0 {
		t.Fatalf("ready count = %d, want 0", stats.Ready)
	}
	if stats.Processing != 0 {
		t.Fatalf("processing count = %d, want 0", stats.Processing)
	}
	if stats.ActiveLeases != 0 {
		t.Fatalf("active lease count = %d, want 0", stats.ActiveLeases)
	}
	if stats.ExpirationHeap != 0 {
		t.Fatalf("heap size = %d, want 0", stats.ExpirationHeap)
	}
}

func TestExpiredLeaseRequeuesTask(t *testing.T) {
	engine := NewEngine()
	task := engine.Enqueue([]byte("first"))
	lease, err := engine.Fetch(10 * time.Millisecond)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	requeued, err := engine.ReapExpired(lease.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap expired returned error: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("requeued count = %d, want 1", requeued)
	}

	stats := engine.Stats()
	if stats.ActiveLeases != 0 {
		t.Fatalf("active lease count = %d, want 0", stats.ActiveLeases)
	}
	if stats.Processing != 0 {
		t.Fatalf("processing count = %d, want 0", stats.Processing)
	}
	if stats.Ready != 1 {
		t.Fatalf("ready count = %d, want 1", stats.Ready)
	}

	next, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch after requeue returned error: %v", err)
	}
	if next.Task.ID != task.ID {
		t.Fatalf("requeued task ID = %q, want %q", next.Task.ID, task.ID)
	}
}

func TestDoubleAckFails(t *testing.T) {
	engine := NewEngine()
	engine.Enqueue([]byte("first"))
	lease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	if err := engine.Ack(lease.LeaseID); err != nil {
		t.Fatalf("first ack returned error: %v", err)
	}
	if err := engine.Ack(lease.LeaseID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("second ack returned %v, want ErrLeaseNotFound", err)
	}
}

func TestFetchEmptyQueueFails(t *testing.T) {
	engine := NewEngine()

	if lease, err := engine.Fetch(time.Minute); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("fetch returned lease %+v and error %v, want ErrQueueEmpty", lease, err)
	}
}

func TestReapExpiredSkipsStaleHeapEntries(t *testing.T) {
	engine := NewEngine()
	engine.Enqueue([]byte("first"))
	engine.Enqueue([]byte("second"))

	first, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch first returned error: %v", err)
	}
	second, err := engine.Fetch(time.Hour)
	if err != nil {
		t.Fatalf("fetch second returned error: %v", err)
	}

	if err := engine.Ack(first.LeaseID); err != nil {
		t.Fatalf("ack first returned error: %v", err)
	}

	requeued, err := engine.ReapExpired(first.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap expired returned error: %v", err)
	}
	if requeued != 0 {
		t.Fatalf("requeued count = %d, want 0", requeued)
	}
	stats := engine.Stats()
	if stats.Ready != 0 {
		t.Fatalf("ready count = %d, want 0", stats.Ready)
	}
	if stats.Processing != 1 {
		t.Fatalf("processing count = %d, want 1", stats.Processing)
	}
	if stats.ActiveLeases != 1 {
		t.Fatalf("active lease count = %d, want 1", stats.ActiveLeases)
	}
	if stats.ExpirationHeap != 1 {
		t.Fatalf("heap size = %d, want 1", stats.ExpirationHeap)
	}

	requeued, err = engine.ReapExpired(second.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("second reap returned error: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("second reap requeued = %d, want 1", requeued)
	}
}

func TestReapExpiredDoesNotDeleteLeaseIfBackendRequeueFails(t *testing.T) {
	backend := queue.NewMemoryQueue()
	engine := newEngine(backend)
	engine.Enqueue([]byte("first"))
	lease, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	engine.ready = &failingRequeueBackend{Backend: backend}

	requeued, err := engine.ReapExpired(lease.ExpiresAt.Add(time.Nanosecond))
	if !errors.Is(err, errRequeueFailed) {
		t.Fatalf("reap expired error = %v, want errRequeueFailed", err)
	}
	if requeued != 0 {
		t.Fatalf("requeued count = %d, want 0", requeued)
	}

	stats := engine.Stats()
	if stats.ActiveLeases != 1 {
		t.Fatalf("active lease count = %d, want 1", stats.ActiveLeases)
	}
	if stats.Processing != 1 {
		t.Fatalf("processing count = %d, want 1", stats.Processing)
	}
	if stats.ExpirationHeap != 1 {
		t.Fatalf("heap size = %d, want 1", stats.ExpirationHeap)
	}
}

func TestReapExpiredRetriesFailedBackendRequeueLater(t *testing.T) {
	backend := queue.NewMemoryQueue()
	engine := newEngine(backend)
	engine.Enqueue([]byte("first"))
	lease, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	failing := &failOnceRequeueBackend{Backend: backend}
	engine.ready = failing

	firstReapAt := lease.ExpiresAt.Add(time.Nanosecond)
	requeued, err := engine.ReapExpired(firstReapAt)
	if !errors.Is(err, errRequeueFailed) {
		t.Fatalf("first reap error = %v, want errRequeueFailed", err)
	}
	if requeued != 0 {
		t.Fatalf("first requeued count = %d, want 0", requeued)
	}

	requeued, err = engine.ReapExpired(firstReapAt.Add(500 * time.Millisecond))
	if err != nil {
		t.Fatalf("second reap before retry delay returned error: %v", err)
	}
	if requeued != 0 {
		t.Fatalf("second requeued count = %d, want 0", requeued)
	}
	stats := engine.Stats()
	if stats.ActiveLeases != 1 {
		t.Fatalf("active lease count before retry = %d, want 1", stats.ActiveLeases)
	}
	if stats.Processing != 1 {
		t.Fatalf("processing count before retry = %d, want 1", stats.Processing)
	}
	if stats.Ready != 0 {
		t.Fatalf("ready count before retry = %d, want 0", stats.Ready)
	}

	requeued, err = engine.ReapExpired(firstReapAt.Add(2 * time.Second))
	if err != nil {
		t.Fatalf("retry reap returned error: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("retry requeued count = %d, want 1", requeued)
	}
	stats = engine.Stats()
	if stats.ActiveLeases != 0 {
		t.Fatalf("active lease count after retry = %d, want 0", stats.ActiveLeases)
	}
	if stats.Processing != 0 {
		t.Fatalf("processing count after retry = %d, want 0", stats.Processing)
	}
	if stats.Ready != 1 {
		t.Fatalf("ready count after retry = %d, want 1", stats.Ready)
	}
}

func TestMultipleLeaseExpirationOrdering(t *testing.T) {
	engine := NewEngine()
	engine.Enqueue([]byte("first"))
	engine.Enqueue([]byte("second"))
	engine.Enqueue([]byte("third"))

	first, err := engine.Fetch(10 * time.Millisecond)
	if err != nil {
		t.Fatalf("fetch first returned error: %v", err)
	}
	second, err := engine.Fetch(30 * time.Millisecond)
	if err != nil {
		t.Fatalf("fetch second returned error: %v", err)
	}
	third, err := engine.Fetch(50 * time.Millisecond)
	if err != nil {
		t.Fatalf("fetch third returned error: %v", err)
	}

	requeued, err := engine.ReapExpired(first.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("first reap returned error: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("first reap requeued = %d, want 1", requeued)
	}
	stats := engine.Stats()
	if stats.Ready != 1 {
		t.Fatalf("ready count after first reap = %d, want 1", stats.Ready)
	}
	if stats.Processing != 2 {
		t.Fatalf("processing count after first reap = %d, want 2", stats.Processing)
	}
	if stats.ActiveLeases != 2 {
		t.Fatalf("active lease count after first reap = %d, want 2", stats.ActiveLeases)
	}

	requeued, err = engine.ReapExpired(second.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("second reap returned error: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("second reap requeued = %d, want 1", requeued)
	}
	stats = engine.Stats()
	if stats.Ready != 2 {
		t.Fatalf("ready count after second reap = %d, want 2", stats.Ready)
	}
	if stats.Processing != 1 {
		t.Fatalf("processing count after second reap = %d, want 1", stats.Processing)
	}
	if stats.ActiveLeases != 1 {
		t.Fatalf("active lease count after second reap = %d, want 1", stats.ActiveLeases)
	}

	requeued, err = engine.ReapExpired(third.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("third reap returned error: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("third reap requeued = %d, want 1", requeued)
	}
	stats = engine.Stats()
	if stats.Ready != 3 {
		t.Fatalf("ready count after third reap = %d, want 3", stats.Ready)
	}
	if stats.Processing != 0 {
		t.Fatalf("processing count after third reap = %d, want 0", stats.Processing)
	}
	if stats.ActiveLeases != 0 {
		t.Fatalf("active lease count after third reap = %d, want 0", stats.ActiveLeases)
	}
}

var errRequeueFailed = errors.New("requeue failed")

type failingRequeueBackend struct {
	queue.Backend
}

func (b *failingRequeueBackend) Requeue(taskID string) error {
	return errRequeueFailed
}

type failOnceRequeueBackend struct {
	queue.Backend
	failed bool
}

func (b *failOnceRequeueBackend) Requeue(taskID string) error {
	if !b.failed {
		b.failed = true
		return errRequeueFailed
	}

	return b.Backend.Requeue(taskID)
}
