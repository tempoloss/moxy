package service

import (
	"errors"
	"testing"
	"time"

	"github.com/an8kk/moxy/internal/core"
	"github.com/an8kk/moxy/internal/queue"
)

func TestEnqueueCreatesQueueLazily(t *testing.T) {
	service := New(memoryBackendFactory)

	if _, ok := service.Stats("jobs"); ok {
		t.Fatal("stats reported queue before it was created")
	}
	task, err := service.Enqueue("jobs", []byte("payload"))
	if err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	if task.ID == "" {
		t.Fatal("task ID is empty")
	}

	stats, ok := service.Stats("jobs")
	if !ok {
		t.Fatal("stats did not report lazily created queue")
	}
	if stats.Ready != 1 {
		t.Fatalf("ready count = %d, want 1", stats.Ready)
	}
}

func TestFetchFromOneQueueDoesNotAffectAnotherQueue(t *testing.T) {
	service := New(memoryBackendFactory)
	if _, err := service.Enqueue("alpha", []byte("a")); err != nil {
		t.Fatalf("enqueue alpha returned error: %v", err)
	}
	if _, err := service.Enqueue("beta", []byte("b")); err != nil {
		t.Fatalf("enqueue beta returned error: %v", err)
	}

	lease, err := service.Fetch("alpha", time.Minute)
	if err != nil {
		t.Fatalf("fetch alpha returned error: %v", err)
	}
	if string(lease.Task.Payload) != "a" {
		t.Fatalf("payload = %q, want %q", lease.Task.Payload, "a")
	}

	alpha, _ := service.Stats("alpha")
	beta, _ := service.Stats("beta")
	if alpha.Ready != 0 || alpha.Processing != 1 || alpha.ActiveLeases != 1 {
		t.Fatalf("alpha stats = %+v, want ready=0 processing=1 active=1", alpha)
	}
	if beta.Ready != 1 || beta.Processing != 0 || beta.ActiveLeases != 0 {
		t.Fatalf("beta stats = %+v, want ready=1 processing=0 active=0", beta)
	}
}

func TestAckFindsLeaseAcrossQueues(t *testing.T) {
	service := New(memoryBackendFactory)
	if _, err := service.Enqueue("alpha", []byte("a")); err != nil {
		t.Fatalf("enqueue alpha returned error: %v", err)
	}
	if _, err := service.Enqueue("beta", []byte("b")); err != nil {
		t.Fatalf("enqueue beta returned error: %v", err)
	}
	if _, err := service.Fetch("alpha", time.Minute); err != nil {
		t.Fatalf("fetch alpha returned error: %v", err)
	}
	betaLease, err := service.Fetch("beta", time.Minute)
	if err != nil {
		t.Fatalf("fetch beta returned error: %v", err)
	}

	if err := service.Ack(betaLease.LeaseID); err != nil {
		t.Fatalf("ack beta lease returned error: %v", err)
	}

	beta, _ := service.Stats("beta")
	if beta.Processing != 0 || beta.ActiveLeases != 0 {
		t.Fatalf("beta stats = %+v, want processing=0 active=0", beta)
	}
}

func TestAckMissingLeaseReturnsErrLeaseNotFound(t *testing.T) {
	service := New(memoryBackendFactory)

	if err := service.Ack("missing"); !errors.Is(err, core.ErrLeaseNotFound) {
		t.Fatalf("ack returned %v, want ErrLeaseNotFound", err)
	}
}

func TestReapExpiredRequeuesExpiredLeasesAcrossAllQueues(t *testing.T) {
	service := New(memoryBackendFactory)
	if _, err := service.Enqueue("alpha", []byte("a")); err != nil {
		t.Fatalf("enqueue alpha returned error: %v", err)
	}
	if _, err := service.Enqueue("beta", []byte("b")); err != nil {
		t.Fatalf("enqueue beta returned error: %v", err)
	}
	alphaLease, err := service.Fetch("alpha", time.Millisecond)
	if err != nil {
		t.Fatalf("fetch alpha returned error: %v", err)
	}
	betaLease, err := service.Fetch("beta", time.Millisecond)
	if err != nil {
		t.Fatalf("fetch beta returned error: %v", err)
	}

	requeued, err := service.ReapExpired(maxTime(alphaLease.ExpiresAt, betaLease.ExpiresAt).Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap expired returned error: %v", err)
	}
	if requeued != 2 {
		t.Fatalf("requeued count = %d, want 2", requeued)
	}

	alpha, _ := service.Stats("alpha")
	beta, _ := service.Stats("beta")
	if alpha.Ready != 1 || alpha.ActiveLeases != 0 {
		t.Fatalf("alpha stats = %+v, want ready=1 active=0", alpha)
	}
	if beta.Ready != 1 || beta.ActiveLeases != 0 {
		t.Fatalf("beta stats = %+v, want ready=1 active=0", beta)
	}
}

func TestEmptyQueueNameFails(t *testing.T) {
	service := New(memoryBackendFactory)

	if _, err := service.Enqueue("", []byte("payload")); !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("enqueue error = %v, want ErrInvalidQueueName", err)
	}
	if _, err := service.Fetch("", time.Minute); !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("fetch error = %v, want ErrInvalidQueueName", err)
	}
	if _, ok := service.Stats(""); ok {
		t.Fatal("stats reported empty queue name")
	}
}

func TestStatsReportsPerQueueState(t *testing.T) {
	service := New(memoryBackendFactory)
	if _, err := service.Enqueue("jobs", []byte("one")); err != nil {
		t.Fatalf("enqueue one returned error: %v", err)
	}
	if _, err := service.Enqueue("jobs", []byte("two")); err != nil {
		t.Fatalf("enqueue two returned error: %v", err)
	}
	if _, err := service.Fetch("jobs", time.Minute); err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	stats, ok := service.Stats("jobs")
	if !ok {
		t.Fatal("stats did not report queue")
	}
	if stats.Ready != 1 || stats.Processing != 1 || stats.ActiveLeases != 1 || stats.ExpirationHeap != 1 {
		t.Fatalf("stats = %+v, want ready=1 processing=1 active=1 heap=1", stats)
	}
}

func memoryBackendFactory(queueName string) queue.Backend {
	return queue.NewMemoryQueue()
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
