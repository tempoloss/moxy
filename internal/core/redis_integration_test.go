package core

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/an8kk/moxy/internal/queue"
	"github.com/redis/go-redis/v9"
)

func TestRedisQueueEngineIntegration(t *testing.T) {
	if os.Getenv("MOXY_REDIS_INTEGRATION") != "1" {
		t.Skip("set MOXY_REDIS_INTEGRATION=1 to run Redis integration tests")
	}

	addr := os.Getenv("MOXY_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis at %s: %v", addr, err)
	}

	queueName := redisEngineQueueName(t)
	backend := queue.NewRedisQueue(client, queueName)
	cleanupRedisEngineQueue(t, client, queueName)
	t.Cleanup(func() {
		cleanupRedisEngineQueue(t, client, queueName)
	})

	engine := newEngine(backend)
	task, err := engine.Enqueue([]byte("payload"))
	if err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	lease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	if lease.Task.ID != task.ID {
		t.Fatalf("lease task ID = %q, want %q", lease.Task.ID, task.ID)
	}
	stats := engine.Stats()
	if stats.Ready != 0 || stats.Processing != 1 || stats.ActiveLeases != 1 {
		t.Fatalf("stats after fetch = %+v, want ready=0 processing=1 active=1", stats)
	}

	if err := engine.Ack(lease.LeaseID); err != nil {
		t.Fatalf("ack returned error: %v", err)
	}
	stats = engine.Stats()
	if stats.Ready != 0 || stats.Processing != 0 || stats.ActiveLeases != 0 {
		t.Fatalf("stats after ack = %+v, want ready=0 processing=0 active=0", stats)
	}

	expiring, err := engine.Enqueue([]byte("expire-me"))
	if err != nil {
		t.Fatalf("enqueue expiring task returned error: %v", err)
	}
	expiringLease, err := engine.Fetch(time.Millisecond)
	if err != nil {
		t.Fatalf("fetch expiring task returned error: %v", err)
	}
	requeued, err := engine.ReapExpired(expiringLease.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap expired returned error: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("requeued count = %d, want 1", requeued)
	}
	stats = engine.Stats()
	if stats.Ready != 1 || stats.Processing != 0 || stats.ActiveLeases != 0 {
		t.Fatalf("stats after expiration = %+v, want ready=1 processing=0 active=0", stats)
	}

	next, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch requeued task returned error: %v", err)
	}
	if next.Task.ID != expiring.ID {
		t.Fatalf("requeued task ID = %q, want %q", next.Task.ID, expiring.ID)
	}
}

func redisEngineQueueName(t *testing.T) string {
	name := strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
	return name + "-" + time.Now().Format("20060102150405.000000000")
}

func cleanupRedisEngineQueue(t *testing.T, client *redis.Client, queueName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	readyKey := fmt.Sprintf("moxy:{%s}:ready", queueName)
	processingKey := fmt.Sprintf("moxy:{%s}:processing", queueName)
	if err := client.Del(ctx, readyKey, processingKey).Err(); err != nil {
		t.Fatalf("cleanup redis engine queue: %v", err)
	}
}
