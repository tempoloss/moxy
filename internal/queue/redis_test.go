package queue

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/an8kk/moxy/internal/task"
	"github.com/redis/go-redis/v9"
)

func TestRedisQueueContract(t *testing.T) {
	client := redisClientForTest(t)

	RunBackendContractTests(t, func(t *testing.T) Backend {
		t.Helper()

		queue := redisQueueForTest(t, client)
		return queue
	})
}

func TestRedisQueueCompleteMissingTaskReturnsErrTaskNotProcessing(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if err := queue.Complete("missing"); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("complete returned %v, want ErrTaskNotProcessing", err)
	}
}

func TestRedisQueueRequeueMissingTaskReturnsErrTaskNotProcessing(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if err := queue.Requeue("missing"); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("requeue returned %v, want ErrTaskNotProcessing", err)
	}
}

func TestRedisQueueRequeueMovesTaskFromProcessingToReady(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if err := queue.Enqueue(task.Task{ID: "task-1", Payload: []byte("payload")}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := queue.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := queue.Requeue(acquired.ID); err != nil {
		t.Fatalf("requeue returned error: %v", err)
	}
	stats := queue.Stats()
	if stats.Ready != 1 || stats.Processing != 0 {
		t.Fatalf("stats after requeue = %+v, want ready=1 processing=0", stats)
	}
}

func TestRedisQueueCompleteRemovesTaskFromProcessing(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if err := queue.Enqueue(task.Task{ID: "task-1"}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := queue.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := queue.Complete(acquired.ID); err != nil {
		t.Fatalf("complete returned error: %v", err)
	}
	stats := queue.Stats()
	if stats.Ready != 0 || stats.Processing != 0 {
		t.Fatalf("stats after complete = %+v, want ready=0 processing=0", stats)
	}
}

func TestRedisQueueRepeatedCompleteFailsCleanly(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if err := queue.Enqueue(task.Task{ID: "task-1"}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := queue.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := queue.Complete(acquired.ID); err != nil {
		t.Fatalf("first complete returned error: %v", err)
	}
	if err := queue.Complete(acquired.ID); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("second complete returned %v, want ErrTaskNotProcessing", err)
	}
}

func TestRedisQueueRepeatedRequeueFailsCleanly(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if err := queue.Enqueue(task.Task{ID: "task-1"}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := queue.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := queue.Requeue(acquired.ID); err != nil {
		t.Fatalf("first requeue returned error: %v", err)
	}
	if err := queue.Requeue(acquired.ID); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("second requeue returned %v, want ErrTaskNotProcessing", err)
	}
}

func redisClientForTest(t *testing.T) *redis.Client {
	t.Helper()

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

	return client
}

func redisQueueForTest(t *testing.T, client *redis.Client) *RedisQueue {
	t.Helper()

	queue := NewRedisQueue(client, redisQueueName(t))
	cleanupRedisQueue(t, client, queue)
	t.Cleanup(func() {
		cleanupRedisQueue(t, client, queue)
	})

	return queue
}

func redisQueueName(t *testing.T) string {
	name := strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
	return name + "-" + time.Now().Format("20060102150405.000000000")
}

func cleanupRedisQueue(t *testing.T, client *redis.Client, queue *RedisQueue) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Del(ctx, queue.readyKey, queue.processingKey).Err(); err != nil {
		t.Fatalf("cleanup redis queue: %v", err)
	}
}
