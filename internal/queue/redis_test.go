package queue

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tempoloss/moxy/internal/task"
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

func TestRedisQueueDeadLetterMovesTaskToDeadKey(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if err := queue.Enqueue(task.Task{ID: "task-1", Payload: []byte("payload")}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := queue.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := queue.DeadLetter(acquired.ID, "expired"); err != nil {
		t.Fatalf("dead letter returned error: %v", err)
	}

	stats := queue.Stats()
	if stats.Ready != 0 || stats.Processing != 0 || stats.Dead != 1 {
		t.Fatalf("stats after dead letter = %+v, want ready=0 processing=0 dead=1", stats)
	}
	dead := redisDeadTasks(t, client, queue)
	if len(dead) != 1 {
		t.Fatalf("dead task count = %d, want 1", len(dead))
	}
	if dead[0].Task.ID != "task-1" || dead[0].Task.Attempts != 1 || dead[0].Reason != "expired" {
		t.Fatalf("dead entry = %+v, want task-1 attempts=1 reason=expired", dead[0])
	}
}

func TestRedisQueueDeadLetterMissingTaskReturnsErrTaskNotProcessing(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if err := queue.DeadLetter("missing", "expired"); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("dead letter returned %v, want ErrTaskNotProcessing", err)
	}
}

func TestRedisQueueRepeatedDeadLetterFailsCleanly(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if err := queue.Enqueue(task.Task{ID: "task-1"}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := queue.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := queue.DeadLetter(acquired.ID, "expired"); err != nil {
		t.Fatalf("first dead letter returned error: %v", err)
	}
	if err := queue.DeadLetter(acquired.ID, "expired"); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("second dead letter returned %v, want ErrTaskNotProcessing", err)
	}
}

func TestRedisQueueCachedScriptsRunRepeatedly(t *testing.T) {
	client := redisClientForTest(t)
	queue := redisQueueForTest(t, client)

	if queue.scripts.complete == nil {
		t.Fatal("complete script is nil")
	}
	if queue.scripts.requeue == nil {
		t.Fatal("requeue script is nil")
	}

	for _, id := range []string{"complete-1", "complete-2"} {
		if err := queue.Enqueue(task.Task{ID: id}); err != nil {
			t.Fatalf("enqueue %s returned error: %v", id, err)
		}
		acquired, err := queue.Acquire()
		if err != nil {
			t.Fatalf("acquire %s returned error: %v", id, err)
		}
		if err := queue.Complete(acquired.ID); err != nil {
			t.Fatalf("complete %s returned error: %v", id, err)
		}
	}

	for _, id := range []string{"requeue-1", "requeue-2"} {
		if err := queue.Enqueue(task.Task{ID: id}); err != nil {
			t.Fatalf("enqueue %s returned error: %v", id, err)
		}
		acquired, err := queue.Acquire()
		if err != nil {
			t.Fatalf("acquire %s returned error: %v", id, err)
		}
		if err := queue.Requeue(acquired.ID); err != nil {
			t.Fatalf("requeue %s returned error: %v", id, err)
		}
	}
}

func redisDeadTasks(t *testing.T, client *redis.Client, queue *RedisQueue) []DeadTask {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	encoded, err := client.LRange(ctx, queue.deadKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read dead key: %v", err)
	}

	dead := make([]DeadTask, 0, len(encoded))
	for _, item := range encoded {
		decoded, err := decodeDeadTask(item)
		if err != nil {
			t.Fatalf("decode dead task %q: %v", item, err)
		}
		dead = append(dead, decoded)
	}
	return dead
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
	if err := client.Del(ctx, queue.readyKey, queue.processingKey, queue.deadKey).Err(); err != nil {
		t.Fatalf("cleanup redis queue: %v", err)
	}
}
