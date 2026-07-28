package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tempoloss/moxy/internal/task"
	"github.com/redis/go-redis/v9"
)

// RedisQueue stores ready and processing tasks in Redis lists.
type RedisQueue struct {
	client        *redis.Client
	readyKey      string
	processingKey string
	deadKey       string
	scripts       redisQueueScripts
}

func NewRedisQueue(client *redis.Client, queueName string) *RedisQueue {
	keys := newRedisQueueKeys(queueName)
	return &RedisQueue{
		client:        client,
		readyKey:      keys.ready,
		processingKey: keys.processing,
		deadKey:       keys.dead,
		scripts:       defaultRedisQueueScripts(),
	}
}

type redisQueueKeys struct {
	ready      string
	processing string
	dead       string
}

func newRedisQueueKeys(queueName string) redisQueueKeys {
	hashTag := fmt.Sprintf("{%s}", queueName)
	prefix := "moxy:" + hashTag
	return redisQueueKeys{
		ready:      prefix + ":ready",
		processing: prefix + ":processing",
		dead:       prefix + ":dead",
	}
}

func redisHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	end := strings.IndexByte(key, '}')
	if start < 0 || end <= start {
		return ""
	}

	return key[start : end+1]
}

// Enqueue serializes a task and appends it to Redis ready storage.
func (q *RedisQueue) Enqueue(task task.Task) error {
	encoded, err := encodeTask(task)
	if err != nil {
		return err
	}

	return q.client.LPush(context.Background(), q.readyKey, encoded).Err()
}

// Acquire atomically moves one task from ready to processing storage.
func (q *RedisQueue) Acquire() (task.Task, error) {
	encoded, err := q.client.LMove(context.Background(), q.readyKey, q.processingKey, "RIGHT", "LEFT").Result()
	if errors.Is(err, redis.Nil) {
		return task.Task{}, ErrQueueEmpty
	}
	if err != nil {
		return task.Task{}, err
	}

	return decodeTask(encoded)
}

// Complete atomically removes a processing task.
func (q *RedisQueue) Complete(taskID string) error {
	found, err := q.runTaskScript(q.scripts.complete, taskID)
	if err != nil {
		return err
	}
	if !found {
		return ErrTaskNotProcessing
	}

	return nil
}

// Requeue atomically moves a processing task back to ready storage.
func (q *RedisQueue) Requeue(taskID string) error {
	found, err := q.runTaskScript(q.scripts.requeue, taskID)
	if err != nil {
		return err
	}
	if !found {
		return ErrTaskNotProcessing
	}

	return nil
}

// DeadLetter atomically moves a processing task to dead-letter storage.
func (q *RedisQueue) DeadLetter(taskID string, reason string) error {
	needle, err := taskIDNeedle(taskID)
	if err != nil {
		return err
	}

	result, err := q.scripts.dead.Run(
		context.Background(),
		q.client,
		[]string{q.processingKey, q.deadKey},
		needle,
		reason,
		time.Now().UTC().Format(time.RFC3339Nano),
	).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return ErrTaskNotProcessing
	}

	return nil
}

// Stats reports Redis ready, processing, and dead list lengths.
func (q *RedisQueue) Stats() Stats {
	ctx := context.Background()
	ready := q.client.LLen(ctx, q.readyKey).Val()
	processing := q.client.LLen(ctx, q.processingKey).Val()
	dead := q.client.LLen(ctx, q.deadKey).Val()

	return Stats{
		Ready:      int(ready),
		Processing: int(processing),
		Dead:       int(dead),
	}
}

func (q *RedisQueue) runTaskScript(script *redis.Script, taskID string) (bool, error) {
	needle, err := taskIDNeedle(taskID)
	if err != nil {
		return false, err
	}

	result, err := script.Run(
		context.Background(),
		q.client,
		[]string{q.processingKey, q.readyKey},
		needle,
	).Int()
	if err != nil {
		return false, err
	}

	return result == 1, nil
}

func taskIDNeedle(taskID string) (string, error) {
	encoded, err := json.Marshal(taskID)
	if err != nil {
		return "", err
	}

	return `"id":` + string(encoded), nil
}

func encodeTask(item task.Task) (string, error) {
	encoded, err := json.Marshal(cloneTask(item))
	if err != nil {
		return "", err
	}

	return string(encoded), nil
}

func decodeTask(encoded string) (task.Task, error) {
	var decoded task.Task
	if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
		return task.Task{}, err
	}

	return cloneTask(decoded), nil
}

func decodeDeadTask(encoded string) (DeadTask, error) {
	var decoded DeadTask
	if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
		return DeadTask{}, err
	}

	decoded.Task = cloneTask(decoded.Task)
	return decoded, nil
}
