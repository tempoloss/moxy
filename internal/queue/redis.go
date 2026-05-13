package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/an8kk/moxy/internal/task"
	"github.com/redis/go-redis/v9"
)

// RedisQueue stores ready and processing tasks in Redis lists.
type RedisQueue struct {
	client        *redis.Client
	readyKey      string
	processingKey string
}

func NewRedisQueue(client *redis.Client, queueName string) *RedisQueue {
	return &RedisQueue{
		client:        client,
		readyKey:      fmt.Sprintf("moxy:{%s}:ready", queueName),
		processingKey: fmt.Sprintf("moxy:{%s}:processing", queueName),
	}
}

func (q *RedisQueue) Enqueue(task task.Task) error {
	encoded, err := encodeTask(task)
	if err != nil {
		return err
	}

	return q.client.LPush(context.Background(), q.readyKey, encoded).Err()
}

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

func (q *RedisQueue) Complete(taskID string) error {
	found, err := q.runTaskScript(completeTaskScript, taskID)
	if err != nil {
		return err
	}
	if !found {
		return ErrTaskNotProcessing
	}

	return nil
}

func (q *RedisQueue) Requeue(taskID string) error {
	found, err := q.runTaskScript(requeueTaskScript, taskID)
	if err != nil {
		return err
	}
	if !found {
		return ErrTaskNotProcessing
	}

	return nil
}

func (q *RedisQueue) Stats() Stats {
	ctx := context.Background()
	ready := q.client.LLen(ctx, q.readyKey).Val()
	processing := q.client.LLen(ctx, q.processingKey).Val()

	return Stats{
		Ready:      int(ready),
		Processing: int(processing),
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

	return `"ID":` + string(encoded), nil
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

var completeTaskScript = redis.NewScript(`
local processing = KEYS[1]
local needle = ARGV[1]
local tasks = redis.call("LRANGE", processing, 0, -1)

for _, encoded in ipairs(tasks) do
	if string.find(encoded, needle, 1, true) then
		local removed = redis.call("LREM", processing, 1, encoded)
		if removed == 0 then
			return 0
		end
		return 1
	end
end

return 0
`)

var requeueTaskScript = redis.NewScript(`
local processing = KEYS[1]
local ready = KEYS[2]
local needle = ARGV[1]
local tasks = redis.call("LRANGE", processing, 0, -1)

for _, encoded in ipairs(tasks) do
	if string.find(encoded, needle, 1, true) then
		local removed = redis.call("LREM", processing, 1, encoded)
		if removed == 0 then
			return 0
		end
		redis.call("LPUSH", ready, encoded)
		return 1
	end
end

return 0
`)
