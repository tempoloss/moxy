package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tempoloss/moxy/internal/task"
)

// RedisQueue stores ready and dead tasks in Redis lists, and in-flight tasks in
// a hash keyed by task id so ACK, requeue and reclamation address one task
// directly instead of scanning a list. See docs/adr/0001-redis-lists-vs-streams.md.
type RedisQueue struct {
	client             *redis.Client
	readyKey           string
	processingKey      string
	processingLeaseKey string
	deadKey            string
	scripts            redisQueueScripts
}

func NewRedisQueue(client *redis.Client, queueName string) *RedisQueue {
	keys := newRedisQueueKeys(queueName)
	return &RedisQueue{
		client:             client,
		readyKey:           keys.ready,
		processingKey:      keys.processing,
		processingLeaseKey: keys.processingLeases,
		deadKey:            keys.dead,
		scripts:            defaultRedisQueueScripts(),
	}
}

type redisQueueKeys struct {
	ready            string
	processing       string
	processingLeases string
	dead             string
}

func newRedisQueueKeys(queueName string) redisQueueKeys {
	hashTag := fmt.Sprintf("{%s}", queueName)
	prefix := "moxy:" + hashTag
	return redisQueueKeys{
		ready:            prefix + ":ready",
		processing:       prefix + ":processing",
		processingLeases: prefix + ":processing:leases",
		dead:             prefix + ":dead",
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

// Acquire atomically moves one task from ready storage into the processing hashes.
func (q *RedisQueue) Acquire(leaseID string) (task.Task, error) {
	encoded, err := q.scripts.acquire.Run(
		context.Background(),
		q.client,
		[]string{q.readyKey, q.processingKey, q.processingLeaseKey},
		leaseID,
	).Text()
	if errors.Is(err, redis.Nil) {
		return task.Task{}, ErrQueueEmpty
	}
	if err != nil {
		return task.Task{}, err
	}

	return decodeTask(encoded)
}

// Complete atomically removes a processing task.
func (q *RedisQueue) Complete(taskID, leaseID string) error {
	return q.runTaskScript(
		q.scripts.complete,
		[]string{q.processingKey, q.processingLeaseKey},
		taskID,
		leaseID,
	)
}

// Requeue atomically moves a processing task back to ready storage.
func (q *RedisQueue) Requeue(taskID, leaseID string) error {
	return q.runTaskScript(
		q.scripts.requeue,
		[]string{q.processingKey, q.processingLeaseKey, q.readyKey},
		taskID,
		leaseID,
	)
}

// DeadLetter atomically moves a processing task to dead-letter storage.
func (q *RedisQueue) DeadLetter(taskID, leaseID, reason string) error {
	result, err := q.scripts.dead.Run(
		context.Background(),
		q.client,
		[]string{q.processingKey, q.processingLeaseKey, q.deadKey},
		taskID,
		leaseID,
		reason,
		time.Now().UTC().Format(time.RFC3339Nano),
	).Int()
	if err != nil {
		return err
	}
	return transitionResultError(result)
}

// RecoverOrphanedProcessing atomically returns processing tasks without a
// recovered active lease to ready storage. Unlike expiry requeueing, recovery
// does not increment attempts because no durable lease reached a worker.
func (q *RedisQueue) RecoverOrphanedProcessing(activeLeases []LeaseFence) (RecoveryResult, error) {
	args := make([]any, 0, len(activeLeases)*2)
	for _, lease := range activeLeases {
		args = append(args, lease.TaskID, lease.LeaseID)
	}

	result, err := q.scripts.recoverOrphans.Run(
		context.Background(),
		q.client,
		[]string{q.processingKey, q.processingLeaseKey, q.readyKey},
		args...,
	).Slice()
	if err != nil {
		return RecoveryResult{}, err
	}
	return decodeRecoveryResult(result)
}

// Stats reports the ready, processing, and dead counts.
func (q *RedisQueue) Stats() Stats {
	ctx := context.Background()
	ready := q.client.LLen(ctx, q.readyKey).Val()
	processing := q.client.HLen(ctx, q.processingKey).Val()
	dead := q.client.LLen(ctx, q.deadKey).Val()

	return Stats{
		Ready:      int(ready),
		Processing: int(processing),
		Dead:       int(dead),
	}
}

func (q *RedisQueue) runTaskScript(script *redis.Script, keys []string, args ...any) error {
	result, err := script.Run(context.Background(), q.client, keys, args...).Int()
	if err != nil {
		return err
	}
	return transitionResultError(result)
}

func transitionResultError(result int) error {
	switch result {
	case 1:
		return nil
	case 0:
		return ErrTaskNotProcessing
	case -1:
		return ErrLeaseFenceMismatch
	default:
		return fmt.Errorf("redis script returned unexpected transition result %d", result)
	}
}

func decodeRecoveryResult(items []interface{}) (RecoveryResult, error) {
	if len(items) == 0 {
		return RecoveryResult{}, errors.New("redis recovery script returned no result")
	}
	moved, ok := items[0].(int64)
	if !ok {
		return RecoveryResult{}, fmt.Errorf("redis recovery moved count has type %T", items[0])
	}

	result := RecoveryResult{
		Moved:           int(moved),
		MatchedLeaseIDs: make(map[string]struct{}, len(items)-1),
	}
	for _, item := range items[1:] {
		leaseID, ok := item.(string)
		if !ok {
			return RecoveryResult{}, fmt.Errorf("redis recovery lease id has type %T", item)
		}
		result.MatchedLeaseIDs[leaseID] = struct{}{}
	}
	return result, nil
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
