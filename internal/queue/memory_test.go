package queue

import (
	"errors"
	"testing"

	"github.com/an8kk/moxy/internal/task"
)

func TestMemoryQueueContract(t *testing.T) {
	RunBackendContractTests(t, func(t *testing.T) Backend {
		t.Helper()
		return NewMemoryQueue()
	})
}

func TestAcquireMovesReadyToProcessing(t *testing.T) {
	testAcquireMovesReadyToProcessing(t, newMemoryBackend)
}

func TestCompleteRemovesProcessingTask(t *testing.T) {
	testCompleteRemovesProcessingTask(t, newMemoryBackend)
}

func TestRequeueMovesProcessingTaskBackToReady(t *testing.T) {
	testRequeueMovesProcessingTaskBackToReady(t, newMemoryBackend)
}

func TestMemoryQueueRequeueIncrementsAttempts(t *testing.T) {
	testRequeueIncrementsAttempts(t, newMemoryBackend)
}

func TestCompleteMissingProcessingTaskFails(t *testing.T) {
	testCompleteMissingProcessingTaskFails(t, newMemoryBackend)
}

func TestRequeueMissingProcessingTaskFails(t *testing.T) {
	testRequeueMissingProcessingTaskFails(t, newMemoryBackend)
}

func TestMemoryQueueStatsReportsReadyAndProcessing(t *testing.T) {
	testStatsReportsReadyAndProcessing(t, newMemoryBackend)
}

func TestMemoryQueueDeadLetterMovesTaskFromProcessingToDead(t *testing.T) {
	queue := NewMemoryQueue()
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

	if got := queue.Stats(); got.Dead != 1 || got.Processing != 0 {
		t.Fatalf("stats after dead letter = %+v, want dead=1 processing=0", got)
	}
	if len(queue.dead) != 1 {
		t.Fatalf("dead storage length = %d, want 1", len(queue.dead))
	}
	if queue.dead[0].Task.Attempts != 1 {
		t.Fatalf("dead task attempts = %d, want 1", queue.dead[0].Task.Attempts)
	}
	if queue.dead[0].Reason != "expired" {
		t.Fatalf("dead reason = %q, want %q", queue.dead[0].Reason, "expired")
	}
}

func TestMemoryQueueDeadLetterMissingTaskReturnsErrTaskNotProcessing(t *testing.T) {
	queue := NewMemoryQueue()

	if err := queue.DeadLetter("missing", "expired"); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("dead letter returned %v, want ErrTaskNotProcessing", err)
	}
}

func TestMemoryQueueDeadLetterClonesPayload(t *testing.T) {
	queue := NewMemoryQueue()
	payload := []byte("payload")
	if err := queue.Enqueue(task.Task{ID: "task-1", Payload: payload}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := queue.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}
	acquired.Payload[0] = 'P'

	if err := queue.DeadLetter(acquired.ID, "expired"); err != nil {
		t.Fatalf("dead letter returned error: %v", err)
	}

	if string(queue.dead[0].Task.Payload) != "payload" {
		t.Fatalf("stored dead payload = %q, want payload", queue.dead[0].Task.Payload)
	}
}

func TestMemoryQueuePayloadCloningStillWorks(t *testing.T) {
	testPayloadCloningPreventsExternalMutation(t, newMemoryBackend)
}

func newMemoryBackend(t *testing.T) Backend {
	t.Helper()
	return NewMemoryQueue()
}
