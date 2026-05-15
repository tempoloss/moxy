package queue

import (
	"errors"
	"fmt"
	"testing"

	"github.com/an8kk/moxy/internal/task"
)

type backendFactory func(t *testing.T) Backend

func RunBackendContractTests(t *testing.T, factory backendFactory) {
	t.Helper()

	t.Run("EnqueueThenAcquireReturnsTask", func(t *testing.T) {
		testEnqueueThenAcquireReturnsTask(t, factory)
	})
	t.Run("AcquireMovesReadyToProcessing", func(t *testing.T) {
		testAcquireMovesReadyToProcessing(t, factory)
	})
	t.Run("AcquireEmptyReturnsQueueEmpty", func(t *testing.T) {
		testAcquireEmptyReturnsQueueEmpty(t, factory)
	})
	t.Run("CompleteRemovesProcessingTask", func(t *testing.T) {
		testCompleteRemovesProcessingTask(t, factory)
	})
	t.Run("CompleteMissingProcessingTaskFails", func(t *testing.T) {
		testCompleteMissingProcessingTaskFails(t, factory)
	})
	t.Run("RequeueMovesProcessingTaskBackToReady", func(t *testing.T) {
		testRequeueMovesProcessingTaskBackToReady(t, factory)
	})
	t.Run("RequeueIncrementsAttempts", func(t *testing.T) {
		testRequeueIncrementsAttempts(t, factory)
	})
	t.Run("RequeueMissingProcessingTaskFails", func(t *testing.T) {
		testRequeueMissingProcessingTaskFails(t, factory)
	})
	t.Run("DeadLetterMovesProcessingTaskToDead", func(t *testing.T) {
		testDeadLetterMovesProcessingTaskToDead(t, factory)
	})
	t.Run("DeadLetterMissingProcessingTaskFails", func(t *testing.T) {
		testDeadLetterMissingProcessingTaskFails(t, factory)
	})
	t.Run("StatsReportsReadyAndProcessing", func(t *testing.T) {
		testStatsReportsReadyAndProcessing(t, factory)
	})
	t.Run("PayloadCloningPreventsExternalMutation", func(t *testing.T) {
		testPayloadCloningPreventsExternalMutation(t, factory)
	})
	t.Run("FIFOPreserved", func(t *testing.T) {
		testFIFOPreserved(t, factory)
	})
}

func testEnqueueThenAcquireReturnsTask(t *testing.T, factory backendFactory) {
	backend := factory(t)
	original := task.Task{
		ID:      "task-1",
		Payload: []byte("payload"),
	}

	if err := backend.Enqueue(original); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}

	got, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}
	if got.ID != original.ID {
		t.Fatalf("task ID = %q, want %q", got.ID, original.ID)
	}
	if string(got.Payload) != string(original.Payload) {
		t.Fatalf("payload = %q, want %q", got.Payload, original.Payload)
	}
}

func testAcquireMovesReadyToProcessing(t *testing.T, factory backendFactory) {
	backend := factory(t)
	if err := backend.Enqueue(task.Task{ID: "task-1", Payload: []byte("payload")}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}

	got, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}
	if got.ID != "task-1" {
		t.Fatalf("task ID = %q, want %q", got.ID, "task-1")
	}

	stats := backend.Stats()
	if stats.Ready != 0 {
		t.Fatalf("ready count = %d, want 0", stats.Ready)
	}
	if stats.Processing != 1 {
		t.Fatalf("processing count = %d, want 1", stats.Processing)
	}
}

func testAcquireEmptyReturnsQueueEmpty(t *testing.T, factory backendFactory) {
	backend := factory(t)

	if got, err := backend.Acquire(); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("acquire returned task %+v and error %v, want ErrQueueEmpty", got, err)
	}
}

func testCompleteRemovesProcessingTask(t *testing.T, factory backendFactory) {
	backend := factory(t)
	if err := backend.Enqueue(task.Task{ID: "task-1"}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := backend.Complete(acquired.ID); err != nil {
		t.Fatalf("complete returned error: %v", err)
	}

	stats := backend.Stats()
	if stats.Ready != 0 {
		t.Fatalf("ready count = %d, want 0", stats.Ready)
	}
	if stats.Processing != 0 {
		t.Fatalf("processing count = %d, want 0", stats.Processing)
	}
}

func testCompleteMissingProcessingTaskFails(t *testing.T, factory backendFactory) {
	backend := factory(t)

	if err := backend.Complete("missing"); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("complete returned %v, want ErrTaskNotProcessing", err)
	}
}

func testRequeueMovesProcessingTaskBackToReady(t *testing.T, factory backendFactory) {
	backend := factory(t)
	if err := backend.Enqueue(task.Task{ID: "task-1", Payload: []byte("payload")}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := backend.Requeue(acquired.ID); err != nil {
		t.Fatalf("requeue returned error: %v", err)
	}

	stats := backend.Stats()
	if stats.Ready != 1 {
		t.Fatalf("ready count = %d, want 1", stats.Ready)
	}
	if stats.Processing != 0 {
		t.Fatalf("processing count = %d, want 0", stats.Processing)
	}

	again, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire after requeue returned error: %v", err)
	}
	if again.ID != acquired.ID {
		t.Fatalf("requeued task ID = %q, want %q", again.ID, acquired.ID)
	}
	if string(again.Payload) != "payload" {
		t.Fatalf("requeued payload = %q, want %q", again.Payload, "payload")
	}
}

func testRequeueMissingProcessingTaskFails(t *testing.T, factory backendFactory) {
	backend := factory(t)

	if err := backend.Requeue("missing"); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("requeue returned %v, want ErrTaskNotProcessing", err)
	}
}

func testRequeueIncrementsAttempts(t *testing.T, factory backendFactory) {
	backend := factory(t)
	if err := backend.Enqueue(task.Task{ID: "task-1", Attempts: 2}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := backend.Requeue(acquired.ID); err != nil {
		t.Fatalf("requeue returned error: %v", err)
	}
	again, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire after requeue returned error: %v", err)
	}
	if again.Attempts != 3 {
		t.Fatalf("attempts after requeue = %d, want 3", again.Attempts)
	}
}

func testDeadLetterMovesProcessingTaskToDead(t *testing.T, factory backendFactory) {
	backend := factory(t)
	if err := backend.Enqueue(task.Task{ID: "task-1", Attempts: 1}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	acquired, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}

	if err := backend.DeadLetter(acquired.ID, "expired"); err != nil {
		t.Fatalf("dead letter returned error: %v", err)
	}

	stats := backend.Stats()
	if stats.Ready != 0 || stats.Processing != 0 || stats.Dead != 1 {
		t.Fatalf("stats after dead letter = %+v, want ready=0 processing=0 dead=1", stats)
	}
}

func testDeadLetterMissingProcessingTaskFails(t *testing.T, factory backendFactory) {
	backend := factory(t)

	if err := backend.DeadLetter("missing", "expired"); !errors.Is(err, ErrTaskNotProcessing) {
		t.Fatalf("dead letter returned %v, want ErrTaskNotProcessing", err)
	}
}

func testStatsReportsReadyAndProcessing(t *testing.T, factory backendFactory) {
	backend := factory(t)

	if got := backend.Stats(); got.Ready != 0 || got.Processing != 0 || got.Dead != 0 {
		t.Fatalf("initial stats = %+v, want ready=0 processing=0 dead=0", got)
	}
	if err := backend.Enqueue(task.Task{ID: "task-1"}); err != nil {
		t.Fatalf("enqueue first returned error: %v", err)
	}
	if err := backend.Enqueue(task.Task{ID: "task-2"}); err != nil {
		t.Fatalf("enqueue second returned error: %v", err)
	}
	if got := backend.Stats(); got.Ready != 2 || got.Processing != 0 || got.Dead != 0 {
		t.Fatalf("stats after enqueue = %+v, want ready=2 processing=0 dead=0", got)
	}

	acquired, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}
	if got := backend.Stats(); got.Ready != 1 || got.Processing != 1 || got.Dead != 0 {
		t.Fatalf("stats after acquire = %+v, want ready=1 processing=1 dead=0", got)
	}
	if err := backend.Complete(acquired.ID); err != nil {
		t.Fatalf("complete returned error: %v", err)
	}
	if got := backend.Stats(); got.Ready != 1 || got.Processing != 0 || got.Dead != 0 {
		t.Fatalf("stats after complete = %+v, want ready=1 processing=0 dead=0", got)
	}
}

func testPayloadCloningPreventsExternalMutation(t *testing.T, factory backendFactory) {
	backend := factory(t)
	original := task.Task{
		ID:      "task-1",
		Payload: []byte("payload"),
	}

	if err := backend.Enqueue(original); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	if err := backend.Enqueue(task.Task{ID: "task-2", Payload: []byte("payload")}); err != nil {
		t.Fatalf("second enqueue returned error: %v", err)
	}
	original.Payload[0] = 'P'

	acquired, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire returned error: %v", err)
	}
	acquired.Payload[1] = 'A'

	again, err := backend.Acquire()
	if err != nil {
		t.Fatalf("second acquire returned error: %v", err)
	}
	if string(again.Payload) != "payload" {
		t.Fatalf("payload = %q, want %q", again.Payload, "payload")
	}

	if err := backend.Requeue(again.ID); err != nil {
		t.Fatalf("requeue returned error: %v", err)
	}
	requeued, err := backend.Acquire()
	if err != nil {
		t.Fatalf("acquire requeued task returned error: %v", err)
	}
	requeued.Payload[2] = 'Y'
	if err := backend.Requeue(requeued.ID); err != nil {
		t.Fatalf("second requeue returned error: %v", err)
	}
	final, err := backend.Acquire()
	if err != nil {
		t.Fatalf("final acquire returned error: %v", err)
	}
	if string(final.Payload) != "payload" {
		t.Fatalf("payload after requeue = %q, want %q", final.Payload, "payload")
	}
}

func testFIFOPreserved(t *testing.T, factory backendFactory) {
	backend := factory(t)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("task-%d", i)
		if err := backend.Enqueue(task.Task{ID: id}); err != nil {
			t.Fatalf("enqueue %s returned error: %v", id, err)
		}
	}

	for i := 1; i <= 3; i++ {
		want := fmt.Sprintf("task-%d", i)
		got, err := backend.Acquire()
		if err != nil {
			t.Fatalf("acquire %d returned error: %v", i, err)
		}
		if got.ID != want {
			t.Fatalf("acquire %d task ID = %q, want %q", i, got.ID, want)
		}
	}
}
