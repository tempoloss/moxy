package command

import (
	"errors"
	"testing"

	"github.com/an8kk/moxy/internal/core"
	"github.com/an8kk/moxy/internal/queue"
	"github.com/an8kk/moxy/internal/service"
	"github.com/an8kk/moxy/internal/task"
)

func TestEnqueueThenFetchReturnsSamePayload(t *testing.T) {
	handler := newTestHandler()

	enqueue, err := handler.Handle(Command{Name: "MOXY.ENQUEUE", Args: []string{"jobs", "payload"}})
	if err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	if enqueue.TaskID == "" {
		t.Fatal("enqueue task ID is empty")
	}

	fetch, err := handler.Handle(Command{Name: "MOXY.FETCH", Args: []string{"jobs", "1000"}})
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	if fetch.LeaseID == "" {
		t.Fatal("fetch lease ID is empty")
	}
	if fetch.TaskID != enqueue.TaskID {
		t.Fatalf("fetch task ID = %q, want %q", fetch.TaskID, enqueue.TaskID)
	}
	if string(fetch.Payload) != "payload" {
		t.Fatalf("payload = %q, want %q", fetch.Payload, "payload")
	}
}

func TestFetchEmptyQueueReturnsQueueEmpty(t *testing.T) {
	handler := newTestHandler()

	if _, err := handler.Handle(Command{Name: "MOXY.FETCH", Args: []string{"jobs", "1000"}}); !errors.Is(err, core.ErrQueueEmpty) {
		t.Fatalf("fetch error = %v, want ErrQueueEmpty", err)
	}
}

func TestAckCompletesFetchedLease(t *testing.T) {
	handler := newTestHandler()
	if _, err := handler.Handle(Command{Name: "MOXY.ENQUEUE", Args: []string{"jobs", "payload"}}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	fetch, err := handler.Handle(Command{Name: "MOXY.FETCH", Args: []string{"jobs", "1000"}})
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	ack, err := handler.Handle(Command{Name: "MOXY.ACK", Args: []string{fetch.LeaseID}})
	if err != nil {
		t.Fatalf("ack returned error: %v", err)
	}
	if !ack.OK {
		t.Fatal("ack response OK is false")
	}

	stats, err := handler.Handle(Command{Name: "MOXY.STATS", Args: []string{"jobs"}})
	if err != nil {
		t.Fatalf("stats returned error: %v", err)
	}
	if stats.Stats.Processing != 0 || stats.Stats.ActiveLeases != 0 {
		t.Fatalf("stats after ack = %+v, want processing=0 active=0", stats.Stats)
	}
}

func TestAckUnknownLeaseReturnsLeaseNotFound(t *testing.T) {
	handler := newTestHandler()

	if _, err := handler.Handle(Command{Name: "MOXY.ACK", Args: []string{"missing"}}); !errors.Is(err, core.ErrLeaseNotFound) {
		t.Fatalf("ack error = %v, want ErrLeaseNotFound", err)
	}
}

func TestInvalidCommandNameFails(t *testing.T) {
	handler := newTestHandler()

	if _, err := handler.Handle(Command{Name: "NOPE", Args: []string{}}); !errors.Is(err, ErrUnknownCommand) {
		t.Fatalf("command error = %v, want ErrUnknownCommand", err)
	}
}

func TestInvalidArgumentCountFails(t *testing.T) {
	handler := newTestHandler()

	if _, err := handler.Handle(Command{Name: "MOXY.ENQUEUE", Args: []string{"jobs"}}); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("enqueue error = %v, want ErrInvalidArguments", err)
	}
	if _, err := handler.Handle(Command{Name: "MOXY.ACK", Args: []string{"one", "two"}}); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("ack error = %v, want ErrInvalidArguments", err)
	}
}

func TestInvalidTimeoutFails(t *testing.T) {
	handler := newTestHandler()

	if _, err := handler.Handle(Command{Name: "MOXY.FETCH", Args: []string{"jobs", "not-a-number"}}); !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("fetch timeout parse error = %v, want ErrInvalidTimeout", err)
	}
	if _, err := handler.Handle(Command{Name: "MOXY.FETCH", Args: []string{"jobs", "0"}}); !errors.Is(err, core.ErrInvalidTimeout) {
		t.Fatalf("fetch zero timeout error = %v, want core.ErrInvalidTimeout", err)
	}
}

func TestEnqueueReturnsBackendError(t *testing.T) {
	handler := NewHandler(service.New(func(queueName string) queue.Backend {
		return &failingEnqueueBackend{Backend: queue.NewMemoryQueue()}
	}))

	if _, err := handler.Handle(Command{Name: "MOXY.ENQUEUE", Args: []string{"jobs", "payload"}}); !errors.Is(err, errEnqueueFailed) {
		t.Fatalf("enqueue error = %v, want errEnqueueFailed", err)
	}
}

func TestStatsReportsQueueState(t *testing.T) {
	handler := newTestHandler()
	if _, err := handler.Handle(Command{Name: "MOXY.ENQUEUE", Args: []string{"jobs", "one"}}); err != nil {
		t.Fatalf("enqueue one returned error: %v", err)
	}
	if _, err := handler.Handle(Command{Name: "MOXY.ENQUEUE", Args: []string{"jobs", "two"}}); err != nil {
		t.Fatalf("enqueue two returned error: %v", err)
	}
	if _, err := handler.Handle(Command{Name: "MOXY.FETCH", Args: []string{"jobs", "1000"}}); err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	stats, err := handler.Handle(Command{Name: "MOXY.STATS", Args: []string{"jobs"}})
	if err != nil {
		t.Fatalf("stats returned error: %v", err)
	}
	if stats.Stats.Ready != 1 || stats.Stats.Processing != 1 || stats.Stats.ActiveLeases != 1 {
		t.Fatalf("stats = %+v, want ready=1 processing=1 active=1", stats.Stats)
	}
}

func newTestHandler() *Handler {
	return NewHandler(service.New(func(queueName string) queue.Backend {
		return queue.NewMemoryQueue()
	}))
}

var errEnqueueFailed = errors.New("enqueue failed")

type failingEnqueueBackend struct {
	queue.Backend
}

func (b *failingEnqueueBackend) Enqueue(task.Task) error {
	return errEnqueueFailed
}
