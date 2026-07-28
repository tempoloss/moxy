package protocol

import (
	"strings"
	"testing"

	"github.com/tempoloss/moxy/internal/command"
	"github.com/tempoloss/moxy/internal/queue"
	"github.com/tempoloss/moxy/internal/resp"
	"github.com/tempoloss/moxy/internal/service"
)

func TestRESPPingReturnsPong(t *testing.T) {
	reply := newTestAdapter().Handle(resp.Array(resp.BulkString("PING")))

	if reply.Type != resp.TypeSimpleString || reply.String != "PONG" {
		t.Fatalf("reply = %+v, want PONG simple string", reply)
	}
}

func TestRESPMoxyEnqueueReturnsTaskID(t *testing.T) {
	reply := newTestAdapter().Handle(resp.Array(
		resp.BulkString("MOXY.ENQUEUE"),
		resp.BulkString("jobs"),
		resp.BulkString("hello"),
	))

	assertArrayLength(t, reply, 2)
	assertBulkString(t, reply.Array[0], "task_id")
	if reply.Array[1].Type != resp.TypeBulkString || reply.Array[1].String == "" {
		t.Fatalf("task id reply = %+v, want non-empty bulk string", reply.Array[1])
	}
}

func TestRESPMoxyFetchReturnsLeaseTaskAndPayload(t *testing.T) {
	adapter := newTestAdapter()
	enqueue := adapter.Handle(resp.Array(resp.BulkString("MOXY.ENQUEUE"), resp.BulkString("jobs"), resp.BulkString("hello")))
	taskID := enqueue.Array[1].String

	reply := adapter.Handle(resp.Array(resp.BulkString("MOXY.FETCH"), resp.BulkString("jobs"), resp.BulkString("30000")))

	assertArrayLength(t, reply, 6)
	assertBulkString(t, reply.Array[0], "lease_id")
	if reply.Array[1].Type != resp.TypeBulkString || reply.Array[1].String == "" {
		t.Fatalf("lease id reply = %+v, want non-empty bulk string", reply.Array[1])
	}
	assertBulkString(t, reply.Array[2], "task_id")
	assertBulkString(t, reply.Array[3], taskID)
	assertBulkString(t, reply.Array[4], "payload")
	assertBulkString(t, reply.Array[5], "hello")
}

func TestRESPMoxyFetchEmptyReturnsNullBulkString(t *testing.T) {
	reply := newTestAdapter().Handle(resp.Array(resp.BulkString("MOXY.FETCH"), resp.BulkString("jobs"), resp.BulkString("30000")))

	if reply.Type != resp.TypeBulkString || !reply.Null {
		t.Fatalf("reply = %+v, want null bulk string", reply)
	}
}

func TestRESPMoxyAckReturnsOK(t *testing.T) {
	adapter := newTestAdapter()
	adapter.Handle(resp.Array(resp.BulkString("MOXY.ENQUEUE"), resp.BulkString("jobs"), resp.BulkString("hello")))
	fetch := adapter.Handle(resp.Array(resp.BulkString("MOXY.FETCH"), resp.BulkString("jobs"), resp.BulkString("30000")))

	reply := adapter.Handle(resp.Array(resp.BulkString("MOXY.ACK"), resp.BulkString(fetch.Array[1].String)))

	if reply.Type != resp.TypeSimpleString || reply.String != "OK" {
		t.Fatalf("reply = %+v, want OK", reply)
	}
}

func TestRESPMoxyStatsReturnsQueueState(t *testing.T) {
	adapter := newTestAdapter()
	adapter.Handle(resp.Array(resp.BulkString("MOXY.ENQUEUE"), resp.BulkString("jobs"), resp.BulkString("one")))
	adapter.Handle(resp.Array(resp.BulkString("MOXY.ENQUEUE"), resp.BulkString("jobs"), resp.BulkString("two")))
	adapter.Handle(resp.Array(resp.BulkString("MOXY.FETCH"), resp.BulkString("jobs"), resp.BulkString("30000")))

	reply := adapter.Handle(resp.Array(resp.BulkString("MOXY.STATS"), resp.BulkString("jobs")))

	assertArrayLength(t, reply, 10)
	assertBulkString(t, reply.Array[0], "ready")
	assertInteger(t, reply.Array[1], 1)
	assertBulkString(t, reply.Array[2], "processing")
	assertInteger(t, reply.Array[3], 1)
	assertBulkString(t, reply.Array[4], "dead")
	assertInteger(t, reply.Array[5], 0)
	assertBulkString(t, reply.Array[6], "active_leases")
	assertInteger(t, reply.Array[7], 1)
	assertBulkString(t, reply.Array[8], "heap")
	assertInteger(t, reply.Array[9], 1)
}

func TestRESPInvalidCommandReturnsError(t *testing.T) {
	reply := newTestAdapter().Handle(resp.Array(resp.BulkString("NOPE")))

	if reply.Type != resp.TypeError || !strings.Contains(reply.String, "unknown command") {
		t.Fatalf("reply = %+v, want unknown command error", reply)
	}
}

func TestRESPLowercaseCommandNameWorks(t *testing.T) {
	reply := newTestAdapter().Handle(resp.Array(resp.BulkString("ping")))

	if reply.Type != resp.TypeSimpleString || reply.String != "PONG" {
		t.Fatalf("reply = %+v, want PONG", reply)
	}
}

func TestRESPNonArrayInputFailsCleanly(t *testing.T) {
	reply := newTestAdapter().Handle(resp.BulkString("PING"))

	if reply.Type != resp.TypeError || !strings.Contains(reply.String, "expected array") {
		t.Fatalf("reply = %+v, want expected array error", reply)
	}
}

func TestRESPArrayContainingNonBulkCommandElementsFailsCleanly(t *testing.T) {
	reply := newTestAdapter().Handle(resp.Array(resp.BulkString("PING"), resp.Integer(1)))

	if reply.Type != resp.TypeError || !strings.Contains(reply.String, "bulk string") {
		t.Fatalf("reply = %+v, want bulk string error", reply)
	}
}

func newTestAdapter() *Adapter {
	svc := service.New(func(queueName string) queue.Backend {
		return queue.NewMemoryQueue()
	})
	return NewAdapter(command.NewHandler(svc))
}

func assertArrayLength(t *testing.T, value resp.Value, want int) {
	t.Helper()

	if value.Type != resp.TypeArray || len(value.Array) != want {
		t.Fatalf("reply = %+v, want array length %d", value, want)
	}
}

func assertBulkString(t *testing.T, value resp.Value, want string) {
	t.Helper()

	if value.Type != resp.TypeBulkString || value.String != want {
		t.Fatalf("value = %+v, want bulk string %q", value, want)
	}
}

func assertInteger(t *testing.T, value resp.Value, want int64) {
	t.Helper()

	if value.Type != resp.TypeInteger || value.Integer != want {
		t.Fatalf("value = %+v, want integer %d", value, want)
	}
}
