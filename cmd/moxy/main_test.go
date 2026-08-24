package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tempoloss/moxy/internal/resp"
)

func TestRunPrintsVersionAndExitsBeforeListening(t *testing.T) {
	originalVersion := version
	version = "v0.1.0-test"
	t.Cleanup(func() {
		version = originalVersion
	})

	var stdout bytes.Buffer
	code := run(context.Background(), []string{"-version"}, &stdout)

	if code != 0 {
		t.Fatalf("run(-version) exit code = %d, want 0", code)
	}
	if got, want := stdout.String(), "v0.1.0-test\n"; got != want {
		t.Fatalf("run(-version) stdout = %q, want %q", got, want)
	}
}

func TestRunRejectsNegativeMaxAttempts(t *testing.T) {
	var stdout bytes.Buffer
	code := run(context.Background(), []string{"-max-attempts", "-1"}, &stdout)

	if code != 2 {
		t.Fatalf("run(-max-attempts -1) exit code = %d, want 2", code)
	}
	if got, want := stdout.String(), "max-attempts must be non-negative\n"; got != want {
		t.Fatalf("run(-max-attempts -1) stdout = %q, want %q", got, want)
	}
}

func TestRunVersionExitsBeforeMaxAttemptsValidation(t *testing.T) {
	originalVersion := version
	version = "v0.1.0-test"
	t.Cleanup(func() {
		version = originalVersion
	})

	var stdout bytes.Buffer
	code := run(context.Background(), []string{"-version", "-max-attempts", "-1"}, &stdout)

	if code != 0 {
		t.Fatalf("run(-version -max-attempts -1) exit code = %d, want 0", code)
	}
	if got, want := stdout.String(), "v0.1.0-test\n"; got != want {
		t.Fatalf("run(-version -max-attempts -1) stdout = %q, want %q", got, want)
	}
}

func TestRunMaxAttemptsOneDeadLettersExpiredLease(t *testing.T) {
	addr := freeLoopbackAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	var stdout safeBuffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"-addr", addr, "-max-attempts", "1"}, &stdout)
	}()

	stopped := false
	t.Cleanup(func() {
		cancel()
		if stopped {
			return
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("run did not stop after cancellation; stdout:\n%s", stdout.String())
		}
	})

	client := dialDaemonRESP(t, addr, done, &stopped, &stdout)
	clientClosed := false
	t.Cleanup(func() {
		if !clientClosed {
			_ = client.Close()
			clientClosed = true
		}
	})

	pong := mustDaemonRESP(t, client, resp.Array(resp.BulkString("PING")))
	if pong.Type != resp.TypeSimpleString || pong.String != "PONG" {
		t.Fatalf("PING reply = %+v, want PONG", pong)
	}
	enqueue := mustDaemonRESP(t, client, resp.Array(resp.BulkString("MOXY.ENQUEUE"), resp.BulkString("jobs"), resp.BulkString("payload")))
	if enqueue.Type != resp.TypeArray || len(enqueue.Array) != 2 || enqueue.Array[1].String == "" {
		t.Fatalf("MOXY.ENQUEUE reply = %+v, want task_id array", enqueue)
	}
	fetch := mustDaemonRESP(t, client, resp.Array(resp.BulkString("MOXY.FETCH"), resp.BulkString("jobs"), resp.BulkString("1")))
	if fetch.Type != resp.TypeArray || len(fetch.Array) != 6 {
		t.Fatalf("MOXY.FETCH reply = %+v, want lease array", fetch)
	}

	waitForDaemonDeadLetter(t, client, done, &stopped, &stdout, "jobs")

	if err := client.Close(); err != nil {
		t.Fatalf("close RESP client: %v", err)
	}
	clientClosed = true
	cancel()
	select {
	case code := <-done:
		stopped = true
		if code != 0 {
			t.Fatalf("run exit code after cancellation = %d, want 0; stdout:\n%s", code, stdout.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("run did not stop after cancellation; stdout:\n%s", stdout.String())
	}
}

func TestShouldWarnUnauthenticatedBind(t *testing.T) {
	for _, tc := range []struct {
		addr string
		want bool
	}{
		{addr: "", want: false},
		{addr: ":6380", want: false},
		{addr: "127.0.0.1:6380", want: false},
		{addr: "[::1]:6380", want: false},
		{addr: "0.0.0.0:6380", want: true},
		{addr: "[::]:6380", want: true},
	} {
		t.Run(tc.addr, func(t *testing.T) {
			if got := shouldWarnUnauthenticatedBind(tc.addr); got != tc.want {
				t.Fatalf("shouldWarnUnauthenticatedBind(%q) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
}

func TestJournalFileNameRejectsNamesThatEscapeTheDirectory(t *testing.T) {
	// Queue names arrive over the network and become file names, so anything
	// that could resolve outside the journal directory has to be refused rather
	// than rewritten into something that might collide with another queue.
	for _, name := range []string{
		"",
		"..",
		"../escape",
		"nested/queue",
		`windows\queue`,
		"queue.with.dots",
		"queue name",
		"queue\x00null",
	} {
		if _, err := journalFileName(name); err == nil {
			t.Errorf("journalFileName(%q) was accepted, want it rejected", name)
		}
	}
}

func TestJournalFileNameAcceptsOrdinaryNames(t *testing.T) {
	for name, want := range map[string]string{
		"jobs":         "jobs.wal",
		"email-send":   "email-send.wal",
		"batch_2":      "batch_2.wal",
		"MixedCase123": "MixedCase123.wal",
	} {
		got, err := journalFileName(name)
		if err != nil {
			t.Errorf("journalFileName(%q) returned error: %v", name, err)
			continue
		}
		if got != want {
			t.Errorf("journalFileName(%q) = %q, want %q", name, got, want)
		}
	}
}

type safeBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *safeBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buffer.Write(payload)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buffer.String()
}

type daemonRESPConn struct {
	conn   net.Conn
	reader *resp.Reader
	writer *resp.Writer
}

func (c *daemonRESPConn) Close() error {
	return c.conn.Close()
}

func (c *daemonRESPConn) RoundTrip(value resp.Value) (resp.Value, error) {
	if err := c.conn.SetDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		return resp.Value{}, err
	}
	defer c.conn.SetDeadline(time.Time{})

	if err := c.writer.WriteValue(value); err != nil {
		return resp.Value{}, err
	}
	return c.reader.ReadValue()
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on loopback port: %v", err)
	}
	defer listener.Close()

	return listener.Addr().String()
}

func dialDaemonRESP(t *testing.T, addr string, done <-chan int, stopped *bool, stdout *safeBuffer) *daemonRESPConn {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		failIfDaemonExited(t, done, stopped, stdout)

		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			return &daemonRESPConn{
				conn:   conn,
				reader: resp.NewReader(conn),
				writer: resp.NewWriter(conn),
			}
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}

	failIfDaemonExited(t, done, stopped, stdout)
	t.Fatalf("daemon did not accept a TCP connection before deadline: %v; stdout:\n%s", lastErr, stdout.String())
	return nil
}

func mustDaemonRESP(t *testing.T, client *daemonRESPConn, value resp.Value) resp.Value {
	t.Helper()

	reply, err := client.RoundTrip(value)
	if err != nil {
		t.Fatalf("RESP round trip failed: %v", err)
	}
	return reply
}

func waitForDaemonDeadLetter(t *testing.T, client *daemonRESPConn, done <-chan int, stopped *bool, stdout *safeBuffer, queue string) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	var lastStats map[string]int64
	var lastErr error
	for time.Now().Before(deadline) {
		failIfDaemonExited(t, done, stopped, stdout)

		reply, err := client.RoundTrip(resp.Array(resp.BulkString("MOXY.STATS"), resp.BulkString(queue)))
		if err == nil {
			stats, statsErr := statsFromReply(reply)
			if statsErr == nil {
				lastStats = stats
				if stats["dead"] == 1 {
					return
				}
			} else {
				lastErr = statsErr
			}
		} else {
			lastErr = err
		}
		time.Sleep(20 * time.Millisecond)
	}

	failIfDaemonExited(t, done, stopped, stdout)
	t.Fatalf("MOXY.STATS %s did not report dead=1 before deadline; last stats=%v error=%v; stdout:\n%s", queue, lastStats, lastErr, stdout.String())
}

func failIfDaemonExited(t *testing.T, done <-chan int, stopped *bool, stdout *safeBuffer) {
	t.Helper()

	select {
	case code := <-done:
		*stopped = true
		t.Fatalf("run exited early with code %d; stdout:\n%s", code, stdout.String())
	default:
	}
}

func statsFromReply(reply resp.Value) (map[string]int64, error) {
	if reply.Type != resp.TypeArray || len(reply.Array)%2 != 0 {
		return nil, fmt.Errorf("stats reply = %+v, want even array", reply)
	}

	stats := make(map[string]int64, len(reply.Array)/2)
	for index := 0; index < len(reply.Array); index += 2 {
		key := reply.Array[index]
		value := reply.Array[index+1]
		if key.Type != resp.TypeBulkString {
			return nil, fmt.Errorf("stats key %d = %+v, want bulk string", index, key)
		}
		if value.Type != resp.TypeInteger {
			return nil, fmt.Errorf("stats value %q = %+v, want integer", key.String, value)
		}
		stats[key.String] = value.Integer
	}

	return stats, nil
}
