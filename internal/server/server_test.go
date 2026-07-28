package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tempoloss/moxy/internal/command"
	"github.com/tempoloss/moxy/internal/queue"
	"github.com/tempoloss/moxy/internal/resp"
	"github.com/tempoloss/moxy/internal/service"
)

func TestServerStartsAndAcceptsConnection(t *testing.T) {
	server, cancel, done := startTestServer(t)
	defer stopTestServer(t, cancel, done)

	conn := dialTestServer(t, server)
	if err := conn.Close(); err != nil {
		t.Fatalf("close connection: %v", err)
	}
}

func TestServerPingReturnsPong(t *testing.T) {
	server, cancel, done := startTestServer(t)
	defer stopTestServer(t, cancel, done)

	conn := dialTestServer(t, server)
	defer conn.Close()

	reply := sendRESP(t, conn, resp.Array(resp.BulkString("PING")))
	if reply.Type != resp.TypeSimpleString || reply.String != "PONG" {
		t.Fatalf("reply = %+v, want PONG", reply)
	}
}

func TestServerMoxyEnqueueReturnsTaskID(t *testing.T) {
	server, cancel, done := startTestServer(t)
	defer stopTestServer(t, cancel, done)

	conn := dialTestServer(t, server)
	defer conn.Close()

	reply := sendRESP(t, conn, resp.Array(resp.BulkString("MOXY.ENQUEUE"), resp.BulkString("jobs"), resp.BulkString("hello")))
	if reply.Type != resp.TypeArray || len(reply.Array) != 2 || reply.Array[1].String == "" {
		t.Fatalf("reply = %+v, want task_id array", reply)
	}
}

func TestServerMoxyFetchReturnsLeaseTaskAndPayload(t *testing.T) {
	server, cancel, done := startTestServer(t)
	defer stopTestServer(t, cancel, done)

	conn := dialTestServer(t, server)
	defer conn.Close()

	enqueue := sendRESP(t, conn, resp.Array(resp.BulkString("MOXY.ENQUEUE"), resp.BulkString("jobs"), resp.BulkString("hello")))
	fetch := sendRESP(t, conn, resp.Array(resp.BulkString("MOXY.FETCH"), resp.BulkString("jobs"), resp.BulkString("30000")))
	if fetch.Type != resp.TypeArray || len(fetch.Array) != 6 {
		t.Fatalf("fetch reply = %+v, want 6-element array", fetch)
	}
	if fetch.Array[3].String != enqueue.Array[1].String {
		t.Fatalf("task id = %q, want %q", fetch.Array[3].String, enqueue.Array[1].String)
	}
	if fetch.Array[5].String != "hello" {
		t.Fatalf("payload = %q, want hello", fetch.Array[5].String)
	}
}

func TestServerMoxyAckReturnsOK(t *testing.T) {
	server, cancel, done := startTestServer(t)
	defer stopTestServer(t, cancel, done)

	conn := dialTestServer(t, server)
	defer conn.Close()

	sendRESP(t, conn, resp.Array(resp.BulkString("MOXY.ENQUEUE"), resp.BulkString("jobs"), resp.BulkString("hello")))
	fetch := sendRESP(t, conn, resp.Array(resp.BulkString("MOXY.FETCH"), resp.BulkString("jobs"), resp.BulkString("30000")))
	ack := sendRESP(t, conn, resp.Array(resp.BulkString("MOXY.ACK"), resp.BulkString(fetch.Array[1].String)))
	if ack.Type != resp.TypeSimpleString || ack.String != "OK" {
		t.Fatalf("ack reply = %+v, want OK", ack)
	}
}

func TestServerMoxyStatsReturnsStats(t *testing.T) {
	server, cancel, done := startTestServer(t)
	defer stopTestServer(t, cancel, done)

	conn := dialTestServer(t, server)
	defer conn.Close()

	sendRESP(t, conn, resp.Array(resp.BulkString("MOXY.ENQUEUE"), resp.BulkString("jobs"), resp.BulkString("hello")))
	stats := sendRESP(t, conn, resp.Array(resp.BulkString("MOXY.STATS"), resp.BulkString("jobs")))
	if stats.Type != resp.TypeArray || len(stats.Array) != 10 {
		t.Fatalf("stats reply = %+v, want 10-element array", stats)
	}
	if stats.Array[0].String != "ready" || stats.Array[1].Integer != 1 {
		t.Fatalf("stats reply = %+v, want ready=1", stats)
	}
	if stats.Array[4].String != "dead" || stats.Array[5].Integer != 0 {
		t.Fatalf("stats reply = %+v, want dead=0", stats)
	}
}

func TestServerMultipleCommandsOnSameConnection(t *testing.T) {
	server, cancel, done := startTestServer(t)
	defer stopTestServer(t, cancel, done)

	conn := dialTestServer(t, server)
	defer conn.Close()

	for i := 0; i < 3; i++ {
		reply := sendRESP(t, conn, resp.Array(resp.BulkString("PING")))
		if reply.Type != resp.TypeSimpleString || reply.String != "PONG" {
			t.Fatalf("reply %d = %+v, want PONG", i, reply)
		}
	}
}

func TestServerMalformedRESPReturnsErrorAndClosesCleanly(t *testing.T) {
	server, cancel, done := startTestServer(t)
	defer stopTestServer(t, cancel, done)

	conn := dialTestServer(t, server)
	defer conn.Close()

	if _, err := io.WriteString(conn, "*x\r\n"); err != nil {
		t.Fatalf("write malformed RESP: %v", err)
	}
	reply, err := resp.NewReader(conn).ReadValue()
	if err != nil {
		t.Fatalf("read error reply: %v", err)
	}
	if reply.Type != resp.TypeError {
		t.Fatalf("reply = %+v, want RESP error", reply)
	}
}

func TestServerShutdownClosesListener(t *testing.T) {
	server, cancel, done := startTestServer(t)
	addr := server.Addr()
	cancel()
	waitServerDone(t, done)

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 10*time.Millisecond)
		if err != nil {
			return
		}
		conn.Close()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("listener at %s still accepted connections after shutdown", addr)
}

func TestServerTwoSimultaneousClients(t *testing.T) {
	server, cancel, done := startTestServer(t)
	defer stopTestServer(t, cancel, done)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := net.Dial("tcp", server.Addr())
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()

			if err := resp.NewWriter(conn).WriteValue(resp.Array(resp.BulkString("PING"))); err != nil {
				errs <- err
				return
			}
			reply, err := resp.NewReader(conn).ReadValue()
			if err != nil {
				errs <- err
				return
			}
			if reply.Type != resp.TypeSimpleString || reply.String != "PONG" {
				errs <- errors.New("unexpected PING reply")
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func startTestServer(t *testing.T) (*Server, context.CancelFunc, <-chan error) {
	t.Helper()

	svc := service.New(func(queueName string) queue.Backend {
		return queue.NewMemoryQueue()
	})
	server := New(command.NewHandler(svc), Config{Addr: "127.0.0.1:0"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.ListenAndServe(ctx)
	}()
	waitForAddr(t, server)
	return server, cancel, done
}

func stopTestServer(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()

	cancel()
	waitServerDone(t, done)
}

func waitForAddr(t *testing.T, server *Server) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if server.Addr() != "" {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("server did not start listening before deadline")
}

func waitServerDone(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop before deadline")
	}
}

func dialTestServer(t *testing.T, server *Server) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	return conn
}

func sendRESP(t *testing.T, conn net.Conn, value resp.Value) resp.Value {
	t.Helper()

	if err := resp.NewWriter(conn).WriteValue(value); err != nil {
		t.Fatalf("write RESP: %v", err)
	}
	reply, err := resp.NewReader(conn).ReadValue()
	if err != nil {
		t.Fatalf("read RESP: %v", err)
	}
	return reply
}
