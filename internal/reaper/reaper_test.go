package reaper

import (
	"context"
	"testing"
	"time"

	"github.com/an8kk/moxy/internal/core"
)

func TestRunStarts(t *testing.T) {
	engine := core.NewEngine()
	ctx, cancel := context.WithCancel(context.Background())
	done := runForTest(ctx, engine, time.Millisecond)

	time.Sleep(3 * time.Millisecond)
	cancel()
	waitForStop(t, done)

	stats := engine.Stats()
	if stats.Ready != 0 {
		t.Fatalf("ready count = %d, want 0", stats.Ready)
	}
	if stats.ActiveLeases != 0 {
		t.Fatalf("active lease count = %d, want 0", stats.ActiveLeases)
	}
}

func TestRunEventuallyRequeuesExpiredLeases(t *testing.T) {
	engine := core.NewEngine()
	if _, err := engine.Enqueue([]byte("first")); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	if _, err := engine.Fetch(time.Millisecond); err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runForTest(ctx, engine, time.Millisecond)
	defer func() {
		cancel()
		waitForStop(t, done)
	}()

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		stats := engine.Stats()
		if stats.Ready == 1 && stats.ActiveLeases == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("lease was not requeued before deadline, stats=%+v", engine.Stats())
}

func TestRunStopsOnContextCancellation(t *testing.T) {
	engine := core.NewEngine()
	ctx, cancel := context.WithCancel(context.Background())
	done := runForTest(ctx, engine, time.Millisecond)

	cancel()
	waitForStop(t, done)
}

func runForTest(ctx context.Context, engine *core.Engine, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, engine, interval)
	}()
	return done
}

func waitForStop(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("reaper did not stop before deadline")
	}
}
