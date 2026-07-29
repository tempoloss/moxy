package core

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tempoloss/moxy/internal/queue"
	"github.com/tempoloss/moxy/internal/wal"
)

var crashMatrixBaseTime = time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)

func TestFetchCrashBoundaryBeforeBackendAcquireLeavesTaskReady(t *testing.T) {
	backend, path := crashMatrixBackendWithTask(t, "fetch-before-acquire")

	recovered, log := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, log)

	crashMatrixRequirePlacement(t, recovered, 1, 0, 0)
	lease, err := recovered.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch after restart returned error: %v", err)
	}
	if string(lease.Task.Payload) != "fetch-before-acquire" {
		t.Fatalf("fetched payload = %q, want %q", lease.Task.Payload, "fetch-before-acquire")
	}
}

func TestFetchCrashBoundaryAfterBackendAcquireBeforeJournalStrandsTask(t *testing.T) {
	backend, path := crashMatrixBackendWithTask(t, "fetch-before-journal")
	if _, err := backend.Acquire(); err != nil {
		t.Fatalf("manual acquire returned error: %v", err)
	}

	recovered, log := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, log)

	crashMatrixRequirePlacement(t, recovered, 0, 1, 0)
	reaped, err := recovered.ReapExpired(crashMatrixBaseTime.Add(24 * time.Hour))
	if err != nil {
		t.Fatalf("reap returned error: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("reaped %d leases, want 0 because no lease was recoverable", reaped)
	}
	crashMatrixRequirePlacement(t, recovered, 0, 1, 0)
}

func TestFetchCrashBoundaryAfterAppendBeforeFsyncRestoresIfRecordSurvives(t *testing.T) {
	backend, path := crashMatrixBackendWithTask(t, "fetch-unsynced-survived")
	task := crashMatrixAcquire(t, backend)
	record := crashMatrixFetchRecord("lease-unsynced-fetch", task, time.Minute)
	log := crashMatrixOpenLog(t, path, false)
	if err := log.Append(record); err != nil {
		t.Fatalf("append returned error: %v", err)
	}
	crashMatrixCloseLog(t, log)

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, recoveredLog)

	crashMatrixRequirePlacement(t, recovered, 0, 1, 1)
	crashMatrixRequireNotReapedBeforeDeadline(t, recovered, record.ExpiresAt)
	crashMatrixRequireRequeuedAfterDeadline(t, recovered, record.ExpiresAt)
}

func TestFetchCrashBoundaryAfterFsyncBeforeMemoryPublishRestoresOriginalDeadline(t *testing.T) {
	backend, path := crashMatrixBackendWithTask(t, "fetch-fsynced-before-publish")
	task := crashMatrixAcquire(t, backend)
	record := crashMatrixFetchRecord("lease-fsynced-fetch", task, 10*time.Second)
	log := crashMatrixOpenLog(t, path, true)
	if err := log.Append(record); err != nil {
		t.Fatalf("append returned error: %v", err)
	}
	crashMatrixCloseLog(t, log)

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, recoveredLog)

	crashMatrixRequirePlacement(t, recovered, 0, 1, 1)
	crashMatrixRequireNotReapedBeforeDeadline(t, recovered, record.ExpiresAt)
	crashMatrixRequireRequeuedAfterDeadline(t, recovered, record.ExpiresAt)
}

func TestFetchCrashBoundaryAfterLeasePublishBeforeReplyUsesOriginalDeadline(t *testing.T) {
	backend, path := crashMatrixBackendWithTask(t, "fetch-published-before-reply")
	engine, log := crashMatrixRestart(t, backend, path)
	lease, err := engine.Fetch(10 * time.Second)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	crashMatrixCloseLog(t, log)

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, recoveredLog)

	crashMatrixRequirePlacement(t, recovered, 0, 1, 1)
	crashMatrixRequireNotReapedBeforeDeadline(t, recovered, lease.ExpiresAt)
	crashMatrixRequireRequeuedAfterDeadline(t, recovered, lease.ExpiresAt)
}

func TestAckCrashBoundaryBeforeBackendCompleteKeepsLeaseRetryable(t *testing.T) {
	backend, path, lease, log := crashMatrixFetchedLease(t, "ack-before-complete", time.Minute)
	crashMatrixCloseLog(t, log)

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, recoveredLog)

	crashMatrixRequirePlacement(t, recovered, 0, 1, 1)
	if err := recovered.Ack(lease.LeaseID); err != nil {
		t.Fatalf("retry ack after restart returned error: %v", err)
	}
	crashMatrixRequirePlacement(t, recovered, 0, 0, 0)
}

func TestAckCrashBoundaryAfterBackendCompleteBeforeJournalIsReconciledByReaper(t *testing.T) {
	backend, path, lease, log := crashMatrixFetchedLease(t, "ack-before-journal", time.Second)
	crashMatrixCloseLog(t, log)
	if err := backend.Complete(lease.Task.ID); err != nil {
		t.Fatalf("manual complete returned error: %v", err)
	}

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	crashMatrixRequirePlacement(t, recovered, 0, 0, 1)
	if err := recovered.Ack(lease.LeaseID); !errors.Is(err, queue.ErrTaskNotProcessing) {
		t.Fatalf("retry ack error = %v, want %v", err, queue.ErrTaskNotProcessing)
	}

	reaped, err := recovered.ReapExpired(lease.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap returned error: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("reaped %d leases, want 1", reaped)
	}
	crashMatrixRequirePlacement(t, recovered, 0, 0, 0)
	crashMatrixCloseLog(t, recoveredLog)

	restartedAgain, finalLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, finalLog)
	crashMatrixRequirePlacement(t, restartedAgain, 0, 0, 0)
}

func TestAckCrashBoundaryAfterAppendBeforeFsyncClosesIfRecordSurvives(t *testing.T) {
	backend, path, lease, log := crashMatrixFetchedLease(t, "ack-unsynced-survived", time.Minute)
	crashMatrixCloseLog(t, log)
	if err := backend.Complete(lease.Task.ID); err != nil {
		t.Fatalf("manual complete returned error: %v", err)
	}
	ackLog := crashMatrixOpenLog(t, path, false)
	if err := ackLog.Append(wal.Record{Op: wal.OpAck, LeaseID: lease.LeaseID}); err != nil {
		t.Fatalf("append ack returned error: %v", err)
	}
	crashMatrixCloseLog(t, ackLog)

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, recoveredLog)

	crashMatrixRequirePlacement(t, recovered, 0, 0, 0)
}

func TestAckCrashBoundaryAfterFsyncBeforeMemoryDeleteKeepsTaskCompleted(t *testing.T) {
	backend, path, lease, log := crashMatrixFetchedLease(t, "ack-fsynced-before-delete", time.Minute)
	crashMatrixCloseLog(t, log)
	if err := backend.Complete(lease.Task.ID); err != nil {
		t.Fatalf("manual complete returned error: %v", err)
	}
	ackLog := crashMatrixOpenLog(t, path, true)
	if err := ackLog.Append(wal.Record{Op: wal.OpAck, LeaseID: lease.LeaseID}); err != nil {
		t.Fatalf("append ack returned error: %v", err)
	}
	crashMatrixCloseLog(t, ackLog)

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, recoveredLog)

	crashMatrixRequirePlacement(t, recovered, 0, 0, 0)
	if err := recovered.Ack(lease.LeaseID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("retry ack error = %v, want %v", err, ErrLeaseNotFound)
	}
}

func TestAckCrashBoundaryAfterMemoryDeleteBeforeReplyRemainsCompleted(t *testing.T) {
	backend, path := crashMatrixBackendWithTask(t, "ack-before-reply")
	engine, log := crashMatrixRestart(t, backend, path)
	lease, err := engine.Fetch(time.Minute)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	if err := engine.Ack(lease.LeaseID); err != nil {
		t.Fatalf("ack returned error: %v", err)
	}
	crashMatrixCloseLog(t, log)

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, recoveredLog)

	crashMatrixRequirePlacement(t, recovered, 0, 0, 0)
	if err := recovered.Ack(lease.LeaseID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("retry ack error = %v, want %v", err, ErrLeaseNotFound)
	}
}

func TestTornFinalFetchRecordIsDroppedAndOnlyEarlierLeasesRecover(t *testing.T) {
	backend, path := crashMatrixBackendWithTask(t, "fetch-intact")
	if err := backend.Enqueue(Task{ID: "task-torn", Payload: []byte("fetch-torn")}); err != nil {
		t.Fatalf("enqueue torn task returned error: %v", err)
	}
	intactTask := crashMatrixAcquire(t, backend)
	intactRecord := crashMatrixFetchRecord("lease-intact", intactTask, time.Second)
	log := crashMatrixOpenLog(t, path, true)
	if err := log.Append(intactRecord); err != nil {
		t.Fatalf("append intact fetch returned error: %v", err)
	}
	goodOffset := log.Size()
	crashMatrixCloseLog(t, log)

	tornTask := crashMatrixAcquire(t, backend)
	tornRecord := crashMatrixFetchRecord("lease-torn", tornTask, time.Second)
	crashMatrixAppendTornRecord(t, path, tornRecord)

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, recoveredLog)

	crashMatrixRequirePlacement(t, recovered, 0, 2, 1)
	if size := recoveredLog.Size(); size != goodOffset {
		t.Fatalf("recovered WAL size = %d, want truncation to last good offset %d", size, goodOffset)
	}
	reaped, err := recovered.ReapExpired(intactRecord.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap returned error: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("reaped %d leases, want only the intact lease", reaped)
	}
	crashMatrixRequirePlacement(t, recovered, 1, 1, 0)
}

func TestTornFinalAckRecordIsDroppedAndReaperDoesNotResurrectCompletedTask(t *testing.T) {
	backend, path, lease, log := crashMatrixFetchedLease(t, "ack-torn", time.Second)
	crashMatrixCloseLog(t, log)
	if err := backend.Complete(lease.Task.ID); err != nil {
		t.Fatalf("manual complete returned error: %v", err)
	}
	beforeTorn, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat WAL before torn append returned error: %v", err)
	}
	crashMatrixAppendTornRecord(t, path, wal.Record{Op: wal.OpAck, LeaseID: lease.LeaseID})

	recovered, recoveredLog := crashMatrixRestart(t, backend, path)
	defer crashMatrixCloseLog(t, recoveredLog)

	crashMatrixRequirePlacement(t, recovered, 0, 0, 1)
	if size := recoveredLog.Size(); size != beforeTorn.Size() {
		t.Fatalf("recovered WAL size = %d, want truncation to last good offset %d", size, beforeTorn.Size())
	}
	reaped, err := recovered.ReapExpired(lease.ExpiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap returned error: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("reaped %d leases, want 1", reaped)
	}
	crashMatrixRequirePlacement(t, recovered, 0, 0, 0)
}

func crashMatrixBackendWithTask(t *testing.T, payload string) (*queue.MemoryQueue, string) {
	t.Helper()
	backend := queue.NewMemoryQueue()
	if err := backend.Enqueue(Task{ID: "task-" + payload, Payload: []byte(payload)}); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}
	return backend, filepath.Join(t.TempDir(), "moxy.wal")
}

func crashMatrixFetchedLease(t *testing.T, payload string, timeout time.Duration) (*queue.MemoryQueue, string, *Lease, *wal.Log) {
	t.Helper()
	backend, path := crashMatrixBackendWithTask(t, payload)
	engine, log := crashMatrixRestart(t, backend, path)
	lease, err := engine.Fetch(timeout)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	return backend, path, lease, log
}

func crashMatrixRestart(t *testing.T, backend queue.Backend, path string) (*Engine, *wal.Log) {
	t.Helper()
	log := crashMatrixOpenLog(t, path, true)
	return NewEngineWithBackendAndConfig(backend, EngineConfig{
		Journal:   log,
		Recovered: log.Recovered(),
	}), log
}

func crashMatrixOpenLog(t *testing.T, path string, sync bool) *wal.Log {
	t.Helper()
	log, err := wal.OpenWith(path, wal.Options{Sync: sync})
	if err != nil {
		t.Fatalf("open WAL returned error: %v", err)
	}
	return log
}

func crashMatrixCloseLog(t *testing.T, log *wal.Log) {
	t.Helper()
	if err := log.Close(); err != nil {
		t.Fatalf("close WAL returned error: %v", err)
	}
}

func crashMatrixAcquire(t *testing.T, backend queue.Backend) Task {
	t.Helper()
	task, err := backend.Acquire()
	if err != nil {
		t.Fatalf("manual acquire returned error: %v", err)
	}
	return task
}

func crashMatrixFetchRecord(leaseID string, task Task, timeout time.Duration) wal.Record {
	return wal.Record{
		Op:        wal.OpFetch,
		LeaseID:   leaseID,
		Task:      task,
		CreatedAt: crashMatrixBaseTime,
		ExpiresAt: crashMatrixBaseTime.Add(timeout),
	}
}

func crashMatrixRequirePlacement(t *testing.T, engine *Engine, ready, processing, active int) {
	t.Helper()
	stats := engine.Stats()
	if stats.Ready != ready {
		t.Fatalf("ready tasks = %d, want %d (full stats: %+v)", stats.Ready, ready, stats)
	}
	if stats.Processing != processing {
		t.Fatalf("processing tasks = %d, want %d (full stats: %+v)", stats.Processing, processing, stats)
	}
	if stats.ActiveLeases != active {
		t.Fatalf("active leases = %d, want %d (full stats: %+v)", stats.ActiveLeases, active, stats)
	}
}

func crashMatrixRequireNotReapedBeforeDeadline(t *testing.T, engine *Engine, expiresAt time.Time) {
	t.Helper()
	reaped, err := engine.ReapExpired(expiresAt.Add(-time.Nanosecond))
	if err != nil {
		t.Fatalf("reap before deadline returned error: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("reaped %d leases before original deadline, want 0", reaped)
	}
	crashMatrixRequirePlacement(t, engine, 0, 1, 1)
}

func crashMatrixRequireRequeuedAfterDeadline(t *testing.T, engine *Engine, expiresAt time.Time) {
	t.Helper()
	reaped, err := engine.ReapExpired(expiresAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("reap after deadline returned error: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("reaped %d leases after original deadline, want 1", reaped)
	}
	crashMatrixRequirePlacement(t, engine, 1, 0, 0)
}

func crashMatrixAppendTornRecord(t *testing.T, path string, record wal.Record) {
	t.Helper()
	frame := crashMatrixEncodeFrame(t, record)
	cut := len(frame) / 2
	if cut < 8 {
		cut = 8
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open WAL for torn append returned error: %v", err)
	}
	if _, err := file.Write(frame[:cut]); err != nil {
		file.Close()
		t.Fatalf("write torn record returned error: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close torn WAL returned error: %v", err)
	}
}

func crashMatrixEncodeFrame(t *testing.T, record wal.Record) []byte {
	t.Helper()
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal record returned error: %v", err)
	}
	frame := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	return frame
}
