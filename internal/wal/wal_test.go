package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tempoloss/moxy/internal/task"
)

func fetchRecord(leaseID, taskID string) Record {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	return Record{
		Op:        OpFetch,
		LeaseID:   leaseID,
		Task:      task.Task{ID: taskID, Payload: []byte("payload-" + taskID)},
		CreatedAt: now,
		ExpiresAt: now.Add(30 * time.Second),
	}
}

func openTemp(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "moxy.wal")
	log, err := OpenWith(path, Options{Sync: false})
	if err != nil {
		t.Fatalf("open returned error: %v", err)
	}
	t.Cleanup(func() { log.Close() })
	return log, path
}

func TestOpenOnMissingFileRecoversNothing(t *testing.T) {
	log, _ := openTemp(t)
	if got := len(log.Recovered()); got != 0 {
		t.Fatalf("recovered %d records from a new journal, want 0", got)
	}
}

func TestRecordsSurviveReopen(t *testing.T) {
	log, path := openTemp(t)
	first := fetchRecord("lease-1", "task-1")
	if err := log.Append(first); err != nil {
		t.Fatalf("append returned error: %v", err)
	}
	if err := log.Append(Record{Op: OpAck, LeaseID: "lease-1"}); err != nil {
		t.Fatalf("append returned error: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}

	reopened, err := OpenWith(path, Options{Sync: false})
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()

	records := reopened.Recovered()
	if len(records) != 2 {
		t.Fatalf("recovered %d records, want 2", len(records))
	}
	if records[0].Op != OpFetch || records[0].LeaseID != "lease-1" {
		t.Fatalf("first record = %+v, want a fetch of lease-1", records[0])
	}
	if string(records[0].Task.Payload) != "payload-task-1" {
		t.Fatalf("payload = %q, want %q", records[0].Task.Payload, "payload-task-1")
	}
	if !records[0].ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("expiry = %v, want %v", records[0].ExpiresAt, first.ExpiresAt)
	}
	if records[1].Op != OpAck {
		t.Fatalf("second record op = %q, want %q", records[1].Op, OpAck)
	}
}

func TestTornTailIsDroppedAndTruncated(t *testing.T) {
	log, path := openTemp(t)
	for i, id := range []string{"lease-1", "lease-2"} {
		if err := log.Append(fetchRecord(id, "task-"+string(rune('1'+i)))); err != nil {
			t.Fatalf("append returned error: %v", err)
		}
	}
	intact := log.Size()
	if err := log.Append(fetchRecord("lease-3", "task-3")); err != nil {
		t.Fatalf("append returned error: %v", err)
	}
	full := log.Size()
	if err := log.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}

	// Simulate a crash midway through the third record.
	if err := os.Truncate(path, intact+(full-intact)/2); err != nil {
		t.Fatalf("truncate returned error: %v", err)
	}

	reopened, err := OpenWith(path, Options{Sync: false})
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()

	if got := len(reopened.Recovered()); got != 2 {
		t.Fatalf("recovered %d records after a torn write, want the 2 intact ones", got)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat returned error: %v", err)
	}
	if info.Size() != intact {
		t.Fatalf("file is %d bytes after recovery, want it truncated back to %d", info.Size(), intact)
	}

	// The journal must remain usable: an append after recovery lands on a clean
	// boundary and survives another reopen.
	if err := reopened.Append(fetchRecord("lease-4", "task-4")); err != nil {
		t.Fatalf("append after recovery returned error: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}

	final, err := OpenWith(path, Options{Sync: false})
	if err != nil {
		t.Fatalf("final reopen returned error: %v", err)
	}
	defer final.Close()
	records := final.Recovered()
	if len(records) != 3 {
		t.Fatalf("recovered %d records, want 3", len(records))
	}
	if records[2].LeaseID != "lease-4" {
		t.Fatalf("last lease = %q, want lease-4", records[2].LeaseID)
	}
}

func TestCorruptChecksumStopsReplay(t *testing.T) {
	log, path := openTemp(t)
	if err := log.Append(fetchRecord("lease-1", "task-1")); err != nil {
		t.Fatalf("append returned error: %v", err)
	}
	firstEnd := log.Size()
	if err := log.Append(fetchRecord("lease-2", "task-2")); err != nil {
		t.Fatalf("append returned error: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}

	// Corrupt one payload byte of the second record; its stored CRC no longer
	// matches, which must end replay at the record boundary before it.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read returned error: %v", err)
	}
	raw[firstEnd+headerSize] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write returned error: %v", err)
	}

	reopened, err := OpenWith(path, Options{Sync: false})
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()

	records := reopened.Recovered()
	if len(records) != 1 {
		t.Fatalf("recovered %d records past a bad checksum, want only the 1 intact record", len(records))
	}
	if records[0].LeaseID != "lease-1" {
		t.Fatalf("recovered lease %q, want lease-1", records[0].LeaseID)
	}
}

func TestImplausibleLengthStopsReplay(t *testing.T) {
	log, path := openTemp(t)
	if err := log.Append(fetchRecord("lease-1", "task-1")); err != nil {
		t.Fatalf("append returned error: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}

	// A garbage header claiming a huge payload must not drive a huge allocation.
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[:4], maxRecordBytes+1)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open for append returned error: %v", err)
	}
	if _, err := file.Write(header); err != nil {
		t.Fatalf("write returned error: %v", err)
	}
	file.Close()

	reopened, err := OpenWith(path, Options{Sync: false})
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()

	if got := len(reopened.Recovered()); got != 1 {
		t.Fatalf("recovered %d records, want 1", got)
	}
}

func TestLiveFoldsClosedLeasesAway(t *testing.T) {
	records := []Record{
		fetchRecord("lease-1", "task-1"),
		fetchRecord("lease-2", "task-2"),
		{Op: OpAck, LeaseID: "lease-1"},
		fetchRecord("lease-3", "task-3"),
		{Op: OpExpire, LeaseID: "lease-3"},
		fetchRecord("lease-4", "task-4"),
		{Op: OpDeadLetter, LeaseID: "lease-4"},
	}

	live := Live(records)
	if len(live) != 1 {
		t.Fatalf("live set has %d leases, want 1", len(live))
	}
	if _, ok := live["lease-2"]; !ok {
		t.Fatalf("live set = %v, want it to hold lease-2", live)
	}
}

func TestCompactKeepsOnlyLiveLeases(t *testing.T) {
	log, path := openTemp(t)
	for _, id := range []string{"lease-1", "lease-2", "lease-3"} {
		if err := log.Append(fetchRecord(id, "task-"+id)); err != nil {
			t.Fatalf("append returned error: %v", err)
		}
	}
	for _, id := range []string{"lease-1", "lease-3"} {
		if err := log.Append(Record{Op: OpAck, LeaseID: id}); err != nil {
			t.Fatalf("append returned error: %v", err)
		}
	}
	before := log.Size()

	if err := log.Compact(); err != nil {
		t.Fatalf("compact returned error: %v", err)
	}
	if log.Size() >= before {
		t.Fatalf("journal is %d bytes after compaction, want less than %d", log.Size(), before)
	}

	// A compacted journal must still accept writes and reopen cleanly.
	if err := log.Append(fetchRecord("lease-4", "task-4")); err != nil {
		t.Fatalf("append after compaction returned error: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}

	reopened, err := OpenWith(path, Options{Sync: false})
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()

	live := Live(reopened.Recovered())
	if len(live) != 2 {
		t.Fatalf("live set has %d leases after compaction, want 2", len(live))
	}
	for _, id := range []string{"lease-2", "lease-4"} {
		if _, ok := live[id]; !ok {
			t.Fatalf("live set = %v, want it to hold %s", live, id)
		}
	}
	if _, ok := live["lease-1"]; ok {
		t.Fatalf("compaction kept acked lease-1")
	}
}

func TestCompactedRecordsCarryTaskDetail(t *testing.T) {
	log, path := openTemp(t)
	if err := log.Append(fetchRecord("lease-1", "task-1")); err != nil {
		t.Fatalf("append returned error: %v", err)
	}
	if err := log.Compact(); err != nil {
		t.Fatalf("compact returned error: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}

	reopened, err := OpenWith(path, Options{Sync: false})
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	defer reopened.Close()

	records := reopened.Recovered()
	if len(records) != 1 {
		t.Fatalf("recovered %d records, want 1", len(records))
	}
	// Compaction must preserve everything recovery needs to rebuild a lease,
	// not just the identifier.
	if records[0].Task.ID != "task-1" {
		t.Fatalf("task id = %q, want task-1", records[0].Task.ID)
	}
	if string(records[0].Task.Payload) != "payload-task-1" {
		t.Fatalf("payload = %q, want payload-task-1", records[0].Task.Payload)
	}
	if records[0].ExpiresAt.IsZero() {
		t.Fatalf("compacted record lost its expiry")
	}
}
