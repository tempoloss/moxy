// Package wal implements an append-only journal of lease transitions.
//
// The engine's authoritative lease state — which worker holds which task and
// when that claim expires — lives in memory. Without a journal a restart loses
// it, and every task already moved into the backend's processing set is
// stranded there: the reaper walks the in-memory expiration heap, so a lease it
// never saw is a lease it can never expire.
//
// The journal records each transition so that state can be rebuilt on boot.
// Records are framed as:
//
//	[uint32 payload length][uint32 CRC32 of payload][payload]
//
// A crash can tear the final frame. Replay stops at the first frame that is
// short, fails its checksum, or does not decode, and the file is truncated back
// to the last good boundary. A half-written record is therefore discarded
// rather than poisoning recovery.
package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/tempoloss/moxy/internal/task"
)

const (
	headerSize     = 8
	maxRecordBytes = 8 << 20
)

// Op is the lease transition a record describes.
type Op string

const (
	OpFetch      Op = "fetch"
	OpAck        Op = "ack"
	OpExpire     Op = "expire"
	OpDeadLetter Op = "dead_letter"
)

// Record is one lease transition. Only OpFetch carries task and timing detail;
// the closing operations need nothing beyond the lease they close.
type Record struct {
	Op        Op        `json:"op"`
	LeaseID   string    `json:"lease_id"`
	Task      task.Task `json:"task"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Options controls durability.
type Options struct {
	// Sync fsyncs after every append. Leaving it off trades crash durability
	// for throughput and is intended for tests and throwaway runs.
	Sync bool
}

// Log is an open journal positioned at the end of the last intact record.
type Log struct {
	path      string
	file      *os.File
	sync      bool
	offset    int64
	recovered []Record
}

// Open opens (or creates) the journal at path, replays it, and truncates any
// torn tail. Records recovered from the replay are available via Recovered.
func Open(path string) (*Log, error) {
	return OpenWith(path, Options{Sync: true})
}

// OpenWith is Open with explicit durability options.
func OpenWith(path string, options Options) (*Log, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("wal: create directory: %w", err)
		}
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	records, good, err := replay(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Truncate(good); err != nil {
		file.Close()
		return nil, fmt.Errorf("wal: truncate torn tail: %w", err)
	}
	if _, err := file.Seek(good, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("wal: seek to tail: %w", err)
	}

	return &Log{
		path:      path,
		file:      file,
		sync:      options.Sync,
		offset:    good,
		recovered: records,
	}, nil
}

// Recovered returns the records read during Open, in write order.
func (l *Log) Recovered() []Record {
	return l.recovered
}

// Append writes one record and, when configured to, fsyncs it.
func (l *Log) Append(record Record) error {
	frame, err := encode(record)
	if err != nil {
		return err
	}
	if _, err := l.file.Write(frame); err != nil {
		return fmt.Errorf("wal: append: %w", err)
	}
	if l.sync {
		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("wal: sync: %w", err)
		}
	}
	l.offset += int64(len(frame))
	return nil
}

// Size reports the journal length in bytes, which is the signal to compact.
func (l *Log) Size() int64 {
	return l.offset
}

// Compact rewrites the journal with one fetch record per still-open lease,
// discarding the history of leases that have already closed. The rewrite lands
// through a temporary file and a rename so a crash mid-compaction leaves either
// the old journal or the new one, never a partial one.
func (l *Log) Compact() error {
	records, _, err := replay(l.file)
	if err != nil {
		return err
	}

	live := Live(records)
	ids := make([]string, 0, len(live))
	for id := range live {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	temporary := l.path + ".compact"
	file, err := os.OpenFile(temporary, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open compaction file: %w", err)
	}

	var written int64
	for _, id := range ids {
		frame, err := encode(live[id])
		if err != nil {
			file.Close()
			os.Remove(temporary)
			return err
		}
		if _, err := file.Write(frame); err != nil {
			file.Close()
			os.Remove(temporary)
			return fmt.Errorf("wal: write compacted record: %w", err)
		}
		written += int64(len(frame))
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(temporary)
		return fmt.Errorf("wal: sync compacted journal: %w", err)
	}
	if err := file.Close(); err != nil {
		os.Remove(temporary)
		return fmt.Errorf("wal: close compacted journal: %w", err)
	}

	// The live handle must go before the rename: Windows refuses to replace a
	// file that is still open.
	if err := l.file.Close(); err != nil {
		os.Remove(temporary)
		return fmt.Errorf("wal: close journal before rename: %w", err)
	}
	if err := os.Rename(temporary, l.path); err != nil {
		return fmt.Errorf("wal: replace journal: %w", err)
	}

	reopened, err := os.OpenFile(l.path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopen compacted journal: %w", err)
	}
	l.file = reopened
	l.offset = written
	return nil
}

// Close releases the journal handle. It is safe to call more than once.
func (l *Log) Close() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	if err != nil {
		return fmt.Errorf("wal: close: %w", err)
	}
	return nil
}

// Live folds a record stream into the leases still open at the end of it. A
// fetch opens a lease; every other operation closes one.
func Live(records []Record) map[string]Record {
	live := make(map[string]Record)
	for _, record := range records {
		switch record.Op {
		case OpFetch:
			live[record.LeaseID] = record
		case OpAck, OpExpire, OpDeadLetter:
			delete(live, record.LeaseID)
		}
	}
	return live
}

func encode(record Record) ([]byte, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("wal: encode record: %w", err)
	}
	if len(payload) > maxRecordBytes {
		return nil, fmt.Errorf("wal: record is %d bytes, over the %d byte limit", len(payload), maxRecordBytes)
	}

	frame := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:headerSize], crc32.ChecksumIEEE(payload))
	copy(frame[headerSize:], payload)
	return frame, nil
}

// replay reads every intact record and reports the offset just past the last
// one. Anything after that offset is a torn or corrupt tail.
func replay(file *os.File) ([]Record, int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("wal: seek to start: %w", err)
	}

	var (
		reader  = bufio.NewReader(file)
		records []Record
		good    int64
		header  [headerSize]byte
	)

	for {
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, good, nil
			}
			return nil, 0, fmt.Errorf("wal: read header: %w", err)
		}

		length := binary.LittleEndian.Uint32(header[:4])
		checksum := binary.LittleEndian.Uint32(header[4:])
		if length == 0 || length > maxRecordBytes {
			return records, good, nil
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, good, nil
			}
			return nil, 0, fmt.Errorf("wal: read payload: %w", err)
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			return records, good, nil
		}

		var record Record
		if err := json.Unmarshal(payload, &record); err != nil {
			return records, good, nil
		}

		records = append(records, record)
		good += headerSize + int64(length)
	}
}
