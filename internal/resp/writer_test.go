package resp

import (
	"bytes"
	"testing"
)

func TestWriteSimpleString(t *testing.T) {
	assertWrittenValue(t, SimpleString("OK"), "+OK\r\n")
}

func TestWriteError(t *testing.T) {
	assertWrittenValue(t, Error("ERR bad command"), "-ERR bad command\r\n")
}

func TestWriteInteger(t *testing.T) {
	assertWrittenValue(t, Integer(42), ":42\r\n")
}

func TestWriteBulkString(t *testing.T) {
	assertWrittenValue(t, BulkString("hello"), "$5\r\nhello\r\n")
}

func TestWriteNullBulkString(t *testing.T) {
	assertWrittenValue(t, NullBulkString(), "$-1\r\n")
}

func TestWriteArrayResponse(t *testing.T) {
	assertWrittenValue(t, Array(
		BulkString("task_id"),
		BulkString("task-1"),
		Integer(2),
	), "*3\r\n$7\r\ntask_id\r\n$6\r\ntask-1\r\n:2\r\n")
}

func assertWrittenValue(t *testing.T, value Value, want string) {
	t.Helper()

	var out bytes.Buffer
	if err := NewWriter(&out).WriteValue(value); err != nil {
		t.Fatalf("WriteValue returned error: %v", err)
	}
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}
