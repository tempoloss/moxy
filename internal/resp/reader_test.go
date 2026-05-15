package resp

import (
	"errors"
	"strings"
	"testing"
)

func TestReadArrayCommandWithBulkStrings(t *testing.T) {
	value := readValue(t, "*3\r\n$12\r\nMOXY.ENQUEUE\r\n$4\r\njobs\r\n$5\r\nhello\r\n")

	if value.Type != TypeArray {
		t.Fatalf("type = %v, want TypeArray", value.Type)
	}
	if len(value.Array) != 3 {
		t.Fatalf("array length = %d, want 3", len(value.Array))
	}
	assertBulkString(t, value.Array[0], "MOXY.ENQUEUE")
	assertBulkString(t, value.Array[1], "jobs")
	assertBulkString(t, value.Array[2], "hello")
}

func TestReadMoxyFetchCommand(t *testing.T) {
	value := readValue(t, "*3\r\n$10\r\nMOXY.FETCH\r\n$5\r\nqueue\r\n$5\r\n30000\r\n")

	if len(value.Array) != 3 {
		t.Fatalf("array length = %d, want 3", len(value.Array))
	}
	assertBulkString(t, value.Array[0], "MOXY.FETCH")
	assertBulkString(t, value.Array[1], "queue")
	assertBulkString(t, value.Array[2], "30000")
}

func TestReadMoxyAckCommand(t *testing.T) {
	value := readValue(t, "*2\r\n$8\r\nMOXY.ACK\r\n$7\r\nlease-1\r\n")

	if len(value.Array) != 2 {
		t.Fatalf("array length = %d, want 2", len(value.Array))
	}
	assertBulkString(t, value.Array[0], "MOXY.ACK")
	assertBulkString(t, value.Array[1], "lease-1")
}

func TestReadSimpleString(t *testing.T) {
	value := readValue(t, "+PING\r\n")

	if value.Type != TypeSimpleString || value.String != "PING" {
		t.Fatalf("value = %+v, want simple string PING", value)
	}
}

func TestReadInteger(t *testing.T) {
	value := readValue(t, ":42\r\n")

	if value.Type != TypeInteger || value.Integer != 42 {
		t.Fatalf("value = %+v, want integer 42", value)
	}
}

func TestReadRejectsMalformedArrayLength(t *testing.T) {
	_, err := NewReader(strings.NewReader("*x\r\n")).ReadValue()
	if !errors.Is(err, ErrMalformedRESP) {
		t.Fatalf("error = %v, want ErrMalformedRESP", err)
	}
}

func TestReadRejectsMalformedBulkLength(t *testing.T) {
	_, err := NewReader(strings.NewReader("$x\r\n")).ReadValue()
	if !errors.Is(err, ErrMalformedRESP) {
		t.Fatalf("error = %v, want ErrMalformedRESP", err)
	}
}

func TestReadRejectsMissingCRLF(t *testing.T) {
	_, err := NewReader(strings.NewReader("$3\r\nabc")).ReadValue()
	if !errors.Is(err, ErrMalformedRESP) {
		t.Fatalf("error = %v, want ErrMalformedRESP", err)
	}
}

func readValue(t *testing.T, input string) Value {
	t.Helper()

	value, err := NewReader(strings.NewReader(input)).ReadValue()
	if err != nil {
		t.Fatalf("ReadValue returned error: %v", err)
	}
	return value
}

func assertBulkString(t *testing.T, value Value, want string) {
	t.Helper()

	if value.Type != TypeBulkString || value.String != want {
		t.Fatalf("value = %+v, want bulk string %q", value, want)
	}
}
