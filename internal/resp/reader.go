package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	maxLineLength  = 4096
	maxBulkLength  = 1024 * 1024
	maxArrayLength = 1024
)

// Reader reads RESP2 values from a stream.
type Reader struct {
	reader *bufio.Reader
}

func NewReader(reader io.Reader) *Reader {
	return &Reader{reader: bufio.NewReader(reader)}
}

func (r *Reader) ReadValue() (Value, error) {
	prefix, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch Type(prefix) {
	case TypeSimpleString:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return SimpleString(line), nil
	case TypeError:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Error(line), nil
	case TypeInteger:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		value, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("%w: invalid integer", ErrMalformedRESP)
		}
		return Integer(value), nil
	case TypeBulkString:
		return r.readBulkString()
	case TypeArray:
		return r.readArray()
	default:
		return Value{}, fmt.Errorf("%w: unknown type byte %q", ErrMalformedRESP, prefix)
	}
}

func (r *Reader) readArray() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	length, err := strconv.Atoi(line)
	if err != nil || length < 0 {
		return Value{}, fmt.Errorf("%w: invalid array length", ErrMalformedRESP)
	}
	if length > maxArrayLength {
		return Value{}, ErrValueTooLarge
	}

	values := make([]Value, 0, length)
	for i := 0; i < length; i++ {
		value, err := r.ReadValue()
		if err != nil {
			return Value{}, err
		}
		values = append(values, value)
	}
	return Array(values...), nil
}

func (r *Reader) readBulkString() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	length, err := strconv.Atoi(line)
	if err != nil || length < -1 {
		return Value{}, fmt.Errorf("%w: invalid bulk length", ErrMalformedRESP)
	}
	if length == -1 {
		return NullBulkString(), nil
	}
	if length > maxBulkLength {
		return Value{}, ErrValueTooLarge
	}

	buf := make([]byte, length+2)
	if _, err := io.ReadFull(r.reader, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Value{}, fmt.Errorf("%w: missing bulk terminator", ErrMalformedRESP)
		}
		return Value{}, err
	}
	if string(buf[length:]) != "\r\n" {
		return Value{}, fmt.Errorf("%w: missing bulk CRLF", ErrMalformedRESP)
	}
	return BulkString(string(buf[:length])), nil
}

func (r *Reader) readLine() (string, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return "", fmt.Errorf("%w: missing CRLF", ErrMalformedRESP)
		}
		return "", err
	}
	if len(line) > maxLineLength {
		return "", ErrValueTooLarge
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("%w: missing CRLF", ErrMalformedRESP)
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}
