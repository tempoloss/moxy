package resp

import (
	"fmt"
	"io"
)

// Writer writes RESP2 values to a stream.
type Writer struct {
	writer io.Writer
}

func NewWriter(writer io.Writer) *Writer {
	return &Writer{writer: writer}
}

func (w *Writer) WriteValue(value Value) error {
	switch value.Type {
	case TypeSimpleString:
		_, err := fmt.Fprintf(w.writer, "+%s\r\n", value.String)
		return err
	case TypeError:
		_, err := fmt.Fprintf(w.writer, "-%s\r\n", value.String)
		return err
	case TypeInteger:
		_, err := fmt.Fprintf(w.writer, ":%d\r\n", value.Integer)
		return err
	case TypeBulkString:
		if value.Null {
			_, err := io.WriteString(w.writer, "$-1\r\n")
			return err
		}
		_, err := fmt.Fprintf(w.writer, "$%d\r\n%s\r\n", len(value.String), value.String)
		return err
	case TypeArray:
		if _, err := fmt.Fprintf(w.writer, "*%d\r\n", len(value.Array)); err != nil {
			return err
		}
		for _, item := range value.Array {
			if err := w.WriteValue(item); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown value type", ErrMalformedRESP)
	}
}
