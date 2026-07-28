package protocol

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tempoloss/moxy/internal/command"
	"github.com/tempoloss/moxy/internal/core"
	"github.com/tempoloss/moxy/internal/resp"
)

const pingName = "PING"

// Adapter translates RESP values to protocol-neutral commands and back.
type Adapter struct {
	handler *command.Handler
}

func NewAdapter(handler *command.Handler) *Adapter {
	return &Adapter{handler: handler}
}

func (a *Adapter) Handle(value resp.Value) resp.Value {
	cmd, err := CommandFromRESP(value)
	if err != nil {
		return errorReply(err)
	}

	if cmd.Name == pingName {
		return resp.SimpleString("PONG")
	}

	response, err := a.handler.Handle(cmd)
	if err != nil {
		if errors.Is(err, core.ErrQueueEmpty) {
			return resp.NullBulkString()
		}
		return errorReply(err)
	}

	return responseToRESP(cmd.Name, response)
}

func CommandFromRESP(value resp.Value) (command.Command, error) {
	if value.Type != resp.TypeArray {
		return command.Command{}, fmt.Errorf("expected array command")
	}
	if len(value.Array) == 0 {
		return command.Command{}, command.ErrInvalidArguments
	}

	parts := make([]string, 0, len(value.Array))
	for _, item := range value.Array {
		if item.Type != resp.TypeBulkString || item.Null {
			return command.Command{}, fmt.Errorf("command elements must be bulk strings")
		}
		parts = append(parts, item.String)
	}

	return command.Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}, nil
}

func responseToRESP(name string, response command.Response) resp.Value {
	switch name {
	case command.EnqueueName:
		return resp.Array(
			resp.BulkString("task_id"),
			resp.BulkString(response.TaskID),
		)
	case command.FetchName:
		return resp.Array(
			resp.BulkString("lease_id"),
			resp.BulkString(response.LeaseID),
			resp.BulkString("task_id"),
			resp.BulkString(response.TaskID),
			resp.BulkString("payload"),
			resp.BulkString(string(response.Payload)),
		)
	case command.AckName:
		return resp.SimpleString("OK")
	case command.StatsName:
		return resp.Array(
			resp.BulkString("ready"),
			resp.Integer(int64(response.Stats.Ready)),
			resp.BulkString("processing"),
			resp.Integer(int64(response.Stats.Processing)),
			resp.BulkString("dead"),
			resp.Integer(int64(response.Stats.Dead)),
			resp.BulkString("active_leases"),
			resp.Integer(int64(response.Stats.ActiveLeases)),
			resp.BulkString("heap"),
			resp.Integer(int64(response.Stats.ExpirationHeap)),
		)
	default:
		return errorReply(command.ErrUnknownCommand)
	}
}

func errorReply(err error) resp.Value {
	return resp.Error("ERR " + err.Error())
}
