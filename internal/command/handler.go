package command

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tempoloss/moxy/internal/service"
)

var (
	ErrUnknownCommand   = errors.New("unknown command")
	ErrInvalidArguments = errors.New("invalid arguments")
	ErrInvalidTimeout   = errors.New("invalid timeout")
)

// Response is the command-layer result shape shared by all supported commands.
type Response struct {
	OK        bool
	TaskID    string
	LeaseID   string
	Payload   []byte
	ExpiresAt time.Time
	Stats     service.Stats
}

// Handler dispatches protocol-neutral commands into the service layer.
type Handler struct {
	service *service.Service
}

// NewHandler creates a command handler over a service.
func NewHandler(service *service.Service) *Handler {
	return &Handler{service: service}
}

// Handle validates and executes a Moxy command.
func (h *Handler) Handle(command Command) (Response, error) {
	switch strings.ToUpper(command.Name) {
	case EnqueueName:
		return h.handleEnqueue(command.Args)
	case FetchName:
		return h.handleFetch(command.Args)
	case AckName:
		return h.handleAck(command.Args)
	case StatsName:
		return h.handleStats(command.Args)
	default:
		return Response{}, ErrUnknownCommand
	}
}

func (h *Handler) handleEnqueue(args []string) (Response, error) {
	if len(args) != 2 {
		return Response{}, ErrInvalidArguments
	}

	task, err := h.service.Enqueue(args[0], []byte(args[1]))
	if err != nil {
		return Response{}, err
	}

	return Response{OK: true, TaskID: task.ID}, nil
}

func (h *Handler) handleFetch(args []string) (Response, error) {
	if len(args) != 2 {
		return Response{}, ErrInvalidArguments
	}

	timeoutMS, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return Response{}, ErrInvalidTimeout
	}

	lease, err := h.service.Fetch(args[0], time.Duration(timeoutMS)*time.Millisecond)
	if err != nil {
		return Response{}, err
	}

	return Response{
		OK:        true,
		TaskID:    lease.Task.ID,
		LeaseID:   lease.LeaseID,
		Payload:   cloneBytes(lease.Task.Payload),
		ExpiresAt: lease.ExpiresAt,
	}, nil
}

func (h *Handler) handleAck(args []string) (Response, error) {
	if len(args) != 1 {
		return Response{}, ErrInvalidArguments
	}

	if err := h.service.Ack(args[0]); err != nil {
		return Response{}, err
	}

	return Response{OK: true}, nil
}

func (h *Handler) handleStats(args []string) (Response, error) {
	if len(args) != 1 {
		return Response{}, ErrInvalidArguments
	}

	stats, ok := h.service.Stats(args[0])
	if !ok {
		return Response{OK: true}, nil
	}

	return Response{OK: true, Stats: stats}, nil
}

func cloneBytes(payload []byte) []byte {
	if payload == nil {
		return nil
	}

	cloned := make([]byte, len(payload))
	copy(cloned, payload)
	return cloned
}
