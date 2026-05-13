package command

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/an8kk/moxy/internal/service"
)

var (
	ErrUnknownCommand   = errors.New("unknown command")
	ErrInvalidArguments = errors.New("invalid arguments")
	ErrInvalidTimeout   = errors.New("invalid timeout")
)

type Response struct {
	OK        bool
	TaskID    string
	LeaseID   string
	Payload   []byte
	ExpiresAt time.Time
	Stats     service.Stats
}

type Handler struct {
	service *service.Service
}

func NewHandler(service *service.Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Handle(command Command) (Response, error) {
	switch strings.ToUpper(command.Name) {
	case "MOXY.ENQUEUE":
		return h.handleEnqueue(command.Args)
	case "MOXY.FETCH":
		return h.handleFetch(command.Args)
	case "MOXY.ACK":
		return h.handleAck(command.Args)
	case "MOXY.STATS":
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
