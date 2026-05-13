package main

import (
	"log/slog"
	"os"

	"github.com/an8kk/moxy/internal/command"
	"github.com/an8kk/moxy/internal/queue"
	"github.com/an8kk/moxy/internal/service"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	svc := service.New(func(queueName string) queue.Backend {
		return queue.NewMemoryQueue()
	})
	handler := command.NewHandler(svc)

	enqueue, err := handler.Handle(command.Command{
		Name: "MOXY.ENQUEUE",
		Args: []string{"jobs", "send welcome email"},
	})
	exitOnError(logger, "enqueue task", err)
	logger.Info("command result", "command", "MOXY.ENQUEUE", "task_id", enqueue.TaskID)

	fetch, err := handler.Handle(command.Command{
		Name: "MOXY.FETCH",
		Args: []string{"jobs", "1000"},
	})
	exitOnError(logger, "fetch task", err)
	logger.Info("command result", "command", "MOXY.FETCH", "task_id", fetch.TaskID, "lease_id", fetch.LeaseID, "payload", string(fetch.Payload), "expires_at", fetch.ExpiresAt)

	ack, err := handler.Handle(command.Command{
		Name: "MOXY.ACK",
		Args: []string{fetch.LeaseID},
	})
	exitOnError(logger, "ack task", err)
	logger.Info("command result", "command", "MOXY.ACK", "ok", ack.OK)

	stats, err := handler.Handle(command.Command{
		Name: "MOXY.STATS",
		Args: []string{"jobs"},
	})
	exitOnError(logger, "stats", err)
	logger.Info("command result", "command", "MOXY.STATS", "stats", stats.Stats)
}

func exitOnError(logger *slog.Logger, message string, err error) {
	if err == nil {
		return
	}

	logger.Error(message, "err", err)
	os.Exit(1)
}
