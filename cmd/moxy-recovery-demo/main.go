package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/an8kk/moxy/internal/core"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))

	engine := core.NewEngine()
	task, err := engine.Enqueue([]byte("charge invoice #4242"))
	exitOnError(logger, "enqueue", err)
	logger.Info("enqueue", "task_id", task.ID, "state", "READY")

	lease, err := engine.Fetch(25 * time.Millisecond)
	exitOnError(logger, "fetch", err)
	logger.Info("worker fetched task", "task_id", lease.Task.ID, "lease_id", lease.LeaseID, "state", "PROCESSING")

	logger.Warn("worker disappeared before ack", "simulated", "kill -9")
	time.Sleep(30 * time.Millisecond)

	requeued, err := engine.ReapExpired(time.Now())
	exitOnError(logger, "reap expired leases", err)
	logger.Info("reaper recovered expired work", "requeued", requeued, "state", "READY")

	recovered, err := engine.Fetch(time.Second)
	exitOnError(logger, "fetch recovered task", err)
	logger.Info("next worker received recovered task", "task_id", recovered.Task.ID, "payload", string(recovered.Task.Payload), "state", "PROCESSING")

	exitOnError(logger, "ack recovered task", engine.Ack(recovered.LeaseID))
	logger.Info("ack", "task_id", recovered.Task.ID, "state", "ACKED")
}

func exitOnError(logger *slog.Logger, message string, err error) {
	if err == nil {
		return
	}

	logger.Error(message, "err", err)
	os.Exit(1)
}
