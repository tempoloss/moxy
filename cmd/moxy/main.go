package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tempoloss/moxy/internal/command"
	"github.com/tempoloss/moxy/internal/core"
	"github.com/tempoloss/moxy/internal/queue"
	"github.com/tempoloss/moxy/internal/reaper"
	"github.com/tempoloss/moxy/internal/server"
	"github.com/tempoloss/moxy/internal/service"
	"github.com/tempoloss/moxy/internal/wal"
)

var version = "dev"

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout))
}

func run(parent context.Context, args []string, stdout io.Writer) int {
	flags := flag.NewFlagSet("moxy", flag.ContinueOnError)
	flags.SetOutput(stdout)

	addr := flags.String("addr", "127.0.0.1:6380", "TCP address to listen on")
	backend := flags.String("backend", "memory", "queue backend: memory or redis")
	redisAddr := flags.String("redis-addr", "localhost:6379", "Redis address for the redis backend")
	walDir := flags.String("wal-dir", "", "directory for lease journals; empty keeps lease state in memory only")
	showVersion := flags.Bool("version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, version)
		return 0
	}

	logger := slog.New(slog.NewTextHandler(stdout, nil))
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	factory, closeBackend := buildBackendFactory(ctx, logger, *backend, *redisAddr)
	defer closeBackend()

	journals, closeJournals := buildJournalFactory(*walDir, logger)
	defer closeJournals()

	svc := service.NewWithConfig(factory, service.ServiceConfig{Journal: journals})
	handler := command.NewHandler(svc)
	srv := server.New(handler, server.Config{Addr: *addr})

	go reaper.Run(ctx, svc, time.Second)

	logger.Info("Moxy listening", "version", version, "addr", *addr, "backend", *backend, "wal_dir", *walDir)
	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("server stopped with error", "err", err)
		return 1
	}
	logger.Info("Moxy stopped")
	return 0
}

func buildBackendFactory(ctx context.Context, logger *slog.Logger, backend, redisAddr string) (service.BackendFactory, func()) {
	switch backend {
	case "memory":
		return func(queueName string) queue.Backend {
			return queue.NewMemoryQueue()
		}, func() {}
	case "redis":
		client := redis.NewClient(&redis.Options{Addr: redisAddr})
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := client.Ping(pingCtx).Err(); err != nil {
			logger.Error("connect redis", "addr", redisAddr, "err", err)
			os.Exit(1)
		}
		return func(queueName string) queue.Backend {
				return queue.NewRedisQueue(client, queueName)
			}, func() {
				if err := client.Close(); err != nil {
					logger.Error("close redis client", "err", err)
				}
			}
	default:
		logger.Error("unknown backend", "backend", backend)
		os.Exit(1)
		return nil, nil
	}
}

// buildJournalFactory opens one journal per queue under dir. An empty dir keeps
// lease state in memory, which is the old behaviour.
func buildJournalFactory(dir string, logger *slog.Logger) (service.JournalFactory, func()) {
	if dir == "" {
		return nil, func() {}
	}

	var (
		mu   sync.Mutex
		open []*wal.Log
	)

	factory := func(queueName string) (core.Journal, []wal.Record, error) {
		name, err := journalFileName(queueName)
		if err != nil {
			return nil, nil, err
		}

		log, err := wal.Open(filepath.Join(dir, name))
		if err != nil {
			return nil, nil, err
		}

		mu.Lock()
		open = append(open, log)
		mu.Unlock()

		records := log.Recovered()
		if live := len(wal.Live(records)); live > 0 {
			logger.Info("recovered open leases", "queue", queueName, "leases", live, "records", len(records))
		}
		return log, records, nil
	}

	return factory, func() {
		mu.Lock()
		defer mu.Unlock()
		for _, log := range open {
			if err := log.Close(); err != nil {
				logger.Error("close journal", "err", err)
			}
		}
	}
}

// journalFileName keeps a queue name from escaping the journal directory. Names
// arrive over the network and would otherwise become paths, so anything outside
// a conservative set is rejected rather than rewritten into something that
// could collide with another queue's journal.
func journalFileName(queueName string) (string, error) {
	if queueName == "" {
		return "", errors.New("queue name must not be empty")
	}
	for _, r := range queueName {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return "", fmt.Errorf(
				"queue %q cannot be journalled: names may use letters, digits, hyphen and underscore",
				queueName,
			)
		}
	}
	return queueName + ".wal", nil
}
