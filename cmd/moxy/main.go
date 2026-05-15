package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/an8kk/moxy/internal/command"
	"github.com/an8kk/moxy/internal/queue"
	"github.com/an8kk/moxy/internal/reaper"
	"github.com/an8kk/moxy/internal/server"
	"github.com/an8kk/moxy/internal/service"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "TCP address to listen on")
	backend := flag.String("backend", "memory", "queue backend: memory or redis")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis address for the redis backend")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	factory, closeBackend := buildBackendFactory(ctx, logger, *backend, *redisAddr)
	defer closeBackend()

	svc := service.New(factory)
	handler := command.NewHandler(svc)
	srv := server.New(handler, server.Config{Addr: *addr})

	go reaper.Run(ctx, svc, time.Second)

	logger.Info("Moxy listening", "addr", *addr, "backend", *backend)
	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("server stopped with error", "err", err)
		os.Exit(1)
	}
	logger.Info("Moxy stopped")
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
