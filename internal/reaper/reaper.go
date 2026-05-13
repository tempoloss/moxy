package reaper

import (
	"context"
	"log/slog"
	"time"

	"github.com/an8kk/moxy/internal/core"
)

// Run periodically reaps expired leases until the context is canceled.
func Run(ctx context.Context, engine *core.Engine, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := engine.ReapExpired(now); err != nil {
				slog.Error("reap expired leases", "err", err)
			}
		}
	}
}
