package reaper

import (
	"context"
	"log/slog"
	"time"
)

// Target is any component that can reap expired leases.
type Target interface {
	ReapExpired(time.Time) (int, error)
}

// Run periodically reaps expired leases until the context is canceled.
func Run(ctx context.Context, target Target, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := target.ReapExpired(now); err != nil {
				slog.Error("reap expired leases", "err", err)
			}
		}
	}
}
