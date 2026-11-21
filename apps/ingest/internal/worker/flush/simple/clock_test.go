package simple

import (
	"context"
	"time"
)

// lossClock ticks every few milliseconds, so a lock taken away mid-flush is
// noticed by the keepalive before the next batch.
type lossClock struct{}

func (lossClock) Now() time.Time { return time.Now() }
func (lossClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (lossClock) Tick(time.Duration) (<-chan time.Time, func()) {
	ticker := time.NewTicker(2 * time.Millisecond)
	return ticker.C, ticker.Stop
}
