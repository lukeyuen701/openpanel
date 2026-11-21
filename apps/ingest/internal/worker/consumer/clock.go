package consumer

import (
	"context"
	"time"
)

// Clock is the consumer's source of wall-clock time and of waiting, so a rig
// controls both.
type Clock interface {
	Now() time.Time
	// Sleep waits d, or until ctx is done, and returns ctx's error then.
	Sleep(ctx context.Context, d time.Duration) error
}

// SystemClock is the real clock.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// Sleep implements Clock.
func (SystemClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
