// Package clock is the worker's source of wall-clock time, waiting and
// periodic ticks, so a test controls all three.
package clock

import (
	"context"
	"time"
)

// Clock is injected wherever the worker waits or reads the time.
type Clock interface {
	Now() time.Time
	// Sleep waits d, or until ctx is done, and returns ctx's error then.
	Sleep(ctx context.Context, d time.Duration) error
	// Tick delivers the time every d until stop is called.
	Tick(d time.Duration) (ticks <-chan time.Time, stop func())
}

// System is the real clock.
type System struct{}

// Now implements Clock.
func (System) Now() time.Time { return time.Now() }

// Sleep implements Clock.
func (System) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Tick implements Clock.
func (System) Tick(d time.Duration) (<-chan time.Time, func()) {
	ticker := time.NewTicker(d)
	return ticker.C, ticker.Stop
}
