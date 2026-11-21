package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	// EventQueueKey is the Redis list the event flush drains.
	EventQueueKey = "event_buffer:queue"

	// DefaultMicroBatchSize and DefaultMicroBatchWait are EVENT_BUFFER_MICRO_BATCH_SIZE
	// and EVENT_BUFFER_MICRO_BATCH_MS when unset.
	DefaultMicroBatchSize = 100
	DefaultMicroBatchWait = 10 * time.Millisecond

	eventBufferName = "event"
)

// EventBufferConfig tunes the micro-batching; zero values take the defaults.
type EventBufferConfig struct {
	MaxSize int
	MaxWait time.Duration
}

type eventWrite struct {
	coversSeq int64
	done      chan struct{}
	err       error
}

type pendingEvent struct {
	line string
	// redeliverable events are dropped, not requeued, when a write fails: the
	// Kafka batch that made them is redelivered instead.
	redeliverable bool
}

// EventBuffer collects event rows in memory for a few milliseconds and pushes
// them to Redis in one RPUSH (buf/event-buffer.ts). Every row gets a sequence
// number so a Kafka batch can ask whether its rows are durable.
type EventBuffer struct {
	redis   Redis
	metrics Metrics
	logger  *slog.Logger
	cfg     EventBufferConfig

	mu                sync.Mutex
	pending           []pendingEvent
	timer             *time.Timer
	inFlight          *eventWrite
	lastQueuedSeq     int64
	lastDurableSeq    int64
	droppedThroughSeq int64
	lastDropFailure   error
	flushRetryCount   int
}

// NewEventBuffer returns an empty buffer. A nil metrics or logger counts and
// logs nothing.
func NewEventBuffer(redis Redis, cfg EventBufferConfig, metrics Metrics, logger *slog.Logger) *EventBuffer {
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = DefaultMicroBatchSize
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = DefaultMicroBatchWait
	}
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &EventBuffer{redis: redis, metrics: metrics, logger: logger, cfg: cfg}
}

// Add queues one serialised row. It never blocks on Redis. A redeliverable row
// is dropped when its write fails; see FlushPendingOrThrow.
func (b *EventBuffer) Add(line string, redeliverable bool) {
	start := time.Now()
	b.mu.Lock()
	b.lastQueuedSeq++
	b.pending = append(b.pending, pendingEvent{line: line, redeliverable: redeliverable})
	if len(b.pending) >= b.cfg.MaxSize {
		b.startWriteLocked()
	} else if b.timer == nil {
		b.armTimerLocked()
	}
	b.mu.Unlock()
	b.metrics.BufferAdded(eventBufferName, time.Since(start))
}

// PendingLocalCount is the number of rows held in memory, before Redis.
func (b *EventBuffer) PendingLocalCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

// OpenWindow implements consumer.Durability: the returned gate resolves once
// everything buffered after this call is in Redis and fails when any of it was
// dropped instead. Opening before the first row is buffered is what makes the
// answer the caller's own.
func (b *EventBuffer) OpenWindow() func(ctx context.Context) error {
	b.mu.Lock()
	since := b.lastQueuedSeq
	b.mu.Unlock()
	return func(ctx context.Context) error { return b.FlushPendingOrThrow(ctx, since) }
}

// FlushPendingOrThrow does not return until every row accepted before the call
// is in Redis, and returns the failure when the write did not land or dropped a
// row after sinceSeq. Shutdown passes DroppedThrough() as sinceSeq.
func (b *EventBuffer) FlushPendingOrThrow(ctx context.Context, sinceSeq int64) error {
	b.mu.Lock()
	target := b.lastQueuedSeq
	b.mu.Unlock()

	// Each pass waits out a write already running or starts the one still
	// missing; a write covers everything pending when it starts, so losing the
	// race to start one is a success.
	for {
		b.mu.Lock()
		if b.lastDurableSeq >= target {
			b.mu.Unlock()
			break
		}
		write := b.inFlight
		if write == nil {
			if len(b.pending) == 0 {
				// Everything queued is settled: written, or dropped for its
				// producer to produce again. The check below tells them apart.
				b.mu.Unlock()
				break
			}
			write = b.startWriteLocked()
			b.mu.Unlock()
			if err := b.wait(ctx, write); err != nil {
				return err
			}
			if write.err != nil {
				return asError(write.err)
			}
			continue
		}
		b.mu.Unlock()
		if err := b.wait(ctx, write); err != nil {
			return err
		}
		// A write that did not carry our rows is not our failure: its rows
		// were requeued and the next write takes them.
		if write.coversSeq >= target && write.err != nil {
			return asError(write.err)
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.droppedThroughSeq > sinceSeq {
		return asError(b.lastDropFailure)
	}
	return nil
}

// DroppedThrough is the highest sequence a failed write dropped; shutdown
// passes it to FlushPendingOrThrow.
func (b *EventBuffer) DroppedThrough() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.droppedThroughSeq
}

func (b *EventBuffer) wait(ctx context.Context, write *eventWrite) error {
	select {
	case <-write.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func asError(err error) error {
	if err == nil {
		return errors.New("event buffer write failed")
	}
	return err
}

func (b *EventBuffer) armTimerLocked() {
	b.timer = time.AfterFunc(b.cfg.MaxWait, func() {
		b.mu.Lock()
		b.timer = nil
		b.startWriteLocked()
		b.mu.Unlock()
	})
}

// startWriteLocked starts the write of everything pending unless one is
// running, and returns the write that covers it, or nil when nothing is pending.
func (b *EventBuffer) startWriteLocked() *eventWrite {
	if b.inFlight != nil {
		return b.inFlight
	}
	if len(b.pending) == 0 {
		return nil
	}
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	batch := b.pending
	b.pending = nil
	write := &eventWrite{coversSeq: b.lastQueuedSeq, done: make(chan struct{})}
	b.inFlight = write
	go b.run(write, batch)
	return write
}

func (b *EventBuffer) run(write *eventWrite, batch []pendingEvent) {
	lines := make([]string, len(batch))
	for i, event := range batch {
		lines[i] = event.line
	}
	// One RPUSH for the whole batch: atomic by itself, and it rejects on
	// WRONGTYPE or out-of-memory, which the batch gate relies on.
	err := b.redis.RPush(context.Background(), EventQueueKey, lines...)

	b.mu.Lock()
	if err == nil {
		b.lastDurableSeq = max(b.lastDurableSeq, write.coversSeq)
		b.flushRetryCount = 0
	} else {
		var orphaned []pendingEvent
		for _, event := range batch {
			if !event.redeliverable {
				orphaned = append(orphaned, event)
			}
		}
		dropped := len(batch) - len(orphaned)
		if dropped > 0 {
			// coversSeq over-states what was dropped when the write mixed both
			// kinds; erring high costs a redelivery, erring low would cost rows.
			b.droppedThroughSeq = max(b.droppedThroughSeq, write.coversSeq)
			b.lastDropFailure = err
		}
		b.pending = append(orphaned, b.pending...)
		b.flushRetryCount++
		b.logger.Warn("failed to flush local buffer to Redis; events re-queued",
			slog.Any("err", err), slog.Int("eventCount", len(batch)), slog.Int("requeuedCount", len(orphaned)),
			slog.Int("droppedForRedeliveryCount", dropped), slog.Int("flushRetryCount", b.flushRetryCount))
		write.err = fmt.Errorf("event buffer: %w", err)
	}
	b.inFlight = nil
	// Rows that arrived during the write get the next micro-batch.
	if len(b.pending) > 0 && b.timer == nil {
		b.armTimerLocked()
	}
	b.mu.Unlock()
	close(write.done)
}
