package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testEpoch = time.Date(2026, 11, 21, 17, 30, 22, 556_000_000, time.UTC)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// recordingLogger counts what was logged at each level.
type recordingLogger struct {
	*slog.Logger
	h *countingHandler
}

type countingHandler struct {
	slog.Handler
	warns, errors atomic.Int64
}

func (h *countingHandler) Handle(ctx context.Context, r slog.Record) error {
	switch {
	case r.Level >= slog.LevelError:
		h.errors.Add(1)
	case r.Level >= slog.LevelWarn:
		h.warns.Add(1)
	}
	return nil
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func newRecordingLogger() *recordingLogger {
	h := &countingHandler{Handler: slog.NewTextHandler(io.Discard, nil)}
	return &recordingLogger{Logger: slog.New(h), h: h}
}

// fakeClock sleeps instantly and remembers how long it was asked to.
type fakeClock struct {
	mu     sync.Mutex
	sleeps []time.Duration
}

func (c *fakeClock) Now() time.Time { return testEpoch }

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	c.mu.Unlock()
	return ctx.Err()
}

func (c *fakeClock) waits() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// never asserts condition stays false for a short while.
func never(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		if condition() {
			t.Fatalf("unexpected: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

type commitCall struct {
	Partition int32
	Offset    int64
	Epoch     int32
}

type commitLog struct {
	mu    sync.Mutex
	calls []commitCall
	fail  func(call commitCall) error
	total atomic.Int64
}

func (c *commitLog) commit(_ context.Context, partition int32, offset int64, epoch int32) error {
	call := commitCall{partition, offset, epoch}
	c.total.Add(1)
	if c.fail != nil {
		if err := c.fail(call); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
	return nil
}

func (c *commitLog) all() []commitCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]commitCall(nil), c.calls...)
}

// recordingMetrics counts calls by label.
type recordingMetrics struct {
	mu          sync.Mutex
	failed      map[string]int
	deadLetters map[string]int
	dropped     map[string]int
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{failed: map[string]int{}, deadLetters: map[string]int{}, dropped: map[string]int{}}
}

func (m *recordingMetrics) HandlerFailed(partition string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed[partition]++
}

func (m *recordingMetrics) DeadLettered(partition, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadLetters[partition+"/"+reason]++
}

func (m *recordingMetrics) DeadLetterFailed(partition string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropped[partition]++
}

// memoryDeadLetter keeps what it was given, or fails.
type memoryDeadLetter struct {
	mu       sync.Mutex
	messages []DeadLetterMessage
	err      error
}

func (d *memoryDeadLetter) Record(_ context.Context, message DeadLetterMessage) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	d.messages = append(d.messages, message)
	return nil
}

func (d *memoryDeadLetter) recorded() []DeadLetterMessage {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]DeadLetterMessage(nil), d.messages...)
}

// fakeDurability opens windows that close with gate's answer.
type fakeDurability struct {
	opened atomic.Int64
	closed atomic.Int64
	onOpen func()
	gate   func() error
}

func (d *fakeDurability) OpenWindow() func(context.Context) error {
	d.opened.Add(1)
	if d.onOpen != nil {
		d.onOpen()
	}
	return func(context.Context) error {
		d.closed.Add(1)
		if d.gate != nil {
			return d.gate()
		}
		return nil
	}
}

var errRedisDown = errors.New("redis is unreachable")

func record(partition int32, offset int64, key string, value string) Record {
	r := Record{Partition: partition, Offset: offset, LeaderEpoch: 3, Value: []byte(value)}
	if key != "-" {
		r.Key = []byte(key)
	}
	return r
}
