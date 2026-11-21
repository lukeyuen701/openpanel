package events

import (
	"context"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/wire"
)

// Redis is the surface of the cache client the consumer path writes through.
// Every method is one round trip with a bounded wait: a down Redis fails the
// call at once, as ioredis does once its offline queue is disabled.
type Redis interface {
	// Get returns the value and whether the key exists.
	Get(ctx context.Context, key string) (string, bool, error)
	// Del deletes the key.
	Del(ctx context.Context, key string) error
	// Exists reports whether EXISTS answered exactly 1.
	Exists(ctx context.Context, key string) (bool, error)
	// RPush appends the values, in order, in one command.
	RPush(ctx context.Context, key string, values ...string) error
	// Transaction runs the commands between MULTI and EXEC and fails when Redis
	// discarded the transaction or any command in it errored.
	Transaction(ctx context.Context, commands [][]string) error
}

// Clock is the wall clock: session wall-clock scores, row insert times, profile
// timestamps. The buffers' timers are real time.
type Clock interface{ Now() time.Time }

// SystemClock reads the real time.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// IDs mints the random ids of the path: request ids and the UUIDs of events
// that carry none and of session_start rows.
type IDs interface {
	wire.IDSource
	// NewUUID is crypto.randomUUID().
	NewUUID() string
}

// Metrics is what the path counts (docs/GO_WORKER_CONTRACTS.md P1.11).
type Metrics interface {
	SessionStarted(kind string)
	SessionEndEnqueued(source string)
	// BufferAdded is a counted add and how long it took.
	BufferAdded(buffer string, elapsed time.Duration)
	BufferAddSkipped(buffer, reason string)
}

// NopMetrics counts nothing.
type NopMetrics struct{}

// SessionStarted implements Metrics.
func (NopMetrics) SessionStarted(string) {}

// SessionEndEnqueued implements Metrics.
func (NopMetrics) SessionEndEnqueued(string) {}

// BufferAdded implements Metrics.
func (NopMetrics) BufferAdded(string, time.Duration) {}

// BufferAddSkipped implements Metrics.
func (NopMetrics) BufferAddSkipped(string, string) {}

// Projects is what the handler reads and writes about a project; *projects.Store.
type Projects interface {
	IsExcluded(ctx context.Context, projectID string, payload *jsjson.Value) (bool, error)
	MarkFirstEvent(ctx context.Context, projectID string) error
	GetAiStoreContent(ctx context.Context, projectID string) (*bool, error)
}

// Notifier checks the project's notification rules for an event and delivers
// the matches; *notify.Dispatcher. It may add "profile" to the payload.
type Notifier interface {
	Check(ctx context.Context, requestID string, payload *jsjson.Value) error
}

// SessionQueue adds the session-end job; *bullmq.Producer.
type SessionQueue interface {
	Add(ctx context.Context, queueName, jobName string, data []byte, opts bullmq.AddOptions) (string, error)
}
