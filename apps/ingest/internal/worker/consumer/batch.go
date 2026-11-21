package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"openpanel/ingest/internal/worker/wire"
)

const (
	// activityEvery marks the loop alive every N messages within a batch, so
	// readiness can tell a long batch that moves from one that is wedged.
	activityEvery = 16
	// durabilityRetryMax caps the wait before a batch whose events were not
	// durable is run again; handler retries have their own cap.
	durabilityRetryMax = 5 * time.Second
	retryBackoffFactor = 2
)

// BatchHandlerConfig wires a BatchHandler.
type BatchHandlerConfig struct {
	Handler    Handler
	DeadLetter DeadLetterSink
	Durability Durability
	Logger     *slog.Logger
	Metrics    Metrics
	Clock      Clock
	// OnActivity marks the consumer alive (the readiness heartbeat).
	OnActivity   func()
	Topic        string
	MaxAttempts  int
	InitialRetry time.Duration
	MaxRetry     time.Duration
}

// BatchHandler is eachBatch: per-key serial groups, the ascending
// contiguous-prefix offset walk, bounded retry, dead letters, and the
// durability gate before any offset is resolved. Delivery is at-least-once.
type BatchHandler struct {
	cfg BatchHandlerConfig

	mu                 sync.Mutex
	durabilityFailures map[string]int
}

// NewBatchHandler returns a handler for Lanes.RunBatch via EachBatch.
func NewBatchHandler(cfg BatchHandlerConfig) *BatchHandler {
	return &BatchHandler{cfg: cfg, durabilityFailures: map[string]int{}}
}

// ResetDurabilityBackoff starts the per-partition backoff over; call it on
// every group join, since partitions may have moved.
func (h *BatchHandler) ResetDurabilityBackoff() {
	h.mu.Lock()
	defer h.mu.Unlock()
	clear(h.durabilityFailures)
}

// EachBatch is a BatchFunc.
func (h *BatchHandler) EachBatch(ctx context.Context, partition int32, items []Record, control *BatchControl) (Outcome, error) {
	if len(items) == 0 {
		return Done, nil
	}
	partitionKey := h.cfg.Topic + "-" + partitionLabel(partition)
	// Opened before the first handler buffers anything, so the gate answers
	// for this batch's events and not for whatever else was pending.
	closeWindow := h.cfg.Durability.OpenWindow()

	groups := groupByKey(items)
	processed := make([]bool, len(items))
	var processedCount atomic.Int64
	var wg sync.WaitGroup
	for _, indexes := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, i := range indexes {
				if !control.IsRunning() || control.IsStale() {
					return
				}
				h.processMessage(ctx, items[i])
				processed[i] = true
				if processedCount.Add(1)%activityEvery == 0 {
					h.cfg.OnActivity()
				}
			}
		}()
	}
	wg.Wait()

	durable := h.eventsAreDurable(ctx, partition, closeWindow)
	switch {
	case durable:
		h.mu.Lock()
		delete(h.durabilityFailures, partitionKey)
		h.mu.Unlock()
		for i, item := range items {
			if !processed[i] {
				break
			}
			control.ResolveOffset(item.Offset)
		}
	case control.IsRunning() && !control.IsStale():
		// Not while shutting down: the redelivery is the next process's
		// problem and the wait would only eat the shutdown budget.
		h.waitForRedelivery(ctx, partitionKey, partition)
	}
	h.cfg.OnActivity()
	if durable {
		return Done, nil
	}
	return Redeliver, nil
}

// groupByKey keeps same-key messages serial: sessions and session-end jobs
// of one device must not race. A null key is its own group; a zero-length
// non-null key is the group "".
func groupByKey(items []Record) [][]int {
	index := map[string]int{}
	var groups [][]int
	for i, item := range items {
		key := "__no_key__:" + strconv.FormatInt(item.Offset, 10)
		if item.Key != nil {
			key = decodeText(item.Key)
		}
		g, ok := index[key]
		if !ok {
			g = len(groups)
			index[key] = g
			groups = append(groups, nil)
		}
		groups[g] = append(groups[g], i)
	}
	return groups
}

func (h *BatchHandler) eventsAreDurable(ctx context.Context, partition int32, closeWindow func(context.Context) error) bool {
	if err := closeWindow(ctx); err != nil {
		h.cfg.Logger.Error("buffered events could not be made durable — batch left unresolved for redelivery", "err", err, "partition", partition)
		return false
	}
	return true
}

// waitForRedelivery holds the partition before returning, so the redelivery
// the batch just asked for does not run at once and fail the same way.
func (h *BatchHandler) waitForRedelivery(ctx context.Context, partitionKey string, partition int32) {
	h.mu.Lock()
	h.durabilityFailures[partitionKey]++
	failures := h.durabilityFailures[partitionKey]
	h.mu.Unlock()

	delay := h.cfg.InitialRetry
	for i := 1; i < failures && delay < durabilityRetryMax; i++ {
		delay *= retryBackoffFactor
	}
	delay = min(delay, durabilityRetryMax)
	// A member that deliberately waits must not look dead.
	h.cfg.OnActivity()
	h.cfg.Logger.Warn("waiting before running the batch again", "partition", partition, "consecutiveFailures", failures, "delayMs", delay.Milliseconds())
	_ = h.cfg.Clock.Sleep(ctx, delay)
}

// processMessage always returns: a message is handled, or dead-lettered and
// dropped, and a dropped one is still finished.
func (h *BatchHandler) processMessage(ctx context.Context, record Record) {
	decoded, err := h.parse(record)
	if err != nil {
		h.cfg.Logger.Error("kafka message parse failed", "err", err, "partition", record.Partition, "offset", record.Offset)
		// Deterministic: retrying identical bytes cannot succeed.
		h.deadLetter(ctx, record, ReasonParseError, err)
		return
	}
	if err := h.runWithRetry(ctx, record, decoded); err != nil {
		h.deadLetter(ctx, record, ReasonHandlerError, err)
	}
}

func (h *BatchHandler) parse(record Record) (wire.Decoded, error) {
	if record.Value == nil {
		return wire.Decoded{}, errNoValue
	}
	return wire.Decode([]byte(decodeText(record.Value)))
}

func (h *BatchHandler) runWithRetry(ctx context.Context, record Record, decoded wire.Decoded) error {
	label := partitionLabel(record.Partition)
	delay := h.cfg.InitialRetry
	var last error
	for attempt := 1; attempt <= h.cfg.MaxAttempts; attempt++ {
		last = h.attempt(ctx, record, decoded)
		if last == nil {
			return nil
		}
		h.cfg.Metrics.HandlerFailed(label)
		h.cfg.Logger.Warn("kafka incomingEvent handler failed", "err", last, "partition", record.Partition, "offset", record.Offset,
			"projectId", decoded.Payload.ProjectID, "attempt", attempt, "maxAttempts", h.cfg.MaxAttempts)
		if attempt < h.cfg.MaxAttempts {
			_ = h.cfg.Clock.Sleep(ctx, delay)
			delay = min(delay*retryBackoffFactor, h.cfg.MaxRetry)
		}
	}
	return last
}

func (h *BatchHandler) attempt(ctx context.Context, record Record, decoded wire.Decoded) (err error) {
	if decoded.ShapeErr != nil {
		return decoded.ShapeErr
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h.cfg.Handler(ctx, decoded.Payload, Delivery{Partition: record.Partition, Offset: record.Offset})
}

// deadLetter parks the message and DROPS it, whether or not the write lands:
// Redis being down is exactly when handlers fail, and an unresolved offset
// there would recreate the redelivery loop. The two counters carry the
// volume, since a capped list makes 50,000 drops look like 1,000.
func (h *BatchHandler) deadLetter(ctx context.Context, record Record, reason string, cause error) {
	label := partitionLabel(record.Partition)
	err := h.cfg.DeadLetter.Record(ctx, DeadLetterMessage{
		Key: record.Key, Value: record.Value, Headers: record.Headers,
		Topic: h.cfg.Topic, Partition: record.Partition, Offset: record.Offset,
		Reason: reason, Err: cause.Error(),
	})
	if err != nil {
		h.cfg.Metrics.DeadLetterFailed(label)
		h.cfg.Logger.Error("kafka message DROPPED without being recorded — dead-letter write failed",
			"err", err, "cause", cause.Error(), "partition", record.Partition, "offset", record.Offset, "reason", reason)
		return
	}
	h.cfg.Metrics.DeadLettered(label, reason)
	h.cfg.Logger.Error("kafka message dead-lettered — recorded and dropped", "err", cause, "partition", record.Partition, "offset", record.Offset, "reason", reason)
}
