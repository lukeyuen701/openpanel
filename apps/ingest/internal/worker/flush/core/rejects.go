package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"openpanel/ingest/internal/worker/consumer"
)

// Why a row could not be read. They are metric label values: keep the set small.
const (
	RejectUnreadable    = "unreadable_json"
	RejectNotObject     = "not_object"
	RejectMissingFields = "missing_fields"
)

// RejectedRow is a buffer row a flush cannot process, with why.
type RejectedRow struct{ Reason, Raw string }

// Rejects parks the rows a flush skips. The Bun flush blocks its list on such a
// row; this one keeps going, but only after the row is recorded, so skipping
// never loses data without a trace.
type Rejects interface {
	Reject(ctx context.Context, buffer string, rows []RejectedRow) error
}

// RejectMetrics counts the rows set aside.
type RejectMetrics interface {
	Rejected(buffer, reason string)
	RejectFailed(buffer string)
}

var errNoRejects = errors.New("flush: a row cannot be read and Deps.Rejects is not set, so it would be dropped unrecorded")

// RedisRejects writes the rows to the capped list consumer.BufferDeadLetterKey.
type RedisRejects struct {
	store      consumer.ListStore
	clock      consumer.Clock
	maxEntries int
	metrics    RejectMetrics
}

// NewRedisRejects keeps the last maxEntries rows. metrics may be nil.
func NewRedisRejects(store consumer.ListStore, clock consumer.Clock, maxEntries int, metrics RejectMetrics) *RedisRejects {
	return &RedisRejects{store: store, clock: clock, maxEntries: maxEntries, metrics: metrics}
}

// Reject implements Rejects. It stops at the first row that fails to record.
func (r *RedisRejects) Reject(ctx context.Context, buffer string, rows []RejectedRow) error {
	for _, row := range rows {
		record := consumer.EncodeBufferDeadLetter(buffer, row.Reason, row.Raw, r.clock.Now())
		if err := r.store.PushCapped(ctx, consumer.BufferDeadLetterKey, record, r.maxEntries); err != nil {
			if r.metrics != nil {
				r.metrics.RejectFailed(buffer)
			}
			return fmt.Errorf("record unreadable %s row: %w", buffer, err)
		}
		if r.metrics != nil {
			r.metrics.Rejected(buffer, row.Reason)
		}
	}
	return nil
}

// PromRejectMetrics is RejectMetrics on Prometheus.
type PromRejectMetrics struct {
	rejected *prometheus.CounterVec
	failed   *prometheus.CounterVec
}

// NewPromRejectMetrics registers the series on reg.
func NewPromRejectMetrics(reg prometheus.Registerer) *PromRejectMetrics {
	factory := promauto.With(reg)
	return &PromRejectMetrics{
		rejected: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "buffer_dead_lettered_total",
			Help: "Buffer rows a flush could not read, recorded in the dead-letter list and skipped",
		}, []string{"buffer", "reason"}),
		failed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "buffer_dead_letter_failed_total",
			Help: "Buffer rows a flush could not read and could not record; the flush failed and kept them",
		}, []string{"buffer"}),
	}
}

// Rejected implements RejectMetrics.
func (m *PromRejectMetrics) Rejected(buffer, reason string) {
	m.rejected.WithLabelValues(buffer, reason).Inc()
}

// RejectFailed implements RejectMetrics.
func (m *PromRejectMetrics) RejectFailed(buffer string) { m.failed.WithLabelValues(buffer).Inc() }

// reject records rows before the caller trims them. A failure fails the flush,
// which leaves every row in the list for the next tick.
func (d Deps) reject(ctx context.Context, buffer string, rows []RejectedRow) error {
	if len(rows) == 0 {
		return nil
	}
	if d.Rejects == nil {
		return errNoRejects
	}
	return d.Rejects.Reject(ctx, buffer, rows)
}
