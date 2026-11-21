package consumer

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Dead-letter reasons; they are metric label values, keep the set small.
const (
	ReasonParseError   = "parse_error"
	ReasonHandlerError = "handler_error"
)

// Metrics is what the consumer counts.
type Metrics interface {
	// HandlerFailed counts every failed handler attempt, retries included.
	HandlerFailed(partition string)
	// DeadLettered counts a message recorded in the dead-letter list and dropped.
	DeadLettered(partition, reason string)
	// DeadLetterFailed counts a message dropped without being recorded.
	DeadLetterFailed(partition string)
}

// NopMetrics counts nothing.
type NopMetrics struct{}

// HandlerFailed implements Metrics.
func (NopMetrics) HandlerFailed(string) {}

// DeadLettered implements Metrics.
func (NopMetrics) DeadLettered(string, string) {}

// DeadLetterFailed implements Metrics.
func (NopMetrics) DeadLetterFailed(string) {}

// PromMetrics registers the same series, with the same names and labels, as
// the Bun worker's ingest.metrics.ts.
type PromMetrics struct {
	handlerFailures *prometheus.CounterVec
	deadLettered    *prometheus.CounterVec
	deadLetterFail  *prometheus.CounterVec
}

// NewPromMetrics registers the collectors on reg.
func NewPromMetrics(reg prometheus.Registerer) *PromMetrics {
	factory := promauto.With(reg)
	return &PromMetrics{
		handlerFailures: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_events_handler_failures_total",
			Help: "Kafka event handler exceptions (each attempt, retries included)",
		}, []string{"partition"}),
		deadLettered: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_events_dead_lettered_total",
			Help: "Kafka event messages recorded in the dead-letter list and dropped",
		}, []string{"partition", "reason"}),
		deadLetterFail: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_events_dead_letter_failed_total",
			Help: "Kafka event messages dropped without being recorded (dead-letter write failed)",
		}, []string{"partition"}),
	}
}

// HandlerFailed implements Metrics.
func (m *PromMetrics) HandlerFailed(partition string) {
	m.handlerFailures.WithLabelValues(partition).Inc()
}

// DeadLettered implements Metrics.
func (m *PromMetrics) DeadLettered(partition, reason string) {
	m.deadLettered.WithLabelValues(partition, reason).Inc()
}

// DeadLetterFailed implements Metrics.
func (m *PromMetrics) DeadLetterFailed(partition string) {
	m.deadLetterFail.WithLabelValues(partition).Inc()
}

func partitionLabel(partition int32) string { return strconv.Itoa(int(partition)) }
