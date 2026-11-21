package sessions

import "github.com/prometheus/client_golang/prometheus"

// Skip reasons of session_ends_skipped_total.
const (
	SkipNotFound             = "not_found"
	SkipAlreadyEmitted       = "already_emitted"
	SkipExtendedAfterEnqueue = "extended_after_enqueue"
)

const (
	reasonDeadman   = "deadman"
	reasonStaleBlob = "stale_blob"
	reasonMissing   = "missing_blob"
)

var (
	durationBucketsMs = []float64{1000, 10_000, 60_000, 300_000, 900_000, 1_800_000, 3_600_000, 86_400_000}
	eventBuckets      = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}
)

// Metrics is what the lifecycle counts (P3.8). session_ends_enqueued_total
// belongs to events.Metrics, which the reaper is handed.
type Metrics interface {
	EndEmitted()
	EndSkipped(reason string)
	Reaped()
	ReaperOrphan()
	Vacuumed(reason string)
	DurationOnClose(ms float64)
	EventsOnClose(count float64)
}

// NopMetrics counts nothing.
type NopMetrics struct{}

func (NopMetrics) EndEmitted()             {}
func (NopMetrics) EndSkipped(string)       {}
func (NopMetrics) Reaped()                 {}
func (NopMetrics) ReaperOrphan()           {}
func (NopMetrics) Vacuumed(string)         {}
func (NopMetrics) DurationOnClose(float64) {}
func (NopMetrics) EventsOnClose(float64)   {}

// PromMetrics registers the series under the names the Bun worker uses.
type PromMetrics struct {
	emitted  prometheus.Counter
	skipped  *prometheus.CounterVec
	reaped   *prometheus.CounterVec
	orphans  *prometheus.CounterVec
	vacuumed *prometheus.CounterVec
	duration prometheus.Histogram
	events   prometheus.Histogram
}

// NewPromMetrics registers the series on reg.
func NewPromMetrics(reg prometheus.Registerer) *PromMetrics {
	m := &PromMetrics{
		emitted:  prometheus.NewCounter(prometheus.CounterOpts{Name: "session_ends_emitted_total", Help: "session_end events written after the idempotency claim"}),
		skipped:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "session_ends_skipped_total", Help: "session_end closes that wrote nothing, by reason"}, []string{"reason"}),
		reaped:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sessions_reaped_total", Help: "sessions the reaper enqueued for close, by reason"}, []string{"reason"}),
		orphans:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sessions_reaper_orphans_total", Help: "wall-clock entries without a session blob the reaper dropped, by reason"}, []string{"reason"}),
		vacuumed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sessions_vacuumed_total", Help: "stale session state the vacuum removed, by reason"}, []string{"reason"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "session_duration_ms_on_close", Help: "duration of sessions at close, ms", Buckets: durationBucketsMs}),
		events:   prometheus.NewHistogram(prometheus.HistogramOpts{Name: "session_events_on_close", Help: "events in sessions at close", Buckets: eventBuckets}),
	}
	reg.MustRegister(m.emitted, m.skipped, m.reaped, m.orphans, m.vacuumed, m.duration, m.events)
	return m
}

func (m *PromMetrics) EndEmitted()                { m.emitted.Inc() }
func (m *PromMetrics) EndSkipped(reason string)   { m.skipped.WithLabelValues(reason).Inc() }
func (m *PromMetrics) Reaped()                    { m.reaped.WithLabelValues(reasonDeadman).Inc() }
func (m *PromMetrics) ReaperOrphan()              { m.orphans.WithLabelValues(reasonDeadman).Inc() }
func (m *PromMetrics) Vacuumed(reason string)     { m.vacuumed.WithLabelValues(reason).Inc() }
func (m *PromMetrics) DurationOnClose(ms float64) { m.duration.Observe(ms) }
func (m *PromMetrics) EventsOnClose(n float64)    { m.events.Observe(n) }
