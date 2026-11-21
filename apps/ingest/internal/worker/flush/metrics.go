package flush

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Bucket arrays, verbatim from packages/core/src/buffers/buffer.metrics.ts.
var (
	flushDurationBucketsMs = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000}
	redisOpBucketsMs       = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}
	chDurationBucketsMs    = []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000}
	llenBuckets            = []float64{0, 10, 100, 500, 1000, 5000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000}
	addDurationBucketsMs   = []float64{0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}
)

const lengthGaugeTimeout = 5 * time.Second

func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Metrics are the buffer_* series of buffer.metrics.ts, with its names,
// labels and buckets. Build one per process: a second registration of the
// same names on one registry panics, so every buffer shares this one and is
// told apart by its buffer label.
type Metrics struct {
	flushDuration   *prometheus.HistogramVec
	flushTotal      *prometheus.CounterVec
	flushRows       *prometheus.CounterVec
	redisOp         *prometheus.HistogramVec
	chInsert        *prometheus.HistogramVec
	chFetch         *prometheus.HistogramVec
	flushLLen       *prometheus.HistogramVec
	addDuration     *prometheus.HistogramVec
	addTotal        *prometheus.CounterVec
	addSkippedTotal *prometheus.CounterVec
}

// NewMetrics registers the series on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		return factory.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return factory.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	return &Metrics{
		flushDuration:   histogram("buffer_flush_duration_ms", "Wall time of a tryFlush call, including lock acquisition", flushDurationBucketsMs, "buffer", "result", "trigger"),
		flushTotal:      counter("buffer_flush_total", "Count of tryFlush invocations by result (success/error/locked/paused) and trigger (add/cron)", "buffer", "result", "trigger"),
		flushRows:       counter("buffer_flush_rows_total", "Rows drained from the buffer per flush (sum)", "buffer"),
		redisOp:         histogram("buffer_redis_op_duration_ms", "Duration of a Redis op during flush", redisOpBucketsMs, "buffer", "op"),
		chInsert:        histogram("buffer_ch_insert_duration_ms", "Duration of the ClickHouse insert(s) inside a single flush", chDurationBucketsMs, "buffer"),
		chFetch:         histogram("buffer_ch_fetch_duration_ms", "Duration of CH SELECT(s) inside a single flush (e.g. profile merge fetch)", chDurationBucketsMs, "buffer"),
		flushLLen:       histogram("buffer_flush_llen_at_start", "LLEN observed at the start of a flush attempt", llenBuckets, "buffer"),
		addDuration:     histogram("buffer_add_duration_ms", "Duration of a single add() call (per-event ingest path)", addDurationBucketsMs, "buffer"),
		addTotal:        counter("buffer_add_total", "Total add() calls per buffer that actually enqueued (excludes skipped).", "buffer"),
		addSkippedTotal: counter("buffer_add_skipped_total", "add() calls that short-circuited (no enqueue). Reason: cached, etc.", "buffer", "reason"),
	}
}

// ObserveFlush implements Observer.
func (m *Metrics) ObserveFlush(obs Observation) {
	result, trigger := string(obs.Result), string(obs.Trigger)
	m.flushTotal.WithLabelValues(obs.Buffer, result, trigger).Inc()
	m.flushDuration.WithLabelValues(obs.Buffer, result, trigger).Observe(milliseconds(obs.Total))
	if obs.LLenKnown {
		m.flushLLen.WithLabelValues(obs.Buffer).Observe(float64(obs.LLenAtStart))
	}
	if obs.Rows > 0 {
		m.flushRows.WithLabelValues(obs.Buffer).Add(float64(obs.Rows))
	}
	if obs.Phases.LRange.Set {
		m.redisOp.WithLabelValues(obs.Buffer, "lrange").Observe(milliseconds(obs.Phases.LRange.Duration))
	}
	if obs.Phases.Trim.Set {
		m.redisOp.WithLabelValues(obs.Buffer, "trim").Observe(milliseconds(obs.Phases.Trim.Duration))
	}
	if obs.Phases.CHFetch.Set {
		m.chFetch.WithLabelValues(obs.Buffer).Observe(milliseconds(obs.Phases.CHFetch.Duration))
	}
	if obs.Phases.CHInsert.Set {
		m.chInsert.WithLabelValues(obs.Buffer).Observe(milliseconds(obs.Phases.CHInsert.Duration))
	}
}

// ObserveAdd records one add() call of a buffer's write path. A skipped add
// counts under buffer_add_skipped_total and stays out of the latency histogram.
func (m *Metrics) ObserveAdd(buffer string, duration time.Duration, skipped bool, skipReason string) {
	if skipped {
		m.addSkippedTotal.WithLabelValues(buffer, skipReason).Inc()
		return
	}
	m.addTotal.WithLabelValues(buffer).Inc()
	m.addDuration.WithLabelValues(buffer).Observe(milliseconds(duration))
}

// ListRef names a buffer and its Redis list for the depth gauge.
type ListRef struct{ Name, ListKey string }

// GaugeName is buffer_<name>_count, with "-" as "_".
func GaugeName(buffer string) string {
	return "buffer_" + strings.ReplaceAll(buffer, "-", "_") + "_count"
}

// lengthCollector reads LLEN of each list at scrape time; a failure leaves
// that buffer's series out of the scrape, as the TypeScript collect() does.
type lengthCollector struct {
	redis   Redis
	buffers []ListRef
	descs   []*prometheus.Desc
}

// RegisterLengthGauges registers buffer_<name>_count for every buffer in the
// order given. It costs one Redis round trip per buffer per scrape, so register
// it only in a process that runs flushes.
func RegisterLengthGauges(reg prometheus.Registerer, redis Redis, buffers []ListRef) error {
	collector := &lengthCollector{redis: redis, buffers: buffers}
	for _, buffer := range buffers {
		collector.descs = append(collector.descs, prometheus.NewDesc(GaugeName(buffer.Name), "Buffer size (LLEN of the Redis list)", nil, nil))
	}
	return reg.Register(collector)
}

// Describe implements prometheus.Collector.
func (c *lengthCollector) Describe(out chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		out <- desc
	}
}

// Collect implements prometheus.Collector.
func (c *lengthCollector) Collect(out chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), lengthGaugeTimeout)
	defer cancel()
	for i, buffer := range c.buffers {
		length, err := c.redis.LLen(ctx, buffer.ListKey)
		if err != nil {
			continue
		}
		out <- prometheus.MustNewConstMetric(c.descs[i], prometheus.GaugeValue, float64(length))
	}
}
