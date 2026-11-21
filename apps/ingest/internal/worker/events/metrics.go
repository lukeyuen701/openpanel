package events

import (
	"context"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"openpanel/ingest/internal/worker/bullmq"
)

const scrapeTimeout = 2 * time.Second

// bufferAddBucketsMs are buffer_add_duration_ms's buckets (buffer.metrics.ts).
var bufferAddBucketsMs = []float64{0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}

// PromMetrics registers the series of docs/GO_WORKER_CONTRACTS.md P1.11 under
// the names the Bun worker uses.
type PromMetrics struct {
	sessionsStarted *prometheus.CounterVec
	sessionEnds     *prometheus.CounterVec
	addTotal        *prometheus.CounterVec
	addDuration     *prometheus.HistogramVec
	addSkipped      *prometheus.CounterVec
}

// NewPromMetrics registers the counters on reg.
func NewPromMetrics(reg prometheus.Registerer) *PromMetrics {
	m := &PromMetrics{
		sessionsStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sessions_started_total", Help: "session_start events emitted, by lifecycle kind",
		}, []string{"kind"}),
		sessionEnds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "session_ends_enqueued_total", Help: "session_end jobs pushed onto the sessions queue, by trigger source",
		}, []string{"source"}),
		addTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "buffer_add_total", Help: "Count of add() calls that enqueued, by buffer",
		}, []string{"buffer"}),
		addDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "buffer_add_duration_ms", Help: "Duration of add() calls, by buffer", Buckets: bufferAddBucketsMs,
		}, []string{"buffer"}),
		addSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "buffer_add_skipped_total", Help: "add() calls that were skipped (no-op), by buffer and reason",
		}, []string{"buffer", "reason"}),
	}
	reg.MustRegister(m.sessionsStarted, m.sessionEnds, m.addTotal, m.addDuration, m.addSkipped)
	return m
}

// SessionStarted implements Metrics.
func (m *PromMetrics) SessionStarted(kind string) { m.sessionsStarted.WithLabelValues(kind).Inc() }

// SessionEndEnqueued implements Metrics.
func (m *PromMetrics) SessionEndEnqueued(source string) { m.sessionEnds.WithLabelValues(source).Inc() }

// BufferAdded implements Metrics.
func (m *PromMetrics) BufferAdded(buffer string, elapsed time.Duration) {
	m.addTotal.WithLabelValues(buffer).Inc()
	m.addDuration.WithLabelValues(buffer).Observe(float64(elapsed) / float64(time.Millisecond))
}

// BufferAddSkipped implements Metrics.
func (m *PromMetrics) BufferAddSkipped(buffer, reason string) {
	m.addSkipped.WithLabelValues(buffer, reason).Inc()
}

// ScrapeRedis is what the scrape-time gauges read.
type ScrapeRedis interface {
	LLen(ctx context.Context, key string) (int64, error)
	ZCard(ctx context.Context, key string) (int64, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	Get(ctx context.Context, key string) (string, bool, error)
}

// bufferLists are the lists whose length buffer_<name>_count reports, in the
// order the Bun worker registers them.
var bufferLists = [...]struct{ name, key string }{
	{"event", EventQueueKey},
	{"profile", ProfileBufferKey},
	{"bot", "bot-events-buffer"},
	{"session", SessionBufferKey},
	{"replay", "replay-buffer"},
	{"group", "group-buffer"},
	{"profile_backfill", "profile-backfill-buffer"},
}

// queueBaseNames are the seven registered BullMQ queues.
var queueBaseNames = [...]string{"sessions", "cron", "notification", "import", "insights", "gsc", "cohortCompute"}

var (
	queueBraces        = regexp.MustCompile("[{}]")
	nonMetricNameChars = regexp.MustCompile("[^a-zA-Z0-9_]")
)

func scrapeGauge(name, help string, read func(ctx context.Context) (float64, error)) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
		defer cancel()
		value, err := read(ctx)
		if err != nil {
			// A Redis blip must not fail the whole scrape.
			return math.NaN()
		}
		return value
	})
}

// RegisterScrapeGauges registers the gauges read at scrape time: the buffer
// lengths, the events buffer's local backlog, the queue counts and the session
// gauges. namespace and cluster are QUEUE_NAMESPACE and QUEUE_CLUSTER.
func RegisterScrapeGauges(reg prometheus.Registerer, redis ScrapeRedis, events *EventBuffer, namespace string, cluster bool) {
	for _, list := range bufferLists {
		key := list.key
		reg.MustRegister(scrapeGauge("buffer_"+list.name+"_count", "Buffer size (LLEN of the Redis list)", func(ctx context.Context) (float64, error) {
			n, err := redis.LLen(ctx, key)
			return float64(n), err
		}))
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "buffer_event_pending_local_count",
		Help: "Events in event-buffer process-local micro-batch (not yet in Redis)",
	}, func() float64 { return float64(events.PendingLocalCount()) }))

	for _, base := range queueBaseNames {
		key := bullmq.QueueKey(base, namespace, cluster)
		prefix := nonMetricNameChars.ReplaceAllString(queueBraces.ReplaceAllString(key, ""), "_")
		queueKeyOf := func(suffix string) string { return "bull:" + key + ":" + suffix }
		length := func(suffixes ...string) func(context.Context) (float64, error) {
			return func(ctx context.Context) (float64, error) {
				var total int64
				for _, suffix := range suffixes {
					n, err := redis.LLen(ctx, queueKeyOf(suffix))
					if err != nil {
						return 0, err
					}
					total += n
				}
				return float64(total), nil
			}
		}
		card := func(suffix string) func(context.Context) (float64, error) {
			return func(ctx context.Context) (float64, error) {
				n, err := redis.ZCard(ctx, queueKeyOf(suffix))
				return float64(n), err
			}
		}
		reg.MustRegister(
			scrapeGauge(prefix+"_active_count", "Active count", length("active")),
			scrapeGauge(prefix+"_delayed_count", "Delayed count", card("delayed")),
			scrapeGauge(prefix+"_failed_count", "Failed count", card("failed")),
			scrapeGauge(prefix+"_completed_count", "Completed count", card("completed")),
			scrapeGauge(prefix+"_waiting_count", "Waiting count", length("wait", "paused")),
		)
	}

	reg.MustRegister(
		scrapeGauge("sessions_active_total", "Active sessions in Redis across all projects", func(ctx context.Context) (float64, error) {
			projects, err := redis.SMembers(ctx, SessionProjectsKey)
			if err != nil {
				return 0, err
			}
			var total int64
			for _, project := range projects {
				n, err := redis.ZCard(ctx, wallclockKey(project))
				if err != nil {
					return 0, err
				}
				total += n
			}
			return float64(total), nil
		}),
		scrapeGauge("sessions_projects_active", "Projects with at least one active session", func(ctx context.Context) (float64, error) {
			projects, err := redis.SMembers(ctx, SessionProjectsKey)
			return float64(len(projects)), err
		}),
		scrapeGauge("sessions_hwm_lag_ms", "Max lag (ms) between wall-clock now and project event-time HWM, across all projects. Big number → queue lag or imports.", func(ctx context.Context) (float64, error) {
			projects, err := redis.SMembers(ctx, SessionProjectsKey)
			if err != nil {
				return 0, err
			}
			now := time.Now().UnixMilli()
			var maxLag int64
			for _, project := range projects {
				text, found, err := redis.Get(ctx, "session:hwm:"+project)
				if err != nil {
					return 0, err
				}
				if !found {
					continue
				}
				hwm, parseErr := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
				if parseErr != nil || hwm <= 0 {
					continue
				}
				maxLag = max(maxLag, now-hwm)
			}
			return float64(maxLag), nil
		}),
	)
}
