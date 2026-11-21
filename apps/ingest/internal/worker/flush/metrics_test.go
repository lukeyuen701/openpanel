package flush

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func gatherByName(t *testing.T, reg *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*dto.MetricFamily{}
	for _, family := range families {
		byName[family.GetName()] = family
	}
	return byName
}

func labelsOf(metric *dto.Metric) map[string]string {
	labels := map[string]string{}
	for _, pair := range metric.GetLabel() {
		labels[pair.GetName()] = pair.GetValue()
	}
	return labels
}

func bucketBounds(histogram *dto.Histogram) []float64 {
	var bounds []float64
	for _, bucket := range histogram.GetBucket() {
		bounds = append(bounds, bucket.GetUpperBound())
	}
	return bounds
}

func equalFloats(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMetricsHaveTheNamesLabelsAndBucketsOfTheBunWorker(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	metrics.ObserveFlush(Observation{
		Buffer: "bot", Result: ResultSuccess, Trigger: TriggerCron, Total: 120 * time.Millisecond,
		Rows: 7, LLenAtStart: 7, LLenKnown: true,
		Phases: Phases{LRange: phase(2 * time.Millisecond), Trim: phase(time.Millisecond), CHInsert: phase(40 * time.Millisecond), CHFetch: phase(15 * time.Millisecond), OnFlush: phase(100 * time.Millisecond)},
	})
	metrics.ObserveFlush(Observation{Buffer: "bot", Result: ResultLocked, Trigger: TriggerCron, Total: time.Millisecond})
	metrics.ObserveAdd("bot", 3*time.Millisecond, false, "")
	metrics.ObserveAdd("profile", time.Millisecond, true, "cached")
	families := gatherByName(t, reg)

	histograms := map[string]struct {
		labels  []string
		buckets []float64
	}{
		"buffer_flush_duration_ms":     {[]string{"buffer", "result", "trigger"}, []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000}},
		"buffer_redis_op_duration_ms":  {[]string{"buffer", "op"}, []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}},
		"buffer_ch_insert_duration_ms": {[]string{"buffer"}, []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000}},
		"buffer_ch_fetch_duration_ms":  {[]string{"buffer"}, []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000}},
		"buffer_flush_llen_at_start":   {[]string{"buffer"}, []float64{0, 10, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000}},
		"buffer_add_duration_ms":       {[]string{"buffer"}, []float64{0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}},
	}
	for name, want := range histograms {
		family, ok := families[name]
		if !ok {
			t.Errorf("%s is missing", name)
			continue
		}
		if family.GetType() != dto.MetricType_HISTOGRAM {
			t.Errorf("%s is not a histogram", name)
			continue
		}
		metric := family.GetMetric()[0]
		if got := bucketBounds(metric.GetHistogram()); !equalFloats(got, want.buckets) {
			t.Errorf("%s buckets %v, want %v", name, got, want.buckets)
		}
		if len(metric.GetLabel()) != len(want.labels) {
			t.Errorf("%s labels %v, want %v", name, labelsOf(metric), want.labels)
		}
		for _, label := range want.labels {
			if _, ok := labelsOf(metric)[label]; !ok {
				t.Errorf("%s has no %s label", name, label)
			}
		}
	}
	counters := map[string]float64{"buffer_flush_rows_total": 7, "buffer_add_total": 1}
	for name, want := range counters {
		if got := families[name].GetMetric()[0].GetCounter().GetValue(); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	flushTotal := families["buffer_flush_total"]
	if len(flushTotal.GetMetric()) != 2 {
		t.Fatalf("buffer_flush_total series: %v", flushTotal.GetMetric())
	}
	skipped := families["buffer_add_skipped_total"].GetMetric()[0]
	if labelsOf(skipped)["reason"] != "cached" || labelsOf(skipped)["buffer"] != "profile" {
		t.Errorf("skipped labels %v", labelsOf(skipped))
	}
	// The locked flush has no phases and no list length: those series saw one observation each.
	if got := families["buffer_flush_llen_at_start"].GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("llen samples %d", got)
	}
	if got := families["buffer_redis_op_duration_ms"].GetMetric(); len(got) != 2 {
		t.Errorf("redis op series: %d, want lrange and trim", len(got))
	}
	// A skipped add stays out of the latency histogram.
	if got := families["buffer_add_duration_ms"].GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("add samples %d", got)
	}
}

func TestZeroRowFlushesDoNotCountRows(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg).ObserveFlush(Observation{Buffer: "bot", Result: ResultSuccess, Trigger: TriggerCron, Rows: 0})
	if _, ok := gatherByName(t, reg)["buffer_flush_rows_total"]; ok {
		t.Fatal("buffer_flush_rows_total must stay empty without rows")
	}
}

func TestLengthGaugesReadLLENAtScrapeTimeAndSkipAFailingBuffer(t *testing.T) {
	redis := NewMemoryRedis()
	redis.Push("bot-events-buffer", "a", "b", "c")
	reg := prometheus.NewRegistry()
	if err := RegisterLengthGauges(reg, redis, []ListRef{{"bot", "bot-events-buffer"}, {"profile-backfill", "profile-backfill-buffer"}}); err != nil {
		t.Fatal(err)
	}
	families := gatherByName(t, reg)
	if got := families["buffer_bot_count"].GetMetric()[0].GetGauge().GetValue(); got != 3 {
		t.Errorf("buffer_bot_count = %v", got)
	}
	if _, ok := families["buffer_profile_backfill_count"]; !ok {
		t.Errorf("hyphens must become underscores: %v", families)
	}
	redis.FailOnce("llen", errors.New("redis down"))
	if _, ok := gatherByName(t, reg)["buffer_bot_count"]; ok {
		t.Error("a failed LLEN must leave the series out of the scrape")
	}
	_ = context.Background()
}

func TestOnePanickingObserverCannotBreakAFlush(t *testing.T) {
	redis := NewMemoryRedis()
	runner, err := NewRunner(testFlusher{flush: func(context.Context, *Run) error { return nil }},
		Deps{Redis: redis, Clock: nopClock{}, Observer: panicObserver{}, Paused: NeverPaused})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.TryFlush(context.Background(), TriggerCron); err != nil {
		t.Fatal(err)
	}
}

type panicObserver struct{}

func (panicObserver) ObserveFlush(Observation) { panic("metrics broke") }

type nopClock struct{}

func (nopClock) Now() time.Time                                { return time.Now() }
func (nopClock) Sleep(context.Context, time.Duration) error    { return nil }
func (nopClock) Tick(time.Duration) (<-chan time.Time, func()) { return nil, func() {} }
