package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const jobStatusFailed = "failed"

// jobDurationBucketsMs are the buckets of job_duration_ms in jobs.metrics.ts.
var jobDurationBucketsMs = []float64{10, 25, 50, 100, 250, 500, 750, 1000, 2000, 5000, 10_000, 30_000}

// newJobDuration is job_duration_ms. It is failure-only, as in the Bun worker:
// a job that ends in a terminal failure is the only one observed, and name is
// the queue's Redis key.
func newJobDuration(reg prometheus.Registerer) *prometheus.HistogramVec {
	return promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "job_duration_ms",
		Help:    "Duration of job processing (in ms)",
		Buckets: jobDurationBucketsMs,
	}, []string{"name", "status"})
}
