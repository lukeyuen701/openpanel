package server

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the receiver's own series, on their own registry.
type Metrics struct {
	Registry    *prometheus.Registry
	Requests    *prometheus.CounterVec
	Duration    *prometheus.HistogramVec
	Forwarded   *prometheus.CounterVec
	Produce     prometheus.Histogram
	LegacyEvent *prometheus.CounterVec
}

// NewMetrics registers the collectors.
func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_http_requests_total",
			Help: "Requests by route and outcome (accepted, duplicate, unauthorized, forwarded:<reason>, …)",
		}, []string{"route", "outcome", "status"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ingest_http_request_duration_seconds",
			Help:    "Request latency by route, native or forwarded",
			Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"route", "handled"}),
		Forwarded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_forwarded_total",
			Help: "Requests handed to the Bun API, by reason",
		}, []string{"reason"}),
		Produce: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ingest_pipeline_duration_seconds",
			Help:    "Event pipeline time including the Kafka acknowledgement",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}),
		LegacyEvent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "openpanel_legacy_event_requests_total",
			Help: "Requests to the legacy POST /event ingestion route, by client id",
		}, []string{"client_id"}),
	}
	m.Registry.MustRegister(m.Requests, m.Duration, m.Forwarded, m.Produce, m.LegacyEvent,
		prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}

type outcomeKey struct{}

// setOutcome records how a request ended, for the log line and the counter.
func setOutcome(r *http.Request, outcome string) {
	if slot, ok := r.Context().Value(outcomeKey{}).(*atomic.Value); ok {
		slot.Store(outcome)
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Unwrap lets http.ResponseController reach the connection (websockets
// through the proxy).
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

const (
	requestIDHeader    = "request-id"
	requestIDMaxLength = 64
)

var loggedHeaders = []string{"openpanel-client-id", "openpanel-sdk-name", "openpanel-sdk-version"}

// sanitizeRequestID keeps [A-Za-z0-9_-] of a caller-supplied id.
func sanitizeRequestID(candidate string) string {
	var b strings.Builder
	for _, c := range candidate {
		if c == '_' || c == '-' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			b.WriteRune(c)
		}
		if b.Len() >= requestIDMaxLength {
			break
		}
	}
	return b.String()
}

// instrument logs one "request done" line and counts the request.
func (s *Server) instrument(route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		slot := &atomic.Value{}
		requestID := sanitizeRequestID(r.Header.Get(requestIDHeader))
		if requestID == "" {
			requestID = newRequestID()
		}
		ctx := context.WithValue(r.Context(), outcomeKey{}, slot)
		ctx = withRequestID(ctx, requestID)
		r = r.WithContext(ctx)
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next(recorder, r)

		outcome, _ := slot.Load().(string)
		if outcome == "" {
			outcome = "unknown"
		}
		handled := "native"
		if strings.HasPrefix(outcome, "forwarded:") {
			handled = "forwarded"
		}
		elapsed := time.Since(start)
		s.Metrics.Requests.WithLabelValues(route, outcome, statusClass(recorder.status)).Inc()
		s.Metrics.Duration.WithLabelValues(route, handled).Observe(elapsed.Seconds())
		if r.Method == http.MethodOptions || route == "other" {
			return
		}
		attrs := []any{
			slog.String("url", r.URL.Path), slog.String("method", r.Method),
			slog.Int64("elapsed", elapsed.Milliseconds()), slog.Int("status", recorder.status),
			slog.String("outcome", outcome),
		}
		for _, name := range loggedHeaders {
			if value := r.Header.Get(name); value != "" {
				attrs = append(attrs, slog.String(name, value))
			}
		}
		s.Logger.InfoContext(ctx, "request done", attrs...)
	}
}

func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	}
	return "2xx"
}
