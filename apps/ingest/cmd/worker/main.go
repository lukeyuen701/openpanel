// Command worker is OpenPanel's Go worker. Any subset of three parts runs in
// one process: the Kafka events consumer (ROLE=worker, ENABLED_QUEUES with
// events), the BullMQ queues listed in GO_WORKER_QUEUES, and the cron jobs
// listed in GO_WORKER_CRON_JOBS. It reads the same environment as the Bun
// worker and serves the same ops routes on API_PORT.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/rueidis"

	"openpanel/ingest/internal/config"
	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/clock"
	workerconfig "openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/events"
	"openpanel/ingest/internal/worker/notify"
	"openpanel/ingest/internal/worker/projects"
)

const (
	// heartbeatStale is EVENTS_HEARTBEAT_STALE_MS: a consumer that has not
	// ticked for this long is not ready.
	heartbeatStale    = 60 * time.Second
	readHeaderTimeout = 10 * time.Second
	httpShutdownWait  = 5 * time.Second
	exitFailure       = 1
	// defaultAPIPort is API_PORT's default.
	defaultAPIPort     = 3000
	healthcheckTimeout = 3 * time.Second
)

// metaOnlyQueues are registry queues the worker never adds to; the Bun
// worker still writes their meta hash at boot, so this does too.
var metaOnlyQueues = [...]string{"cron", "import", "insights", "gsc", "cohortCompute"}

func main() {
	// The image has no shell or curl; a worker deployment's healthcheck runs
	// the binary.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(exitFailure)
	}
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "trace", "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error", "fatal":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler).With(slog.String("name", "worker"))
}

// healthcheck asks this container's own ops port whether the worker is ready.
func healthcheck() int {
	port := os.Getenv("API_PORT")
	if port == "" {
		port = strconv.Itoa(defaultAPIPort)
	}
	client := http.Client{Timeout: healthcheckTimeout}
	response, err := client.Get("http://127.0.0.1:" + port + "/healthz/ready")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return exitFailure
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status", response.StatusCode)
		return exitFailure
	}
	return 0
}

// state is what readiness reads.
type state struct {
	booting      atomic.Bool
	shuttingDown atomic.Bool
	lastActivity atomic.Int64
	// heartbeatRequired is set when this process runs the Kafka consumer, whose
	// loop is what ticks; a process without one has nothing to go stale.
	heartbeatRequired bool
}

func (s *state) markActivity() { s.lastActivity.Store(time.Now().UnixMilli()) }

// readiness is evaluateReadiness: not ready while booting or shutting down, or
// when the consumer loop has not ticked for heartbeatStale.
func (s *state) readiness(now time.Time) (bool, string) {
	switch {
	case s.shuttingDown.Load():
		return false, "shutting down"
	case s.booting.Load():
		return false, "booting"
	}
	idle := now.UnixMilli() - s.lastActivity.Load()
	if s.heartbeatRequired && idle > heartbeatStale.Milliseconds() {
		return false, "events consumer heartbeat stale"
	}
	return true, ""
}

func run() error {
	cfg, err := workerconfig.FromEnvironment()
	if err != nil {
		return err
	}
	plan := planOf(cfg)
	if plan.nothing() {
		return errors.New("nothing to run: set ROLE=worker (or all) with ENABLED_QUEUES=events, GO_WORKER_QUEUES or GO_WORKER_CRON_JOBS")
	}
	logger := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	status := &state{heartbeatRequired: plan.events}
	status.booting.Store(true)
	status.markActivity()

	redisOptions, err := rueidis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("REDIS_URL: %w", err)
	}
	// Plain commands only: client-side caching needs tracking the API's Redis
	// may not grant.
	redisOptions.DisableCache = true
	redisClient, err := rueidis.NewClient(redisOptions)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer redisClient.Close()
	redis := events.NewRueidisRedis(redisClient, cfg.RedisCacheCommandTimeout)

	pgURL, err := config.PostgresURL(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	db, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return fmt.Errorf("DATABASE_URL: %w", err)
	}
	defer db.Close()

	if len(cfg.ClickHouseURLs) == 0 {
		return errors.New("CLICKHOUSE_URL is not set")
	}
	clickhouse, err := notify.NewClickHouseHTTP(cfg.ClickHouseURLs[0])
	if err != nil {
		return err
	}
	chClient, err := chclient.New(chclient.FromWorkerConfig(cfg), chclient.Deps{Clock: clock.System{}, Logger: logger})
	if err != nil {
		return fmt.Errorf("clickhouse: %w", err)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	// The flush series live in a registry of their own: its buffer_add_*
	// families are the ones the events metrics register, and stay empty here.
	flushRegistry := prometheus.NewRegistry()
	gatherer := prometheus.Gatherers{registry, flushRegistry}

	queues := []bullmq.Queue{bullmq.SessionsQueue(), bullmq.NotificationQueue()}
	for _, name := range metaOnlyQueues {
		queues = append(queues, bullmq.Queue{Name: name})
	}
	producer, err := bullmq.NewProducer(bullmq.Config{
		Redis:      bullmq.NewRueidis(redisClient),
		Queues:     queues,
		Namespace:  cfg.QueueNamespace,
		Cluster:    cfg.QueueCluster,
		Clock:      bullmq.SystemClock{},
		Logger:     logger,
		Registerer: registry,
	})
	if err != nil {
		return err
	}
	if err := producer.Init(ctx); err != nil {
		return fmt.Errorf("bullmq init: %w", err)
	}

	metrics := events.NewPromMetrics(registry)
	eventClock := events.SystemClock{}
	eventBuffer := events.NewEventBuffer(redis, events.EventBufferConfig{
		MaxSize: cfg.EventBufferMicroBatchSize,
		MaxWait: cfg.EventBufferMicroBatchWait,
	}, metrics, logger)
	events.RegisterScrapeGauges(registry, redis, eventBuffer, cfg.QueueNamespace, cfg.QueueCluster)

	matcher := notify.NewMatcher(logger)
	projectStore := projects.NewStore(redis, db, eventClock, matcher, logger)
	dispatcher := &notify.Dispatcher{
		Matcher:  matcher,
		Projects: projectStore,
		Rules:    notify.NewRuleStore(redis, db),
		Profiles: notify.NewProfileStore(redis, clickhouse),
		DB:       db,
		Queue:    events.NotificationQueue{Producer: producer},
		Clock:    eventClock,
	}
	handler := events.NewHandler(events.HandlerConfig{
		Events:   eventBuffer,
		Sessions: events.NewSessionBuffer(redis, eventClock, cfg.SessionTimeout, logger),
		Profiles: events.NewProfileBuffer(redis, metrics, logger),
		AI:       events.NewAIEventBuffer(redis, logger),
		Projects: projectStore,
		Notifier: dispatcher,
		Queue:    producer,
		Clock:    eventClock,
		IDs:      events.RandomIDs{},
		Metrics:  metrics,
		Logger:   logger,
	})

	admin := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(cfg.APIPort)),
		Handler:           adminHandler(gatherer, status),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	// The server listens before anything is consumed, as the Bun worker does
	// (readiness answers 503 while booting), and a port that is taken fails the
	// boot before any group is joined.
	listener, err := net.Listen("tcp", admin.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", admin.Addr, err)
	}
	serveErrors := make(chan error, 1)
	go func() {
		if err := admin.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErrors <- fmt.Errorf("serve %s: %w", admin.Addr, err)
		}
	}()

	lifecycle := &sessionLifecycle{
		cfg: cfg, logger: logger, registry: registry, redisClient: redisClient, redis: redis,
		db: db, chClient: chClient, handler: handler, eventBuffer: eventBuffer, metrics: metrics,
		dispatcher: dispatcher, producer: producer,
	}
	running := &runningParts{}
	if plan.events {
		running.kafka, err = startEventsConsumer(ctx, cfg, logger, registry, redisClient, handler, eventBuffer, status)
		if err != nil {
			return fmt.Errorf("kafka consumer: %w", err)
		}
	}
	if len(plan.queues) > 0 {
		running.queueWorker, err = lifecycle.startSessionsWorker(ctx)
		if err != nil {
			return err
		}
	}
	if len(plan.cron) > 0 {
		running.scheduler, err = lifecycle.startScheduler(ctx, flushRegistry, plan.cron)
		if err != nil {
			return err
		}
	}
	status.markActivity()
	status.booting.Store(false)
	logger.Info("worker running", slog.String("admin", admin.Addr), slog.Bool("events", plan.events),
		slog.Any("queues", plan.queues), slog.Any("cronJobs", plan.cron))

	select {
	case err := <-serveErrors:
		return err
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	return shutdown(cfg, status, admin, running, eventBuffer, logger)
}

// shutdownOrder stops taking work in the order the work arrives: Kafka
// first (nothing new reaches the buffers), then the schedulers and the queue
// worker (the jobs running finish), then what was taken is made durable, the
// group is left and only then the clients close (deferred in run). A buffer that
// cannot be flushed aborts without leaving the group, so the broker redelivers
// what was not committed.
func shutdown(cfg workerconfig.Config, status *state, admin *http.Server, running *runningParts, buffer *events.EventBuffer, logger *slog.Logger) error {
	status.shuttingDown.Store(true)
	forceExit := time.AfterFunc(cfg.ShutdownForceExit, func() {
		logger.Error("shutdown did not finish in time, forcing exit")
		os.Exit(exitFailure)
	})
	defer forceExit.Stop()

	httpCtx, cancel := context.WithTimeout(context.Background(), httpShutdownWait)
	defer cancel()
	httpErr := admin.Shutdown(httpCtx)
	if running.kafka != nil {
		running.kafka.StopConsuming()
	}
	running.stopScheduler()
	running.stopQueueWorker()

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), cfg.ShutdownForceExit)
	defer cancelFlush()
	if err := buffer.FlushPendingOrThrow(flushCtx, buffer.DroppedThrough()); err != nil {
		return fmt.Errorf("event buffer could not be flushed, not leaving the group: %w", err)
	}
	if running.kafka != nil {
		running.kafka.Stop()
	}
	return httpErr
}

// adminHandler is liveness, readiness, metrics and profiling, on a port the
// edge does not route to.
func adminHandler(registry prometheus.Gatherer, status *state) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /healthz/ready", func(w http.ResponseWriter, _ *http.Request) {
		ready, reason := status.readiness(time.Now())
		if !ready {
			http.Error(w, reason, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	return mux
}
