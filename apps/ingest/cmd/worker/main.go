// Command worker is OpenPanel's Go events consumer: the Kafka consumer path of
// the Bun worker started with ROLE=worker ENABLED_QUEUES=events (incomingEvent,
// the Redis buffers it writes, the BullMQ jobs it adds). It reads the same
// environment as that worker and serves the same ops routes on API_PORT.
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
	workerconfig "openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/consumer"
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
	if idle > heartbeatStale.Milliseconds() {
		return false, "events consumer heartbeat stale"
	}
	return true, ""
}

func run() error {
	cfg, err := workerconfig.FromEnvironment()
	if err != nil {
		return err
	}
	if !cfg.ConsumesEvents() {
		return errors.New("this process runs the events consumer: set ROLE=worker (or all) and ENABLED_QUEUES=events")
	}
	logger := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	status := &state{}
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

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

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
	clock := events.SystemClock{}
	eventBuffer := events.NewEventBuffer(redis, events.EventBufferConfig{
		MaxSize: cfg.EventBufferMicroBatchSize,
		MaxWait: cfg.EventBufferMicroBatchWait,
	}, metrics, logger)
	events.RegisterScrapeGauges(registry, redis, eventBuffer, cfg.QueueNamespace, cfg.QueueCluster)

	matcher := notify.NewMatcher(logger)
	projectStore := projects.NewStore(redis, db, clock, matcher, logger)
	dispatcher := &notify.Dispatcher{
		Matcher:  matcher,
		Projects: projectStore,
		Rules:    notify.NewRuleStore(redis, db),
		Profiles: notify.NewProfileStore(redis, clickhouse),
		DB:       db,
		Queue:    events.NotificationQueue{Producer: producer},
		Clock:    clock,
	}
	handler := events.NewHandler(events.HandlerConfig{
		Events:   eventBuffer,
		Sessions: events.NewSessionBuffer(redis, clock, cfg.SessionTimeout, logger),
		Profiles: events.NewProfileBuffer(redis, metrics, logger),
		AI:       events.NewAIEventBuffer(redis, logger),
		Projects: projectStore,
		Notifier: dispatcher,
		Queue:    producer,
		Clock:    clock,
		IDs:      events.RandomIDs{},
		Metrics:  metrics,
		Logger:   logger,
	})

	admin := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(cfg.APIPort)),
		Handler:           adminHandler(registry, status),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	// The server listens before the consumer starts, as the Bun worker does
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

	kafkaConfig := cfg.Kafka
	running, err := consumer.Start(ctx, consumer.Config{
		NewBroker: consumer.NewKafkaBrokerFactory(consumer.KafkaConfig{
			Brokers:                 kafkaConfig.Brokers,
			ClientID:                kafkaConfig.ClientID,
			Topic:                   kafkaConfig.EventsTopic,
			Group:                   kafkaConfig.ConsumerGroup,
			SessionTimeout:          kafkaConfig.SessionTimeout,
			HeartbeatInterval:       kafkaConfig.HeartbeatInterval,
			RebalanceTimeout:        kafkaConfig.RebalanceTimeout,
			RequestTimeout:          kafkaConfig.RequestTimeout,
			ConnectionTimeout:       kafkaConfig.ConnectionTimeout,
			MaxWait:                 kafkaConfig.MaxWait,
			MinMessages:             kafkaConfig.MinMessages,
			MaxMessagesPerPartition: kafkaConfig.MaxMessagesPerPartition,
			TLS:                     kafkaConfig.TLS,
			SASL:                    kafkaConfig.SASL,
		}),
		Handler:              handler.Handle,
		DeadLetter:           consumer.NewRedisDeadLetter(consumer.NewRueidisListStore(redisClient, cfg.RedisCacheCommandTimeout), consumer.SystemClock{}, cfg.DeadLetterMaxEntries),
		Durability:           eventBuffer,
		Logger:               logger,
		Metrics:              consumer.NewPromMetrics(registry),
		Clock:                consumer.SystemClock{},
		OnActivity:           status.markActivity,
		Topic:                kafkaConfig.EventsTopic,
		PartitionsConcurrent: kafkaConfig.PartitionsConcurrent,
		MaxMessagesPerBatch:  kafkaConfig.MaxMessagesPerPartition,
		PollTimeout:          kafkaConfig.MaxWait,
		MaxAttempts:          kafkaConfig.HandlerMaxAttempts,
		InitialRetry:         kafkaConfig.HandlerRetryInitial,
		MaxRetry:             kafkaConfig.HandlerRetryMax,
	})
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	status.markActivity()
	status.booting.Store(false)
	logger.Info("worker running", slog.String("admin", admin.Addr), slog.String("topic", kafkaConfig.EventsTopic), slog.String("group", kafkaConfig.ConsumerGroup))

	select {
	case err := <-serveErrors:
		return err
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	return shutdown(cfg, status, admin, running, eventBuffer, logger)
}

// shutdown stops taking work, makes what was taken durable, leaves the group
// and only then lets go of Redis. A buffer that cannot be flushed aborts
// without leaving the group, so the broker redelivers what was not committed.
func shutdown(cfg workerconfig.Config, status *state, admin *http.Server, running *consumer.Consumer, buffer *events.EventBuffer, logger *slog.Logger) error {
	status.shuttingDown.Store(true)
	forceExit := time.AfterFunc(cfg.ShutdownForceExit, func() {
		logger.Error("shutdown did not finish in time, forcing exit")
		os.Exit(exitFailure)
	})
	defer forceExit.Stop()

	httpCtx, cancel := context.WithTimeout(context.Background(), httpShutdownWait)
	defer cancel()
	httpErr := admin.Shutdown(httpCtx)
	running.StopConsuming()

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), cfg.ShutdownForceExit)
	defer cancelFlush()
	if err := buffer.FlushPendingOrThrow(flushCtx, buffer.DroppedThrough()); err != nil {
		return fmt.Errorf("event buffer could not be flushed, not leaving the group: %w", err)
	}
	running.Stop()
	return httpErr
}

// adminHandler is liveness, readiness, metrics and profiling, on a port the
// edge does not route to.
func adminHandler(registry *prometheus.Registry, status *state) http.Handler {
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
