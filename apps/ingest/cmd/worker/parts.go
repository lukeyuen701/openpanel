package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/rueidis"

	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/clock"
	workerconfig "openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/consumer"
	"openpanel/ingest/internal/worker/events"
	"openpanel/ingest/internal/worker/flush"
	"openpanel/ingest/internal/worker/flush/core"
	"openpanel/ingest/internal/worker/flush/simple"
	"openpanel/ingest/internal/worker/notify"
	"openpanel/ingest/internal/worker/sessions"
	"openpanel/ingest/internal/worker/wire"
)

// plan is what this process runs.
type plan struct {
	events bool
	queues []string
	cron   []string
}

func planOf(cfg workerconfig.Config) plan {
	if cfg.Role == workerconfig.RoleAPI || cfg.DisableWorkers {
		return plan{}
	}
	return plan{events: cfg.ConsumesEvents(), queues: cfg.GoWorkerQueues, cron: cfg.GoWorkerCronJobs}
}

func (p plan) nothing() bool { return !p.events && len(p.queues) == 0 && len(p.cron) == 0 }

// runningParts are what shutdown stops.
type runningParts struct {
	kafka       *consumer.Consumer
	scheduler   *background
	queueWorker *background
}

// background is a goroutine that stops when its context is cancelled.
type background struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func startBackground(ctx context.Context, run func(ctx context.Context)) *background {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	part := &background{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(part.done)
		run(runCtx)
	}()
	return part
}

func (b *background) stop() {
	if b == nil {
		return
	}
	b.cancel()
	<-b.done
}

func (r *runningParts) stopScheduler()   { r.scheduler.stop() }
func (r *runningParts) stopQueueWorker() { r.queueWorker.stop() }

func startEventsConsumer(ctx context.Context, cfg workerconfig.Config, logger *slog.Logger, registry *prometheus.Registry, redisClient rueidis.Client, handler *events.Handler, eventBuffer *events.EventBuffer, status *state) (*consumer.Consumer, error) {
	kafkaConfig := cfg.Kafka
	return consumer.Start(ctx, consumer.Config{
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
}

// sessionLifecycle builds the session job and the cron jobs from the shared clients.
type sessionLifecycle struct {
	cfg         workerconfig.Config
	logger      *slog.Logger
	registry    *prometheus.Registry
	redisClient rueidis.Client
	redis       *events.RueidisRedis
	db          *pgxpool.Pool
	chClient    *chclient.Client
	handler     *events.Handler
	eventBuffer *events.EventBuffer
	metrics     *events.PromMetrics
	dispatcher  *notify.Dispatcher
	producer    *bullmq.Producer

	sessionMetricsOnce sync.Once
	sessionMetrics     *sessions.PromMetrics
}

func (l *sessionLifecycle) sessionsRedis() *sessions.RueidisRedis {
	return sessions.NewRueidis(l.redisClient, l.cfg.RedisCacheCommandTimeout)
}

func (l *sessionLifecycle) promMetrics() *sessions.PromMetrics {
	l.sessionMetricsOnce.Do(func() { l.sessionMetrics = sessions.NewPromMetrics(l.registry) })
	return l.sessionMetrics
}

// startSessionsWorker consumes the sessions queue next to, or instead of, the
// Bun worker.
func (l *sessionLifecycle) startSessionsWorker(ctx context.Context) (*background, error) {
	redis := l.sessionsRedis()
	jobDuration := newJobDuration(l.registry)
	ender := &sessions.Ender{
		Redis:  redis,
		Events: l.handler,
		FlushEvents: func(ctx context.Context) error {
			return l.eventBuffer.FlushPendingOrThrow(ctx, l.eventBuffer.DroppedThrough())
		},
		CH:       l.chClient,
		Rules:    notify.NewRuleStore(l.redis, l.db),
		Funnel:   l.dispatcher,
		Backfill: sessions.Backfill{Enabled: l.cfg.Sessions.ProfileBackfill, Projects: l.cfg.Sessions.ProfileBackfillProjects},
		Metrics:  l.promMetrics(),
		Logger:   l.logger,
	}
	usage := &sessions.Usage{
		DB: l.db, CH: l.chClient, Redis: redis,
		Alerts:          sessions.BullAlertQueue{Producer: l.producer, IDs: wire.RandomIDs{}},
		Clock:           clock.System{},
		SelfHosted:      l.cfg.SelfHosted,
		PeriodsDisabled: l.cfg.PeriodsDisabled,
		Logger:          l.logger,
	}
	worker, err := bullmq.NewWorker(bullmq.NewRueidis(l.redisClient), sessions.QueueName,
		sessions.Handler(ender, usage, wire.RandomIDs{}, l.logger), bullmq.WorkerOptions{
			Concurrency: l.cfg.QueueConcurrency[sessions.QueueName],
			Namespace:   l.cfg.QueueNamespace,
			Cluster:     l.cfg.QueueCluster,
			Clock:       bullmq.SystemClock{},
			Logger:      l.logger,
			OnError: func(err error) {
				l.logger.Error("worker error", slog.String("queue", sessions.QueueName), slog.Any("err", err))
			},
			OnStalled: func(ids []string) {
				l.logger.Warn("job stalled and will be retried", slog.String("queue", sessions.QueueName), slog.Any("jobIds", ids))
			},
			OnTerminalFailure: func(queueKey string, duration time.Duration) {
				jobDuration.WithLabelValues(queueKey, jobStatusFailed).Observe(float64(duration.Milliseconds()))
			},
		})
	if err != nil {
		return nil, fmt.Errorf("sessions worker: %w", err)
	}
	return startBackground(ctx, func(runCtx context.Context) {
		// Run returns once its context ends and the running jobs finished.
		if err := worker.Run(runCtx); err != nil {
			l.logger.Error("sessions worker stopped", slog.Any("err", err))
		}
	}), nil
}

// startScheduler runs the cron jobs named in GO_WORKER_CRON_JOBS: the flushes
// the flush packages register and the session reaper and vacuum.
func (l *sessionLifecycle) startScheduler(ctx context.Context, flushRegistry *prometheus.Registry, enabled []string) (*background, error) {
	deps := flush.Deps{
		Redis:    flush.NewRueidis(l.redisClient, l.cfg.RedisCacheCommandTimeout),
		Clock:    clock.System{},
		Logger:   l.logger,
		Observer: flush.NewMetrics(flushRegistry),
		Paused:   flush.CronPauseCheck(flush.NewRueidis(l.redisClient, l.cfg.RedisCacheCommandTimeout), l.cfg.QueueNamespace, l.cfg.QueueCluster),
	}
	rejects := core.NewRedisRejects(
		consumer.NewRueidisListStore(l.redisClient, l.cfg.RedisCacheCommandTimeout), consumer.SystemClock{},
		l.cfg.DeadLetterMaxEntries, core.NewPromRejectMetrics(flushRegistry))
	flush.ResetRegistry()
	flush.Register("simple", func(runner flush.Deps) ([]flush.Job, error) {
		buffers := simple.Buffers(l.cfg.Buffers, simple.Deps{
			Redis: runner.Redis, Inserter: l.chClient,
			AsyncInserts: l.cfg.Buffers.AsyncInserts, InsertConcurrency: l.cfg.Buffers.InsertConcurrency,
		})
		return simple.Jobs(buffers, runner, false)
	})
	flush.Register("core", func(runner flush.Deps) ([]flush.Job, error) {
		buffers := core.Buffers(l.cfg.Buffers, core.Deps{
			Redis:     core.NewRueidis(l.redisClient, l.cfg.RedisCacheCommandTimeout),
			CH:        l.chClient,
			Logger:    l.logger,
			Clustered: l.cfg.ClickHouse.Cluster,
			Rejects:   rejects,
		})
		// The Bun worker records flush metrics for every buffer it flushes.
		return simple.Jobs(buffers, runner, true)
	})
	flush.Register("sessions", func(runner flush.Deps) ([]flush.Job, error) {
		sessionRedis := l.sessionsRedis()
		reaper := &sessions.Reaper{
			Redis: sessionRedis, Queue: l.handler, Clock: events.SystemClock{}, IDs: wire.RandomIDs{},
			Config:   sessions.ReaperConfig{Enabled: l.cfg.Sessions.ReaperEnabled, BatchSize: l.cfg.Sessions.ReaperBatch, Deadman: l.cfg.Sessions.ReaperDeadman},
			Metrics:  l.promMetrics(),
			Enqueued: l.metrics,
			Logger:   l.logger.With(slog.String("job", "session-reaper")),
		}
		vacuum := &sessions.Vacuum{
			Redis: sessionRedis, Clock: events.SystemClock{},
			Config:  sessions.VacuumConfig{Enabled: l.cfg.Sessions.VacuumEnabled, BatchSize: l.cfg.Sessions.VacuumBatch, StaleThreshold: l.cfg.Sessions.VacuumStale},
			Metrics: l.promMetrics(),
			Logger:  l.logger.With(slog.String("job", "session-vacuum")),
		}
		return sessions.CronJobs(reaper, vacuum, runner.Paused), nil
	})
	jobs, err := flush.RegisteredJobs(deps)
	if err != nil {
		return nil, err
	}
	scheduler, err := flush.NewScheduler(jobs, enabled, flush.SchedulerDeps{Clock: clock.System{}, Logger: l.logger})
	if err != nil {
		return nil, err
	}
	l.logger.Info("cron jobs", slog.Any("running", scheduler.JobNames()))
	return startBackground(ctx, scheduler.Run), nil
}
