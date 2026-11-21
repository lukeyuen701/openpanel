package sessions

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/rueidis"

	"openpanel/ingest/internal/config"
	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/clock"
	workerconfig "openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/events"
	"openpanel/ingest/internal/worker/notify"
	"openpanel/ingest/internal/worker/projects"
	"openpanel/ingest/internal/worker/wire"
)

// TestP3ParitySide is the Go half of apps/ingest/parity/p3-diff.ts: it runs the
// scenario file that script wrote against the live stack, with the real
// handlers, and leaves the state for the script to compare. It does nothing
// unless WPARITY_P3_SCENARIO is set.
func TestP3ParitySide(t *testing.T) {
	path := os.Getenv("WPARITY_P3_SCENARIO")
	if path == "" {
		t.Skip("WPARITY_P3_SCENARIO is not set; run apps/ingest/parity/p3-diff.ts")
	}
	var scenario struct {
		Out   string `json:"out"`
		Steps []struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		} `json:"steps"`
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &scenario); err != nil {
		t.Fatal(err)
	}

	cfg, err := workerconfig.FromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	redisOptions, err := rueidis.ParseURL(cfg.RedisURL)
	if err != nil {
		t.Fatal(err)
	}
	redisOptions.DisableCache = true
	client, err := rueidis.NewClient(redisOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	eventsRedis := events.NewRueidisRedis(client, cfg.RedisCacheCommandTimeout)
	pgURL, err := config.PostgresURL(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	db, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	chClient, err := chclient.New(chclient.FromWorkerConfig(cfg), chclient.Deps{Clock: clock.System{}, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := bullmq.NewProducer(bullmq.Config{
		Redis:      bullmq.NewRueidis(client),
		Queues:     []bullmq.Queue{bullmq.SessionsQueue(), bullmq.NotificationQueue()},
		Namespace:  cfg.QueueNamespace,
		Cluster:    cfg.QueueCluster,
		Clock:      bullmq.SystemClock{},
		Logger:     logger,
		Registerer: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Init(ctx); err != nil {
		t.Fatal(err)
	}

	eventClock := events.SystemClock{}
	eventBuffer := events.NewEventBuffer(eventsRedis, events.EventBufferConfig{}, nil, logger)
	matcher := notify.NewMatcher(logger)
	projectStore := projects.NewStore(eventsRedis, db, eventClock, matcher, logger)
	clickhouseHTTP, err := notify.NewClickHouseHTTP(cfg.ClickHouseURLs[0])
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &notify.Dispatcher{
		Matcher: matcher, Projects: projectStore, Rules: notify.NewRuleStore(eventsRedis, db),
		Profiles: notify.NewProfileStore(eventsRedis, clickhouseHTTP), DB: db,
		Queue: events.NotificationQueue{Producer: producer}, Clock: eventClock,
	}
	handler := events.NewHandler(events.HandlerConfig{
		Events: eventBuffer, Profiles: events.NewProfileBuffer(eventsRedis, nil, logger),
		Queue: producer, Clock: eventClock, IDs: events.RandomIDs{}, Logger: logger,
	})

	redis := NewRueidis(client, cfg.RedisCacheCommandTimeout)
	ender := &Ender{
		Redis: redis, Events: handler, CH: chClient, Rules: notify.NewRuleStore(eventsRedis, db), Funnel: dispatcher,
		FlushEvents: func(ctx context.Context) error {
			return eventBuffer.FlushPendingOrThrow(ctx, eventBuffer.DroppedThrough())
		},
		Backfill: Backfill{Enabled: cfg.Sessions.ProfileBackfill, Projects: cfg.Sessions.ProfileBackfillProjects},
		Logger:   logger,
	}
	usage := &Usage{
		DB: db, CH: chClient, Redis: redis, Alerts: BullAlertQueue{Producer: producer, IDs: wire.RandomIDs{}},
		Clock: clock.System{}, SelfHosted: cfg.SelfHosted, PeriodsDisabled: cfg.PeriodsDisabled, Logger: logger,
	}
	sessionJob := Handler(ender, usage, wire.RandomIDs{}, logger)
	reaper := &Reaper{
		Redis: redis, Queue: handler, Clock: eventClock, IDs: wire.RandomIDs{}, Logger: logger,
		Config: ReaperConfig{Enabled: cfg.Sessions.ReaperEnabled, BatchSize: cfg.Sessions.ReaperBatch, Deadman: cfg.Sessions.ReaperDeadman},
	}
	vacuum := &Vacuum{
		Redis: redis, Clock: eventClock, Logger: logger,
		Config: VacuumConfig{Enabled: cfg.Sessions.VacuumEnabled, BatchSize: cfg.Sessions.VacuumBatch, StaleThreshold: cfg.Sessions.VacuumStale},
	}

	type outcome struct {
		Type   string `json:"type"`
		Failed bool   `json:"failed"`
		Error  string `json:"error,omitempty"`
	}
	outcomes := []outcome{}
	for _, step := range scenario.Steps {
		var err error
		switch step.Type {
		case "job":
			err = sessionJob(ctx, bullmq.Job{Name: JobName, Data: step.Data})
		case "reap":
			err = reaper.Run(ctx)
		case "vacuum":
			err = vacuum.Run(ctx)
		}
		result := outcome{Type: step.Type, Failed: err != nil}
		if err != nil {
			result.Error = err.Error()
		}
		outcomes = append(outcomes, result)
	}
	if err := eventBuffer.FlushPendingOrThrow(ctx, eventBuffer.DroppedThrough()); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(outcomes)
	if err := os.WriteFile(scenario.Out, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}
