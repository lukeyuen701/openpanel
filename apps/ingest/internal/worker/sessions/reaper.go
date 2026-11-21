package sessions

import (
	"context"
	"log/slog"
	"math"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/events"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/wire"
)

const (
	// ReaperInterval is the reaper's cron every.
	ReaperInterval = 5 * time.Minute
	// ReaperJob and VacuumJob are the cron job ids of the Bun registry.
	ReaperJob = "sessionReaper"
	VacuumJob = "sessionVacuum"

	sourceReaper = "reaper"
	// localDateLayouts are how V8 reads the space-separated ClickHouse date as a local time.
	localDateWithMillis = "2006-01-02 15:04:05.000"
	localDate           = "2006-01-02 15:04:05"
)

// Enqueuer adds a session-end job: *events.Handler.
type Enqueuer interface {
	EnqueueSessionEnd(ctx context.Context, requestID string, event, closed *jsjson.Obj) error
}

// ReaperConfig is the reaper's environment.
type ReaperConfig struct {
	Enabled   bool
	BatchSize int
	// Deadman is how long after its last received event a session is closed.
	Deadman time.Duration
}

// Reaper is reapIdleSessions.
type Reaper struct {
	Redis   Redis
	Queue   Enqueuer
	Clock   events.Clock
	IDs     wire.IDSource
	Config  ReaperConfig
	Metrics Metrics
	// Enqueued counts session_ends_enqueued_total.
	Enqueued events.Metrics
	Logger   *slog.Logger
}

// Run closes every session whose last event was received more than the
// deadman ago. It is idempotent through the job id of the close.
func (r *Reaper) Run(ctx context.Context) error {
	if !r.Config.Enabled {
		return nil
	}
	logger := r.logger()
	projectIDs, err := r.Redis.SMembers(ctx, ProjectsKey)
	if err != nil || len(projectIDs) == 0 {
		return err
	}
	// A scheduled job has no originating request, so it gets its own id.
	requestID := r.IDs.NewID(wire.RequestIDLength)
	reaped, failed := 0, 0
	for _, projectID := range projectIDs {
		count, err := r.reapProject(ctx, requestID, projectID)
		if err != nil {
			failed++
			logger.Error("Failed to reap project", slog.Any("err", err), slog.String("projectId", projectID))
			continue
		}
		reaped += count
	}
	if reaped > 0 || failed > 0 {
		logger.Info("Reaper tick complete", slog.Int("reaped", reaped), slog.Int("errors", failed), slog.Int("projects", len(projectIDs)))
	}
	return nil
}

func (r *Reaper) logger() *slog.Logger {
	if r.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return r.Logger
}

func (r *Reaper) metrics() Metrics {
	if r.Metrics == nil {
		return NopMetrics{}
	}
	return r.Metrics
}

func (r *Reaper) reapProject(ctx context.Context, requestID, projectID string) (int, error) {
	locked, err := r.Redis.SetNXEx(ctx, reaperLockKey(projectID), "1", reaperLockTTL)
	if err != nil || !locked {
		return 0, err
	}
	// Not value-checked, like the TypeScript: a lock that expired mid-tick and
	// was taken by another worker is released with it.
	defer func() { _ = r.Redis.Del(context.WithoutCancel(ctx), reaperLockKey(projectID)) }()

	cutoff := r.Clock.Now().UnixMilli() - r.Config.Deadman.Milliseconds()
	candidates, err := r.Redis.ZRangeByScore(ctx, wallclockKey(projectID), cutoff, r.Config.BatchSize)
	if err != nil {
		return 0, err
	}
	reaped := 0
	for _, deviceID := range candidates {
		closed, err := r.closeSession(ctx, requestID, projectID, deviceID)
		if err != nil {
			return reaped, err
		}
		if closed {
			reaped++
		}
	}
	remaining, err := r.Redis.ZCard(ctx, wallclockKey(projectID))
	if err != nil {
		return reaped, err
	}
	if remaining == 0 {
		if err := r.Redis.SRem(ctx, ProjectsKey, projectID); err != nil {
			return reaped, err
		}
	}
	return reaped, nil
}

func (r *Reaper) closeSession(ctx context.Context, requestID, projectID, deviceID string) (bool, error) {
	logger := r.logger()
	session, err := getExistingSession(ctx, r.Redis, projectID, deviceID)
	if err != nil {
		return false, err
	}
	if !isTruthy(session) {
		// A wall-clock entry without a blob is a partly failed cleanup.
		r.metrics().ReaperOrphan()
		logger.Warn("Reaper found wallclock entry without blob — likely a partial cleanup failure", slog.String("projectId", projectID), slog.String("deviceId", deviceID))
		return false, r.Redis.ZRem(ctx, wallclockKey(projectID), deviceID)
	}
	if !session.IsObject() {
		logger.Error("Failed to enqueue session_end during reap: the session blob is not an object", slog.String("projectId", projectID), slog.String("deviceId", deviceID))
		return false, nil
	}
	if err := r.Queue.EnqueueSessionEnd(ctx, requestID, reaperPayload(session), session.Obj); err != nil {
		logger.Error("Failed to enqueue session_end during reap", slog.Any("err", err), slog.String("projectId", projectID), slog.String("deviceId", deviceID))
		return false, nil
	}
	r.metrics().Reaped()
	if r.Enqueued != nil {
		r.Enqueued.SessionEndEnqueued(sourceReaper)
	}
	return true, nil
}

// reaperPayload is sessionEndPayload of session-reaper.ts, in its key order.
func reaperPayload(session *jsjson.Value) *jsjson.Obj {
	text := func(key string) *jsjson.Value { return orDefault(session.Get(key), str("")) }
	payload := jsjson.NewObj()
	payload.Set("projectId", session.Get("project_id"))
	payload.Set("deviceId", session.Get("device_id"))
	payload.Set("sessionId", session.Get("id"))
	payload.Set("profileId", text("profile_id"))
	payload.Set("name", str(nameSessionEnd))
	payload.Set("properties", jsjson.NewObject(jsjson.NewObj()))
	payload.Set("groups", orDefault(session.Get("groups"), &jsjson.Value{Kind: jsjson.Array}))
	payload.Set("createdAt", dateValue(localDateOf(stringOr(session.Get("ended_at")))))
	payload.Set("duration", orDefault(session.Get("duration"), num(0)))
	payload.Set("sdkName", str(""))
	payload.Set("sdkVersion", str(""))
	payload.Set("city", text("city"))
	payload.Set("country", text("country"))
	payload.Set("region", text("region"))
	payload.Set("longitude", undefinedIfNull(session.Get("longitude")))
	payload.Set("latitude", undefinedIfNull(session.Get("latitude")))
	payload.Set("path", text("exit_path"))
	payload.Set("origin", text("exit_origin"))
	payload.Set("referrer", text("referrer"))
	payload.Set("referrerName", text("referrer_name"))
	payload.Set("referrerType", text("referrer_type"))
	payload.Set("os", text("os"))
	payload.Set("osVersion", text("os_version"))
	payload.Set("browser", text("browser"))
	payload.Set("browserVersion", text("browser_version"))
	payload.Set("device", text("device"))
	payload.Set("brand", text("brand"))
	payload.Set("model", text("model"))
	return payload
}

// localDateOf is new Date("yyyy-MM-dd HH:mm:ss.SSS"): V8 reads it as local
// time. The close overwrites the value before any row is written; it only
// travels in the job data.
func localDateOf(text string) jsport.Date {
	for _, layout := range [...]string{localDateWithMillis, localDate} {
		if parsed, err := time.ParseInLocation(layout, text, time.Local); err == nil {
			return jsport.DateFromTime(parsed)
		}
	}
	return jsport.DateFromMillis(math.NaN())
}
