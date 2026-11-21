package sessions

import (
	"context"
	"log/slog"
	"math"
	"slices"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/notify"
)

const (
	// endOffsetMs puts the session_end event one second after the last event.
	endOffsetMs    = 1000
	nameSessionEnd = "session_end"
)

// EventWriter is createEvent: *events.Handler.
type EventWriter interface {
	CreateEvent(ctx context.Context, payload *jsjson.Obj, createdAt jsport.Date) (*jsjson.Obj, error)
}

// FunnelChecker is checkNotificationRulesForSessionEnd: *notify.Dispatcher.
type FunnelChecker interface {
	CheckSessionEnd(ctx context.Context, requestID string, events []notify.SessionEvent) error
}

// Backfill is the profile-backfill switch of the environment.
type Backfill struct {
	Enabled bool
	// Projects is the allowlist; empty means every project.
	Projects []string
}

func (b Backfill) covers(projectID string) bool {
	return b.Enabled && (len(b.Projects) == 0 || slices.Contains(b.Projects, projectID))
}

// Ender is createSessionEnd.
type Ender struct {
	Redis  Redis
	Events EventWriter
	// FlushEvents waits until the rows createEvent queued are in Redis; nil skips.
	FlushEvents func(ctx context.Context) error
	CH          Querier
	Rules       notify.Rules
	Funnel      FunnelChecker
	Backfill    Backfill
	Metrics     Metrics
	Logger      *slog.Logger
}

// Job is a decoded session job: the closing event payload and the session
// as it was when the close was enqueued.
type Job struct {
	Event     *jsjson.Obj
	Snapshot  *jsjson.Obj
	RequestID string
}

// Create emits the session_end event of a closed session. It returns the row
// written, nil when nothing was emitted (extended, gone, or already done).
func (e *Ender) Create(ctx context.Context, job Job) (*jsjson.Obj, error) {
	metrics, logger := e.metrics(), e.logger()
	projectID, deviceID := job.Event.Get("projectId"), job.Event.Get("deviceId")
	snapshot := jsjson.NewObject(job.Snapshot)

	live, err := getExistingSession(ctx, e.Redis, stringOr(projectID), stringOr(deviceID))
	if err != nil {
		return nil, err
	}
	// Same session, extended after enqueue: the reaper retries once it idles again.
	// A different session in the slot (a boundary) or no blob: the snapshot.
	sameSession := isTruthy(live) && strictEqual(live.Get("id"), snapshot.Get("id"))
	if sameSession && isString(live.Get("ended_at")) && isString(snapshot.Get("ended_at")) && live.Get("ended_at").Str > snapshot.Get("ended_at").Str {
		metrics.EndSkipped(SkipExtendedAfterEnqueue)
		logger.Info("session was extended after close was enqueued, skipping",
			slog.String("sessionId", stringOr(live.Get("id"))), slog.String("projectId", stringOr(live.Get("project_id"))))
		return nil, nil
	}
	session := snapshot
	if sameSession {
		session = live
	}

	claimed, err := e.Redis.SetNXEx(ctx, claimKey(stringOr(session.Get("project_id")), stringOr(session.Get("device_id")), stringOr(session.Get("id"))), "1", claimTTL)
	if err != nil {
		return nil, err
	}
	if !claimed {
		metrics.EndSkipped(SkipAlreadyEmitted)
		logger.Info("session_end already emitted, skipping", slog.String("sessionId", stringOr(session.Get("id"))))
		return nil, nil
	}

	metrics.EndEmitted()
	metrics.DurationOnClose(math.Max(0, numberOrZero(orDefault(session.Get("duration"), num(0)))))
	metrics.EventsOnClose(numberOrZero(orDefault(session.Get("event_count"), num(0))) + numberOrZero(orDefault(session.Get("screen_view_count"), num(0))))

	profileID := orElse(session.Get("profile_id"), job.Event.Get("profileId"))
	if !strictEqual(profileID, session.Get("device_id")) && e.Backfill.covers(stringOr(projectID)) {
		e.addBackfill(ctx, projectID, session.Get("id"), profileID)
	}

	document, err := e.createEvent(ctx, job, session, profileID)
	if err != nil {
		return nil, err
	}
	if e.FlushEvents != nil {
		if err := e.FlushEvents(ctx); err != nil {
			logger.Error("session_end row is not in Redis yet; the buffer keeps retrying", slog.Any("err", err))
		}
	}

	if err := e.notifyFunnelRules(ctx, job.RequestID, session, stringOr(projectID), document); err != nil {
		logger.Error("Creating notificatios for session end failed", slog.Any("err", err))
	}
	if err := cleanup(ctx, e.Redis, session); err != nil {
		logger.Error("Failed to cleanup session state after session_end", slog.Any("err", err), slog.String("sessionId", stringOr(session.Get("id"))))
	}
	return document, nil
}

func (e *Ender) metrics() Metrics {
	if e.Metrics == nil {
		return NopMetrics{}
	}
	return e.Metrics
}

func (e *Ender) logger() *slog.Logger {
	if e.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return e.Logger
}

// numberOrZero is a number as Math.max and + read it; anything else is NaN.
func numberOrZero(v *jsjson.Value) float64 { return numberOf(v) }

// addBackfill is profileBackfill.add: it swallows its own failures.
func (e *Ender) addBackfill(ctx context.Context, projectID, sessionID, profileID *jsjson.Value) {
	entry := jsjson.NewObj()
	entry.Set("projectId", projectID)
	entry.Set("sessionId", sessionID)
	entry.Set("profileId", profileID)
	line, err := jsport.Stringify(jsjson.NewObject(entry))
	if err == nil {
		err = e.Redis.RPush(ctx, ProfileBackfillKey, line)
	}
	if err != nil {
		e.logger().Error("Failed to add profile backfill entry", slog.Any("err", err))
	}
}

// createEvent builds the createEvent payload of P3.4: the job's event, then the overrides.
func (e *Ender) createEvent(ctx context.Context, job Job, session *jsjson.Value, profileID *jsjson.Value) (*jsjson.Obj, error) {
	payload := shallowCopy(job.Event)
	payload.Set("sessionId", session.Get("id"))
	properties := jsjson.NewObj()
	spreadObject(properties, job.Event.Get("properties"))
	properties.Set("__bounce", session.Get("is_bounce"))
	payload.Set("properties", jsjson.NewObject(properties))
	payload.Set("name", str(nameSessionEnd))
	payload.Set("duration", orDefault(session.Get("duration"), num(0)))
	payload.Set("path", orDefault(session.Get("exit_path"), str("")))
	endedAt := clickhouseDate(stringOr(session.Get("ended_at")))
	createdAt := jsport.DateFromMillis(math.NaN())
	if ms, ok := endedAt.Millis(); ok {
		createdAt = jsport.DateFromMillis(float64(ms + endOffsetMs))
	}
	payload.Set("createdAt", dateValue(createdAt))
	payload.Set("profileId", profileID)
	return e.Events.CreateEvent(ctx, payload, createdAt)
}

func (e *Ender) notifyFunnelRules(ctx context.Context, requestID string, session *jsjson.Value, projectID string, document *jsjson.Obj) error {
	rules, err := e.Rules.ByProject(ctx, projectID)
	if err != nil {
		return err
	}
	hasFunnel, err := notify.HasFunnelRules(rules)
	if err != nil {
		return err
	}
	count := numberOf(session.Get("event_count")) + numberOf(session.Get("screen_view_count"))
	if !(hasFunnel && count < maxSessionEvents) {
		return nil
	}
	events, err := sessionEvents(ctx, e.CH, session)
	if err != nil {
		return err
	}
	events = append(events, transformEvent(jsjson.NewObject(document)))
	return e.Funnel.CheckSessionEnd(ctx, requestID, events)
}

// getExistingSession is getExistingSession: nil when the blob is missing or
// unparsable.
func getExistingSession(ctx context.Context, redis Redis, projectID, deviceID string) (*jsjson.Value, error) {
	text, found, err := redis.Get(ctx, sessionKey(projectID, deviceID))
	if err != nil {
		return nil, err
	}
	if !found || text == "" {
		return nil, nil
	}
	value, err := jsport.ParseJSON(text)
	if err != nil {
		return nil, nil
	}
	return value, nil
}

// cleanup is the id-gated cleanup of the session buffer. The profile pointer
// is only passed when the profile differs from the device.
func cleanup(ctx context.Context, redis Redis, session *jsjson.Value) error {
	projectID, deviceID := stringOr(session.Get("project_id")), stringOr(session.Get("device_id"))
	profileID := session.Get("profile_id")
	pointer := ""
	if isTruthy(profileID) && !strictEqual(profileID, session.Get("device_id")) {
		pointer = profilePointerKey(projectID, stringOr(profileID))
	}
	_, err := redis.Cleanup(ctx, [3]string{sessionKey(projectID, deviceID), wallclockKey(projectID), pointer}, stringOr(session.Get("id")), deviceID)
	return err
}
