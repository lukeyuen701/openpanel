package events

import (
	"context"
	"log/slog"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

const (
	// ProfileBufferKey is the list the profile flush drains; the Bun API's
	// identify writes it too.
	ProfileBufferKey = "profile-buffer"
	// ProfileCachePrefix precedes project and profile id in the key the profile
	// flush caches a written profile under.
	ProfileCachePrefix = "profile-cache:"
	// AIEventBufferKey is the list of ai_events rows.
	AIEventBufferKey = "ai-event-buffer"

	profileBufferName = "profile"
	skipReasonCached  = "cached"
)

// ProfileBuffer adds profiles to the Redis list the profile flush reads
// (buf/profile-buffer.ts, add).
type ProfileBuffer struct {
	redis   Redis
	metrics Metrics
	logger  *slog.Logger
}

// NewProfileBuffer returns a ProfileBuffer.
func NewProfileBuffer(redis Redis, metrics Metrics, logger *slog.Logger) *ProfileBuffer {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &ProfileBuffer{redis: redis, metrics: metrics, logger: logger}
}

// Add queues the profile row. A profile that came from an event is skipped
// while the flush's cache entry for it exists, so one profile re-enters the
// buffer at most once per cache lifetime. Failures are logged, not returned.
func (b *ProfileBuffer) Add(ctx context.Context, profile *jsjson.Obj, isFromEvent bool) {
	start := time.Now()
	skipped := false
	// Counted unless skipped, failures included: the Bun buffer's add timer
	// wraps a body that swallows its own errors.
	defer func() {
		if !skipped {
			b.metrics.BufferAdded(profileBufferName, time.Since(start))
		}
	}()
	projectID, profileID := stringOf(get(profile, "project_id")), stringOf(get(profile, "id"))
	if isFromEvent {
		cached, err := b.redis.Exists(ctx, ProfileCachePrefix+projectID+":"+profileID)
		if err != nil {
			b.logger.Error("Failed to add profile", slog.Any("err", err))
			return
		}
		if cached {
			skipped = true
			b.metrics.BufferAddSkipped(profileBufferName, skipReasonCached)
			return
		}
	}
	line, err := stringify(jsjson.NewObject(profile))
	if err == nil {
		err = b.redis.RPush(ctx, ProfileBufferKey, line)
	}
	if err != nil {
		b.logger.Error("Failed to add profile", slog.Any("err", err))
	}
}

// profileRow is upsertProfile's row for an event: the identity of the profile,
// the event's attributes as its properties, and the wall-clock time twice.
func profileRow(payload *jsjson.Obj, clock Clock) *jsjson.Obj {
	properties := jsjson.NewObj()
	for _, entry := range [...]struct{ key, from string }{
		{"path", "path"}, {"country", "country"}, {"city", "city"}, {"region", "region"},
		{"longitude", "longitude"}, {"latitude", "latitude"}, {"os", "os"},
		{"os_version", "osVersion"}, {"browser", "browser"}, {"browser_version", "browserVersion"},
		{"device", "device"}, {"brand", "brand"}, {"model", "model"}, {"referrer", "referrer"},
		{"referrer_name", "referrerName"}, {"referrer_type", "referrerType"},
	} {
		value := get(payload, entry.from)
		// ramda reject(anyPass([isEmpty, isNil])): a 0 or false stays.
		if isNullish(value) || isEmptyValue(value) {
			continue
		}
		properties.Set(entry.key, value)
	}
	now, _ := jsport.DateFromTime(clock.Now()).ClickhouseDate()
	profileID := get(payload, "profileId")

	row := jsjson.NewObj()
	row.Set("id", str(stringOf(profileID)))
	row.Set("first_name", str(""))
	row.Set("last_name", str(""))
	row.Set("email", str(""))
	row.Set("avatar", str(""))
	row.Set("properties", jsjson.NewObject(properties))
	row.Set("project_id", get(payload, "projectId"))
	row.Set("created_at", str(now))
	row.Set("last_seen_at", str(now))
	row.Set("is_external", jsjson.NewBool(!strictEqual(profileID, get(payload, "deviceId"))))
	row.Set("groups", &jsjson.Value{Kind: jsjson.Array})
	return row
}

// AIEventBuffer adds ai_events rows (buf/ai-event-buffer.ts).
type AIEventBuffer struct {
	redis  Redis
	logger *slog.Logger
}

// NewAIEventBuffer returns an AIEventBuffer.
func NewAIEventBuffer(redis Redis, logger *slog.Logger) *AIEventBuffer {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &AIEventBuffer{redis: redis, logger: logger}
}

// Add pushes the row; a failure is logged and dropped.
func (b *AIEventBuffer) Add(ctx context.Context, row *jsjson.Obj) {
	line, err := stringify(jsjson.NewObject(row))
	if err == nil {
		err = b.redis.RPush(ctx, AIEventBufferKey, line)
	}
	if err != nil {
		b.logger.Error("Failed to add ai event", slog.Any("err", err))
	}
}

// buildAIEventRow is buildAiEventRow: the draft in its own key order, the
// content blanked when the project opted out, and the identity of the event
// row it follows.
func buildAIEventRow(draft *jsjson.Value, document *jsjson.Obj, storeContent bool) *jsjson.Obj {
	row := shallowCopy(asObject(draft))
	row.Set("span_id", orElse(get(row, "span_id"), get(document, "id")))
	for _, key := range [...]string{"input", "output", "tools"} {
		if storeContent {
			row.Set(key, get(row, key))
		} else {
			row.Set(key, str(""))
		}
	}
	for _, key := range [...]string{"project_id", "id", "profile_id", "device_id", "session_id", "groups", "created_at"} {
		row.Set(key, get(document, key))
	}
	return row
}
