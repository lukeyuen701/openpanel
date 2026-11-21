package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

const (
	// SessionBufferKey is the list the session flush drains.
	SessionBufferKey = "session-buffer"
	// SessionProjectsKey is the set of projects with live sessions.
	SessionProjectsKey = "session:projects"

	// DefaultSessionTimeout is SESSION_TIMEOUT_MS when unset.
	DefaultSessionTimeout = 30 * time.Minute

	// profilePointerTTLFactor times the idle timeout is how long a profile
	// keeps pointing at its device.
	profilePointerTTLFactor = 2
	// maxSessionGroups caps the groups a session row carries.
	maxSessionGroups = 100

	nameSessionStart = "session_start"
	nameSessionEnd   = "session_end"
	nameScreenView   = "screen_view"
	nameRevenue      = "revenue"
)

// Kinds of session decision.
const (
	SessionNew      = "new"
	SessionExtend   = "extend"
	SessionBoundary = "boundary"
)

var sessionUTMKeys = [...]string{"utm_medium", "utm_source", "utm_campaign", "utm_content", "utm_term"}

// SessionResult is what Ingest decided. Closed is set for a boundary: the
// session the new one replaced.
type SessionResult struct {
	Kind    string
	Current *jsjson.Obj
	Closed  *jsjson.Obj
}

// SessionBuffer is the session lifecycle in Redis (buf/session-buffer.ts): the
// blob per device, the profile pointer, the wall-clock index and the rows the
// session flush writes to ClickHouse.
type SessionBuffer struct {
	redis       Redis
	clock       Clock
	idleTimeout time.Duration
	logger      *slog.Logger
}

// NewSessionBuffer returns a SessionBuffer; idleTimeout is SESSION_TIMEOUT_MS.
func NewSessionBuffer(redis Redis, clock Clock, idleTimeout time.Duration, logger *slog.Logger) *SessionBuffer {
	if idleTimeout <= 0 {
		idleTimeout = DefaultSessionTimeout
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &SessionBuffer{redis: redis, clock: clock, idleTimeout: idleTimeout, logger: logger}
}

func sessionKey(projectID, deviceID string) string {
	return "session:" + projectID + ":" + deviceID
}

func profileIndexKey(projectID, profileID string) string {
	return "session:profile:" + projectID + ":" + profileID
}

func wallclockKey(projectID string) string { return "session:wallclock:" + projectID }

// parseBlob is getSafeJson: JSON.parse, null on a syntax error.
func parseBlob(text string) *jsjson.Value {
	value, err := jsport.ParseJSON(text)
	if err != nil {
		return nil
	}
	return value
}

func (s *SessionBuffer) readSession(ctx context.Context, key string) (*jsjson.Value, error) {
	text, found, err := s.redis.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found || text == "" {
		return nil, nil
	}
	return parseBlob(text), nil
}

// GetByDevice is getExistingSession by device. A hit that is not an object
// (JSON the writer never produces) comes back as is.
func (s *SessionBuffer) GetByDevice(ctx context.Context, projectID, deviceID string) (*jsjson.Value, error) {
	return s.readSession(ctx, sessionKey(projectID, deviceID))
}

// GetByProfile is getExistingSession by profile: the pointer, then the
// session of the device it names.
func (s *SessionBuffer) GetByProfile(ctx context.Context, projectID, profileID string) (*jsjson.Value, error) {
	deviceID, found, err := s.redis.Get(ctx, profileIndexKey(projectID, profileID))
	if err != nil {
		return nil, err
	}
	if !found || deviceID == "" {
		return nil, nil
	}
	return s.readSession(ctx, sessionKey(projectID, deviceID))
}

// Ingest drives one client event through the session lifecycle. It returns nil
// when the event has no session effect (no ids, a derived event) and when
// anything failed: the error is logged, never raised, so the event is written
// as if it had no session.
func (s *SessionBuffer) Ingest(ctx context.Context, payload *eventPayload) *SessionResult {
	projectID, deviceID := get(payload.obj, "projectId"), get(payload.obj, "deviceId")
	if !isTruthy(projectID) || !isTruthy(deviceID) {
		return nil
	}
	if name := stringOf(get(payload.obj, "name")); name == nameSessionStart || name == nameSessionEnd {
		return nil
	}
	result, err := s.ingest(ctx, payload, stringOf(projectID), stringOf(deviceID))
	if err != nil {
		s.logger.Error("Failed to ingest session", slog.Any("err", err))
		return nil
	}
	return result
}

var errNotSessionObject = errors.New("session blob is not an object")

func (s *SessionBuffer) ingest(ctx context.Context, payload *eventPayload, projectID, deviceID string) (*SessionResult, error) {
	value, err := s.GetByDevice(ctx, projectID, deviceID)
	if err != nil {
		return nil, err
	}
	var existing *jsjson.Obj
	if isTruthy(value) {
		if value.Kind != jsjson.Object {
			return nil, errNotSessionObject
		}
		existing = value.Obj
	}

	if existing != nil {
		endedMs, err := clickhouseMillis(get(existing, "ended_at"))
		if err != nil {
			return nil, err
		}
		eventMs := payload.millis()
		// Event time, not wall clock; NaN compares false, so an unparseable
		// time extends the session.
		isBoundary := eventMs-endedMs > float64(s.idleTimeout.Milliseconds())
		if !isBoundary {
			current, rows, err := s.extend(existing, payload)
			if err != nil {
				return nil, err
			}
			if err := s.persist(ctx, current, rows, existing); err != nil {
				return nil, err
			}
			return &SessionResult{Kind: SessionExtend, Current: current}, nil
		}
	}

	current := s.newSession(payload)
	if err := s.persist(ctx, current, []*jsjson.Obj{current}, nil); err != nil {
		return nil, err
	}
	if existing != nil {
		return &SessionResult{Kind: SessionBoundary, Current: current, Closed: existing}, nil
	}
	return &SessionResult{Kind: SessionNew, Current: current}, nil
}

// clickhouseMillis is fromClickhouseDate(s).getTime(): NaN for text that is not
// a ClickHouse date, an error for what Luxon would throw on.
func clickhouseMillis(v *jsjson.Value) (float64, error) {
	if v == nil || v.Kind != jsjson.String {
		return 0, fmt.Errorf("session date is not a string")
	}
	ms, ok := jsport.ParseClickhouseDateTime(v.Str).Millis()
	if !ok {
		return math.NaN(), nil
	}
	return float64(ms), nil
}

func clickhouseMillisOrNaN(v *jsjson.Value) float64 {
	ms, err := clickhouseMillis(v)
	if err != nil {
		return math.NaN()
	}
	return ms
}

// extend is extendSession: the session with this event folded in, and the
// pair of rows ClickHouse needs to replace the previous one (the new row, then
// the same row with sign -1).
func (s *SessionBuffer) extend(existing *jsjson.Obj, payload *eventPayload) (*jsjson.Obj, []*jsjson.Obj, error) {
	old := shallowCopy(existing)
	old.Set("sign", num(-1))
	current := shallowCopy(existing)
	current.Set("sign", num(1))
	current.Set("version", num(numberOf(get(existing, "version"))+1))

	eventMs := payload.millis()
	startMs, err := clickhouseMillis(get(current, "created_at"))
	if err != nil {
		return nil, nil, err
	}
	endMs, err := clickhouseMillis(get(current, "ended_at"))
	if err != nil {
		return nil, nil, err
	}
	eventCH := str(payload.createdAt.LuxonFormat())
	path, origin := get(payload.obj, "path"), get(payload.obj, "origin")

	if eventMs >= endMs {
		current.Set("ended_at", eventCH)
		if isTruthy(path) {
			current.Set("exit_path", path)
		}
		if isTruthy(origin) {
			current.Set("exit_origin", origin)
		}
	}
	if eventMs < startMs {
		current.Set("created_at", eventCH)
		if isTruthy(path) {
			current.Set("entry_path", path)
		}
		if isTruthy(origin) {
			current.Set("entry_origin", origin)
		}
	} else {
		if !isTruthy(get(current, "entry_path")) && isTruthy(path) {
			current.Set("entry_path", path)
		}
		if !isTruthy(get(current, "entry_origin")) && isTruthy(origin) {
			current.Set("entry_origin", origin)
		}
	}

	endedMs, err := clickhouseMillis(get(current, "ended_at"))
	if err != nil {
		return nil, nil, err
	}
	createdMs, err := clickhouseMillis(get(current, "created_at"))
	if err != nil {
		return nil, nil, err
	}
	current.Set("duration", num(endedMs-createdMs))

	name := stringOf(get(payload.obj, "name"))
	if name == nameRevenue {
		revenue := numberOf(orDefault(get(current, "revenue"), num(0))) + numberOf(orDefault(get(payload.obj, "revenue"), num(0)))
		current.Set("revenue", num(revenue))
	}
	if name == nameScreenView && isTruthy(path) {
		current.Set("screen_view_count", num(numberOf(get(current, "screen_view_count"))+1))
	} else {
		current.Set("event_count", num(numberOf(get(current, "event_count"))+1))
	}
	if numberOf(get(current, "screen_view_count")) > 1 {
		current.Set("is_bounce", jsjson.NewBool(false))
	}

	profileID := get(payload.obj, "profileId")
	if isTruthy(profileID) && !strictEqual(profileID, get(payload.obj, "deviceId")) {
		current.Set("profile_id", profileID)
	}

	if groups := get(payload.obj, "groups"); groups != nil && groups.Kind == jsjson.Array && len(groups.Arr) > 0 {
		current.Set("groups", unionGroups(get(current, "groups"), groups))
	}
	return current, []*jsjson.Obj{current, old}, nil
}

// unionGroups is [...new Set([...(current ?? []), ...added])].slice(0, 100).
func unionGroups(current, added *jsjson.Value) *jsjson.Value {
	seen := map[string]bool{}
	var out []*jsjson.Value
	for _, list := range []*jsjson.Value{current, added} {
		if list == nil || list.Kind != jsjson.Array {
			continue
		}
		for _, item := range list.Arr {
			key := sameValueKey(item)
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, item)
		}
	}
	if len(out) > maxSessionGroups {
		out = out[:maxSessionGroups]
	}
	return &jsjson.Value{Kind: jsjson.Array, Arr: out}
}

// newSession is newSession(payload): the first row of a device or the one
// after an idle gap.
func (s *SessionBuffer) newSession(payload *eventPayload) *jsjson.Obj {
	p := payload.obj
	createdAt := str(payload.createdAt.LuxonFormat())
	name := stringOf(get(p, "name"))
	isScreenView := name == nameScreenView
	empty := str("")
	field := func(key string) *jsjson.Value { return orDefault(get(p, key), empty) }

	revenue := num(0)
	if name == nameRevenue {
		revenue = orDefault(get(p, "revenue"), num(0))
	}
	eventCount, screenViewCount := num(1), num(0)
	if isScreenView {
		eventCount, screenViewCount = num(0), num(1)
	}

	out := jsjson.NewObj()
	out.Set("id", get(p, "sessionId"))
	out.Set("project_id", get(p, "projectId"))
	out.Set("device_id", get(p, "deviceId"))
	out.Set("profile_id", orElse(get(p, "profileId"), get(p, "deviceId")))
	out.Set("is_bounce", jsjson.NewBool(true))
	out.Set("created_at", createdAt)
	out.Set("ended_at", createdAt)
	out.Set("event_count", eventCount)
	out.Set("screen_view_count", screenViewCount)
	out.Set("entry_path", field("path"))
	out.Set("entry_origin", field("origin"))
	out.Set("exit_path", field("path"))
	out.Set("exit_origin", field("origin"))
	out.Set("revenue", revenue)
	out.Set("referrer", field("referrer"))
	out.Set("referrer_name", field("referrerName"))
	out.Set("referrer_type", field("referrerType"))
	out.Set("os", field("os"))
	out.Set("os_version", field("osVersion"))
	out.Set("browser", field("browser"))
	out.Set("browser_version", field("browserVersion"))
	out.Set("device", field("device"))
	out.Set("brand", field("brand"))
	out.Set("model", field("model"))
	out.Set("country", field("country"))
	out.Set("region", field("region"))
	out.Set("city", field("city"))
	out.Set("longitude", orDefault(get(p, "longitude"), jsjson.NewNull()))
	out.Set("latitude", orDefault(get(p, "latitude"), jsjson.NewNull()))
	out.Set("duration", orDefault(get(p, "duration"), num(0)))
	for _, key := range sessionUTMKeys {
		out.Set(key, pickUTM(p, key))
	}
	groups := orDefault(get(p, "groups"), &jsjson.Value{Kind: jsjson.Array})
	if groups.Kind == jsjson.Array && len(groups.Arr) > maxSessionGroups {
		groups = &jsjson.Value{Kind: jsjson.Array, Arr: groups.Arr[:maxSessionGroups]}
	}
	out.Set("groups", groups)
	out.Set("sign", num(1))
	out.Set("version", num(1))
	return out
}

// pickUTM is pickUtm: String(properties.__query[key]) when truthy, else "".
func pickUTM(payload *jsjson.Obj, key string) *jsjson.Value {
	properties := get(payload, "properties")
	if properties == nil || properties.Kind != jsjson.Object {
		return str("")
	}
	query := properties.Obj.Get("__query")
	if query == nil || query.Kind != jsjson.Object {
		return str("")
	}
	value := query.Obj.Get(key)
	if !isTruthy(value) {
		return str("")
	}
	return str(stringOf(value))
}

// persist is the write-back: blob, wall-clock index, project set, profile
// pointer and the ClickHouse rows, in one MULTI.
func (s *SessionBuffer) persist(ctx context.Context, current *jsjson.Obj, rows []*jsjson.Obj, previous *jsjson.Obj) error {
	projectID := stringOf(get(current, "project_id"))
	deviceID := stringOf(get(current, "device_id"))
	currentProfile := get(current, "profile_id")
	hasPointer := isTruthy(currentProfile) && !strictEqual(currentProfile, get(current, "device_id"))
	previousProfile := get(previous, "profile_id")
	hasStalePointer := isTruthy(previousProfile) &&
		!strictEqual(previousProfile, get(current, "device_id")) &&
		!strictEqual(previousProfile, currentProfile)

	blob, err := stringify(jsjson.NewObject(current))
	if err != nil {
		return err
	}
	commands := [][]string{
		{"SET", sessionKey(projectID, deviceID), blob},
		{"ZADD", wallclockKey(projectID), strconv.FormatInt(s.clock.Now().UnixMilli(), 10), deviceID},
		{"SADD", SessionProjectsKey, projectID},
	}
	if hasStalePointer {
		commands = append(commands, []string{"DEL", profileIndexKey(projectID, stringOf(previousProfile))})
	}
	if hasPointer {
		ttl := (s.idleTimeout * profilePointerTTLFactor).Milliseconds()
		commands = append(commands, []string{"SET", profileIndexKey(projectID, stringOf(currentProfile)), deviceID, "PX", strconv.FormatInt(ttl, 10)})
	}
	push := []string{"RPUSH", SessionBufferKey}
	for _, row := range rows {
		line, err := stringify(jsjson.NewObject(row))
		if err != nil {
			return err
		}
		push = append(push, line)
	}
	commands = append(commands, push)
	return s.redis.Transaction(ctx, commands)
}
