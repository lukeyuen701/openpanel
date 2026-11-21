package events

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

const (
	testProject = "project-1"
	testDevice  = "device-1"
	testSession = "session-1"
	minute      = 60 * 1000
)

var baseTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

// makePayload is the TypeScript test's makePayload: a screen_view of profile-1.
func makePayload(createdAtMs int64, overrides map[string]*jsjson.Value) *eventPayload {
	o := jsjson.NewObj()
	set := func(key string, value *jsjson.Value) {
		if override, ok := overrides[key]; ok {
			value = override
		}
		o.Set(key, value)
	}
	date := jsport.DateFromMillis(float64(createdAtMs))
	set("name", str("screen_view"))
	set("profileId", str("profile-1"))
	set("projectId", str(testProject))
	set("deviceId", str(testDevice))
	set("sessionId", str(testSession))
	set("properties", jsjson.NewObject(jsjson.NewObj()))
	set("groups", &jsjson.Value{Kind: jsjson.Array})
	set("createdAt", dateValue(date))
	set("duration", num(0))
	set("sdkName", str("web"))
	set("sdkVersion", str("1.0.0"))
	set("city", str(""))
	set("country", str(""))
	set("region", str(""))
	set("longitude", num(0))
	set("latitude", num(0))
	set("path", str("/home"))
	set("origin", str(""))
	set("referrer", str(""))
	set("referrerName", str(""))
	set("referrerType", str(""))
	for _, key := range []string{"os", "osVersion", "browser", "browserVersion", "device", "brand", "model"} {
		set(key, str(""))
	}
	for key, value := range overrides {
		if !o.Has(key) {
			o.Set(key, value)
		}
	}
	return &eventPayload{obj: o, createdAt: date}
}

func newSessions(t *testing.T) (*SessionBuffer, *fakeRedis) {
	t.Helper()
	redis := newFakeRedis()
	return NewSessionBuffer(redis, fixedClock{now: fixedNow}, DefaultSessionTimeout, nil), redis
}

func sessionBlob(t *testing.T, redis *fakeRedis) map[string]any {
	t.Helper()
	text, ok, _ := redis.Get(context.Background(), "session:"+testProject+":"+testDevice)
	if !ok {
		t.Fatal("no session blob")
	}
	var blob map[string]any
	if err := json.Unmarshal([]byte(text), &blob); err != nil {
		t.Fatal(err)
	}
	return blob
}

func TestIngestOpensASessionAtItsKey(t *testing.T) {
	sessions, redis := newSessions(t)
	result := sessions.Ingest(context.Background(), makePayload(baseTime, nil))
	if result == nil || result.Kind != SessionNew {
		t.Fatalf("result = %+v", result)
	}
	blob := sessionBlob(t, redis)
	if blob["id"] != testSession || blob["screen_view_count"] != float64(1) || blob["event_count"] != float64(0) ||
		blob["sign"] != float64(1) || blob["version"] != float64(1) || blob["is_bounce"] != true {
		t.Fatalf("blob = %v", blob)
	}
	if rows := redis.list(SessionBufferKey); len(rows) != 1 {
		t.Fatalf("%d session rows, want 1", len(rows))
	}
}

func TestIngestRegistersWallclockAndProjectSet(t *testing.T) {
	sessions, redis := newSessions(t)
	sessions.Ingest(context.Background(), makePayload(baseTime, nil))
	if redis.zsets["session:wallclock:"+testProject][testDevice] == "" || !redis.sets[SessionProjectsKey][testProject] {
		t.Fatalf("zsets=%v sets=%v", redis.zsets, redis.sets)
	}
	if score := redis.zsets["session:wallclock:"+testProject][testDevice]; score != strconv.FormatInt(fixedNow.UnixMilli(), 10) {
		t.Fatalf("wallclock score = %s, want the clock's milliseconds", score)
	}
}

func TestBlobHasNoTTLButTheProfilePointerDoes(t *testing.T) {
	sessions, redis := newSessions(t)
	sessions.Ingest(context.Background(), makePayload(baseTime, nil))
	blobKey := "session:" + testProject + ":" + testDevice
	pointerKey := "session:profile:" + testProject + ":profile-1"
	if _, has := redis.ttl[blobKey]; has {
		t.Fatal("the session blob has a TTL")
	}
	if redis.strings[pointerKey] != testDevice || redis.ttl[pointerKey] != 2*DefaultSessionTimeout.Milliseconds() {
		t.Fatalf("pointer = %q ttl %d", redis.strings[pointerKey], redis.ttl[pointerKey])
	}
}

func TestNoPointerWhenProfileIsTheDevice(t *testing.T) {
	sessions, redis := newSessions(t)
	sessions.Ingest(context.Background(), makePayload(baseTime, map[string]*jsjson.Value{"profileId": str(testDevice)}))
	for key := range redis.strings {
		if strings.HasPrefix(key, "session:profile:") {
			t.Fatalf("unexpected pointer %s", key)
		}
	}
}

func TestReidentifyingDropsThePreviousPointerAndSameProfileKeepsIt(t *testing.T) {
	sessions, redis := newSessions(t)
	ctx := context.Background()
	sessions.Ingest(ctx, makePayload(baseTime, nil))
	sessions.Ingest(ctx, makePayload(baseTime+1000, nil)) // same profile: pointer stays
	if _, ok := redis.strings["session:profile:"+testProject+":profile-1"]; !ok {
		t.Fatal("the pointer vanished on an extend with the same profile")
	}
	sessions.Ingest(ctx, makePayload(baseTime+2000, map[string]*jsjson.Value{"profileId": str("profile-2")}))
	if _, ok := redis.strings["session:profile:"+testProject+":profile-1"]; ok {
		t.Fatal("the old pointer survived a re-identification")
	}
	if redis.strings["session:profile:"+testProject+":profile-2"] != testDevice {
		t.Fatal("no pointer for the new profile")
	}
}

func TestSessionStartAndEndAreSkipped(t *testing.T) {
	sessions, redis := newSessions(t)
	for _, name := range []string{"session_start", "session_end"} {
		if got := sessions.Ingest(context.Background(), makePayload(baseTime, map[string]*jsjson.Value{"name": str(name)})); got != nil {
			t.Fatalf("%s produced %+v", name, got)
		}
	}
	if len(redis.commands) != 0 {
		t.Fatalf("commands = %v", redis.commands)
	}
}

func TestExtendWritesTheNewRowThenTheCancelling(t *testing.T) {
	sessions, redis := newSessions(t)
	ctx := context.Background()
	sessions.Ingest(ctx, makePayload(baseTime, nil))
	result := sessions.Ingest(ctx, makePayload(baseTime+minute, map[string]*jsjson.Value{"path": str("/second")}))
	if result == nil || result.Kind != SessionExtend {
		t.Fatalf("result = %+v", result)
	}
	rows := redis.list(SessionBufferKey)
	if len(rows) != 3 {
		t.Fatalf("%d rows, want 1 + 2", len(rows))
	}
	var current, old map[string]any
	_ = json.Unmarshal([]byte(rows[1]), &current)
	_ = json.Unmarshal([]byte(rows[2]), &old)
	if current["sign"] != float64(1) || current["version"] != float64(2) || old["sign"] != float64(-1) || old["version"] != float64(1) {
		t.Fatalf("current=%v old=%v", current, old)
	}
	if current["exit_path"] != "/second" || current["duration"] != float64(minute) || current["screen_view_count"] != float64(2) || current["is_bounce"] != false {
		t.Fatalf("current = %v", current)
	}
}

func TestBoundaryIsStrictlyGreaterThanTheTimeout(t *testing.T) {
	timeout := DefaultSessionTimeout.Milliseconds()
	for _, test := range []struct {
		gap  int64
		kind string
	}{{timeout - 1, SessionExtend}, {timeout, SessionExtend}, {timeout + 1, SessionBoundary}} {
		sessions, _ := newSessions(t)
		ctx := context.Background()
		sessions.Ingest(ctx, makePayload(baseTime, nil))
		result := sessions.Ingest(ctx, makePayload(baseTime+test.gap, map[string]*jsjson.Value{"sessionId": str("session-2")}))
		if result == nil || result.Kind != test.kind {
			t.Fatalf("gap %d: result = %+v, want %s", test.gap, result, test.kind)
		}
		if test.kind == SessionBoundary {
			if jsjson.JSString(get(result.Closed, "id")) != testSession || jsjson.JSString(get(result.Current, "id")) != "session-2" {
				t.Fatalf("closed %v current %v", result.Closed, result.Current)
			}
		}
	}
}

func TestUTMFieldsComeFromTheQuery(t *testing.T) {
	sessions, redis := newSessions(t)
	query := jsjson.NewObj()
	query.Set("utm_source", str("google"))
	query.Set("utm_campaign", str("spring"))
	properties := jsjson.NewObj()
	properties.Set("__query", jsjson.NewObject(query))
	sessions.Ingest(context.Background(), makePayload(baseTime, map[string]*jsjson.Value{"properties": jsjson.NewObject(properties)}))
	blob := sessionBlob(t, redis)
	if blob["utm_source"] != "google" || blob["utm_campaign"] != "spring" || blob["utm_medium"] != "" {
		t.Fatalf("blob = %v", blob)
	}
}

func TestOutOfOrderEventExtendsBackwardsWithoutNegativeDuration(t *testing.T) {
	sessions, redis := newSessions(t)
	ctx := context.Background()
	sessions.Ingest(ctx, makePayload(baseTime+30*minute, nil))
	sessions.Ingest(ctx, makePayload(baseTime+5*minute, map[string]*jsjson.Value{"path": str("/earlier")}))
	blob := sessionBlob(t, redis)
	if blob["created_at"] != "2026-01-01 00:05:00.000" || blob["ended_at"] != "2026-01-01 00:30:00.000" ||
		blob["entry_path"] != "/earlier" || blob["duration"] != float64(25*minute) {
		t.Fatalf("blob = %v", blob)
	}
}

func TestIngestSendsOnlyPlainCommands(t *testing.T) {
	sessions, redis := newSessions(t)
	ctx := context.Background()
	sessions.Ingest(ctx, makePayload(baseTime, nil))
	sessions.Ingest(ctx, makePayload(baseTime+minute, nil))
	for _, name := range redis.usedCommands() {
		switch name {
		case "GET", "MULTI", "EXEC", "SET", "ZADD", "SADD", "DEL", "RPUSH":
		default:
			t.Fatalf("session ingest sent %s", name)
		}
	}
}

func TestIngestSwallowsRedisFailures(t *testing.T) {
	sessions, redis := newSessions(t)
	redis.failTx = errRedisDown
	if got := sessions.Ingest(context.Background(), makePayload(baseTime, nil)); got != nil {
		t.Fatalf("a failed write-back returned %+v", got)
	}
	redis.failTx = nil
	redis.failGet = errRedisDown
	if got := sessions.Ingest(context.Background(), makePayload(baseTime, nil)); got != nil {
		t.Fatalf("a failed read returned %+v", got)
	}
}

func TestRevenueAndGroupsAccumulateOnExtend(t *testing.T) {
	sessions, redis := newSessions(t)
	ctx := context.Background()
	groups := func(names ...string) *jsjson.Value {
		out := &jsjson.Value{Kind: jsjson.Array}
		for _, n := range names {
			out.Arr = append(out.Arr, str(n))
		}
		return out
	}
	sessions.Ingest(ctx, makePayload(baseTime, map[string]*jsjson.Value{"name": str("revenue"), "revenue": num(10), "groups": groups("a", "b")}))
	sessions.Ingest(ctx, makePayload(baseTime+1000, map[string]*jsjson.Value{"name": str("revenue"), "revenue": num(5.5), "groups": groups("b", "c")}))
	blob := sessionBlob(t, redis)
	if blob["revenue"] != 15.5 || blob["event_count"] != float64(2) {
		t.Fatalf("blob = %v", blob)
	}
	if got, _ := json.Marshal(blob["groups"]); string(got) != `["a","b","c"]` {
		t.Fatalf("groups = %s", got)
	}
}
