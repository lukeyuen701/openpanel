package sessions

import (
	"context"
	"strings"
	"testing"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/notify"
	"openpanel/ingest/internal/worker/wire"
)

const (
	blobEnded    = `{"id":"s1","project_id":"p1","device_id":"d1","profile_id":"d1","created_at":"2026-06-08 10:30:00.000","ended_at":"2026-06-08 11:00:00.000","is_bounce":false,"duration":1800000,"event_count":3,"screen_view_count":5,"exit_path":"/end","groups":[]}`
	jobEventBase = `{"projectId":"p1","deviceId":"d1","sessionId":"s1","profileId":"u1","name":"screen_view","properties":{"__query":{"a":"b"}},"country":"SE","createdAt":"2026-06-08T11:40:00.000Z"}`
)

func toJSON(v *jsjson.Value) string {
	text, _ := jsport.Stringify(v)
	return text
}

func parse(t *testing.T, text string) *jsjson.Value {
	t.Helper()
	value, err := jsjson.Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type endRig struct {
	redis  *memoryRedis
	events *fakeEvents
	funnel *fakeFunnel
	ch     *fakeClickHouse
	ender  *Ender
	rules  *fakeRules
}

func newEndRig() *endRig {
	rig := &endRig{redis: newMemoryRedis(), events: &fakeEvents{}, funnel: &fakeFunnel{}, ch: &fakeClickHouse{}, rules: &fakeRules{}}
	rig.ender = &Ender{Redis: rig.redis, Events: rig.events, CH: rig.ch, Rules: rig.rules, Funnel: rig.funnel}
	return rig
}

func (r *endRig) job(t *testing.T, snapshot, event string) Job {
	return Job{Event: parse(t, event).Obj, Snapshot: parse(t, snapshot).Obj, RequestID: "req"}
}

func TestCreateEmitsSessionEndAndCleansUp(t *testing.T) {
	rig := newEndRig()
	snapshot := strings.Replace(blobEnded, `"profile_id":"d1"`, `"profile_id":"u1"`, 1)
	rig.redis.putSession("p1", "d1", snapshot, 1)
	rig.redis.strings[profilePointerKey("p1", "u1")] = "d1"

	doc, err := rig.ender.Create(context.Background(), rig.job(t, snapshot, jobEventBase))
	if err != nil || doc == nil {
		t.Fatalf("doc = %v, err = %v", doc, err)
	}
	created := rig.events.created[0]
	payload := toJSON((jsjson.NewObject(created.payload)))
	for _, want := range []string{`"name":"session_end"`, `"sessionId":"s1"`, `"duration":1800000`, `"path":"/end"`,
		`"profileId":"u1"`, `"country":"SE"`, `"__bounce":false`, `"__query":{"a":"b"}`, `"createdAt":"2026-06-08T11:00:01.000Z"`} {
		if !strings.Contains(payload, want) {
			t.Errorf("payload %s lacks %s", payload, want)
		}
	}
	if ms, _ := created.createdAt.Millis(); ms != time.Date(2026, 6, 8, 11, 0, 1, 0, time.UTC).UnixMilli() {
		t.Errorf("createdAt = %d, want ended_at + 1s", ms)
	}
	if _, ok := rig.redis.strings[sessionKey("p1", "d1")]; ok {
		t.Error("the session blob is still there")
	}
	if _, ok := rig.redis.strings[profilePointerKey("p1", "u1")]; ok {
		t.Error("the profile pointer is still there")
	}
	claim := claimKey("p1", "d1", "s1")
	if rig.redis.strings[claim] != "1" || rig.redis.ttls[claim] != 2*time.Hour {
		t.Errorf("claim = %q ttl %v", rig.redis.strings[claim], rig.redis.ttls[claim])
	}
}

func TestCreateSkipsASessionExtendedAfterEnqueue(t *testing.T) {
	rig := newEndRig()
	live := strings.Replace(blobEnded, "11:00:00.000", "11:10:00.000", 1)
	rig.redis.putSession("p1", "d1", live, 1)
	doc, err := rig.ender.Create(context.Background(), rig.job(t, blobEnded, jobEventBase))
	if err != nil || doc != nil || len(rig.events.created) != 0 {
		t.Fatalf("doc = %v, err = %v, created = %d", doc, err, len(rig.events.created))
	}
	if _, ok := rig.redis.strings[sessionKey("p1", "d1")]; !ok {
		t.Error("the blob must stay for the reaper to retry")
	}
	if len(rig.redis.strings) != 1 {
		t.Error("no claim may be taken")
	}
}

func TestCreateSkipsWhenTheClaimIsTakenAndLeavesTheBlob(t *testing.T) {
	rig := newEndRig()
	rig.redis.putSession("p1", "d1", blobEnded, 1)
	rig.redis.strings[claimKey("p1", "d1", "s1")] = "1"
	doc, err := rig.ender.Create(context.Background(), rig.job(t, blobEnded, jobEventBase))
	if err != nil || doc != nil || len(rig.events.created) != 0 {
		t.Fatalf("doc = %v, err = %v", doc, err)
	}
	if _, ok := rig.redis.strings[sessionKey("p1", "d1")]; !ok {
		t.Error("TypeScript runs no cleanup for an already emitted session")
	}
}

func TestCreateUsesTheSnapshotAtABoundaryAndKeepsTheNewSession(t *testing.T) {
	rig := newEndRig()
	newer := strings.Replace(blobEnded, `"id":"s1"`, `"id":"s2"`, 1)
	rig.redis.putSession("p1", "d1", newer, 1)
	doc, err := rig.ender.Create(context.Background(), rig.job(t, blobEnded, jobEventBase))
	if err != nil || doc == nil {
		t.Fatalf("doc = %v, err = %v", doc, err)
	}
	if got := rig.redis.strings[sessionKey("p1", "d1")]; got != newer {
		t.Errorf("the new session was touched: %s", got)
	}
	if _, ok := rig.redis.zsets[wallclockKey("p1")]["d1"]; !ok {
		t.Error("the wall-clock entry of the new session was removed")
	}
}

func TestCreateFallsBackToTheSnapshotWhenTheBlobIsGone(t *testing.T) {
	rig := newEndRig()
	doc, err := rig.ender.Create(context.Background(), rig.job(t, blobEnded, jobEventBase))
	if err != nil || doc == nil || len(rig.events.created) != 1 {
		t.Fatalf("doc = %v, err = %v", doc, err)
	}
}

func TestCreateBackfillEntryOnlyWhenEnabledAndProfileDiffers(t *testing.T) {
	snapshot := strings.Replace(blobEnded, `"profile_id":"d1"`, `"profile_id":"u1"`, 1)
	for name, tc := range map[string]struct {
		backfill Backfill
		want     bool
	}{
		"off":            {Backfill{}, false},
		"on":             {Backfill{Enabled: true}, true},
		"allowlist hit":  {Backfill{Enabled: true, Projects: []string{"p1"}}, true},
		"allowlist miss": {Backfill{Enabled: true, Projects: []string{"other"}}, false},
	} {
		rig := newEndRig()
		rig.ender.Backfill = tc.backfill
		if _, err := rig.ender.Create(context.Background(), rig.job(t, snapshot, jobEventBase)); err != nil {
			t.Fatal(err)
		}
		got := rig.redis.lists[ProfileBackfillKey]
		if (len(got) == 1) != tc.want {
			t.Errorf("%s: list = %v", name, got)
		}
		if tc.want && got[0] != `{"projectId":"p1","sessionId":"s1","profileId":"u1"}` {
			t.Errorf("%s: entry = %s", name, got[0])
		}
	}
	rig := newEndRig()
	rig.ender.Backfill = Backfill{Enabled: true}
	if _, err := rig.ender.Create(context.Background(), rig.job(t, blobEnded, jobEventBase)); err != nil {
		t.Fatal(err)
	}
	// The job's profile id u1 differs from the device, but the session's own profile id (d1) wins.
	if len(rig.redis.lists[ProfileBackfillKey]) != 0 {
		t.Error("a session whose profile is its device has nothing to backfill")
	}
}

func funnelRule(t *testing.T) notify.Rule {
	return notify.Rule{ID: "r1", Name: "F", Config: parse(t, `{"type":"funnel","events":[{"name":"screen_view","filters":[]}]}`)}
}

func TestCreateChecksFunnelRulesWithTheSessionsEvents(t *testing.T) {
	rig := newEndRig()
	rig.rules.rules = []notify.Rule{funnelRule(t)}
	rig.ch.rows = []string{`{"id":"e1","name":"purchase","device_id":"d1","profile_id":"d1","project_id":"p1","session_id":"s1","properties":{"a":"1"},"created_at":"2026-06-08 10:45:00.000","country":"SE\u0000\u0000","longitude":null,"imported_at":null,"groups":[],"revenue":"0"}`}
	if _, err := rig.ender.Create(context.Background(), rig.job(t, blobEnded, jobEventBase)); err != nil {
		t.Fatal(err)
	}
	if len(rig.funnel.calls) != 1 || rig.funnel.called != "req" {
		t.Fatalf("calls = %d, request id %q", len(rig.funnel.calls), rig.funnel.called)
	}
	events := rig.funnel.calls[0]
	if len(events) != 3 {
		t.Fatalf("events = %d", len(events))
	}
	names := []string{}
	for _, e := range events {
		names = append(names, jsjson.JSString(e.Value.Get("name")))
	}
	if strings.Join(names, ",") != "screen_view,purchase,session_end" {
		t.Errorf("order = %v", names)
	}
	queried := toJSON((events[1].Value))
	if !strings.Contains(queried, `"country":"SE"`) || !strings.Contains(queried, `"createdAt":"2026-06-08T10:45:00.000Z"`) {
		t.Errorf("queried event = %s", queried)
	}
	params := rig.ch.params[0]
	if params[2].Value != "'2026-06-08 10:29:59'" && params[2].Value != "2026-06-08 10:29:59" {
		t.Errorf("start param = %q", params[2].Value)
	}
	if !strings.Contains(params[3].Value, "2026-06-08 11:00:01") || params[4].Value != "500" {
		t.Errorf("params = %+v", params)
	}
}

func TestCreateSkipsFunnelChecksForLongSessionsAndNoRules(t *testing.T) {
	rig := newEndRig()
	if _, err := rig.ender.Create(context.Background(), rig.job(t, blobEnded, jobEventBase)); err != nil {
		t.Fatal(err)
	}
	long := newEndRig()
	long.rules.rules = []notify.Rule{funnelRule(t)}
	big := strings.Replace(blobEnded, `"event_count":3`, `"event_count":495`, 1)
	if _, err := long.ender.Create(context.Background(), long.job(t, big, jobEventBase)); err != nil {
		t.Fatal(err)
	}
	if len(rig.funnel.calls)+len(long.funnel.calls)+len(rig.ch.statements)+len(long.ch.statements) != 0 {
		t.Error("no events query or funnel check expected")
	}
}

func TestCreateFunnelFailureDoesNotStopCleanup(t *testing.T) {
	rig := newEndRig()
	rig.redis.putSession("p1", "d1", blobEnded, 1)
	rig.rules.rules = []notify.Rule{funnelRule(t)}
	rig.funnel.err = errBoom
	if doc, err := rig.ender.Create(context.Background(), rig.job(t, blobEnded, jobEventBase)); err != nil || doc == nil {
		t.Fatalf("doc = %v, err = %v", doc, err)
	}
	if _, ok := rig.redis.strings[sessionKey("p1", "d1")]; ok {
		t.Error("cleanup must still run")
	}
}

func TestCreateRedisFailureBeforeTheClaimFailsTheJob(t *testing.T) {
	rig := newEndRig()
	rig.redis.fail["setnx"] = errBoom
	if _, err := rig.ender.Create(context.Background(), rig.job(t, blobEnded, jobEventBase)); err == nil {
		t.Fatal("expected the claim failure to fail the attempt")
	}
	if len(rig.events.created) != 0 {
		t.Error("nothing may be written without the claim")
	}
}

func TestDecodeJob(t *testing.T) {
	ids := wire.RandomIDs{}
	body := `{"event":` + jobEventBase + `,"snapshot":` + blobEnded + `}`
	job, err := DecodeJob("session", []byte(`{"payload":`+body+`,"meta":{"requestId":"abc"}}`), ids)
	if err != nil || job.RequestID != "abc" {
		t.Fatalf("job = %+v, err = %v", job, err)
	}
	job, err = DecodeJob("session", []byte(`{"payload":`+body+`,"meta":{}}`), ids)
	if err != nil || len(job.RequestID) != wire.RequestIDLength {
		t.Fatalf("a job without a request id gets one: %+v, %v", job, err)
	}
	legacy := `{"type":"createSessionEnd","payload":` + jobEventBase + `,"snapshot":` + blobEnded + `}`
	if _, err := DecodeJob("session", []byte(legacy), ids); err != nil {
		t.Fatalf("legacy shape: %v", err)
	}
	for name, data := range map[string]string{
		"unknown job":    `{"payload":` + body + `,"meta":{}}`,
		"not resolvable": `{"foo":1}`,
		"bad snapshot":   `{"payload":{"event":` + jobEventBase + `,"snapshot":{"id":"s1"}},"meta":{}}`,
		"bad event":      `{"payload":{"event":{"projectId":1,"deviceId":"d"},"snapshot":` + blobEnded + `},"meta":{}}`,
	} {
		jobName := "session"
		if name == "unknown job" {
			jobName = "other"
		}
		if _, err := DecodeJob(jobName, []byte(data), ids); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
