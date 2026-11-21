package events

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"openpanel/ingest/internal/worker/consumer"
	"openpanel/ingest/internal/worker/wire"
)

const (
	hProject     = "test-project"
	hDevice      = "device-123"
	hSession     = "a1b2c3d4-e5f6-4789-a012-345678901234"
	hProducedID  = "11111111-2222-4333-8444-555555555555"
	hEventTime   = "2026-01-01T00:00:00.000Z"
	browserUA    = `{"isServer":false,"device":"desktop","os":"Windows","osVersion":"10","browser":"Chrome","browserVersion":"91.0.4472.124","brand":"","model":""}`
	serverUA     = `{"isServer":true,"device":"server","os":"","osVersion":"","browser":"","browserVersion":"","brand":"","model":""}`
	hGeo         = `{"country":"US","city":"New York","region":"NY","longitude":0,"latitude":0}`
	hHeaders     = `{"request-id":"123","user-agent":"Mozilla/5.0","openpanel-sdk-name":"web","openpanel-sdk-version":"1.0.0"}`
	uuidV4Format = `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
)

// record builds the topic value; each argument replaces one part of the
// default browser event.
type record struct {
	id, event, ua, headers, extra string
	timestamp                     string
	deviceID                      string
}

func (r record) json() string {
	if r.event == "" {
		r.event = `"name":"test_event","properties":{"__path":"https://example.com/test"}`
	}
	if r.ua == "" {
		r.ua = browserUA
	}
	if r.headers == "" {
		r.headers = hHeaders
	}
	if r.timestamp == "" {
		r.timestamp = `"` + hEventTime + `"`
	}
	if r.deviceID == "" {
		r.deviceID = hDevice
	}
	id := ""
	if r.id != "" {
		id = `"id":"` + r.id + `",`
	}
	return `{` + id + `"projectId":"` + hProject + `","event":{` + r.event + `,"timestamp":` + r.timestamp +
		`,"isTimestampFromThePast":false},"uaInfo":` + r.ua + `,"geo":` + hGeo + `,"headers":` + r.headers +
		`,"deviceId":"` + r.deviceID + `","sessionId":"` + hSession + `"` + r.extra + `}`
}

func (r *rig) handle(t *testing.T, text string) error {
	t.Helper()
	decoded, err := wire.Decode([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ShapeErr != nil {
		return decoded.ShapeErr
	}
	err = r.handler.Handle(context.Background(), decoded.Payload, consumer.Delivery{})
	r.flush(t)
	return err
}

func (r *rig) rows(t *testing.T, key string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range r.redis.list(key) {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	return out
}

func names(rows []map[string]any) []string {
	var out []string
	for _, row := range rows {
		out = append(out, row["name"].(string))
	}
	return out
}

func TestNewSessionEmitsSessionStartBeforeTheEvent(t *testing.T) {
	r := newRig(t)
	if err := r.handle(t, record{id: hProducedID}.json()); err != nil {
		t.Fatal(err)
	}
	rows := r.rows(t, EventQueueKey)
	if got := names(rows); len(got) != 2 || got[0] != "session_start" || got[1] != "test_event" {
		t.Fatalf("rows = %v", got)
	}
	if rows[0]["created_at"] != "2025-12-31 23:59:59.900" || rows[1]["created_at"] != "2026-01-01 00:00:00.000" {
		t.Fatalf("created_at = %v, %v: session_start is 100 ms earlier", rows[0]["created_at"], rows[1]["created_at"])
	}
	if rows[1]["id"] != hProducedID || rows[0]["id"] == hProducedID {
		t.Fatalf("ids = %v, %v: session_start must not share the producer id", rows[0]["id"], rows[1]["id"])
	}
	if len(r.metrics.started) != 1 || r.metrics.started[0] != "new" {
		t.Fatalf("started = %v", r.metrics.started)
	}
}

func TestExtendEmitsNoSessionStart(t *testing.T) {
	r := newRig(t)
	for i := range 3 {
		ts := []string{`"2026-01-01T00:00:00.000Z"`, `"2026-01-01T00:00:01.000Z"`, `"2026-01-01T00:00:02.000Z"`}[i]
		if err := r.handle(t, record{timestamp: ts}.json()); err != nil {
			t.Fatal(err)
		}
	}
	got := names(r.rows(t, EventQueueKey))
	starts := 0
	for _, name := range got {
		if name == "session_start" {
			starts++
		}
	}
	if starts != 1 || len(got) != 4 {
		t.Fatalf("rows = %v: want one session_start over three rapid events", got)
	}
}

func TestBoundaryClosesTheOldSessionAndStartsANewOne(t *testing.T) {
	r := newRig(t)
	if err := r.handle(t, record{}.json()); err != nil {
		t.Fatal(err)
	}
	later := record{timestamp: `"2026-01-01T01:00:00.000Z"`, extra: ""}
	if err := r.handle(t, strings.Replace(later.json(), hSession, "session-new", 1)); err != nil {
		t.Fatal(err)
	}
	if len(r.queue.jobs) != 1 {
		t.Fatalf("%d session-end jobs, want 1", len(r.queue.jobs))
	}
	job := r.queue.jobs[0]
	if job.queue != "sessions" || job.name != "session" || job.opts.JobID != "sessionEnd:v2:"+hSession ||
		job.opts.Attempts != 3 || !job.opts.AttemptsSet || job.opts.Backoff.Type != "exponential" || job.opts.Backoff.DelayMs != 200 {
		t.Fatalf("job = %+v", job)
	}
	var data struct {
		Payload struct {
			Event    map[string]any `json:"event"`
			Snapshot map[string]any `json:"snapshot"`
		} `json:"payload"`
		Meta map[string]any `json:"meta"`
	}
	if err := json.Unmarshal([]byte(job.data), &data); err != nil {
		t.Fatal(err)
	}
	if data.Payload.Snapshot["id"] != hSession || data.Payload.Event["sessionId"] != hSession ||
		data.Payload.Event["createdAt"] != "2026-01-01T01:00:00.000Z" || data.Meta["requestId"] != "123" {
		t.Fatalf("data = %s", job.data)
	}
	if got := r.metrics.started; len(got) != 2 || got[1] != "boundary" {
		t.Fatalf("started = %v", got)
	}
	if len(r.metrics.ended) != 1 || r.metrics.ended[0] != "boundary" {
		t.Fatalf("ended = %v", r.metrics.ended)
	}
}

func TestAFailedSessionEndEnqueueDoesNotFailTheMessage(t *testing.T) {
	r := newRig(t)
	if err := r.handle(t, record{}.json()); err != nil {
		t.Fatal(err)
	}
	r.queue.err = errors.New("queue down")
	if err := r.handle(t, record{timestamp: `"2026-01-01T01:00:00.000Z"`}.json()); err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(r.metrics.ended) != 0 {
		t.Fatal("a failed enqueue was counted")
	}
}

func TestEventInheritsTheSessionsReferrerAndUTM(t *testing.T) {
	r := newRig(t)
	landing := record{event: `"name":"screen_view","properties":{"__path":"https://example.com/?utm_source=newsletter&utm_medium=email","__referrer":"https://www.google.com/"}`}
	if err := r.handle(t, landing.json()); err != nil {
		t.Fatal(err)
	}
	next := record{event: `"name":"screen_view","properties":{"__path":"https://example.com/docs"}`, timestamp: `"2026-01-01T00:01:00.000Z"`}
	if err := r.handle(t, next.json()); err != nil {
		t.Fatal(err)
	}
	rows := r.rows(t, EventQueueKey)
	last := rows[len(rows)-1]
	properties := last["properties"].(map[string]any)
	if last["referrer"] != "https://www.google.com" || last["referrer_name"] != "newsletter" ||
		properties["__query.utm_source"] != "newsletter" || properties["__query.utm_medium"] != "email" {
		t.Fatalf("row = %v", last)
	}
}

func TestOwnUTMWinsOverTheSessions(t *testing.T) {
	r := newRig(t)
	first := record{event: `"name":"screen_view","properties":{"__path":"https://example.com/?utm_source=newsletter"}`}
	_ = r.handle(t, first.json())
	own := record{event: `"name":"screen_view","properties":{"__path":"https://example.com/x?utm_source=twitter"}`, timestamp: `"2026-01-01T00:01:00.000Z"`}
	_ = r.handle(t, own.json())
	rows := r.rows(t, EventQueueKey)
	if got := rows[len(rows)-1]["properties"].(map[string]any)["__query.utm_source"]; got != "twitter" {
		t.Fatalf("utm_source = %v", got)
	}
}

func TestClientQueryIsKeptAndTheURLQueryWinsOverIt(t *testing.T) {
	r := newRig(t)
	rec := record{event: `"name":"screen_view","properties":{"__path":"https://example.com/?utm_source=url","__query":{"utm_source":"client","utm_medium":"client-medium"}}`}
	if err := r.handle(t, rec.json()); err != nil {
		t.Fatal(err)
	}
	rows := r.rows(t, EventQueueKey)
	properties := rows[len(rows)-1]["properties"].(map[string]any)
	if properties["__query.utm_source"] != "url" || properties["__query.utm_medium"] != "client-medium" {
		t.Fatalf("properties = %v", properties)
	}
}

func TestServerEventRidesTheProfilesSession(t *testing.T) {
	r := newRig(t)
	client := record{event: `"name":"screen_view","profileId":"user-1","properties":{"__path":"https://example.com/?utm_source=ads"}`}
	if err := r.handle(t, client.json()); err != nil {
		t.Fatal(err)
	}
	before := len(r.redis.list(SessionBufferKey))
	server := record{id: hProducedID, ua: serverUA, deviceID: "", event: `"name":"renewed","profileId":"user-1"`}
	if err := r.handle(t, server.json()); err != nil {
		t.Fatal(err)
	}
	rows := r.rows(t, EventQueueKey)
	last := rows[len(rows)-1]
	if last["device_id"] != hDevice || last["session_id"] != hSession || last["id"] != hProducedID ||
		last["properties"].(map[string]any)["__query.utm_source"] != "ads" {
		t.Fatalf("row = %v", last)
	}
	if len(r.redis.list(SessionBufferKey)) != before {
		t.Fatal("a server event wrote session rows")
	}
	for _, name := range names(rows[len(rows)-1:]) {
		if name == "session_start" {
			t.Fatal("a server event opened a session")
		}
	}
}

func TestServerEventWithoutASessionTakesItsOwnIDsAndLosesItsReferrer(t *testing.T) {
	r := newRig(t)
	server := record{ua: serverUA, deviceID: "dev-given", event: `"name":"cron","profileId":"nobody","properties":{"__path":"/x","__referrer":"https://www.google.com/"}`}
	if err := r.handle(t, server.json()); err != nil {
		t.Fatal(err)
	}
	rows := r.rows(t, EventQueueKey)
	if len(rows) != 1 || rows[0]["device_id"] != "dev-given" || rows[0]["session_id"] != hSession || rows[0]["referrer"] != "" {
		t.Fatalf("rows = %v", rows)
	}
	if len(r.redis.list(SessionBufferKey)) != 0 {
		t.Fatal("session rows written")
	}
	for _, command := range r.redis.commands {
		if strings.HasPrefix(command, "GET session:") && !strings.HasPrefix(command, "GET session:profile:") {
			t.Fatalf("read a device session: %s", command)
		}
	}
}

func TestServerEventWithoutAProfileReadsNoSession(t *testing.T) {
	r := newRig(t)
	if err := r.handle(t, record{ua: serverUA, event: `"name":"cron"`}.json()); err != nil {
		t.Fatal(err)
	}
	if len(r.redis.commands) != 1 || !strings.HasPrefix(r.redis.commands[0], "RPUSH") {
		t.Fatalf("commands = %v", r.redis.commands)
	}
}

func TestRedeliveryRecreatesTheSameRowID(t *testing.T) {
	r := newRig(t)
	rec := record{id: hProducedID}.json()
	_ = r.handle(t, rec)
	_ = r.handle(t, rec)
	var ids []string
	for _, row := range r.rows(t, EventQueueKey) {
		if row["name"] == "test_event" {
			ids = append(ids, row["id"].(string))
		}
	}
	if len(ids) != 2 || ids[0] != hProducedID || ids[1] != hProducedID {
		t.Fatalf("ids = %v", ids)
	}
}

func TestRecordWithoutAnIDGetsAFreshUUIDEachTime(t *testing.T) {
	r := newRig(t)
	rec := record{}.json()
	_ = r.handle(t, rec)
	_ = r.handle(t, rec)
	format := regexp.MustCompile(uuidV4Format)
	seen := map[string]bool{}
	for _, row := range r.rows(t, EventQueueKey) {
		id := row["id"].(string)
		if !format.MatchString(id) || seen[id] {
			t.Fatalf("id %q is not a fresh v4 uuid", id)
		}
		seen[id] = true
	}
}

func TestAIEventWritesItsRowAfterTheEventWithTheResolvedIdentity(t *testing.T) {
	r := newRig(t)
	client := record{event: `"name":"screen_view","profileId":"user-1"`}
	_ = r.handle(t, client.json())
	ai := record{id: hProducedID, ua: browserUA, deviceID: "", event: `"name":"ai.generation","profileId":"user-1"`,
		extra: `,"ai":{"type":"generation","span_id":"","input":"hello","output":"world","tools":"[]","model":"m"}`}
	if err := r.handle(t, ai.json()); err != nil {
		t.Fatal(err)
	}
	rows := r.rows(t, AIEventBufferKey)
	if len(rows) != 1 {
		t.Fatalf("%d ai rows", len(rows))
	}
	row := rows[0]
	if row["device_id"] != hDevice || row["session_id"] != hSession || row["id"] != hProducedID || row["span_id"] != hProducedID ||
		row["profile_id"] != "user-1" || row["input"] != "hello" || row["project_id"] != hProject {
		t.Fatalf("ai row = %v", row)
	}
	// An AI event is server side even with a browser user-agent: no session_start.
	for _, row := range r.rows(t, EventQueueKey) {
		if row["name"] == "ai.generation" && row["session_id"] != hSession {
			t.Fatalf("ai event row = %v", row)
		}
	}
}

func TestPrivateProjectBlanksTheAIContent(t *testing.T) {
	r := newRig(t)
	r.projects.aiStoreContent[hProject] = false
	ai := record{ua: serverUA, deviceID: "", event: `"name":"ai.generation"`,
		extra: `,"ai":{"input":"hello","output":"world","tools":"[1]","span_id":"s1"}`}
	if err := r.handle(t, ai.json()); err != nil {
		t.Fatal(err)
	}
	row := r.rows(t, AIEventBufferKey)[0]
	if row["input"] != "" || row["output"] != "" || row["tools"] != "" || row["span_id"] != "s1" {
		t.Fatalf("ai row = %v", row)
	}
}

func TestAnonymousAIEventOpensNoSessionAndKeepsNoDevice(t *testing.T) {
	r := newRig(t)
	ai := record{deviceID: "x", event: `"name":"ai.generation"`, extra: `,"ai":{"model":"m"}`}
	text := strings.Replace(ai.json(), `"deviceId":"x"`, `"deviceId":""`, 1)
	if err := r.handle(t, text); err != nil {
		t.Fatal(err)
	}
	rows := r.rows(t, EventQueueKey)
	if len(rows) != 1 || rows[0]["name"] != "ai.generation" || rows[0]["device_id"] != "" {
		t.Fatalf("rows = %v", rows)
	}
	if len(r.redis.list(SessionBufferKey)) != 0 {
		t.Fatal("session rows written")
	}
}

func TestExcludedEventLeavesNoRowNoSessionAndNoAIRow(t *testing.T) {
	r := newRig(t)
	r.projects.excludedByName[hProject] = "secret"
	if err := r.handle(t, record{event: `"name":"secret"`}.json()); err != nil {
		t.Fatal(err)
	}
	if len(r.redis.commands) != 0 || len(r.redis.lists) != 0 {
		t.Fatalf("an excluded event wrote: %v", r.redis.commands)
	}
	ai := record{ua: serverUA, deviceID: "", event: `"name":"secret"`, extra: `,"ai":{"model":"m"}`}
	if err := r.handle(t, ai.json()); err != nil {
		t.Fatal(err)
	}
	if len(r.redis.list(AIEventBufferKey)) != 0 {
		t.Fatal("an excluded event left an ai row")
	}
}

func TestRecordWithoutHeadersFailsTheMessage(t *testing.T) {
	r := newRig(t)
	text := strings.Replace(record{}.json(), `"headers":`+hHeaders+`,`, "", 1)
	if err := r.handle(t, text); err == nil {
		t.Fatal("a record without headers must fail like the TypeScript handler")
	}
}

func TestFilterFailurePropagatesButNotificationFailureDoesNot(t *testing.T) {
	r := newRig(t)
	r.projects.failFilter = errors.New("TypeError")
	if err := r.handle(t, record{}.json()); err == nil {
		t.Fatal("a failing project filter must fail the message")
	}
	r2 := newRig(t)
	r2.notifier.err = errors.New("rules failed")
	if err := r2.handle(t, record{}.json()); err != nil {
		t.Fatalf("a failing notification check failed the event: %v", err)
	}
	if len(r2.redis.list(EventQueueKey)) != 2 {
		t.Fatal("the event rows were not written")
	}
}

func TestAnonymousVisitorsProfileIsTheDeviceAndUpsertsOncePerSession(t *testing.T) {
	r := newRig(t)
	_ = r.handle(t, record{}.json())
	profiles := r.rows(t, ProfileBufferKey)
	if len(profiles) != 1 || profiles[0]["id"] != hDevice || profiles[0]["is_external"] != false {
		t.Fatalf("profiles = %v", profiles)
	}
	// A cached profile is skipped.
	r2 := newRig(t)
	r2.redis.existsCache["profile-cache:"+hProject+":"+hDevice] = true
	_ = r2.handle(t, record{}.json())
	if len(r2.redis.list(ProfileBufferKey)) != 0 || len(r2.metrics.skipped) != 1 || r2.metrics.skipped[0] != "profile/cached" {
		t.Fatalf("profile rows %v skipped %v", r2.redis.list(ProfileBufferKey), r2.metrics.skipped)
	}
}

func TestRevenueEventCarriesItsRevenueIntoTheSessionStartToo(t *testing.T) {
	r := newRig(t)
	rec := record{event: `"name":"revenue","properties":{"__path":"/pay","__revenue":"12.5"}`}
	if err := r.handle(t, rec.json()); err != nil {
		t.Fatal(err)
	}
	for _, row := range r.rows(t, EventQueueKey) {
		if row["revenue"] != 12.5 {
			t.Fatalf("row %v has no revenue", row["name"])
		}
	}
}

// A date string outside the ISO 8601 forms jsport reads ("Jan 5 2026", an RFC
// 2822 date) is a number in Bun's engine and an error here: the record is
// retried and dead-lettered instead of written. Pinned so the divergence is a
// decision, not an accident (docs/GO_WORKER_CONTRACTS.md P1.17 #7, owner decision).
func TestUnsupportedTimestampFormFailsTheMessage(t *testing.T) {
	r := newRig(t)
	for _, form := range []string{`"Jan 5 2026"`, `"Mon, 02 Jan 2006 15:04:05 GMT"`, `"2026-13-45T00:00:00Z"`} {
		if err := r.handle(t, record{timestamp: form}.json()); err == nil {
			t.Fatalf("timestamp %s was accepted", form)
		}
	}
	if len(r.redis.commands) != 0 {
		t.Fatalf("a failed message wrote: %v", r.redis.commands)
	}
}

func TestInvalidTimestampWritesInvalidDateTime(t *testing.T) {
	r := newRig(t)
	rec := record{timestamp: `8640000000000001`}
	if err := r.handle(t, rec.json()); err != nil {
		t.Fatal(err)
	}
	for _, row := range r.rows(t, EventQueueKey) {
		if row["created_at"] != "Invalid DateTime" {
			t.Fatalf("created_at = %v", row["created_at"])
		}
	}
}
