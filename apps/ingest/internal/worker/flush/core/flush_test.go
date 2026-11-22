package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/flush"
	"openpanel/ingest/internal/worker/jsport"
)

// lossClock ticks every few milliseconds, so a lock taken away mid-flush is
// noticed by the keepalive before the batch is trimmed.
type lossClock struct{}

func (lossClock) Now() time.Time { return time.Now() }
func (lossClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (lossClock) Tick(time.Duration) (<-chan time.Time, func()) {
	ticker := time.NewTicker(2 * time.Millisecond)
	return ticker.C, ticker.Stop
}

func (r *rig) flushWithClock(t *testing.T, f flush.Flusher, clk lossClock) flush.Observation {
	t.Helper()
	runner, err := flush.NewRunner(f, flush.Deps{Redis: r.redis, Clock: clk, Observer: r.obs, Paused: flush.NeverPaused})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.TryFlush(context.Background(), flush.TriggerCron); err != nil {
		t.Fatal(err)
	}
	return r.obs.last()
}

func TestExtractProjectID(t *testing.T) {
	cases := []struct {
		name string
		line string
		want string
		ok   bool
	}{
		{"top level", `{"id":"evt1","name":"foo","project_id":"real-project"}`, "real-project", true},
		{"absent", `{"id":"evt1","name":"foo"}`, "", false},
		{"malformed", "not json at all", "", false},
		{"empty line", "", "", false},
		{"empty value", `{"project_id":""}`, "", false},
		{"nested after", `{"id":"evt1","project_id":"real-project","properties":{"project_id":"user-supplied"}}`, "real-project", true},
		{"nested before", `{"properties":{"project_id":"user-supplied"},"project_id":"real-project"}`, "real-project", true},
		{"escaped quotes in a value", "{\"id\":\"evt1\",\"project_id\":\"real-project\",\"properties\":{\"note\":\"has \\\"project_id\\\":\\\"fake\\\" inside\"}}", "real-project", true},
		{"deeply nested", `{"project_id":"real-project","properties":{"nested":{"project_id":"deep-fake"}}}`, "real-project", true},
		{"two hits and not valid JSON", `{"project_id":"a","project_id":"b"`, "", false},
		{"two hits, the last top-level value is not a string", `{"project_id":"a","p":{"project_id":"x"},"project_id":2}`, "", false},
		{"raw escapes are returned as written", `{"project_id":"a\u00e9"}`, `a\u00e9`, true},
		{"unterminated value", `{"project_id":"abc`, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := ExtractProjectID(c.line)
			if got != c.want || ok != c.ok {
				t.Fatalf("ExtractProjectID(%q) = %q, %v; want %q, %v", c.line, got, ok, c.want, c.ok)
			}
		})
	}
}

func eventLine(i int, project string) string {
	return fmt.Sprintf(`{"id":"e%d","name":"event%d","project_id":"%s"}`, i, i, project)
}

func TestEventFlushInsertsTheLinesUntouchedAndPublishesTheCounts(t *testing.T) {
	r := newRig()
	lines := []string{eventLine(1, "p7"), eventLine(2, "p7"), eventLine(3, "p8")}
	r.redis.Push(EventKey, lines...)

	observation := r.flush(t, NewEvent(buffersConfig(t, nil), r.deps, Overrides{}))

	if observation.Result != flush.ResultSuccess || observation.Rows != 3 {
		t.Fatalf("observation %+v", observation)
	}
	if got := r.ch.insertedLines(); strings.Join(got, "\n") != strings.Join(lines, "\n") {
		t.Fatalf("inserted %q", got)
	}
	if r.ch.inserts[0].Table != "events" || r.ch.inserts[0].Settings != nil {
		t.Fatalf("insert %+v", r.ch.inserts[0])
	}
	if len(r.redis.List(EventKey)) != 0 {
		t.Fatal("rows left in Redis")
	}
	var messages []string
	for _, p := range r.redis.publications() {
		messages = append(messages, p.Channel+" "+p.Message)
	}
	want := []string{
		`events:batch {"json":{"projectId":"p7","count":2}}`,
		`events:batch {"json":{"projectId":"p8","count":1}}`,
	}
	if strings.Join(messages, "|") != strings.Join(want, "|") {
		t.Fatalf("published %q", messages)
	}
	phases := observation.Phases
	if !phases.LRange.Set || !phases.CHInsert.Set || !phases.Trim.Set || phases.Transform.Set || phases.CHFetch.Set {
		t.Fatalf("phases %+v", phases)
	}
}

func TestEventFlushChunksAndAsyncSettings(t *testing.T) {
	r := newRig()
	for i := range 4 {
		r.redis.Push(EventKey, eventLine(i, "p8"))
	}
	cfg := buffersConfig(t, map[string]string{"EVENT_BUFFER_CHUNK_SIZE": "2", "BUFFER_ASYNC_INSERTS": "false"})
	r.flush(t, NewEvent(cfg, r.deps, Overrides{}))
	if len(r.ch.inserts) != 2 || len(r.ch.inserts[0].Lines) != 2 || len(r.ch.inserts[1].Lines) != 2 {
		t.Fatalf("inserts %+v", r.ch.inserts)
	}
	// BUFFER_ASYNC_INSERTS is truthy for any value, "false" included.
	want := chclient.AsyncInsertSettings(true)
	for _, call := range r.ch.inserts {
		if fmt.Sprint(call.Settings) != fmt.Sprint(want) {
			t.Fatalf("settings %v", call.Settings)
		}
	}
}

func TestEventFlushDrainsABacklogLargerThanOneBatch(t *testing.T) {
	r := newRig()
	for i := range 10 {
		r.redis.Push(EventKey, eventLine(i, "p13"))
	}
	cfg := buffersConfig(t, map[string]string{"EVENT_BUFFER_BATCH_SIZE": "3"})
	observation := r.flush(t, NewEvent(cfg, r.deps, Overrides{}))
	if len(r.ch.inserts) != 4 || observation.Rows != 10 || len(r.redis.List(EventKey)) != 0 {
		t.Fatalf("%d inserts, %d rows, %d left", len(r.ch.inserts), observation.Rows, len(r.redis.List(EventKey)))
	}
	for i, line := range r.ch.insertedLines() {
		if line != eventLine(i, "p13") {
			t.Fatalf("line %d is %s", i, line)
		}
	}
}

func TestEventFlushAFailedBatchStopsTheDrainAndKeepsTheRest(t *testing.T) {
	r := newRig()
	for i := range 10 {
		r.redis.Push(EventKey, eventLine(i, "p14"))
	}
	r.ch.failInsertAt = 2
	cfg := buffersConfig(t, map[string]string{"EVENT_BUFFER_BATCH_SIZE": "3"})
	observation := r.flush(t, NewEvent(cfg, r.deps, Overrides{}))
	if observation.Result != flush.ResultError || !errors.Is(observation.Err, errClickHouseDown) {
		t.Fatalf("observation %+v", observation)
	}
	if left := len(r.redis.List(EventKey)); left != 7 {
		t.Fatalf("%d rows left, want 7", left)
	}
	if got := len(r.redis.publications()); got != 1 {
		t.Fatalf("%d messages: only the batch that landed publishes", got)
	}
}

func TestEventFlushAPublishFailureNeverFailsTheFlush(t *testing.T) {
	r := newRig()
	r.redis.Push(EventKey, eventLine(1, "p1"), eventLine(2, "p2"))
	r.redis.publishErr = errors.New("connection reset")
	observation := r.flush(t, NewEvent(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || len(r.redis.List(EventKey)) != 0 {
		t.Fatalf("observation %+v", observation)
	}
	if got := r.redis.publications(); len(got) != 1 {
		t.Fatalf("the second project still publishes: %v", got)
	}
}

func TestEventFlushInsertConcurrencyIsBounded(t *testing.T) {
	r := newRig()
	for i := range 40 {
		r.redis.Push(EventKey, eventLine(i, "p1"))
	}
	r.ch.insertDelay = 5 * time.Millisecond
	cfg := buffersConfig(t, map[string]string{"EVENT_BUFFER_BATCH_SIZE": "40", "EVENT_BUFFER_CHUNK_SIZE": "2", "BUFFER_CH_INSERT_CONCURRENCY": "3"})
	r.flush(t, NewEvent(cfg, r.deps, Overrides{}))
	if got := r.ch.maxInFlight.Load(); got != 3 {
		t.Fatalf("%d inserts in flight at most, want 3", got)
	}
}

func sessionRow(id string, sign, version int) string {
	return fmt.Sprintf(`{"id":"%s","project_id":"p","sign":%d,"version":%d,"duration":0}`, id, sign, version)
}

func TestSessionSquashDoesNotOrphanTheCreationRow(t *testing.T) {
	r := newRig()
	r.redis.Push(SessionKey, sessionRow("s", 1, 1))
	for version := 2; version <= 8; version++ {
		r.redis.Push(SessionKey, sessionRow("s", -1, version-1), sessionRow("s", 1, version))
	}
	r.flush(t, NewSession(buffersConfig(t, nil), r.deps, Overrides{}))
	if got := r.ch.insertedLines(); len(got) != 1 || got[0] != sessionRow("s", 1, 8) {
		t.Fatalf("inserted %q, want only the final +1 row", got)
	}
	if len(r.redis.List(SessionKey)) != 0 {
		t.Fatal("rows left in Redis")
	}
}

func TestSessionFlushInsertsAWholeBatchInOneInsertByDefault(t *testing.T) {
	r := newRig()
	for i := range 1500 {
		r.redis.Push(SessionKey, sessionRow(fmt.Sprintf("session-%d", i), 1, 1))
	}
	r.flush(t, NewSession(buffersConfig(t, nil), r.deps, Overrides{}))
	if len(r.ch.inserts) != 1 || len(r.ch.inserts[0].Lines) != 1500 {
		t.Fatalf("%d inserts", len(r.ch.inserts))
	}
}

func TestSessionFlushNeverUsesTheAsyncSettings(t *testing.T) {
	r := newRig()
	r.redis.Push(SessionKey, sessionRow("a", 1, 1))
	r.flush(t, NewSession(buffersConfig(t, map[string]string{"BUFFER_ASYNC_INSERTS": "1"}), r.deps, Overrides{}))
	if len(r.ch.inserts) != 1 || len(r.ch.inserts[0].Settings) != 0 {
		t.Fatalf("inserts %+v", r.ch.inserts)
	}
}

func pushSessions(r *rig, n int) {
	for i := range n {
		r.redis.Push(SessionKey, sessionRow(fmt.Sprintf("session-%d", i), 1, 1))
	}
}

func TestSessionFlushInsertsTheChunksOfOneBatchConcurrently(t *testing.T) {
	r := newRig()
	pushSessions(r, 10)
	r.ch.insertDelay = 10 * time.Millisecond
	cfg := buffersConfig(t, map[string]string{"SESSION_BUFFER_CHUNK_SIZE": "2"})
	r.flush(t, NewSession(cfg, r.deps, Overrides{}))
	if len(r.ch.inserts) != 5 || r.ch.maxInFlight.Load() < 2 || len(r.redis.List(SessionKey)) != 0 {
		t.Fatalf("%d inserts, %d in flight at most, %d left", len(r.ch.inserts), r.ch.maxInFlight.Load(), len(r.redis.List(SessionKey)))
	}
}

func TestSessionFlushKeepsTheWholeBatchWhenOneChunkFails(t *testing.T) {
	r := newRig()
	pushSessions(r, 10)
	r.ch.insertDelay = 10 * time.Millisecond
	r.ch.failInsertAt = 3
	cfg := buffersConfig(t, map[string]string{"SESSION_BUFFER_CHUNK_SIZE": "2"})
	observation := r.flush(t, NewSession(cfg, r.deps, Overrides{}))
	if observation.Result != flush.ResultError || len(r.redis.List(SessionKey)) != 10 {
		t.Fatalf("observation %+v, %d left", observation, len(r.redis.List(SessionKey)))
	}
}

func TestSessionFlushAfterAFailedChunkStartsNoNewChunk(t *testing.T) {
	r := newRig()
	pushSessions(r, 20)
	var started int
	var mutex sync.Mutex
	r.ch.failInsertAt = 1
	r.ch.onInsert = func() {
		mutex.Lock()
		started++
		mutex.Unlock()
	}
	r.ch.insertDelay = 20 * time.Millisecond
	cfg := buffersConfig(t, map[string]string{"SESSION_BUFFER_CHUNK_SIZE": "2"})
	r.flush(t, NewSession(cfg, r.deps, Overrides{}))
	mutex.Lock()
	defer mutex.Unlock()
	finished := len(r.ch.inserts)
	// Ten chunks, five at a time: the first wave runs, nothing after it, and the
	// flush returns only once the running ones settled.
	if started != 5 || finished != 4 || len(r.redis.List(SessionKey)) != 20 {
		t.Fatalf("started %d, finished %d, %d left", started, finished, len(r.redis.List(SessionKey)))
	}
}

func TestSessionFlushReportsEveryPhase(t *testing.T) {
	r := newRig()
	pushSessions(r, 1)
	observation := r.flush(t, NewSession(buffersConfig(t, nil), r.deps, Overrides{}))
	phases := observation.Phases
	if observation.Result != flush.ResultSuccess || observation.Rows != 1 ||
		!phases.LRange.Set || !phases.Transform.Set || !phases.CHInsert.Set || !phases.Trim.Set {
		t.Fatalf("observation %+v", observation)
	}
}

func TestSessionFlushDrainsABacklogLargerThanOneBatch(t *testing.T) {
	r := newRig()
	pushSessions(r, 10)
	cfg := buffersConfig(t, map[string]string{"SESSION_BUFFER_BATCH_SIZE": "3"})
	r.flush(t, NewSession(cfg, r.deps, Overrides{}))
	if len(r.ch.inserts) != 4 || len(r.redis.List(SessionKey)) != 0 {
		t.Fatalf("%d inserts, %d left", len(r.ch.inserts), len(r.redis.List(SessionKey)))
	}
}

func TestSessionRowsThatAreNotObjectsAreSkippedAndTrimmed(t *testing.T) {
	// The TypeScript flush throws on a truthy non-object and the batch blocks
	// the list for good.
	r := newRig()
	r.redis.Push(SessionKey, "5", "[]", `"s"`, "null", "{", sessionRow("ok", 1, 1))
	observation := r.flush(t, NewSession(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || observation.Rows != 6 || len(r.ch.insertedLines()) != 1 || len(r.redis.List(SessionKey)) != 0 {
		t.Fatalf("observation %+v, inserted %q", observation, r.ch.insertedLines())
	}
}

func TestSquashSessionsKeepsAFirstAppearanceOrderAndCopies(t *testing.T) {
	parse := func(text string) *jsjson.Obj {
		row, ok := parseObject(text)
		if !ok {
			t.Fatal(text)
		}
		return row
	}
	rows := []*jsjson.Obj{
		parse(sessionRow("b", 1, 1)), parse(sessionRow("a", -1, 2)), parse(sessionRow("b", -1, 1)),
		parse(sessionRow("c", -1, 4)), parse(sessionRow("c", -1, 4)), parse(sessionRow("b", 1, 2)), parse(sessionRow("c", 1, 5)),
	}
	lines, err := serialize(SquashSessions(rows))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{sessionRow("b", 1, 2), sessionRow("a", -1, 2), sessionRow("c", -1, 4), sessionRow("c", -1, 4), sessionRow("c", 1, 5)}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("squashed to\n%s", strings.Join(lines, "\n"))
	}
	if single := SquashSessions(rows[:1]); len(single) != 1 {
		t.Fatal("a single row passes through")
	}
}

func profileLine(id, firstName, email string, groups ...string) string {
	quoted := make([]string, len(groups))
	for i, g := range groups {
		quoted[i] = `"` + g + `"`
	}
	return fmt.Sprintf(`{"id":"%s","first_name":"%s","last_name":"","email":"%s","avatar":"","properties":{},"project_id":"project-1","is_external":true,"created_at":"2026-10-08 09:00:00","last_seen_at":"2026-10-08 09:00:00","groups":[%s]}`,
		id, firstName, email, strings.Join(quoted, ","))
}

func cachedProfile(t *testing.T, r *rig, id string) *jsjson.Obj {
	t.Helper()
	text, ok := r.redis.Get(ProfileCachePrefix + "project-1:" + id)
	if !ok {
		t.Fatalf("no cache entry for %s", id)
	}
	row, ok := parseObject(text)
	if !ok {
		t.Fatalf("cache entry %q", text)
	}
	return row
}

func stringOf(row *jsjson.Obj, key string) string { return jsjson.JSString(row.Get(key)) }

func dump(row *jsjson.Obj) string {
	text, _ := jsport.Stringify(jsjson.NewObject(row))
	return text
}

func rows(lines ...string) []json.RawMessage {
	out := make([]json.RawMessage, len(lines))
	for i, line := range lines {
		out[i] = json.RawMessage(line)
	}
	return out
}

func TestProfileFlushMergesSequentialUpdatesAtFlushTime(t *testing.T) {
	r := newRig()
	r.redis.Push(ProfileKey, profileLine("profile-1", "John", "john@example.com"), profileLine("profile-1", "", "", "group-abc"))
	observation := r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || len(r.redis.List(ProfileKey)) != 0 {
		t.Fatalf("observation %+v", observation)
	}
	cached := cachedProfile(t, r, "profile-1")
	if stringOf(cached, "first_name") != "John" || stringOf(cached, "email") != "john@example.com" || stringOf(cached, "groups") != "group-abc" {
		t.Fatalf("cached %s", dump(cached))
	}
	if ttl := r.redis.ttls[ProfileCachePrefix+"project-1:profile-1"]; ttl != time.Hour {
		t.Fatalf("ttl %v", ttl)
	}
}

func TestProfileFlushUsesTheStoredProfileForCacheMisses(t *testing.T) {
	r := newRig()
	r.ch.rows = rows(profileLine("profile-1", "Jane", "jane@example.com", "existing-group"))
	r.redis.Push(ProfileKey, profileLine("profile-1", "", "", "new-group"))
	r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	cached := cachedProfile(t, r, "profile-1")
	if stringOf(cached, "first_name") != "Jane" || stringOf(cached, "email") != "jane@example.com" || stringOf(cached, "groups") != "existing-group,new-group" {
		t.Fatalf("cached %s", dump(cached))
	}
}

func TestProfileFlushLookupHasNoRecencyWindowAndKeepsCreatedAt(t *testing.T) {
	r := newRig()
	weekAgo := "2026-10-01 00:00:00.000"
	stored := strings.Replace(profileLine("device-uuid", "", ""), `"created_at":"2026-10-08 09:00:00"`, `"created_at":"`+weekAgo+`"`, 1)
	r.ch.rows = rows(strings.Replace(stored, `"properties":{}`, `"properties":{"os":"iOS","first_referrer":"google"}`, 1))
	r.redis.Push(ProfileKey, strings.Replace(profileLine("device-uuid", "", ""), `"properties":{}`, `"properties":{"os":"iOS"}`, 1))
	r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if len(r.ch.queries) != 1 || strings.Contains(r.ch.queries[0].Statement, "INTERVAL") {
		t.Fatalf("queries %+v", r.ch.queries)
	}
	cached := cachedProfile(t, r, "device-uuid")
	if stringOf(cached, "created_at") != weekAgo || jsjson.JSString(cached.Get("properties").Get("first_referrer")) != "google" {
		t.Fatalf("cached %s", dump(cached))
	}
}

func TestProfileFlushInsertsNewWhenTheLookupFails(t *testing.T) {
	r := newRig()
	r.ch.queryErr = errClickHouseDown
	r.redis.Push(ProfileKey, profileLine("profile-1", "John", ""))
	observation := r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || len(r.ch.inserts) != 1 || len(r.redis.List(ProfileKey)) != 0 {
		t.Fatalf("observation %+v", observation)
	}
	if !observation.Phases.CHFetch.Set {
		t.Fatal("the fetch phase is reported")
	}
}

func TestProfileFlushKeepsTheRowsWhenTheInsertFails(t *testing.T) {
	r := newRig()
	r.ch.failInsertAt = 1
	r.redis.Push(ProfileKey, profileLine("profile-1", "John", ""))
	observation := r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultError || len(r.redis.List(ProfileKey)) != 1 {
		t.Fatalf("observation %+v", observation)
	}
	if _, cached := r.redis.Get(ProfileCachePrefix + "project-1:profile-1"); cached {
		t.Fatal("a profile that did not land must not be cached")
	}
}

func TestProfileFlushKeepsTheRowsWhenTheCommitFails(t *testing.T) {
	r := newRig()
	r.redis.commitErr = errors.New("EXECABORT")
	r.redis.Push(ProfileKey, profileLine("profile-1", "John", ""))
	observation := r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultError || len(r.redis.List(ProfileKey)) != 1 {
		t.Fatalf("observation %+v", observation)
	}
}

func TestProfileFlushDoesNotSwallowAFailedLookupWhenShuttingDown(t *testing.T) {
	r := newRig()
	r.redis.Push(ProfileKey, profileLine("profile-1", "John", ""))
	ctx, cancel := context.WithCancel(context.Background())
	r.ch.queryErr = context.Canceled
	r.ch.onQuery = cancel
	runner, err := flush.NewRunner(NewProfile(buffersConfig(t, nil), r.deps, Overrides{}), flush.Deps{Redis: r.redis, Clock: lossClock{}, Observer: r.obs, Paused: flush.NeverPaused})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.TryFlush(ctx, flush.TriggerCron); err != nil {
		t.Fatal(err)
	}
	if got := r.obs.last(); got.Result != flush.ResultError || len(r.ch.inserts) != 0 || len(r.redis.List(ProfileKey)) != 1 {
		t.Fatalf("observation %+v", got)
	}
}

func TestProfileFlushOfUnreadableRowsOnlyTrimsThem(t *testing.T) {
	r := newRig()
	r.redis.Push(ProfileKey, "not json", "null", "7", "[]")
	observation := r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || observation.Rows != 4 || len(r.redis.List(ProfileKey)) != 0 || len(r.ch.inserts) != 0 || r.redis.mgets != 0 {
		t.Fatalf("observation %+v", observation)
	}
}

func TestProfileWithoutProjectOrPropertiesDoesNotBlockTheList(t *testing.T) {
	r := newRig()
	r.ch.rows = rows(`{"id":"x","project_id":"undefined","first_name":"S"}`)
	r.redis.Push(ProfileKey, `{"id":"x"}`, `{"id":"x","properties":{"device":"server"}}`, profileLine("ok", "A", ""))
	observation := r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || len(r.redis.List(ProfileKey)) != 0 {
		t.Fatalf("observation %+v", observation)
	}
}

func TestProfileFetchChunksRunTwoAtATimeAndShareOneResult(t *testing.T) {
	// Run with -race: the two lookups write one result map.
	r := newRig()
	var answers []string
	for i := range 40 {
		r.redis.Push(ProfileKey, profileLine(fmt.Sprintf("p%d", i), "N", ""))
		answers = append(answers, profileLine(fmt.Sprintf("p%d", i), "Stored", "", "g"))
	}
	r.ch.rows = rows(answers...)
	cfg := buffersConfig(t, map[string]string{"PROFILE_BUFFER_BATCH_SIZE": "40", "PROFILE_BUFFER_FETCH_CHUNK_SIZE": "5"})
	r.flush(t, NewProfile(cfg, r.deps, Overrides{}))
	if len(r.ch.queries) != 8 || len(r.ch.inserts) != 1 || len(r.ch.inserts[0].Lines) != 40 {
		t.Fatalf("%d queries, %d inserts", len(r.ch.queries), len(r.ch.inserts))
	}
}

func TestBackfillReadsOneBatchAndDoesNotDrain(t *testing.T) {
	r := newRig()
	for i := range 1100 {
		r.redis.Push(ProfileBackfillKey, fmt.Sprintf(`{"projectId":"p","sessionId":"s%d","profileId":"u%d"}`, i, i))
	}
	observation := r.flush(t, NewProfileBackfill(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Rows != 1000 || len(r.redis.List(ProfileBackfillKey)) != 100 || len(r.ch.commands) != 5 {
		t.Fatalf("%d rows, %d left, %d commands", observation.Rows, len(r.redis.List(ProfileBackfillKey)), len(r.ch.commands))
	}
	if !observation.Phases.CHInsert.Set {
		t.Fatal("the UPDATE time is reported as the insert phase")
	}
}

func TestBackfillStatementsStayUnderClickHousesFieldLimit(t *testing.T) {
	// http_max_fields is 1000: every parameter is one URL field.
	const httpMaxFields, settingsOnTheURL = 1000, 40
	if fields := backfillChunkSize*4 + settingsOnTheURL; fields > httpMaxFields {
		t.Fatalf("a chunk of %d sessions is %d URL fields", backfillChunkSize, fields)
	}
	r := newRig()
	for i := range 450 {
		r.redis.Push(ProfileBackfillKey, fmt.Sprintf(`{"projectId":"p","sessionId":"s%d","profileId":"u%d"}`, i, i))
	}
	r.flush(t, NewProfileBackfill(buffersConfig(t, nil), r.deps, Overrides{}))
	for _, command := range r.ch.commands {
		if len(command.Params) > httpMaxFields-settingsOnTheURL {
			t.Fatalf("%d parameters in one statement", len(command.Params))
		}
	}
	if len(r.ch.commands) != 3 {
		t.Fatalf("%d statements for 450 sessions", len(r.ch.commands))
	}
}

func TestBackfillKeepsTheBatchWhenAChunkFails(t *testing.T) {
	r := newRig()
	for i := range 600 {
		r.redis.Push(ProfileBackfillKey, fmt.Sprintf(`{"projectId":"p","sessionId":"s%d","profileId":"u%d"}`, i, i))
	}
	r.ch.failCommandAt = 2
	observation := r.flush(t, NewProfileBackfill(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultError || len(r.redis.List(ProfileBackfillKey)) != 600 {
		t.Fatalf("observation %+v", observation)
	}
}

func TestBackfillSkipsEntriesWithoutThreeStrings(t *testing.T) {
	entries, rejected := parseBackfillEntries([]string{
		`{"projectId":"p","sessionId":"s"}`, `{"projectId":"p","sessionId":1,"profileId":"u"}`, `[1]`,
		`{"projectId":"p","sessionId":"s","profileId":"u"}`,
	})
	if len(entries) != 1 || entries[0].ProfileID != "u" {
		t.Fatalf("entries %+v", entries)
	}
	want := []RejectedRow{
		{RejectMissingFields, `{"projectId":"p","sessionId":"s"}`},
		{RejectMissingFields, `{"projectId":"p","sessionId":1,"profileId":"u"}`},
		{RejectNotObject, `[1]`},
	}
	if !slices.Equal(rejected, want) {
		t.Fatalf("rejected %+v", rejected)
	}
}

// commandHook lets the keepalive run while an UPDATE is in flight.
type commandHook struct {
	*fakeCH
	hook func()
}

func (c *commandHook) CommandWithParams(ctx context.Context, statement string, params []chclient.Param, settings chclient.Settings) error {
	c.hook()
	return c.fakeCH.CommandWithParams(ctx, statement, params, settings)
}

func TestBuffersAreTheFourCronJobs(t *testing.T) {
	var jobs []string
	for _, buffer := range Buffers(buffersConfig(t, nil), newRig().deps) {
		jobs = append(jobs, buffer.Job+"="+buffer.Flusher.Name()+":"+buffer.Flusher.ListKey())
		if _, ok := flush.Interval(buffer.Job); !ok {
			t.Errorf("%s has no interval", buffer.Job)
		}
	}
	want := "flushEvents=event:event_buffer:queue flushProfiles=profile:profile-buffer flushSessions=session:session-buffer flushProfileBackfill=profile-backfill:profile-backfill-buffer"
	if strings.Join(jobs, " ") != want {
		t.Fatalf("jobs %v", jobs)
	}
}

func TestBatchMessageIsWhatSuperJSONWrites(t *testing.T) {
	if got := batchMessage("p1", 12); got != `{"json":{"projectId":"p1","count":12}}` {
		t.Fatal(got)
	}
	lone, err := jsport.ParseJSON(`"a\ud800"`)
	if err != nil {
		t.Fatal(err)
	}
	if got := batchMessage(lone.Str, 1); !strings.Contains(got, `"a\ud800"`) {
		t.Fatal(got)
	}
}
