package core

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/clock"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
	"openpanel/ingest/internal/worker/flush/chtest"
)

// The live parity tests: the same Redis lists (and cache keys, and stored
// profiles) are flushed once by the real TypeScript buffers (a Bun process)
// into one ClickHouse database and once by the Go flush into another, and the
// tables, the Redis state left behind and the events:batch messages are
// compared. They need WPARITY_CH, and bun (WPARITY_BUN or on PATH). Redis is
// parity database 13, where the buffers' production key names are used and
// must be empty.

const (
	liveRedisDB   = 13
	tsFlushScript = "../../../../parity/flush-ts-core.ts"
	coreDir       = "../../../../../../packages/core"
	bunEnv        = "WPARITY_BUN"
	// liveProject is the prefix of every project id the tests use: events:batch
	// is a server-wide channel, and other processes publish on it.
	liveProject = "wparity-p"
	cachePrefix = ProfileCachePrefix + liveProject
)

var liveListKeys = []string{EventKey, SessionKey, ProfileKey, ProfileBackfillKey, "lock:event", "lock:session", "lock:profile", "lock:profile-backfill"}

type liveRig struct {
	base     string
	client   rueidis.Client
	redisURL string
}

func newLiveRig(t *testing.T) *liveRig {
	t.Helper()
	base := chtest.BaseURL(t)
	redisURL := chtest.RedisURLForDB(t, liveRedisDB)
	options, err := rueidis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	options.DisableCache = true
	client, err := rueidis.NewClient(options)
	if err != nil {
		t.Fatalf("Redis: %v", err)
	}
	t.Cleanup(client.Close)
	rig := &liveRig{base: base, client: client, redisURL: redisURL}
	ctx := context.Background()
	for _, key := range liveListKeys {
		exists, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsInt64()
		if err != nil || exists != 0 {
			t.Skipf("%s in Redis db %d is not empty (%v): not touching it", key, liveRedisDB, err)
		}
	}
	if keys := rig.cacheKeys(t); len(keys) != 0 {
		t.Skipf("%d profile-cache keys of the tests are in Redis db %d: not touching them", len(keys), liveRedisDB)
	}
	t.Cleanup(func() { rig.wipe(t) })
	return rig
}

func (r *liveRig) cacheKeys(t *testing.T) []string {
	t.Helper()
	var keys []string
	var cursor uint64
	for {
		entry, err := r.client.Do(context.Background(), r.client.B().Scan().Cursor(cursor).Match(cachePrefix+"*").Count(1000).Build()).AsScanEntry()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, entry.Elements...)
		if cursor = entry.Cursor; cursor == 0 {
			return keys
		}
	}
}

func (r *liveRig) wipe(t *testing.T) {
	t.Helper()
	keys := append(r.cacheKeys(t), liveListKeys...)
	if err := r.client.Do(context.Background(), r.client.B().Del().Key(keys...).Build()).Error(); err != nil {
		t.Errorf("cleaning Redis: %v", err)
	}
}

func (r *liveRig) rpush(t *testing.T, key string, lines []string) {
	t.Helper()
	for start := 0; start < len(lines); start += 200 {
		end := min(start+200, len(lines))
		if err := r.client.Do(context.Background(), r.client.B().Rpush().Key(key).Element(lines[start:end]...).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
}

func (r *liveRig) length(t *testing.T, key string) int64 {
	t.Helper()
	length, err := r.client.Do(context.Background(), r.client.B().Llen().Key(key).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return length
}

type cacheEntry struct {
	Value string
	TTL   int64
}

func (r *liveRig) cacheDump(t *testing.T) map[string]cacheEntry {
	t.Helper()
	dump := map[string]cacheEntry{}
	for _, key := range r.cacheKeys(t) {
		value, err := r.client.Do(context.Background(), r.client.B().Get().Key(key).Build()).ToString()
		if err != nil {
			t.Fatal(err)
		}
		ttl, err := r.client.Do(context.Background(), r.client.B().Ttl().Key(key).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		dump[key] = cacheEntry{value, ttl}
	}
	return dump
}

func (r *liveRig) setCache(t *testing.T, entries map[string]string) {
	t.Helper()
	for key, value := range entries {
		if err := r.client.Do(context.Background(), r.client.B().Set().Key(key).Value(value).ExSeconds(1800).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
}

// listener collects the events:batch messages of the tests' projects.
type listener struct {
	mutex    sync.Mutex
	messages []string
	cancel   func()
}

func (r *liveRig) listen(t *testing.T) *listener {
	t.Helper()
	conn, cancel := r.client.Dedicate()
	l := &listener{cancel: cancel}
	t.Cleanup(cancel)
	conn.SetPubSubHooks(rueidis.PubSubHooks{OnMessage: func(m rueidis.PubSubMessage) {
		if strings.Contains(m.Message, `"`+liveProject) {
			l.mutex.Lock()
			l.messages = append(l.messages, m.Message)
			l.mutex.Unlock()
		}
	}})
	if err := conn.Do(context.Background(), conn.B().Subscribe().Channel(EventsBatchChannel).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	return l
}

// drain waits for the messages in flight and returns them sorted: the
// TypeScript sends them from an un-awaited publish, so their order is not stable.
func (l *listener) drain() []string {
	time.Sleep(400 * time.Millisecond)
	l.mutex.Lock()
	defer l.mutex.Unlock()
	out := append([]string(nil), l.messages...)
	l.messages = nil
	sort.Strings(out)
	return out
}

func bunBinary(t *testing.T) string {
	t.Helper()
	if path := os.Getenv(bunEnv); path != "" {
		return path
	}
	path, err := exec.LookPath("bun")
	if err != nil {
		t.Skipf("no bun: set %s", bunEnv)
	}
	return path
}

type tsResult struct {
	Result string
	Rows   int
	Err    string
}

func (r *liveRig) flushWithTypeScript(t *testing.T, database string, async bool, buffers ...string) map[string]tsResult {
	t.Helper()
	script, err := filepath.Abs(tsFlushScript)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bunBinary(t), append([]string{script}, buffers...)...)
	cmd.Dir = coreDir
	cmd.Env = append(os.Environ(), "REDIS_URL="+r.redisURL, "CLICKHOUSE_URL="+chtest.URLWithDatabase(r.base, database))
	if async {
		cmd.Env = append(cmd.Env, "TS_ASYNC_INSERTS=1")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var results map[string]tsResult
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for scanner.Scan() {
		if rest, ok := strings.CutPrefix(scanner.Text(), "RESULT "); ok {
			if err := json.Unmarshal([]byte(rest), &results); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("bun: %v", err)
	}
	for name, got := range results {
		if got.Result != "success" {
			t.Fatalf("TypeScript %s flush: %s %s", name, got.Result, got.Err)
		}
	}
	if len(results) != len(buffers) {
		t.Fatalf("TypeScript flushed %v", results)
	}
	return results
}

func (r *liveRig) flushWithGo(t *testing.T, database string, async bool) map[string]flush.Observation {
	t.Helper()
	chCfg := config.ClickHouseConfig{RequestCompression: true, MaxOpenConnections: 50, RequestTimeout: 30 * time.Second, UnhealthyMark: 5 * time.Second}
	ch, err := chclient.New(chclient.Config{URLs: []string{chtest.URLWithDatabase(r.base, database)}, ClickHouseConfig: chCfg}, chclient.Deps{Clock: clock.System{}})
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{}
	if async {
		env["BUFFER_ASYNC_INSERTS"] = "1"
	}
	cfg := buffersConfig(t, env)
	redis := NewRueidis(r.client, 5*time.Second)
	recorder := &recorder{}
	deps := Deps{Redis: redis, CH: ch, Clustered: ch.Cluster()}
	out := map[string]flush.Observation{}
	for _, buffer := range Buffers(cfg, deps) {
		runner, err := flush.NewRunner(buffer.Flusher, flush.Deps{Redis: redis, Clock: clock.System{}, Observer: recorder, Paused: flush.NeverPaused})
		if err != nil {
			t.Fatal(err)
		}
		if err := runner.TryFlush(context.Background(), flush.TriggerCron); err != nil {
			t.Fatal(err)
		}
		observation := recorder.last()
		if observation.Result != flush.ResultSuccess {
			t.Fatalf("Go %s flush: %s %v", buffer.Flusher.Name(), observation.Result, observation.Err)
		}
		out[buffer.Flusher.Name()] = observation
	}
	return out
}

// dump is a query's rows as the server prints them.
func queryRows(t *testing.T, base, database, query string) string {
	t.Helper()
	return chtest.Exec(t, base, database, query+" FORMAT JSONEachRow")
}

func countLines(text string) int { return strings.Count(text, "\n") }

// compareTables fails when the two databases hold different rows for query,
// and returns how many rows were compared.
func compareTables(t *testing.T, base, goDB, tsDB, label, query string) int {
	t.Helper()
	goRows, tsRows := queryRows(t, base, goDB, query), queryRows(t, base, tsDB, query)
	if goRows == "" {
		t.Errorf("%s: nothing landed", label)
	}
	if goRows != tsRows {
		goLines, tsLines := strings.Split(goRows, "\n"), strings.Split(tsRows, "\n")
		for i := 0; i < min(len(goLines), len(tsLines)); i++ {
			if goLines[i] != tsLines[i] {
				t.Errorf("%s: row %d differs\n go: %s\n ts: %s", label, i, goLines[i], tsLines[i])
				break
			}
		}
		t.Errorf("%s differs between the Go flush and the TypeScript flush (%d and %d rows)", label, countLines(goRows), countLines(tsRows))
	}
	return countLines(goRows)
}

func stopMerges(t *testing.T, base, database string, tables ...string) {
	t.Helper()
	for _, table := range tables {
		chtest.Exec(t, base, database, "SYSTEM STOP MERGES "+database+"."+table)
	}
}

type liveDatabases struct{ goDB, tsDB string }

func (r *liveRig) databases(t *testing.T, name string, tables ...string) liveDatabases {
	t.Helper()
	dbs := liveDatabases{goDB: chtest.Database(t, r.base, name+"go"), tsDB: chtest.Database(t, r.base, name+"ts")}
	stopMerges(t, r.base, dbs.goDB, tables...)
	stopMerges(t, r.base, dbs.tsDB, tables...)
	return dbs
}

func marshal(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// --- event ---

type liveEvent struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	DeviceID       string            `json:"device_id"`
	ProfileID      string            `json:"profile_id"`
	ProjectID      string            `json:"project_id"`
	SessionID      string            `json:"session_id"`
	Properties     map[string]string `json:"properties"`
	Path           string            `json:"path"`
	Origin         string            `json:"origin"`
	CreatedAt      string            `json:"created_at"`
	Country        string            `json:"country"`
	City           string            `json:"city"`
	Region         string            `json:"region"`
	Longitude      *float64          `json:"longitude"`
	Latitude       *float64          `json:"latitude"`
	OS             string            `json:"os"`
	OSVersion      string            `json:"os_version"`
	Browser        string            `json:"browser"`
	BrowserVersion string            `json:"browser_version"`
	Device         string            `json:"device"`
	Brand          string            `json:"brand"`
	Model          string            `json:"model"`
	Duration       int               `json:"duration"`
	Referrer       string            `json:"referrer"`
	ReferrerName   string            `json:"referrer_name"`
	ReferrerType   string            `json:"referrer_type"`
	ImportedAt     *string           `json:"imported_at"`
	InsertedAt     string            `json:"inserted_at"`
	SDKName        string            `json:"sdk_name"`
	SDKVersion     string            `json:"sdk_version"`
	Revenue        *int              `json:"revenue,omitempty"`
	Groups         []string          `json:"groups"`
}

func uuid(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

func liveEventRows(count int) []string {
	lines := make([]string, count)
	for i := range count {
		longitude, latitude := 18.07+float64(i%7)/100, 59.33+float64(i%5)/100
		event := liveEvent{
			ID: uuid(i), Name: []string{"screen_view", "click", "purchase", "session_start"}[i%4],
			DeviceID: fmt.Sprintf("dev-%d", i%37), ProfileID: fmt.Sprintf("dev-%d", i%37),
			ProjectID: fmt.Sprintf("%s%d", liveProject, i%5), SessionID: fmt.Sprintf("ses-%d", i%61),
			Properties: map[string]string{"__path": fmt.Sprintf("/p/%d", i), "a.b": "é😀", "n": fmt.Sprint(i)},
			Path:       fmt.Sprintf("/p/%d", i), Origin: "https://example.com",
			CreatedAt: fmt.Sprintf("2026-10-08 09:%02d:%02d.%03d", i/3600%60, i/60%60, i%1000),
			Country:   "SE", City: "Malmö", Longitude: &longitude, Latitude: &latitude,
			OS: "iOS", OSVersion: "17", Browser: "Safari", BrowserVersion: "17", Device: "mobile",
			InsertedAt: "2026-10-08 10:00:00.000", SDKName: "web", SDKVersion: "1.0", Groups: []string{},
		}
		if i%11 == 0 {
			revenue := i * 3
			event.Revenue = &revenue
			event.Groups = []string{"g1", "g2"}
		}
		if i%13 == 0 {
			event.Longitude, event.Latitude = nil, nil
		}
		if i%97 == 0 {
			// A user property named like the field: the extraction must not misattribute it.
			event.Properties["project_id"] = "user-supplied"
		}
		raw, _ := json.Marshal(event)
		lines[i] = string(raw)
	}
	return lines
}

func runEventParity(t *testing.T, async bool) {
	rig := newLiveRig(t)
	name := "event"
	if async {
		name = "eventasync"
	}
	dbs := rig.databases(t, name, "events")
	rows := liveEventRows(4500)

	rig.rpush(t, EventKey, rows)
	tsMessages := rig.listen(t)
	rig.flushWithTypeScript(t, dbs.tsDB, async, "event")
	tsPublished := tsMessages.drain()
	if left := rig.length(t, EventKey); left != 0 {
		t.Fatalf("TypeScript left %d rows", left)
	}

	rig.rpush(t, EventKey, rows)
	goMessages := rig.listen(t)
	observations := rig.flushWithGo(t, dbs.goDB, async)
	goPublished := goMessages.drain()
	if left := rig.length(t, EventKey); left != 0 {
		t.Fatalf("Go left %d rows", left)
	}
	if got := observations[EventName]; got.Rows != len(rows) || got.LLenAtStart != int64(len(rows)) {
		t.Errorf("observation %+v", got)
	}

	if async {
		for _, database := range []string{dbs.goDB, dbs.tsDB} {
			chtest.Exec(t, rig.base, database, "SYSTEM FLUSH ASYNC INSERT QUEUE")
		}
	}
	compared := compareTables(t, rig.base, dbs.goDB, dbs.tsDB, "events", "SELECT * FROM events ORDER BY id")
	if compared != len(rows) {
		t.Errorf("%d events landed, want %d", compared, len(rows))
	}
	if strings.Join(goPublished, "\n") != strings.Join(tsPublished, "\n") || len(goPublished) < 5 {
		t.Errorf("events:batch messages differ\n go: %q\n ts: %q", goPublished, tsPublished)
	}
	t.Logf("event: %d rows compared, %d events:batch messages compared, 0 differences", compared, len(goPublished))
}

func TestLiveEventFlushWritesWhatTheTypeScriptFlushWrites(t *testing.T) { runEventParity(t, false) }

func TestLiveEventFlushWithAsyncInserts(t *testing.T) { runEventParity(t, true) }

// --- session ---

type liveSession struct {
	ID              string   `json:"id"`
	ProjectID       string   `json:"project_id"`
	ProfileID       string   `json:"profile_id"`
	DeviceID        string   `json:"device_id"`
	Groups          []string `json:"groups"`
	CreatedAt       string   `json:"created_at"`
	EndedAt         string   `json:"ended_at"`
	IsBounce        bool     `json:"is_bounce"`
	EntryOrigin     string   `json:"entry_origin"`
	EntryPath       string   `json:"entry_path"`
	ExitOrigin      string   `json:"exit_origin"`
	ExitPath        string   `json:"exit_path"`
	ScreenViewCount int      `json:"screen_view_count"`
	Revenue         float64  `json:"revenue"`
	EventCount      int      `json:"event_count"`
	Duration        any      `json:"duration"`
	Country         string   `json:"country"`
	Region          string   `json:"region"`
	City            string   `json:"city"`
	Longitude       *float64 `json:"longitude"`
	Latitude        *float64 `json:"latitude"`
	Device          string   `json:"device"`
	Brand           string   `json:"brand"`
	Model           string   `json:"model"`
	Browser         string   `json:"browser"`
	BrowserVersion  string   `json:"browser_version"`
	OS              string   `json:"os"`
	OSVersion       string   `json:"os_version"`
	UTMMedium       string   `json:"utm_medium"`
	UTMSource       string   `json:"utm_source"`
	UTMCampaign     string   `json:"utm_campaign"`
	UTMContent      string   `json:"utm_content"`
	UTMTerm         string   `json:"utm_term"`
	Referrer        string   `json:"referrer"`
	ReferrerName    string   `json:"referrer_name"`
	ReferrerType    string   `json:"referrer_type"`
	Sign            int      `json:"sign"`
	Version         int      `json:"version"`
}

func liveSessionRow(index, sign, version int) string {
	duration := any(version * 1500)
	switch {
	case index%53 == 0:
		duration = -7 * version // clamped to 0
	case index%59 == 0:
		duration = nil
	case index%61 == 0:
		duration = fmt.Sprint(version * 10)
	}
	row := liveSession{
		ID: fmt.Sprintf("wparity-ses-%05d", index), ProjectID: fmt.Sprintf("%s%d", liveProject, index%4), ProfileID: fmt.Sprintf("u%d", index),
		DeviceID: fmt.Sprintf("d%d", index), Groups: []string{}, CreatedAt: "2026-10-08 09:00:00.000",
		EndedAt:  fmt.Sprintf("2026-10-08 09:%02d:%02d.%03d", version%60, index%60, version),
		IsBounce: version < 2, EntryOrigin: "https://example.com", EntryPath: "/", ExitOrigin: "https://example.com",
		ExitPath: fmt.Sprintf("/p/%d", version), ScreenViewCount: version, Revenue: float64(version) / 4, EventCount: version,
		Duration: duration, Country: "SE", City: "Malmö", Device: "mobile", Browser: "Safari", BrowserVersion: "17",
		OS: "iOS", OSVersion: "17", UTMSource: "news", ReferrerType: "search", Sign: sign, Version: version,
	}
	return marshal2(row)
}

func marshal2(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

// liveSessionRows is what ingest queues: each session a creation row and then,
// per extension, a (-1, old) and a (+1, new), the sessions interleaved so that
// one batch holds several versions of the same session.
func liveSessionRows(sessions int) []string {
	updates := func(index int) int { return index % 5 }
	var rows []string
	for step := 0; step <= 4*2; step++ {
		for index := range sessions {
			resident := index%25 == 0 // started in an earlier flush: its first row is not in this list
			extension := (step - 1) / 2
			switch {
			case step == 0 && !resident:
				rows = append(rows, liveSessionRow(index, 1, 1))
			case step > 0 && extension < updates(index):
				version := extension + 1
				if step%2 == 1 {
					rows = append(rows, liveSessionRow(index, -1, version))
				} else {
					rows = append(rows, liveSessionRow(index, 1, version+1))
				}
			}
		}
	}
	return rows
}

func TestLiveSessionFlushWritesWhatTheTypeScriptFlushWrites(t *testing.T) {
	rig := newLiveRig(t)
	dbs := rig.databases(t, "session", "sessions")
	rows := liveSessionRows(2800)
	if len(rows) <= 10_000 {
		t.Fatalf("%d rows do not exceed one batch", len(rows))
	}

	rig.rpush(t, SessionKey, rows)
	rig.flushWithTypeScript(t, dbs.tsDB, false, "session")
	if left := rig.length(t, SessionKey); left != 0 {
		t.Fatalf("TypeScript left %d rows", left)
	}
	rig.rpush(t, SessionKey, rows)
	observations := rig.flushWithGo(t, dbs.goDB, false)
	if left := rig.length(t, SessionKey); left != 0 {
		t.Fatalf("Go left %d rows", left)
	}
	if got := observations[SessionName]; got.Rows != len(rows) {
		t.Errorf("observation %+v", got)
	}

	compared := compareTables(t, rig.base, dbs.goDB, dbs.tsDB, "sessions", "SELECT * FROM sessions ORDER BY id, version, sign")
	collapsed := compareTables(t, rig.base, dbs.goDB, dbs.tsDB, "sessions collapsed", "SELECT * FROM sessions FINAL ORDER BY id")
	if compared >= len(rows) {
		t.Errorf("squash removed nothing: %d rows landed of %d", compared, len(rows))
	}
	t.Logf("session: %d queued rows, %d rows compared (squashed), %d collapsed rows compared, 0 differences", len(rows), compared, collapsed)
}

// --- profile ---

type liveProfile struct {
	ID         string            `json:"id"`
	FirstName  string            `json:"first_name"`
	LastName   string            `json:"last_name"`
	Email      string            `json:"email"`
	Avatar     string            `json:"avatar"`
	Properties map[string]string `json:"properties"`
	ProjectID  string            `json:"project_id"`
	IsExternal bool              `json:"is_external"`
	CreatedAt  string            `json:"created_at"`
	LastSeenAt string            `json:"last_seen_at"`
	Groups     []string          `json:"groups"`
}

const (
	profileSeenAt   = "2026-10-08 09:00:00"
	profileStoredAt = "2026-09-01 00:00:00.000"
)

func liveProfileRow(id, project string, mutate func(*liveProfile)) liveProfile {
	profile := liveProfile{
		ID: id, Properties: map[string]string{}, ProjectID: project, IsExternal: true,
		CreatedAt: profileSeenAt, LastSeenAt: profileSeenAt, Groups: []string{},
	}
	if mutate != nil {
		mutate(&profile)
	}
	return profile
}

type profileFixture struct {
	incoming []string
	// storedOld and stored are the rows of `profiles` in ClickHouse before the flush,
	// inserted in this order and as two inserts, so that an old and a newer row of
	// one profile are in different parts.
	storedOld []string
	stored    []string
	cache     map[string]string
}

func liveProfiles() profileFixture {
	var fixture profileFixture
	fixture.cache = map[string]string{}
	project := func(i int) string { return fmt.Sprintf("%s%d", liveProject, i%2+1) }
	incoming := func(p liveProfile) { fixture.incoming = append(fixture.incoming, marshal2(p)) }
	stored := func(p liveProfile) { fixture.stored = append(fixture.stored, marshal2(p)) }

	for i := range 60 { // new profiles, one or two rows each
		id := fmt.Sprintf("new-%d", i)
		incoming(liveProfileRow(id, project(i), func(p *liveProfile) {
			p.FirstName, p.Properties = "Ada é😀", map[string]string{"plan": "free", "10": "a", "2": "b"}
		}))
		if i%3 == 0 {
			incoming(liveProfileRow(id, project(i), func(p *liveProfile) {
				p.Email, p.Groups, p.LastSeenAt = id+"@example.com", []string{"g-" + id}, "2026-10-08 09:00:05"
			}))
		}
	}
	for i := range 40 { // stored in ClickHouse: an old row and a newer one
		id := fmt.Sprintf("ch-%d", i)
		fixture.storedOld = append(fixture.storedOld, marshal2(liveProfileRow(id, project(i), func(p *liveProfile) {
			p.FirstName, p.CreatedAt, p.LastSeenAt = "Old", profileStoredAt, "2026-09-02 00:00:00.000"
			p.Properties = map[string]string{"old": "1", "plan": "free"}
		})))
		stored(liveProfileRow(id, project(i), func(p *liveProfile) {
			p.FirstName, p.Email, p.CreatedAt, p.LastSeenAt = "Stored", "stored@example.com", profileStoredAt, "2026-09-05 00:00:00.000"
			p.Properties, p.Groups = map[string]string{"plan": "team", "keep": "me"}, []string{"g0", "g1"}
		}))
		incoming(liveProfileRow(id, project(i), func(p *liveProfile) {
			p.Properties, p.Groups = map[string]string{"plan": "pro", "new": "1"}, []string{"g1", "g2"}
			if i%2 == 0 {
				p.FirstName, p.Email = "", ""
			}
		}))
	}
	for i := range 20 { // in the cache only
		id := fmt.Sprintf("cache-%d", i)
		key := ProfileCachePrefix + project(i) + ":" + id
		fixture.cache[key] = marshal2(liveProfileRow(id, project(i), func(p *liveProfile) {
			p.FirstName, p.CreatedAt, p.LastSeenAt = "Cached", "2026-09-10 00:00:00.000", "2026-09-11 00:00:00.000"
			p.Properties, p.Groups = map[string]string{"c": "1"}, []string{"gc"}
		}))
		incoming(liveProfileRow(id, project(i), func(p *liveProfile) { p.Properties = map[string]string{"c": "2", "d": "3"} }))
	}
	for i := range 10 { // in both: the cache answers
		id := fmt.Sprintf("both-%d", i)
		stored(liveProfileRow(id, project(i), func(p *liveProfile) {
			p.FirstName, p.CreatedAt, p.LastSeenAt = "FromClickHouse", profileStoredAt, "2026-09-05 00:00:00.000"
		}))
		fixture.cache[ProfileCachePrefix+project(i)+":"+id] = marshal2(liveProfileRow(id, project(i), func(p *liveProfile) {
			p.FirstName, p.CreatedAt, p.LastSeenAt = "FromCache", "2026-09-06 00:00:00.000", "2026-09-07 00:00:00.000"
		}))
		incoming(liveProfileRow(id, project(i), nil))
	}
	for i := range 10 { // a server-side update of a browser profile
		id := fmt.Sprintf("srv-%d", i)
		stored(liveProfileRow(id, project(i), func(p *liveProfile) {
			p.CreatedAt, p.LastSeenAt = profileStoredAt, "2026-09-05 00:00:00.000"
			p.Properties = map[string]string{"device": "desktop", "country": "SE", "browser": "Chrome", "plan": "free"}
		}))
		incoming(liveProfileRow(id, project(i), func(p *liveProfile) {
			p.Properties = map[string]string{"device": "server", "country": "US", "plan": "pro", "isServer": "true", "custom": "k"}
		}))
	}
	fixture.incoming = append(fixture.incoming, "not json", "null")
	return fixture
}

func (r *liveRig) seedProfiles(t *testing.T, database string, fixture profileFixture) {
	t.Helper()
	for _, rows := range [][]string{fixture.storedOld, fixture.stored} {
		chtest.Exec(t, r.base, database, "INSERT INTO profiles FORMAT JSONEachRow\n"+strings.Join(rows, "\n")+"\n")
	}
}

func TestLiveProfileFlushMergesWithStoredProfilesLikeTheTypeScriptFlush(t *testing.T) {
	rig := newLiveRig(t)
	dbs := rig.databases(t, "profile", "profiles")
	fixture := liveProfiles()
	rig.seedProfiles(t, dbs.goDB, fixture)
	rig.seedProfiles(t, dbs.tsDB, fixture)

	rig.rpush(t, ProfileKey, fixture.incoming)
	rig.setCache(t, fixture.cache)
	rig.flushWithTypeScript(t, dbs.tsDB, false, "profile")
	if left := rig.length(t, ProfileKey); left != 0 {
		t.Fatalf("TypeScript left %d rows", left)
	}
	tsCache := rig.cacheDump(t)
	rig.wipe(t)

	rig.rpush(t, ProfileKey, fixture.incoming)
	rig.setCache(t, fixture.cache)
	observations := rig.flushWithGo(t, dbs.goDB, false)
	if left := rig.length(t, ProfileKey); left != 0 {
		t.Fatalf("Go left %d rows", left)
	}
	goCache := rig.cacheDump(t)
	if got := observations[ProfileName]; got.Rows != len(fixture.incoming) {
		t.Errorf("observation %+v", got)
	}

	compared := compareTables(t, rig.base, dbs.goDB, dbs.tsDB, "profiles", "SELECT * FROM profiles ORDER BY project_id, id, last_seen_at")
	if want := len(fixture.storedOld) + len(fixture.stored) + 60 + 40 + 20 + 10 + 10; compared != want {
		t.Errorf("%d profile rows compared, want %d", compared, want)
	}
	if len(goCache) != len(tsCache) || len(goCache) != 140 {
		t.Errorf("%d cache entries from Go, %d from TypeScript, want 140", len(goCache), len(tsCache))
	}
	for key, want := range tsCache {
		got, ok := goCache[key]
		if !ok || got.Value != want.Value {
			t.Errorf("%s\n go: %q\n ts: %q", key, got.Value, want.Value)
		}
		if math.Abs(float64(got.TTL-want.TTL)) > 5 || got.TTL < 3500 {
			t.Errorf("%s ttl %d (Go), %d (TypeScript)", key, got.TTL, want.TTL)
		}
	}
	t.Logf("profile: %d queued rows, %d profile rows and %d cache entries compared, 0 differences", len(fixture.incoming), compared, len(goCache))
}

// --- profile backfill ---

// backfillSessions stays below what ClickHouse accepts in one request of the
// TypeScript flush: its UPDATE of 500 sessions is refused (see backfillChunkSize).
const backfillSessions = 200

func TestLiveProfileBackfillRewritesTheSameEventsAsTheTypeScriptFlush(t *testing.T) {
	rig := newLiveRig(t)
	dbs := rig.databases(t, "backfill", "events")
	now := time.Now().UTC()
	var seed []string
	for i := range backfillSessions {
		createdAt := now.Add(-time.Hour)
		if i%2 == 1 {
			createdAt = now.Add(-10 * time.Hour) // outside the six-hour window
		}
		event := liveEvent{
			ID: uuid(100_000 + i), Name: "screen_view", DeviceID: fmt.Sprintf("dev-%d", i), ProfileID: fmt.Sprintf("dev-%d", i),
			ProjectID: fmt.Sprintf("%s%d", liveProject, i%2+1), SessionID: fmt.Sprintf("wparity-s%d", i),
			Properties: map[string]string{}, CreatedAt: createdAt.Format("2006-01-02 15:04:05.000"),
			InsertedAt: createdAt.Format("2006-01-02 15:04:05.000"), Groups: []string{},
		}
		seed = append(seed, marshal(t, event))
	}
	for _, database := range []string{dbs.goDB, dbs.tsDB} {
		chtest.Exec(t, rig.base, database, "INSERT INTO events FORMAT JSONEachRow\n"+strings.Join(seed, "\n")+"\n")
	}

	var entries []string
	entry := func(i int, profile string) string {
		return fmt.Sprintf(`{"projectId":"%s%d","sessionId":"wparity-s%d","profileId":"%s"}`, liveProject, i%2+1, i, profile)
	}
	for i := range backfillSessions {
		entries = append(entries, entry(i, fmt.Sprintf("early-%d", i)))
	}
	entries = append(entries, "not json", entry(999, "nobody"))
	for i := 0; i < backfillSessions; i += 3 {
		entries = append(entries, entry(i, fmt.Sprintf("user-%d", i))) // a later entry for the same session wins
	}
	expectedChanged := 0
	for i := range backfillSessions {
		if i%2 == 0 {
			expectedChanged++
		}
	}

	read := func(database string) string {
		return queryRows(t, rig.base, database, "SELECT id, session_id, profile_id FROM events ORDER BY id")
	}
	waitForUpdate := func(database string) string {
		deadline := time.Now().Add(30 * time.Second)
		for {
			rows := read(database)
			if strings.Count(rows, `"profile_id":"early-`)+strings.Count(rows, `"profile_id":"user-`) == expectedChanged || time.Now().After(deadline) {
				return rows
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	rig.rpush(t, ProfileBackfillKey, entries)
	rig.flushWithTypeScript(t, dbs.tsDB, false, "backfill")
	if left := rig.length(t, ProfileBackfillKey); left != 0 {
		t.Fatalf("TypeScript left %d rows", left)
	}
	tsRows := waitForUpdate(dbs.tsDB)

	rig.rpush(t, ProfileBackfillKey, entries)
	rig.flushWithGo(t, dbs.goDB, false)
	if left := rig.length(t, ProfileBackfillKey); left != 0 {
		t.Fatalf("Go left %d rows", left)
	}
	goRows := waitForUpdate(dbs.goDB)

	if changed := strings.Count(tsRows, `"profile_id":"early-`) + strings.Count(tsRows, `"profile_id":"user-`); changed != expectedChanged {
		t.Fatalf("the TypeScript flush rewrote %d events, want %d", changed, expectedChanged)
	}
	if goRows != tsRows {
		t.Errorf("events differ between the Go flush and the TypeScript flush\n go: %.300s\n ts: %.300s", goRows, tsRows)
	}
	t.Logf("profile-backfill: %d events compared (%d rewritten inside the window, %d outside left alone), 0 differences", countLines(goRows), expectedChanged, backfillSessions-expectedChanged)
}

// The TypeScript flush sends 500 sessions per UPDATE, which ClickHouse refuses
// above about 245 ("Too many form fields", http_max_fields); the Go flush
// sends chunks it accepts, so a batch of a thousand sessions is rewritten.
func TestLiveProfileBackfillOfAFullBatchIsAccepted(t *testing.T) {
	rig := newLiveRig(t)
	dbs := rig.databases(t, "backfillfull", "events")
	now := time.Now().UTC().Add(-time.Hour).Format("2006-01-02 15:04:05.000")
	const sessions = 1000
	var seed, entries []string
	for i := range sessions {
		event := liveEvent{
			ID: uuid(200_000 + i), Name: "screen_view", DeviceID: "dev", ProfileID: "dev", ProjectID: liveProject + "1",
			SessionID: fmt.Sprintf("wparity-f%d", i), Properties: map[string]string{}, CreatedAt: now, InsertedAt: now, Groups: []string{},
		}
		seed = append(seed, marshal(t, event))
		entries = append(entries, fmt.Sprintf(`{"projectId":"%s1","sessionId":"wparity-f%d","profileId":"user-%d"}`, liveProject, i, i))
	}
	chtest.Exec(t, rig.base, dbs.goDB, "INSERT INTO events FORMAT JSONEachRow\n"+strings.Join(seed, "\n")+"\n")
	rig.rpush(t, ProfileBackfillKey, entries)
	observations := rig.flushWithGo(t, dbs.goDB, false)
	if got := observations[ProfileBackfillName]; got.Rows != sessions {
		t.Fatalf("observation %+v", got)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		updated := strings.TrimSpace(chtest.Exec(t, rig.base, dbs.goDB, "SELECT count() FROM events WHERE startsWith(profile_id, 'user-')"))
		if updated == fmt.Sprint(sessions) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s of %d events were rewritten", updated, sessions)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if left := rig.length(t, ProfileBackfillKey); left != 0 {
		t.Fatalf("%d rows left", left)
	}
}
