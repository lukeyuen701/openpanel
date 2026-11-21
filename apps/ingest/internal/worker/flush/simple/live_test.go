package simple

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/clock"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
	"openpanel/ingest/internal/worker/flush/chtest"
)

// The live parity test: the same Redis lists are flushed once by the real
// TypeScript buffers (a Bun process) into one ClickHouse database and once by
// the Go flush into another, and the tables are compared. Needs WPARITY_CH,
// and bun (WPARITY_BUN or on PATH). Redis is parity database 14, where the
// buffers' production list names are used and must be empty.

const (
	parityRedisDB = 14
	tsFlushScript = "../../../../parity/flush-ts-flush.ts"
	coreDir       = "../../../../../../packages/core"
	bunEnv        = "WPARITY_BUN"
)

var liveLists = []string{BotKey, AiKey, ReplayKey, GroupKey, "lock:" + BotName, "lock:" + AiName, "lock:" + ReplayName, "lock:" + GroupName}

func uuid(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

// liveRows are lines shaped like the ones the Bun API and the consumer write.
func liveRows() map[string][]string {
	rows := map[string][]string{}
	// Recent: replay chunks and the AI columns carry a 30-day TTL, which would
	// otherwise empty both flushes' tables at the server's pace.
	today := time.Now().UTC().Format("2006-01-02")
	for i := range 1500 {
		rows[BotKey] = append(rows[BotKey], fmt.Sprintf(`{"id":"%s","name":"Bot é %d","type":"crawler","project_id":"wparity-p%d","path":"/p/%d?q=\"x\"","created_at":"2026-10-08 09:%02d:%02d"}`, uuid(i), i, i%3, i, i/60%60, i%60))
	}
	for i := range 450 {
		rows[GroupKey] = append(rows[GroupKey], fmt.Sprintf(`{"project_id":"wparity-p%d","id":"g%d","type":"company","name":"Group ü %d","properties":{"plan":"pro","a.b":"%d"},"created_at":"2026-10-08 09:00:00","version":"%d","deleted":0}`, i%2, i%300, i, i, 1760000000000+i))
	}
	for i := range 700 {
		rows[AiKey] = append(rows[AiKey], fmt.Sprintf(`{"project_id":"wparity-p1","id":"%s","type":"generation","trace_id":"t%d","span_id":"s","parent_id":"","conversation_id":"c","name":"chat","model":"m","provider":"p","input_tokens":%d,"output_tokens":2,"cache_read_tokens":0,"cache_write_tokens":0,"reasoning_tokens":0,"input_cost":0.5,"output_cost":0.25,"total_cost":0.75,"cost_source":"calc","latency_ms":10,"time_to_first_token_ms":3,"is_error":false,"http_status":200,"stop_reason":"stop","is_stream":true,"model_parameters":"{}","input":"hi \"there\"","output":"😀","tools":"[]","error":"","profile_id":"u","device_id":"d","session_id":"se","groups":["g1"],"properties":{"k":"v"},"created_at":"%s 09:00:00.%03d"}`, uuid(i), i, i, today, i%1000))
	}
	for i := range 120 {
		payload := strings.Repeat(fmt.Sprintf("snapshot-é-%d-", i), 2000)
		rows[ReplayKey] = append(rows[ReplayKey], fmt.Sprintf(`{"project_id":"wparity-p1","session_id":"s%d","chunk_index":%d,"started_at":"%s 09:00:00.000","ended_at":"%s 09:00:10.000","events_count":%d,"is_full_snapshot":%v,"payload":"%s"}`, i%7, i, today, today, i%50, i%2 == 0, payload))
	}
	return rows
}

type liveRig struct {
	base      string
	client    rueidis.Client
	redisURL  string
	flushDeps Deps
}

func newLiveRig(t *testing.T) *liveRig {
	t.Helper()
	base := chtest.BaseURL(t)
	redisURL := chtest.RedisURLForDB(t, parityRedisDB)
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
	ctx := context.Background()
	for _, key := range liveLists {
		exists, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsInt64()
		if err != nil || exists != 0 {
			t.Skipf("%s in Redis db %d is not empty (%v): not touching it", key, parityRedisDB, err)
		}
	}
	t.Cleanup(func() {
		client.Do(context.Background(), client.B().Del().Key(liveLists...).Build())
	})
	return &liveRig{base: base, client: client, redisURL: redisURL}
}

func (r *liveRig) load(t *testing.T, rows map[string][]string) {
	t.Helper()
	for key, lines := range rows {
		for start := 0; start < len(lines); start += 100 {
			end := min(start+100, len(lines))
			if err := r.client.Do(context.Background(), r.client.B().Rpush().Key(key).Element(lines[start:end]...).Build()).Error(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func (r *liveRig) lengths(t *testing.T) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, key := range []string{BotKey, AiKey, ReplayKey, GroupKey} {
		length, err := r.client.Do(context.Background(), r.client.B().Llen().Key(key).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		out[key] = length
	}
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

// flushWithTypeScript runs the TypeScript buffers over what is in Redis.
func (r *liveRig) flushWithTypeScript(t *testing.T, database string, async bool) {
	t.Helper()
	script, err := filepath.Abs(tsFlushScript)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bunBinary(t), script, "bot", "replay", "group", "ai-event")
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
	var results map[string]struct {
		Result string
		Rows   int
		Err    string
	}
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
	if len(results) != 4 {
		t.Fatalf("TypeScript flushed %v", results)
	}
}

func (r *liveRig) flushWithGo(t *testing.T, database string, async bool) map[string]flush.Observation {
	t.Helper()
	cfg := config.BuffersConfig{
		AsyncInserts: async, InsertConcurrency: 5,
		Bot: config.Sizing{Batch: 1000}, AiEvent: config.Sizing{Batch: 1000, Chunk: 500},
		Group: config.Sizing{Batch: 200, Chunk: 1000}, Replay: config.Sizing{Batch: 500, Chunk: 500},
	}
	chCfg := config.ClickHouseConfig{RequestCompression: true, MaxOpenConnections: 50, RequestTimeout: 30 * time.Second, UnhealthyMark: 5 * time.Second}
	ch, err := chclient.New(chclient.Config{URLs: []string{chtest.URLWithDatabase(r.base, database)}, ClickHouseConfig: chCfg}, chclient.Deps{Clock: clock.System{}})
	if err != nil {
		t.Fatal(err)
	}
	redis := flush.NewRueidis(r.client, 5*time.Second)
	obs := &recorder{}
	deps := Deps{Redis: redis, Inserter: ch, AsyncInserts: async, InsertConcurrency: cfg.InsertConcurrency}
	jobs, err := Jobs(Buffers(cfg, deps), flush.Deps{Redis: redis, Clock: clock.System{}, Observer: obs, Paused: flush.NeverPaused}, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs {
		if err := job.Runner.TryFlush(context.Background(), flush.TriggerCron); err != nil {
			t.Fatal(err)
		}
	}
	out := map[string]flush.Observation{}
	for _, o := range obs.all {
		out[o.Buffer] = o
	}
	return out
}

// dump is each table's rows in a stable order, as the server prints them.
func dump(t *testing.T, base, database, table, order string) string {
	t.Helper()
	// groups is a ReplacingMergeTree: which of its parts have merged is the
	// server's business, so it is read the way the dashboard reads it.
	final := ""
	if table == "groups" {
		final = " FINAL"
	}
	return chtest.Exec(t, base, database, fmt.Sprintf("SELECT * FROM %s%s ORDER BY %s FORMAT JSONEachRow", table, final, order))
}

var parityTables = []struct{ table, order string }{
	{"events_bots", "id"},
	{"groups", "project_id, id, version"},
	{"ai_events", "id"},
	{"session_replay_chunks", "project_id, session_id, chunk_index"},
}

func runParity(t *testing.T, async bool) {
	rig := newLiveRig(t)
	rows := liveRows()
	suffix := "simple"
	if async {
		suffix = "simpleasync"
	}
	goDB := chtest.Database(t, rig.base, suffix+"go")
	tsDB := chtest.Database(t, rig.base, suffix+"ts")

	rig.load(t, rows)
	rig.flushWithTypeScript(t, tsDB, async)
	for key, length := range rig.lengths(t) {
		if length != 0 {
			t.Fatalf("TypeScript left %d rows in %s", length, key)
		}
	}

	rig.load(t, rows)
	observations := rig.flushWithGo(t, goDB, async)
	for key, length := range rig.lengths(t) {
		if length != 0 {
			t.Fatalf("Go left %d rows in %s", length, key)
		}
	}
	for name, want := range map[string]int{BotName: 1500, GroupName: 450, AiName: 700, ReplayName: 120} {
		if got := observations[name]; got.Result != flush.ResultSuccess || got.Rows != want || got.LLenAtStart != int64(want) {
			t.Errorf("%s observation %+v, want %d rows", name, got, want)
		}
	}

	if async {
		for _, database := range []string{goDB, tsDB} {
			chtest.Exec(t, rig.base, database, "SYSTEM FLUSH ASYNC INSERT QUEUE")
		}
	}
	for _, table := range parityTables {
		goRows := dump(t, rig.base, goDB, table.table, table.order)
		tsRows := dump(t, rig.base, tsDB, table.table, table.order)
		if goRows == "" {
			t.Errorf("%s: nothing landed", table.table)
		}
		if goRows != tsRows {
			t.Errorf("%s differs between the Go flush and the TypeScript flush (%d and %d bytes)", table.table, len(goRows), len(tsRows))
		}
	}
	if got := strings.TrimSpace(chtest.Exec(t, rig.base, goDB, "SELECT count() FROM groups FINAL")); got != "300" {
		t.Errorf("groups after replacing: %s, want 300 ids", got)
	}
}

func TestLiveGoFlushWritesWhatTheTypeScriptFlushWrites(t *testing.T) { runParity(t, false) }

func TestLiveAsyncInsertsWriteWhatTheTypeScriptFlushWrites(t *testing.T) { runParity(t, true) }

// A poison line stops its batch where it is, in Redis; it does not vanish, and
// rows behind it wait. Same as the TypeScript flush; removing the line unblocks.
func TestLivePoisonLineBlocksTheBatchAndLosesNothing(t *testing.T) {
	rig := newLiveRig(t)
	goDB := chtest.Database(t, rig.base, "simplepoison")
	good := liveRows()[BotKey][:6]
	lines := append(append([]string{}, good[:3]...), `{"id": not json`)
	lines = append(lines, good[3:]...)
	rig.load(t, map[string][]string{BotKey: lines})

	observations := rig.flushWithGo(t, goDB, false)
	if observations[BotName].Result != flush.ResultError {
		t.Fatalf("observation %+v", observations[BotName])
	}
	if got := rig.lengths(t)[BotKey]; got != 7 {
		t.Fatalf("%d rows left in Redis, want all 7", got)
	}
	if got := strings.TrimSpace(chtest.Exec(t, rig.base, goDB, "SELECT count() FROM events_bots")); got != "0" {
		t.Fatalf("%s rows of the failed batch landed", got)
	}

	if err := rig.client.Do(context.Background(), rig.client.B().Lset().Key(BotKey).Index(3).Element(good[3]).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	rig.flushWithGo(t, goDB, false)
	if got := strings.TrimSpace(chtest.Exec(t, rig.base, goDB, "SELECT count() FROM events_bots")); got != "7" || rig.lengths(t)[BotKey] != 0 {
		t.Fatalf("after the fix: %s rows landed", got)
	}
}
