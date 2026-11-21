package simple

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/clock"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
)

type insertCall struct {
	table    string
	lines    []string
	settings chclient.Settings
}

type fakeInserter struct {
	mutex sync.Mutex
	calls []insertCall
	// failAt makes the n-th insert (1-based) fail, once.
	failAt int
	err    error
	// onInsert runs before an insert is recorded.
	onInsert func()
}

func (f *fakeInserter) Insert(_ context.Context, table string, lines []string, settings chclient.Settings) error {
	if f.onInsert != nil {
		f.onInsert()
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if f.failAt > 0 && len(f.calls)+1 == f.failAt {
		f.failAt = 0
		return f.err
	}
	f.calls = append(f.calls, insertCall{table, append([]string(nil), lines...), settings})
	return nil
}

func (f *fakeInserter) inserted() []string {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	var all []string
	for _, call := range f.calls {
		all = append(all, call.lines...)
	}
	return all
}

type rig struct {
	redis    *flush.MemoryRedis
	inserter *fakeInserter
	deps     Deps
	obs      *recorder
}

type recorder struct {
	mutex sync.Mutex
	all   []flush.Observation
}

func (r *recorder) ObserveFlush(o flush.Observation) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.all = append(r.all, o)
}

func newRig() *rig {
	redis := flush.NewMemoryRedis()
	inserter := &fakeInserter{}
	return &rig{redis: redis, inserter: inserter, obs: &recorder{}, deps: Deps{Redis: redis, Inserter: inserter, InsertConcurrency: 5}}
}

func (r *rig) flush(t *testing.T, f flush.Flusher) flush.Observation {
	t.Helper()
	runner, err := flush.NewRunner(f, flush.Deps{Redis: r.redis, Clock: clock.System{}, Observer: r.obs, Paused: flush.NeverPaused})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.TryFlush(context.Background(), flush.TriggerCron); err != nil {
		t.Fatal(err)
	}
	r.obs.mutex.Lock()
	defer r.obs.mutex.Unlock()
	return r.obs.all[len(r.obs.all)-1]
}

func testConfig() config.BuffersConfig {
	return config.BuffersConfig{
		InsertConcurrency: 5,
		Bot:               config.Sizing{Batch: 1000},
		AiEvent:           config.Sizing{Batch: 1000, Chunk: 500},
		Group:             config.Sizing{Batch: 200, Chunk: 1000},
		Replay:            config.Sizing{Batch: 500, Chunk: 500},
	}
}

func lines(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(`{"n":%d}`, i)
	}
	return out
}

func TestBotFlushInsertsTheBatchInOneCallAndTrimsIt(t *testing.T) {
	r := newRig()
	rows := lines(5)
	r.redis.Push(BotKey, rows...)
	obs := r.flush(t, NewBot(testConfig(), r.deps, Overrides{}))
	if obs.Result != flush.ResultSuccess || obs.Rows != 5 {
		t.Fatalf("observation %+v", obs)
	}
	if len(r.inserter.calls) != 1 || r.inserter.calls[0].table != "events_bots" || strings.Join(r.inserter.calls[0].lines, "|") != strings.Join(rows, "|") {
		t.Fatalf("calls %+v", r.inserter.calls)
	}
	if left := r.redis.List(BotKey); len(left) != 0 {
		t.Fatalf("left in Redis: %v", left)
	}
	if !obs.Phases.LRange.Set || !obs.Phases.CHInsert.Set || !obs.Phases.Trim.Set {
		t.Fatalf("phases %+v", obs.Phases)
	}
}

func TestEmptyListInsertsNothingAndStillTimesTheRead(t *testing.T) {
	r := newRig()
	obs := r.flush(t, NewBot(testConfig(), r.deps, Overrides{}))
	if len(r.inserter.calls) != 0 || obs.Rows != 0 || !obs.Phases.LRange.Set || obs.Phases.CHInsert.Set || obs.Phases.Trim.Set {
		t.Fatalf("observation %+v calls %d", obs, len(r.inserter.calls))
	}
}

func TestFlushDrainsABacklogBatchByBatch(t *testing.T) {
	r := newRig()
	r.redis.Push(BotKey, lines(2500)...)
	obs := r.flush(t, NewBot(testConfig(), r.deps, Overrides{}))
	if obs.Rows != 2500 || len(r.inserter.calls) != 3 || len(r.inserter.calls[0].lines) != 1000 || len(r.inserter.calls[2].lines) != 500 {
		t.Fatalf("rows %d, %d inserts", obs.Rows, len(r.inserter.calls))
	}
}

func TestAFailedInsertLeavesTheBatchForTheNextTick(t *testing.T) {
	r := newRig()
	rows := lines(5)
	r.redis.Push(BotKey, rows...)
	r.inserter.failAt, r.inserter.err = 1, errors.New("clickhouse down")
	obs := r.flush(t, NewBot(testConfig(), r.deps, Overrides{}))
	if obs.Result != flush.ResultError || len(r.redis.List(BotKey)) != 5 {
		t.Fatalf("result %s, %d rows left", obs.Result, len(r.redis.List(BotKey)))
	}
	if r.flush(t, NewBot(testConfig(), r.deps, Overrides{})).Result != flush.ResultSuccess || len(r.redis.List(BotKey)) != 0 {
		t.Fatal("the next tick must drain it")
	}
	if got := r.inserter.inserted(); len(got) != 5 {
		t.Fatalf("inserted %v", got)
	}
}

func TestRowsAppendedDuringTheFlushSurviveTheTrim(t *testing.T) {
	r := newRig()
	r.redis.Push(BotKey, lines(3)...)
	r.inserter.onInsert = func() { r.redis.Push(BotKey, "late-1", "late-2") }
	r.flush(t, NewBot(testConfig(), r.deps, Overrides{}))
	if left := r.redis.List(BotKey); len(left) != 2 || left[0] != "late-1" {
		t.Fatalf("left %v", left)
	}
}

func TestAChunkThatFailsLeavesTheWholeBatch(t *testing.T) {
	r := newRig()
	cfg := testConfig()
	cfg.AiEvent = config.Sizing{Batch: 10, Chunk: 3}
	r.deps.InsertConcurrency = 1
	r.redis.Push(AiKey, lines(10)...)
	r.inserter.failAt, r.inserter.err = 3, errors.New("boom")
	obs := r.flush(t, NewAiEvent(cfg, r.deps, Overrides{}))
	if obs.Result != flush.ResultError || len(r.redis.List(AiKey)) != 10 {
		t.Fatalf("result %s, %d left", obs.Result, len(r.redis.List(AiKey)))
	}
	if len(r.inserter.calls) != 2 {
		t.Fatalf("chunks 1 and 2 landed, 3 failed, 4 never started: %d inserts", len(r.inserter.calls))
	}
}

func TestChunkingAndTables(t *testing.T) {
	cases := []struct {
		name    string
		build   func(config.BuffersConfig, Deps, Overrides) flush.Flusher
		key     string
		table   string
		chunks  []int
		rows    int
		setting func(*config.BuffersConfig)
	}{
		{"ai-event", NewAiEvent, AiKey, "ai_events", []int{500, 500, 100}, 1100, func(c *config.BuffersConfig) { c.AiEvent.Batch = 2000 }},
		{"group", NewGroup, GroupKey, "groups", []int{150, 50}, 200, func(c *config.BuffersConfig) { c.Group.Chunk = 150 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig()
			r.deps.InsertConcurrency = 1
			cfg := testConfig()
			tc.setting(&cfg)
			r.redis.Push(tc.key, lines(tc.rows)...)
			r.flush(t, tc.build(cfg, r.deps, Overrides{}))
			var sizes []int
			for _, call := range r.inserter.calls {
				if call.table != tc.table {
					t.Fatalf("table %s", call.table)
				}
				sizes = append(sizes, len(call.lines))
			}
			if fmt.Sprint(sizes) != fmt.Sprint(tc.chunks) {
				t.Fatalf("chunks %v, want %v", sizes, tc.chunks)
			}
		})
	}
}

func TestAsyncInsertSettingsAreSentOnlyWhenTheFlagIsOn(t *testing.T) {
	r := newRig()
	r.redis.Push(BotKey, lines(1)...)
	r.flush(t, NewBot(testConfig(), r.deps, Overrides{}))
	if len(r.inserter.calls[0].settings) != 0 {
		t.Fatalf("settings without the flag: %v", r.inserter.calls[0].settings)
	}
	r.deps.AsyncInserts = true
	r.redis.Push(BotKey, lines(1)...)
	r.flush(t, NewBot(testConfig(), r.deps, Overrides{}))
	got := r.inserter.calls[1].settings
	if len(got) != 3 || got[0].Key != "async_insert" || got[0].Value != "1" || got[1].Key != "wait_for_async_insert" || got[1].Value != "0" || got[2].Key != "parallel_view_processing" {
		t.Fatalf("settings %v", got)
	}
}

func TestOverridesRenameTheListAndTheLock(t *testing.T) {
	r := newRig()
	r.redis.Push("wparity:bot", lines(2)...)
	bot := NewBot(testConfig(), r.deps, Overrides{Name: "wparity-bot", ListKey: "wparity:bot"})
	if bot.Name() != "wparity-bot" || bot.ListKey() != "wparity:bot" {
		t.Fatal("overrides ignored")
	}
	if obs := r.flush(t, bot); obs.Rows != 2 || obs.Buffer != "wparity-bot" {
		t.Fatalf("%+v", obs)
	}
}

// replay-buffer.test.ts
func chunkRow(index, payloadBytes int) string {
	return fmt.Sprintf(`{"project_id":"project-1","session_id":"session-1","chunk_index":%d,"started_at":"2026-10-08 09:00:00","ended_at":"2026-10-08 09:00:10","events_count":1,"is_full_snapshot":false,"payload":"%s"}`,
		index, strings.Repeat("x", payloadBytes))
}

func replayConfig(batch int) config.BuffersConfig {
	cfg := testConfig()
	cfg.Replay = config.Sizing{Batch: batch, Chunk: 500}
	return cfg
}

func TestReplayDrainsA1MBBacklogInByteBoundedBatchesInOneFlush(t *testing.T) {
	const oneMB, backlog, maxRowsPerBatch = 1024 * 1024, 30, 10
	r := newRig()
	r.deps.InsertConcurrency = 1
	for i := range backlog {
		r.redis.Push(ReplayKey, chunkRow(i, oneMB))
	}
	obs := r.flush(t, NewReplay(replayConfig(1000), r.deps, Overrides{}))
	if obs.Result != flush.ResultSuccess || len(r.redis.List(ReplayKey)) != 0 || obs.Rows != backlog {
		t.Fatalf("result %s, left %d, rows %d", obs.Result, len(r.redis.List(ReplayKey)), obs.Rows)
	}
	if len(r.inserter.calls) < 2 {
		t.Fatalf("a backlog of 30 MB must take several batches, took %d insert(s)", len(r.inserter.calls))
	}
	for i, row := range r.inserter.inserted() {
		if !strings.Contains(row, fmt.Sprintf(`"chunk_index":%d,`, i)) {
			t.Fatalf("row %d out of order", i)
		}
	}
	for _, call := range r.inserter.calls {
		if len(call.lines) > maxRowsPerBatch {
			t.Fatalf("a batch of %d rows", len(call.lines))
		}
	}
	if call := r.inserter.calls[0]; call.table != "session_replay_chunks" {
		t.Fatalf("table %s", call.table)
	}
}

func TestReplayReadsInSlicesOfTen(t *testing.T) {
	r := newRig()
	r.redis.Push(ReplayKey, lines(25)...)
	r.flush(t, NewReplay(replayConfig(1000), r.deps, Overrides{}))
	// 3 slices (10, 10, 5: short, so the end) in the one round, plus nothing more.
	if got := r.redis.CallCount("lrange"); got != 3 {
		t.Fatalf("%d LRANGE calls", got)
	}
	if len(r.inserter.calls) != 1 || len(r.inserter.calls[0].lines) != 25 {
		t.Fatalf("inserts %d", len(r.inserter.calls))
	}
}

func TestReplayKeepsTheBatchWhenTheInsertFailsAndDrainsItNextFlush(t *testing.T) {
	r := newRig()
	r.redis.Push(ReplayKey, lines(3)...)
	r.inserter.failAt, r.inserter.err = 1, errors.New("clickhouse down")
	f := NewReplay(replayConfig(1000), r.deps, Overrides{})
	if obs := r.flush(t, f); obs.Result != flush.ResultError || len(r.redis.List(ReplayKey)) != 3 {
		t.Fatalf("result %s, left %d", obs.Result, len(r.redis.List(ReplayKey)))
	}
	if obs := r.flush(t, f); obs.Result != flush.ResultSuccess || len(r.redis.List(ReplayKey)) != 0 || len(r.inserter.inserted()) != 3 {
		t.Fatalf("second flush: %+v", obs)
	}
}

func TestReplayBatchSizeCapsRowsAcrossSlices(t *testing.T) {
	r := newRig()
	r.deps.InsertConcurrency = 1
	r.redis.Push(ReplayKey, lines(35)...)
	r.flush(t, NewReplay(replayConfig(15), r.deps, Overrides{}))
	var sizes []int
	for _, call := range r.inserter.calls {
		sizes = append(sizes, len(call.lines))
	}
	if fmt.Sprint(sizes) != "[15 15 5]" {
		t.Fatalf("batches %v", sizes)
	}
}

// P2.10 #6: the TypeScript replay drain ignored a lost lock and could trim a
// batch nobody inserted. Here the lock is taken away after the first batch.
func TestReplayStopsWhenTheLockIsLost(t *testing.T) {
	r := newRig()
	r.deps.InsertConcurrency = 1
	r.redis.Push(ReplayKey, lines(30)...)
	f := NewReplay(replayConfig(10), r.deps, Overrides{})
	r.inserter.onInsert = func() {
		r.redis.Set("lock:replay", "someone-else")
		time.Sleep(40 * time.Millisecond) // long enough for the keepalive tick to notice
	}
	runner, _ := flush.NewRunner(f, flush.Deps{Redis: r.redis, Clock: lossClock{}, Paused: flush.NeverPaused, LockTTL: 30 * time.Second})
	if err := runner.TryFlush(context.Background(), flush.TriggerCron); err != nil {
		t.Fatal(err)
	}
	if len(r.inserter.calls) != 1 || len(r.redis.List(ReplayKey)) != 20 {
		t.Fatalf("%d inserts, %d rows left: a flush that lost its lock kept going", len(r.inserter.calls), len(r.redis.List(ReplayKey)))
	}
}
