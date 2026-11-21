package config

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func workerEnv(extra map[string]string) func(string) string {
	base := map[string]string{"ROLE": "worker", "ENABLED_QUEUES": "cron"}
	for key, value := range extra {
		base[key] = value
	}
	return func(name string) string { return base[name] }
}

type goldenSizing struct {
	BatchSize  *int  `json:"batchSize"`
	ChunkSize  *int  `json:"chunkSize"`
	TTLSeconds *int  `json:"ttlSeconds"`
	FetchChunk *int  `json:"fetchChunkSize"`
	Squash     *bool `json:"squash"`
}

type goldenBuffers struct {
	AsyncInserts      bool         `json:"asyncInserts"`
	InsertConcurrency *int         `json:"chInsertConcurrency"`
	Bot               goldenSizing `json:"bot"`
	Event             goldenSizing `json:"event"`
	Group             goldenSizing `json:"group"`
	AiEvent           goldenSizing `json:"aiEvent"`
	Profile           goldenSizing `json:"profile"`
	ProfileBackfill   goldenSizing `json:"profileBackfill"`
	Replay            goldenSizing `json:"replay"`
	Session           goldenSizing `json:"session"`
}

// expect fails unless got is what TypeScript read, or the Go default when
// TypeScript read nothing (its buffers apply the default themselves).
func expect(t *testing.T, label string, got int, ts *int, fallback int) {
	t.Helper()
	want := fallback
	if ts != nil {
		want = *ts
	}
	if got != want {
		t.Errorf("%s = %d, want %d", label, got, want)
	}
}

// TestBufferEnvMatchesTheTypeScriptLoader runs every env set of
// parity/golden-chclient.ts through loadConfig and through Load.
func TestBufferEnvMatchesTheTypeScriptLoader(t *testing.T) {
	raw, err := os.ReadFile("testdata/flushenv.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Env     map[string]string `json:"env"`
		Buffers goldenBuffers     `json:"buffers"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		cfg, err := Load(workerEnv(tc.Env))
		if err != nil {
			t.Fatalf("%v: %v", tc.Env, err)
		}
		got, want := cfg.Buffers, tc.Buffers
		label := func(name string) string { return name + " for " + strings.TrimSpace(mustJSON(tc.Env)) }
		if got.AsyncInserts != want.AsyncInserts {
			t.Errorf("%s: %v, TypeScript %v", label("AsyncInserts"), got.AsyncInserts, want.AsyncInserts)
		}
		expect(t, label("InsertConcurrency"), got.InsertConcurrency, want.InsertConcurrency, defaultInsertConcurrency)
		expect(t, label("Bot.Batch"), got.Bot.Batch, want.Bot.BatchSize, defaultBotBatch)
		expect(t, label("Event.Batch"), got.Event.Batch, want.Event.BatchSize, defaultEventBatch)
		expect(t, label("Event.Chunk"), got.Event.Chunk, want.Event.ChunkSize, defaultEventChunk)
		expect(t, label("Group.Batch"), got.Group.Batch, want.Group.BatchSize, defaultGroupBatch)
		expect(t, label("Group.Chunk"), got.Group.Chunk, want.Group.ChunkSize, defaultGroupChunk)
		expect(t, label("GroupTTL"), int(got.GroupTTL/time.Second), want.Group.TTLSeconds, defaultGroupTTL)
		expect(t, label("AiEvent.Batch"), got.AiEvent.Batch, want.AiEvent.BatchSize, defaultAiEventBatch)
		expect(t, label("AiEvent.Chunk"), got.AiEvent.Chunk, want.AiEvent.ChunkSize, defaultAiEventChunk)
		expect(t, label("Profile.Batch"), got.Profile.Batch, want.Profile.BatchSize, defaultProfileBatch)
		expect(t, label("Profile.Chunk"), got.Profile.Chunk, want.Profile.ChunkSize, defaultProfileChunk)
		expect(t, label("ProfileTTL"), int(got.ProfileTTL/time.Second), want.Profile.TTLSeconds, defaultProfileTTL)
		expect(t, label("ProfileFetchChunk"), got.ProfileFetchChunk, want.Profile.FetchChunk, defaultProfileFetchChunk)
		expect(t, label("ProfileBackfillSize"), got.ProfileBackfillSize, want.ProfileBackfill.BatchSize, defaultProfileBackfillSize)
		expect(t, label("Replay.Batch"), got.Replay.Batch, want.Replay.BatchSize, defaultReplayBatch)
		expect(t, label("Replay.Chunk"), got.Replay.Chunk, want.Replay.ChunkSize, defaultReplayChunk)
		expect(t, label("Session.Batch"), got.Session.Batch, want.Session.BatchSize, defaultSessionBatch)
		// A session's chunk defaults to its batch.
		expect(t, label("Session.Chunk"), got.Session.Chunk, want.Session.ChunkSize, got.Session.Batch)
		if want.Session.Squash != nil && got.SessionSquash != *want.Session.Squash {
			t.Errorf("%s: %v, TypeScript %v", label("SessionSquash"), got.SessionSquash, *want.Session.Squash)
		}
	}
}

func mustJSON(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

func TestCronJobNamesAreTheBunCronQueueJobs(t *testing.T) {
	raw, err := os.ReadFile("testdata/cronjobs.json")
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, CronJobNames) {
		t.Fatalf("CronJobNames\n go: %v\n ts: %v", CronJobNames, want)
	}
}

func TestGoWorkerCronJobs(t *testing.T) {
	cfg, err := Load(workerEnv(nil))
	if err != nil || len(cfg.GoWorkerCronJobs) != 0 {
		t.Fatalf("the default runs nothing: %v %v", cfg.GoWorkerCronJobs, err)
	}
	cfg, err = Load(workerEnv(map[string]string{"GO_WORKER_CRON_JOBS": " flushBots , flushReplay ,"}))
	if err != nil || !reflect.DeepEqual(cfg.GoWorkerCronJobs, []string{"flushBots", "flushReplay"}) {
		t.Fatalf("%v %v", cfg.GoWorkerCronJobs, err)
	}
	if cfg, err = Load(workerEnv(map[string]string{"GO_WORKER_CRON_JOBS": "  "})); err != nil || len(cfg.GoWorkerCronJobs) != 0 {
		t.Fatalf("blank is unset: %v %v", cfg.GoWorkerCronJobs, err)
	}
	_, err = Load(workerEnv(map[string]string{"GO_WORKER_CRON_JOBS": "flushBots,flushBotz"}))
	if err == nil || !strings.Contains(err.Error(), `GO_WORKER_CRON_JOBS: unknown cron job "flushBotz"`) || !strings.Contains(err.Error(), "flushEvents") {
		t.Fatalf("unknown name must fail the load, naming it and the accepted values: %v", err)
	}
}

func TestClickHouseEnv(t *testing.T) {
	cfg, err := Load(workerEnv(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := ClickHouseConfig{RequestCompression: true, MaxOpenConnections: 50, RequestTimeout: 30 * time.Second, UnhealthyMark: 5 * time.Second}
	if !reflect.DeepEqual(cfg.ClickHouse, want) {
		t.Fatalf("defaults %+v", cfg.ClickHouse)
	}
	cfg, err = Load(workerEnv(map[string]string{
		"CLICKHOUSE_CLUSTER": "1", "CLICKHOUSE_REQUEST_COMPRESSION": "false", "CLICKHOUSE_MAX_OPEN_CONNECTIONS": "0",
		"CLICKHOUSE_REQUEST_TIMEOUT_MS": "10", "CLICKHOUSE_UNHEALTHY_MARK_MS": "-5",
		"CLICKHOUSE_SETTINGS_REMOVE_CONVERT_ANY_JOIN": "x",
		"CLICKHOUSE_SETTINGS":                         `{"b":2,"a":"x","b":3,"c":true,"d":null,"e":{"f":1},"g":1.5}`,
	}))
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.ClickHouse
	if !got.Cluster || got.RequestCompression || got.MaxOpenConnections != 1 || got.RequestTimeout != time.Second || got.UnhealthyMark != 0 || !got.RemoveConvertAnyJoin {
		t.Fatalf("%+v", got)
	}
	wantSettings := []Setting{{"b", "3"}, {"a", "x"}, {"c", "1"}, {"g", "1.5"}}
	if !reflect.DeepEqual(got.Settings, wantSettings) {
		t.Fatalf("settings %v, want %v", got.Settings, wantSettings)
	}
	for _, bad := range []string{"not json", "[1]", "42", `{"a":1`, ""} {
		cfg, _ := Load(workerEnv(map[string]string{"CLICKHOUSE_SETTINGS": bad}))
		if len(cfg.ClickHouse.Settings) != 0 {
			t.Errorf("%q gave %v", bad, cfg.ClickHouse.Settings)
		}
	}
}

func TestParseIntPrefixIsNumberParseInt(t *testing.T) {
	cases := map[string]int{"30000abc": 30000, "  7": 7, "-4": -4, "+9": 9, "12.9": 12, "0x10": 0}
	for text, want := range cases {
		if got, ok := parseIntPrefix(text); !ok || got != want {
			t.Errorf("%q = %d %v, want %d", text, got, ok, want)
		}
	}
	for _, text := range []string{"abc", "", "-", ".5"} {
		if _, ok := parseIntPrefix(text); ok {
			t.Errorf("%q parsed", text)
		}
	}
}
