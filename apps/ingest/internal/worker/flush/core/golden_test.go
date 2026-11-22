package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/flush"
)

// The goldens are recorded from the real TypeScript buffers by
// parity/golden-flushcore.ts: each scenario's input, and what the buffer sent to
// ClickHouse, published and left in Redis. The Go flush gets the same input.

type goldenSettings = [][2]string

type goldenInsert struct {
	Table    string         `json:"table"`
	Lines    []string       `json:"lines"`
	Settings goldenSettings `json:"settings"`
}

type goldenCall struct {
	Query      *string        `json:"query"`
	Params     [][2]any       `json:"params"`
	Settings   goldenSettings `json:"settings"`
	Hash       string         `json:"hash"`
	ParamCount int            `json:"paramCount"`
}

type goldenExpect struct {
	Result    string         `json:"result"`
	Rows      int            `json:"rows"`
	Err       string         `json:"err"`
	Inserts   []goldenInsert `json:"inserts"`
	Queries   []goldenCall   `json:"queries"`
	Commands  []goldenCall   `json:"commands"`
	Published []string       `json:"published"`
	Remaining []string       `json:"remaining"`
	Cache     map[string]struct {
		Value string `json:"value"`
	} `json:"cache"`
	TTL int `json:"ttlSeconds"`
}

type goldenScenario struct {
	Name          string            `json:"name"`
	Kind          string            `json:"kind"`
	Sizing        map[string]any    `json:"sizing"`
	AsyncInserts  bool              `json:"asyncInserts"`
	Clustered     bool              `json:"clustered"`
	List          []string          `json:"list"`
	Cache         map[string]string `json:"cache"`
	ChRows        []string          `json:"chRows"`
	ChQueryFails  bool              `json:"chQueryFails"`
	FailInsertAt  int               `json:"failInsertAt"`
	FailCommandAt int               `json:"failCommandAt"`
	Expect        goldenExpect      `json:"expect"`
}

func loadGoldens(t *testing.T) []goldenScenario {
	t.Helper()
	raw, err := os.ReadFile("testdata/flush.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Scenarios []goldenScenario `json:"scenarios"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	return file.Scenarios
}

// sizingEnv maps a scenario's sizing keys to the environment variables the
// worker reads.
func sizingEnv(t *testing.T, kind string, sizing map[string]any, async bool) map[string]string {
	t.Helper()
	names := map[string]map[string]string{
		"event":    {"batchSize": "EVENT_BUFFER_BATCH_SIZE", "chunkSize": "EVENT_BUFFER_CHUNK_SIZE"},
		"session":  {"batchSize": "SESSION_BUFFER_BATCH_SIZE", "chunkSize": "SESSION_BUFFER_CHUNK_SIZE", "squash": "SESSION_BUFFER_SQUASH"},
		"profile":  {"batchSize": "PROFILE_BUFFER_BATCH_SIZE", "chunkSize": "PROFILE_BUFFER_CHUNK_SIZE", "fetchChunkSize": "PROFILE_BUFFER_FETCH_CHUNK_SIZE"},
		"backfill": {"batchSize": "PROFILE_BACKFILL_BUFFER_BATCH_SIZE"},
	}[kind]
	env := map[string]string{}
	for key, value := range sizing {
		name, ok := names[key]
		if !ok {
			t.Fatalf("unknown sizing key %q", key)
		}
		env[name] = fmt.Sprint(value)
	}
	if async {
		env["BUFFER_ASYNC_INSERTS"] = "1"
	}
	return env
}

func goldenSettingsOf(settings chclient.Settings) goldenSettings {
	out := goldenSettings{}
	for _, setting := range settings {
		out = append(out, [2]string{setting.Key, setting.Value})
	}
	return out
}

// unescapeParam undoes chclient.FormatString: the golden holds the raw value.
func unescapeParam(value string) string {
	return strings.NewReplacer("\\\\", "\\", "\\t", "\t", "\\n", "\n", "\\r", "\r", "\\'", "'").Replace(value)
}

func digest(statement string, params []chclient.Param) string {
	pairs := make([]string, len(params))
	for i, param := range params {
		pairs[i] = param.Name + "=" + unescapeParam(param.Value)
	}
	sum := sha256.Sum256([]byte(statement + "\x00" + strings.Join(pairs, "\x01")))
	return hex.EncodeToString(sum[:])
}

func sortedKey(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	return lines[0]
}

func TestGoldensFromTheTypeScriptBuffers(t *testing.T) {
	for _, scenario := range loadGoldens(t) {
		t.Run(scenario.Name, func(t *testing.T) {
			cfg := buffersConfig(t, sizingEnv(t, scenario.Kind, scenario.Sizing, scenario.AsyncInserts))
			rig := newRig()
			rig.deps.Clustered = scenario.Clustered
			// A real insert takes time, so concurrent chunks all start before the first fails.
			rig.ch.insertDelay = 3 * time.Millisecond
			rig.ch.failInsertAt, rig.ch.failCommandAt = scenario.FailInsertAt, scenario.FailCommandAt
			if scenario.ChQueryFails {
				rig.ch.queryErr = errClickHouseDown
			}
			for _, row := range scenario.ChRows {
				rig.ch.rows = append(rig.ch.rows, json.RawMessage(row))
			}
			for key, value := range scenario.Cache {
				rig.redis.Set(key, value)
			}

			var flusher flush.Flusher
			var listKey string
			switch scenario.Kind {
			case "event":
				flusher, listKey = NewEvent(cfg, rig.deps, Overrides{}), EventKey
			case "session":
				flusher, listKey = NewSession(cfg, rig.deps, Overrides{}), SessionKey
			case "profile":
				flusher, listKey = NewProfile(cfg, rig.deps, Overrides{}), ProfileKey
			case "backfill":
				flusher, listKey = NewProfileBackfill(cfg, rig.deps, Overrides{}), ProfileBackfillKey
			}
			rig.redis.Push(listKey, scenario.List...)

			observation := rig.flush(t, flusher)
			want := scenario.Expect

			if scenario.Name == "profile: only unreadable rows" {
				// TypeScript sends MGET with no keys, which Redis refuses, and the rows
				// block the list for good. Go trims them.
				if observation.Result != flush.ResultSuccess || len(rig.redis.List(listKey)) != 0 || len(rig.ch.inserts) != 0 {
					t.Fatalf("observation %+v, remaining %v", observation, rig.redis.List(listKey))
				}
				return
			}

			if string(observation.Result) != want.Result {
				t.Fatalf("result %s (%v), TypeScript %s (%s)", observation.Result, observation.Err, want.Result, want.Err)
			}
			if want.Result == "success" && observation.Rows != want.Rows {
				t.Errorf("rows %d, TypeScript %d", observation.Rows, want.Rows)
			}
			if got := rig.redis.List(listKey); !reflect.DeepEqual(append([]string{}, got...), append([]string{}, want.Remaining...)) {
				t.Errorf("rows left in Redis differ: %d, TypeScript %d", len(got), len(want.Remaining))
			}

			compareInserts(t, scenario, rig.ch, want)
			compareCalls(t, "query", rig.ch.queries, want.Queries, scenario.FailInsertAt != 0)
			if scenario.Kind == "backfill" && (len(want.Commands) > 1 || scenario.FailCommandAt != 0) {
				compareBackfillAtTypeScriptChunking(t, scenario, rig.ch, want)
			} else {
				compareCalls(t, "command", rig.ch.commands, want.Commands, false)
			}

			var messages []string
			for _, publication := range rig.redis.publications() {
				if publication.Channel != EventsBatchChannel {
					t.Errorf("published on %q", publication.Channel)
				}
				messages = append(messages, publication.Message)
			}
			// TypeScript sends these from an un-awaited publish on a connection that may
			// still be opening, so their order is not stable; Go sends them in order.
			sort.Strings(messages)
			sort.Strings(want.Published)
			if !reflect.DeepEqual(append([]string{}, messages...), append([]string{}, want.Published...)) {
				t.Errorf("published\n go: %q\n ts: %q", messages, want.Published)
			}

			for key, entry := range want.Cache {
				got, ok := rig.redis.Get(key)
				if !ok || got != entry.Value {
					t.Errorf("%s\n go: %q\n ts: %q", key, got, entry.Value)
				}
				if ttl := rig.redis.ttls[key]; ttl != time.Duration(want.TTL)*time.Second {
					t.Errorf("%s ttl %v, want %ds", key, ttl, want.TTL)
				}
			}
			for key := range rig.redis.ttls {
				if _, ok := want.Cache[key]; !ok {
					t.Errorf("Go cached %s, TypeScript did not", key)
				}
			}
		})
	}
}

func compareInserts(t *testing.T, scenario goldenScenario, ch *fakeCH, want goldenExpect) {
	t.Helper()
	got := append([]insertCall(nil), ch.inserts...)
	if len(got) != len(want.Inserts) {
		t.Errorf("%d inserts, TypeScript %d", len(got), len(want.Inserts))
		return
	}
	if scenario.FailInsertAt != 0 {
		// Which chunk is "the second call" is a race between the concurrent
		// inserts; what must agree is how many landed.
		return
	}
	// Chunks run concurrently: compare them in a stable order.
	sort.Slice(got, func(i, j int) bool { return sortedKey(got[i].Lines) < sortedKey(got[j].Lines) })
	expected := append([]goldenInsert(nil), want.Inserts...)
	sort.Slice(expected, func(i, j int) bool { return sortedKey(expected[i].Lines) < sortedKey(expected[j].Lines) })
	for i := range got {
		if got[i].Table != expected[i].Table {
			t.Errorf("insert %d into %s, TypeScript %s", i, got[i].Table, expected[i].Table)
		}
		if !reflect.DeepEqual(append([]string{}, got[i].Lines...), append([]string{}, expected[i].Lines...)) {
			t.Errorf("insert %d lines differ\n go: %q\n ts: %q", i, got[i].Lines, expected[i].Lines)
		}
		if !reflect.DeepEqual(goldenSettingsOf(got[i].Settings), append(goldenSettings{}, expected[i].Settings...)) {
			t.Errorf("insert %d settings %v, TypeScript %v", i, got[i].Settings, expected[i].Settings)
		}
	}
}

func compareCalls(t *testing.T, label string, got []queryCall, want []goldenCall, countOnly bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%d %ss, TypeScript %d", len(got), label, len(want))
		return
	}
	if countOnly {
		return
	}
	// Fetch chunks run two at a time: compare in a stable order.
	key := func(statement string, params []chclient.Param) string {
		return statement + fmt.Sprint(params)
	}
	sort.Slice(got, func(i, j int) bool {
		return key(got[i].Statement, got[i].Params) < key(got[j].Statement, got[j].Params)
	})
	type expectedCall struct {
		call goldenCall
		key  string
	}
	expected := make([]expectedCall, len(want))
	for i, call := range want {
		expected[i] = expectedCall{call: call}
		if call.Query != nil {
			var params []chclient.Param
			for _, pair := range call.Params {
				params = append(params, chclient.Param{Name: pair[0].(string), Value: chclient.FormatString(fmt.Sprint(pair[1]))})
			}
			expected[i].key = key(*call.Query, params)
		}
	}
	if len(want) > 0 && want[0].Query != nil {
		sort.Slice(expected, func(i, j int) bool { return expected[i].key < expected[j].key })
	}
	for i := range got {
		call := expected[i].call
		if !reflect.DeepEqual(goldenSettingsOf(got[i].Settings), append(goldenSettings{}, call.Settings...)) && !(call.Settings == nil && len(got[i].Settings) == 0) {
			t.Errorf("%s %d settings %v, TypeScript %v", label, i, got[i].Settings, call.Settings)
		}
		if call.Query == nil {
			if len(got[i].Params) != call.ParamCount {
				t.Errorf("%s %d: %d params, TypeScript %d", label, i, len(got[i].Params), call.ParamCount)
			}
			if h := digest(got[i].Statement, got[i].Params); h != call.Hash {
				t.Errorf("%s %d differs from the TypeScript (hash %s, want %s)", label, i, h, call.Hash)
			}
			continue
		}
		if got[i].Statement != *call.Query {
			t.Errorf("%s %d statement\n go: %q\n ts: %q", label, i, got[i].Statement, *call.Query)
		}
		if len(got[i].Params) != len(call.Params) {
			t.Errorf("%s %d: %d params, TypeScript %d", label, i, len(got[i].Params), len(call.Params))
			continue
		}
		for j, param := range got[i].Params {
			name, raw := call.Params[j][0].(string), fmt.Sprint(call.Params[j][1])
			if param.Name != name || param.Value != chclient.FormatString(raw) {
				t.Errorf("%s %d param %d: %s=%q, TypeScript %s=%q", label, i, j, param.Name, param.Value, name, raw)
			}
		}
	}
}

// compareBackfillAtTypeScriptChunking checks the statements the TypeScript sends
// for a batch that spans several chunks of 500, which ClickHouse refuses beyond
// about 245 sessions (see backfillChunkSize): Go renders them identically, and
// sends the same sessions in smaller statements.
func compareBackfillAtTypeScriptChunking(t *testing.T, scenario goldenScenario, ch *fakeCH, want goldenExpect) {
	t.Helper()
	entries, _ := parseBackfillEntries(scenario.List[:min(len(scenario.List), batchOf(t, scenario))])
	chunks := flush.Chunks(entries, 500)
	if scenario.FailCommandAt != 0 {
		chunks = chunks[:len(want.Commands)]
	}
	if len(chunks) != len(want.Commands) {
		t.Fatalf("%d chunks, TypeScript sent %d statements", len(chunks), len(want.Commands))
	}
	for i, chunk := range chunks {
		statement, params := BackfillStatement(scenario.Clustered, chunk)
		call := want.Commands[i]
		if call.Query != nil || len(params) != call.ParamCount || digest(statement, params) != call.Hash {
			t.Errorf("statement %d differs from the TypeScript (hash %s, want %s)", i, digest(statement, params), call.Hash)
		}
	}
	// The same sessions, in chunks of backfillChunkSize.
	var sent int
	for _, call := range ch.commands {
		sent += len(call.Params) / 4
	}
	if scenario.FailCommandAt == 0 && sent != len(entries) {
		t.Errorf("Go sent %d sessions, want %d", sent, len(entries))
	}
}

func batchOf(t *testing.T, scenario goldenScenario) int {
	t.Helper()
	if size, ok := scenario.Sizing["batchSize"]; ok {
		return int(size.(float64))
	}
	return 1000
}
