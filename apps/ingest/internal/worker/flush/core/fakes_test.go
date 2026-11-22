package core

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/clock"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
)

var errClickHouseDown = errors.New("ClickHouse unavailable")

// memRedis is flush.MemoryRedis plus the commands the four flushes add.
type memRedis struct {
	*flush.MemoryRedis
	mutex     sync.Mutex
	ttls      map[string]time.Duration
	published []publication
	// commitErr and publishErr are returned once.
	commitErr  error
	publishErr error
	mgets      int
}

type publication struct{ Channel, Message string }

func newMemRedis() *memRedis {
	return &memRedis{MemoryRedis: flush.NewMemoryRedis(), ttls: map[string]time.Duration{}}
}

func (m *memRedis) MGet(_ context.Context, keys []string) ([]string, error) {
	m.mutex.Lock()
	m.mgets++
	m.mutex.Unlock()
	if len(keys) == 0 {
		return nil, errors.New("ERR wrong number of arguments for 'mget' command")
	}
	values := make([]string, len(keys))
	for i, key := range keys {
		values[i], _ = m.Get(key)
	}
	return values, nil
}

func (m *memRedis) CommitProfiles(ctx context.Context, writes []CacheWrite, ttl time.Duration, listKey string, consumed int) error {
	m.mutex.Lock()
	err := m.commitErr
	m.commitErr = nil
	m.mutex.Unlock()
	if err != nil {
		return err
	}
	for _, write := range writes {
		m.Set(write.Key, write.Value)
		m.mutex.Lock()
		m.ttls[write.Key] = ttl
		m.mutex.Unlock()
	}
	return m.LTrim(ctx, listKey, int64(consumed), -1)
}

func (m *memRedis) Publish(_ context.Context, channel, message string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.publishErr != nil {
		err := m.publishErr
		m.publishErr = nil
		return err
	}
	m.published = append(m.published, publication{channel, message})
	return nil
}

func (m *memRedis) publications() []publication {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return append([]publication(nil), m.published...)
}

type insertCall struct {
	Table    string
	Lines    []string
	Settings chclient.Settings
}

type queryCall struct {
	Statement string
	Params    []chclient.Param
	Settings  chclient.Settings
}

// fakeCH records what the flushes send and answers every query with rows.
type fakeCH struct {
	mutex    sync.Mutex
	inserts  []insertCall
	queries  []queryCall
	commands []queryCall
	rows     []json.RawMessage

	// failInsertAt, failCommandAt: the n-th call (1-based, counted when it starts) fails once.
	failInsertAt  int
	failCommandAt int
	queryErr      error
	insertCalls   int
	commandCalls  int
	// onInsert runs at the start of an insert, before it is counted; onQuery at the start of a query.
	onInsert func()
	onQuery  func()
	// inFlight and maxInFlight measure insert concurrency; insertDelay holds each insert.
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	insertDelay time.Duration
}

func (f *fakeCH) Insert(ctx context.Context, table string, lines []string, settings chclient.Settings) error {
	if f.onInsert != nil {
		f.onInsert()
	}
	running := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		seen := f.maxInFlight.Load()
		if running <= seen || f.maxInFlight.CompareAndSwap(seen, running) {
			break
		}
	}
	if f.insertDelay > 0 {
		select {
		case <-time.After(f.insertDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.insertCalls++
	if f.failInsertAt == f.insertCalls {
		return errClickHouseDown
	}
	f.inserts = append(f.inserts, insertCall{table, append([]string(nil), lines...), settings})
	return nil
}

func (f *fakeCH) Query(_ context.Context, statement string, params []chclient.Param, settings chclient.Settings) (*chclient.Result, error) {
	if f.onQuery != nil {
		f.onQuery()
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.queries = append(f.queries, queryCall{statement, params, settings})
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &chclient.Result{Data: f.rows, Rows: int64(len(f.rows))}, nil
}

func (f *fakeCH) CommandWithParams(_ context.Context, statement string, params []chclient.Param, settings chclient.Settings) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.commandCalls++
	if f.failCommandAt == f.commandCalls {
		return errClickHouseDown
	}
	f.commands = append(f.commands, queryCall{statement, params, settings})
	return nil
}

func (f *fakeCH) insertedLines() []string {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	var all []string
	for _, call := range f.inserts {
		all = append(all, call.Lines...)
	}
	return all
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

func (r *recorder) last() flush.Observation {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.all[len(r.all)-1]
}

// memList is the dead-letter list: LPUSH then LTRIM, with a failure to inject.
type memList struct {
	mutex   sync.Mutex
	entries []string
	err     error
	// listKey is the key of the last push.
	listKey string
}

func (m *memList) PushCapped(_ context.Context, key, value string, maxEntries int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.err != nil {
		return m.err
	}
	m.listKey = key
	m.entries = append([]string{value}, m.entries...)
	if len(m.entries) > maxEntries {
		m.entries = m.entries[:maxEntries]
	}
	return nil
}

func (m *memList) records(t *testing.T) []map[string]string {
	t.Helper()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	records := make([]map[string]string, len(m.entries))
	for i, entry := range m.entries {
		if err := json.Unmarshal([]byte(entry), &records[i]); err != nil {
			t.Fatal(err)
		}
	}
	return records
}

type fixedClock struct{}

func (fixedClock) Now() time.Time                             { return time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC) }
func (fixedClock) Sleep(context.Context, time.Duration) error { return nil }

type countingRejects struct {
	mutex    sync.Mutex
	rejected map[string]int
	failed   int
}

func (c *countingRejects) Rejected(buffer, reason string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.rejected == nil {
		c.rejected = map[string]int{}
	}
	c.rejected[buffer+"/"+reason]++
}

func (c *countingRejects) RejectFailed(string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.failed++
}

const testRejectCap = 100

type rig struct {
	redis   *memRedis
	ch      *fakeCH
	obs     *recorder
	deps    Deps
	dead    *memList
	counted *countingRejects
}

func newRig() *rig {
	redis, ch := newMemRedis(), &fakeCH{}
	dead, counted := &memList{}, &countingRejects{}
	deps := Deps{Redis: redis, CH: ch, Rejects: NewRedisRejects(dead, fixedClock{}, testRejectCap, counted)}
	return &rig{redis: redis, ch: ch, obs: &recorder{}, deps: deps, dead: dead, counted: counted}
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
	return r.obs.last()
}

// buffersConfig is what the worker reads from env, as the TypeScript flush
// reads it: unset knobs take the defaults.
func buffersConfig(t *testing.T, env map[string]string) config.BuffersConfig {
	t.Helper()
	base := map[string]string{"ROLE": "worker", "ENABLED_QUEUES": "cron"}
	for key, value := range env {
		base[key] = value
	}
	cfg, err := config.Load(func(name string) string { return base[name] })
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Buffers
}
