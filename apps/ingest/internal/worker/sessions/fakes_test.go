package sessions

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/notify"
)

// memoryRedis is Redis in memory, with the cleanup script's semantics.
type memoryRedis struct {
	mutex   sync.Mutex
	strings map[string]string
	sets    map[string]map[string]bool
	zsets   map[string]map[string]int64
	lists   map[string][]string
	ttls    map[string]time.Duration
	fail    map[string]error
	calls   []string
}

func newMemoryRedis() *memoryRedis {
	return &memoryRedis{
		strings: map[string]string{}, sets: map[string]map[string]bool{}, zsets: map[string]map[string]int64{},
		lists: map[string][]string{}, ttls: map[string]time.Duration{}, fail: map[string]error{},
	}
}

func (r *memoryRedis) enter(command string) error {
	r.calls = append(r.calls, command)
	return r.fail[command]
}

func (r *memoryRedis) Get(_ context.Context, key string) (string, bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("get"); err != nil {
		return "", false, err
	}
	value, ok := r.strings[key]
	return value, ok, nil
}

func (r *memoryRedis) SetNXEx(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("setnx"); err != nil {
		return false, err
	}
	if _, taken := r.strings[key]; taken {
		return false, nil
	}
	r.strings[key], r.ttls[key] = value, ttl
	return true, nil
}

func (r *memoryRedis) SetEx(_ context.Context, key, value string, ttl time.Duration) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.strings[key], r.ttls[key] = value, ttl
	return nil
}

func (r *memoryRedis) Del(_ context.Context, key string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.strings, key)
	return nil
}

func (r *memoryRedis) SMembers(_ context.Context, key string) ([]string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("smembers"); err != nil {
		return nil, err
	}
	var out []string
	for member := range r.sets[key] {
		out = append(out, member)
	}
	sort.Strings(out)
	return out, nil
}

func (r *memoryRedis) SRem(_ context.Context, key, member string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.sets[key], member)
	return nil
}

func (r *memoryRedis) ZRangeByScore(_ context.Context, key string, max int64, limit int) ([]string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	type scored struct {
		member string
		score  int64
	}
	var entries []scored
	for member, score := range r.zsets[key] {
		if score <= max {
			entries = append(entries, scored{member, score})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score != entries[j].score {
			return entries[i].score < entries[j].score
		}
		return entries[i].member < entries[j].member
	})
	var out []string
	for _, entry := range entries {
		if len(out) == limit {
			break
		}
		out = append(out, entry.member)
	}
	return out, nil
}

func (r *memoryRedis) ZCard(_ context.Context, key string) (int64, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return int64(len(r.zsets[key])), nil
}

func (r *memoryRedis) ZRem(_ context.Context, key, member string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.zsets[key], member)
	return nil
}

func (r *memoryRedis) RPush(_ context.Context, key, value string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("rpush"); err != nil {
		return err
	}
	r.lists[key] = append(r.lists[key], value)
	return nil
}

// Cleanup is the Lua of cleanupLua.
func (r *memoryRedis) Cleanup(_ context.Context, keys [3]string, sessionID, deviceID string) (int64, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("cleanup"); err != nil {
		return 0, err
	}
	if current, ok := r.strings[keys[0]]; ok {
		if parsed, err := jsport.ParseJSON(current); err == nil && parsed.IsObject() {
			if id := parsed.Get("id"); id != nil && (id.Kind != jsjson.String || id.Str != sessionID) {
				return 0, nil
			}
		}
	}
	delete(r.strings, keys[0])
	delete(r.zsets[keys[1]], deviceID)
	if keys[2] != "" {
		delete(r.strings, keys[2])
	}
	return 1, nil
}

func (r *memoryRedis) putSession(projectID, deviceID, blob string, wallclock int64) {
	r.strings[sessionKey(projectID, deviceID)] = blob
	if r.zsets[wallclockKey(projectID)] == nil {
		r.zsets[wallclockKey(projectID)] = map[string]int64{}
	}
	r.zsets[wallclockKey(projectID)][deviceID] = wallclock
	if r.sets[ProjectsKey] == nil {
		r.sets[ProjectsKey] = map[string]bool{}
	}
	r.sets[ProjectsKey][projectID] = true
}

type createdEvent struct {
	payload   *jsjson.Obj
	createdAt jsport.Date
}

type fakeEvents struct {
	created []createdEvent
	err     error
}

func (f *fakeEvents) CreateEvent(_ context.Context, payload *jsjson.Obj, createdAt jsport.Date) (*jsjson.Obj, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, createdEvent{payload, createdAt})
	row := jsjson.NewObj()
	row.Set("id", str("event-1"))
	row.Set("name", payload.Get("name"))
	row.Set("device_id", payload.Get("deviceId"))
	row.Set("profile_id", payload.Get("profileId"))
	row.Set("project_id", payload.Get("projectId"))
	row.Set("session_id", payload.Get("sessionId"))
	row.Set("properties", payload.Get("properties"))
	row.Set("created_at", str(createdAt.LuxonFormat()))
	row.Set("path", payload.Get("path"))
	row.Set("country", str(""))
	row.Set("imported_at", jsjson.NewNull())
	return row, nil
}

type fakeFunnel struct {
	calls  [][]notify.SessionEvent
	err    error
	called string
}

func (f *fakeFunnel) CheckSessionEnd(_ context.Context, requestID string, events []notify.SessionEvent) error {
	f.called = requestID
	f.calls = append(f.calls, events)
	return f.err
}

type fakeRules struct {
	rules []notify.Rule
	err   error
}

func (f fakeRules) ByProject(context.Context, string) ([]notify.Rule, error) { return f.rules, f.err }

type fakeClickHouse struct {
	rows       []string
	statements []string
	params     [][]chclient.Param
	err        error
}

func (f *fakeClickHouse) Query(_ context.Context, statement string, params []chclient.Param, _ chclient.Settings) (*chclient.Result, error) {
	f.statements = append(f.statements, statement)
	f.params = append(f.params, params)
	if f.err != nil {
		return nil, f.err
	}
	result := &chclient.Result{}
	for _, row := range f.rows {
		result.Data = append(result.Data, []byte(row))
	}
	return result, nil
}

var errBoom = errors.New("boom")
