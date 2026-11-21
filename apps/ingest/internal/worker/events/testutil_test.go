package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/jsport"
)

var fixedNow = time.Date(2026, 11, 22, 12, 0, 0, 123_000_000, time.UTC)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// seqIDs mints the ids the TypeScript golden run minted: a counter in the last
// group of a v4-shaped UUID.
type seqIDs struct {
	mu      sync.Mutex
	counter int
}

func (s *seqIDs) NewID(length int) string { return strings.Repeat("r", length) }

func (s *seqIDs) NewUUID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counter++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", s.counter)
}

// fakeRedis is the Redis surface in memory, with the failure switches the
// durability tests need.
type fakeRedis struct {
	mu       sync.Mutex
	strings  map[string]string
	ttl      map[string]int64
	lists    map[string][]string
	sets     map[string]map[string]bool
	zsets    map[string]map[string]string
	commands []string

	failRPush   func(key string, values []string) error
	rpushGate   chan struct{}
	failGet     error
	failTx      error
	rpushCalls  [][]string
	existsCache map[string]bool
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{
		strings: map[string]string{}, ttl: map[string]int64{}, lists: map[string][]string{},
		sets: map[string]map[string]bool{}, zsets: map[string]map[string]string{}, existsCache: map[string]bool{},
	}
}

func (r *fakeRedis) record(parts ...string) {
	r.commands = append(r.commands, strings.Join(parts, " "))
}

func (r *fakeRedis) Get(_ context.Context, key string) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.record("GET", key)
	if r.failGet != nil {
		return "", false, r.failGet
	}
	value, ok := r.strings[key]
	return value, ok, nil
}

func (r *fakeRedis) Del(_ context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.record("DEL", key)
	delete(r.strings, key)
	return nil
}

func (r *fakeRedis) Exists(_ context.Context, key string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.record("EXISTS", key)
	return r.existsCache[key], nil
}

func (r *fakeRedis) RPush(_ context.Context, key string, values ...string) error {
	if r.rpushGate != nil {
		<-r.rpushGate
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.record("RPUSH", key)
	r.rpushCalls = append(r.rpushCalls, append([]string{key}, values...))
	if r.failRPush != nil {
		if err := r.failRPush(key, values); err != nil {
			return err
		}
	}
	r.lists[key] = append(r.lists[key], values...)
	return nil
}

func (r *fakeRedis) Transaction(_ context.Context, commands [][]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failTx != nil {
		return r.failTx
	}
	r.record("MULTI")
	for _, command := range commands {
		r.record(command...)
		switch command[0] {
		case "SET":
			r.strings[command[1]] = command[2]
			if len(command) == 5 && command[3] == "PX" {
				ttl, _ := strconv.ParseInt(command[4], 10, 64)
				r.ttl[command[1]] = ttl
			} else {
				delete(r.ttl, command[1])
			}
		case "ZADD":
			if r.zsets[command[1]] == nil {
				r.zsets[command[1]] = map[string]string{}
			}
			r.zsets[command[1]][command[3]] = command[2]
		case "SADD":
			if r.sets[command[1]] == nil {
				r.sets[command[1]] = map[string]bool{}
			}
			r.sets[command[1]][command[2]] = true
		case "DEL":
			delete(r.strings, command[1])
		case "RPUSH":
			r.lists[command[1]] = append(r.lists[command[1]], command[2:]...)
		default:
			return fmt.Errorf("fakeRedis: unsupported %s", command[0])
		}
	}
	r.record("EXEC")
	return nil
}

func (r *fakeRedis) list(key string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.lists[key]...)
}

func (r *fakeRedis) usedCommands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[string]bool{}
	for _, line := range r.commands {
		seen[strings.Fields(line)[0]] = true
	}
	var out []string
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// recordingMetrics counts what the path counts.
type recordingMetrics struct {
	mu      sync.Mutex
	started []string
	ended   []string
	added   map[string]int
	skipped []string
}

func (m *recordingMetrics) SessionStarted(kind string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = append(m.started, kind)
}

func (m *recordingMetrics) SessionEndEnqueued(source string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ended = append(m.ended, source)
}

func (m *recordingMetrics) BufferAdded(buffer string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.added == nil {
		m.added = map[string]int{}
	}
	m.added[buffer]++
}

func (m *recordingMetrics) BufferAddSkipped(buffer, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skipped = append(m.skipped, buffer+"/"+reason)
}

// fakeProjects excludes events by name per project and records first-event marks.
type fakeProjects struct {
	mu             sync.Mutex
	excludedByName map[string]string
	aiStoreContent map[string]bool
	markedFirst    []string
	failFilter     error
}

func (p *fakeProjects) IsExcluded(_ context.Context, projectID string, payload *jsjson.Value) (bool, error) {
	if p.failFilter != nil {
		return false, p.failFilter
	}
	name, ok := p.excludedByName[projectID]
	return ok && jsjson.JSString(payload.Get("name")) == name, nil
}

func (p *fakeProjects) MarkFirstEvent(_ context.Context, projectID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markedFirst = append(p.markedFirst, projectID)
	return nil
}

func (p *fakeProjects) GetAiStoreContent(_ context.Context, projectID string) (*bool, error) {
	value, ok := p.aiStoreContent[projectID]
	if !ok {
		return nil, nil
	}
	return &value, nil
}

// recordingNotifier keeps the payload text at the moment the check ran.
type recordingNotifier struct {
	mu       sync.Mutex
	payloads []string
	err      error
}

func (n *recordingNotifier) Check(_ context.Context, _ string, payload *jsjson.Value) error {
	text, _ := jsport.Stringify(payload)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.payloads = append(n.payloads, text)
	return n.err
}

type queuedJob struct {
	queue, name string
	data        string
	opts        bullmq.AddOptions
}

type recordingQueue struct {
	mu   sync.Mutex
	jobs []queuedJob
	err  error
}

func (q *recordingQueue) Add(_ context.Context, queue, name string, data []byte, opts bullmq.AddOptions) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return "", q.err
	}
	q.jobs = append(q.jobs, queuedJob{queue: queue, name: name, data: string(data), opts: opts})
	return strconv.Itoa(len(q.jobs)), nil
}

// rig is a Handler over in-memory dependencies.
type rig struct {
	redis    *fakeRedis
	events   *EventBuffer
	sessions *SessionBuffer
	profiles *ProfileBuffer
	projects *fakeProjects
	notifier *recordingNotifier
	queue    *recordingQueue
	metrics  *recordingMetrics
	ids      *seqIDs
	handler  *Handler
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{
		redis:    newFakeRedis(),
		projects: &fakeProjects{excludedByName: map[string]string{}, aiStoreContent: map[string]bool{}},
		notifier: &recordingNotifier{},
		queue:    &recordingQueue{},
		metrics:  &recordingMetrics{},
		ids:      &seqIDs{},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clock := fixedClock{now: fixedNow}
	r.events = NewEventBuffer(r.redis, EventBufferConfig{}, r.metrics, logger)
	r.sessions = NewSessionBuffer(r.redis, clock, DefaultSessionTimeout, logger)
	r.profiles = NewProfileBuffer(r.redis, r.metrics, logger)
	r.handler = NewHandler(HandlerConfig{
		Events: r.events, Sessions: r.sessions, Profiles: r.profiles, AI: NewAIEventBuffer(r.redis, logger),
		Projects: r.projects, Notifier: r.notifier, Queue: r.queue, Clock: clock, IDs: r.ids,
		Metrics: r.metrics, Logger: logger,
	})
	return r
}

// flush makes everything buffered durable, as the batch gate does.
func (r *rig) flush(t *testing.T) {
	t.Helper()
	if err := r.events.FlushPendingOrThrow(context.Background(), 0); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

var errRedisDown = errors.New("redis down")
