package sessions

import (
	"context"
	"strings"
	"testing"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/wire"
)

type fixedNow time.Time

func (f fixedNow) Now() time.Time { return time.Time(f) }

type fakeQueue struct {
	requestIDs []string
	events     []*jsjson.Obj
	closed     []*jsjson.Obj
	err        error
}

func (q *fakeQueue) EnqueueSessionEnd(_ context.Context, requestID string, event, closed *jsjson.Obj) error {
	if q.err != nil {
		return q.err
	}
	q.requestIDs = append(q.requestIDs, requestID)
	q.events, q.closed = append(q.events, event), append(q.closed, closed)
	return nil
}

type countingMetrics struct {
	NopMetrics
	reaped, orphans int
	vacuumed        map[string]int
}

func (m *countingMetrics) Reaped()       { m.reaped++ }
func (m *countingMetrics) ReaperOrphan() { m.orphans++ }
func (m *countingMetrics) Vacuumed(reason string) {
	if m.vacuumed == nil {
		m.vacuumed = map[string]int{}
	}
	m.vacuumed[reason]++
}

var now = time.UnixMilli(10_000_000_000)

func newReaper(redis *memoryRedis, queue *fakeQueue) (*Reaper, *countingMetrics) {
	metrics := &countingMetrics{}
	return &Reaper{
		Redis: redis, Queue: queue, Clock: fixedNow(now), IDs: wire.RandomIDs{}, Metrics: metrics,
		Config: ReaperConfig{Enabled: true, BatchSize: 100, Deadman: 30 * time.Minute},
	}, metrics
}

func TestReaperEnqueuesIdleSessionsOnly(t *testing.T) {
	redis := newMemoryRedis()
	idle := now.UnixMilli() - (31 * time.Minute).Milliseconds()
	redis.putSession("p1", "d1", blobEnded, idle)
	redis.putSession("p1", "d2", blobEnded, now.UnixMilli()-1000)
	queue := &fakeQueue{}
	reaper, metrics := newReaper(redis, queue)
	if err := reaper.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(queue.closed) != 1 || metrics.reaped != 1 {
		t.Fatalf("closed = %d, reaped = %d", len(queue.closed), metrics.reaped)
	}
	payload := toJSON((jsjson.NewObject(queue.events[0])))
	for _, want := range []string{`"projectId":"p1"`, `"name":"session_end"`, `"properties":{}`, `"sdkName":""`, `"path":"/end"`, `"profileId":"d1"`} {
		if !strings.Contains(payload, want) {
			t.Errorf("payload %s lacks %s", payload, want)
		}
	}
	if _, kept := redis.zsets[wallclockKey("p1")]["d1"]; !kept {
		t.Error("the candidate stays in the index until the close removes it")
	}
	if _, locked := redis.strings[reaperLockKey("p1")]; !locked {
		// The fake's Del is the real one: the lock must be gone after the tick.
		return
	}
	t.Error("the project lock must be released")
}

func TestReaperDropsOrphansAndEmptiedProjects(t *testing.T) {
	redis := newMemoryRedis()
	redis.putSession("p1", "d1", blobEnded, 1)
	delete(redis.strings, sessionKey("p1", "d1"))
	queue := &fakeQueue{}
	reaper, metrics := newReaper(redis, queue)
	if err := reaper.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(queue.closed) != 0 || metrics.orphans != 1 || metrics.reaped != 0 {
		t.Fatalf("closed = %d, orphans = %d", len(queue.closed), metrics.orphans)
	}
	if len(redis.zsets[wallclockKey("p1")]) != 0 || redis.sets[ProjectsKey]["p1"] {
		t.Error("the orphan and the empty project must be removed")
	}
}

func TestReaperSkipsAProjectWhoseLockIsHeld(t *testing.T) {
	redis := newMemoryRedis()
	redis.putSession("p1", "d1", blobEnded, 1)
	redis.strings[reaperLockKey("p1")] = "1"
	queue := &fakeQueue{}
	reaper, _ := newReaper(redis, queue)
	if err := reaper.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(queue.closed) != 0 {
		t.Error("a locked project is skipped")
	}
}

func TestReaperKeepsTheMemberWhenTheEnqueueFails(t *testing.T) {
	redis := newMemoryRedis()
	redis.putSession("p1", "d1", blobEnded, 1)
	queue := &fakeQueue{err: errBoom}
	reaper, metrics := newReaper(redis, queue)
	if err := reaper.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if metrics.reaped != 0 || len(redis.zsets[wallclockKey("p1")]) != 1 {
		t.Error("a failed enqueue neither counts nor removes the member")
	}
}

func TestReaperDisabled(t *testing.T) {
	redis := newMemoryRedis()
	redis.putSession("p1", "d1", blobEnded, 1)
	reaper, _ := newReaper(redis, &fakeQueue{})
	reaper.Config.Enabled = false
	if err := reaper.Run(context.Background()); err != nil || len(redis.calls) != 0 {
		t.Fatalf("err = %v, redis calls = %v", err, redis.calls)
	}
}

func newVacuum(redis *memoryRedis) (*Vacuum, *countingMetrics) {
	metrics := &countingMetrics{}
	return &Vacuum{Redis: redis, Clock: fixedNow(now), Metrics: metrics,
		Config: VacuumConfig{Enabled: true, BatchSize: 10, StaleThreshold: time.Hour}}, metrics
}

func TestVacuumCleansStaleBlobsAndDropsMissingOnes(t *testing.T) {
	redis := newMemoryRedis()
	stale := now.UnixMilli() - 2*time.Hour.Milliseconds()
	redis.putSession("p1", "d1", blobEnded, stale)
	redis.putSession("p1", "d2", blobEnded, stale)
	delete(redis.strings, sessionKey("p1", "d2"))
	redis.putSession("p1", "d3", blobEnded, now.UnixMilli())
	vacuum, metrics := newVacuum(redis)
	if err := vacuum.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if metrics.vacuumed[reasonStaleBlob] != 1 || metrics.vacuumed[reasonMissing] != 1 {
		t.Fatalf("vacuumed = %v", metrics.vacuumed)
	}
	if _, ok := redis.strings[sessionKey("p1", "d1")]; ok {
		t.Error("the stale blob must go")
	}
	if len(redis.zsets[wallclockKey("p1")]) != 1 {
		t.Errorf("index = %v", redis.zsets[wallclockKey("p1")])
	}
	if !redis.sets[ProjectsKey]["p1"] {
		t.Error("the vacuum never removes a project from the set")
	}
}

func TestVacuumDisabled(t *testing.T) {
	redis := newMemoryRedis()
	vacuum, _ := newVacuum(redis)
	vacuum.Config.Enabled = false
	if err := vacuum.Run(context.Background()); err != nil || len(redis.calls) != 0 {
		t.Fatalf("err = %v, calls = %v", err, redis.calls)
	}
}

func TestNextVacuumIsFourLocal(t *testing.T) {
	before := time.Date(2026, 6, 8, 3, 0, 0, 0, time.Local)
	after := time.Date(2026, 6, 8, 4, 0, 0, 0, time.Local)
	if got := nextVacuum(before); !got.Equal(after) {
		t.Errorf("before 04:00: %v", got)
	}
	if got := nextVacuum(after); !got.Equal(after.AddDate(0, 0, 1)) {
		t.Errorf("at 04:00: %v", got)
	}
}

func TestAlertDueAndNextExceededAt(t *testing.T) {
	queue := &recordingAlerts{}
	usage := &Usage{Alerts: queue}
	org := &organization{id: "o1", eventsLimit: 1000}
	for count, want := range map[float64]bool{799: false, 800: true, 1000: true, 1001: true} {
		queue.calls = nil
		if err := usage.alert(context.Background(), org, count); err != nil {
			t.Fatal(err)
		}
		if (len(queue.calls) == 1) != want {
			t.Errorf("count %v: alerts = %v", count, queue.calls)
		}
	}
	sent := time.Now()
	queue.calls = nil
	_ = usage.alert(context.Background(), &organization{id: "o1", eventsLimit: 1000, usageWarningSentAt: &sent}, 900)
	_ = usage.alert(context.Background(), &organization{id: "o1", eventsLimit: 1000, usageExceededSentAt: &sent}, 1100)
	_ = usage.alert(context.Background(), &organization{id: "o1", eventsLimit: 0}, 5)
	if len(queue.calls) != 0 {
		t.Errorf("an alert already sent, or no limit, must not be enqueued: %v", queue.calls)
	}
	if got := usage.nextExceededAt(&organization{eventsLimit: 10}, 11, now); got == nil {
		t.Error("crossing the limit sets the marker")
	}
	if got := usage.nextExceededAt(&organization{eventsLimit: 10, exceededAt: &sent}, 9, now); got != nil {
		t.Error("back under the limit clears the marker")
	}
	if got := usage.nextExceededAt(&organization{eventsLimit: 10, exceededAt: &sent}, 12, now); got != &sent {
		t.Error("the marker of an earlier crossing is kept")
	}
	usage.SelfHosted = true
	if got := usage.nextExceededAt(&organization{eventsLimit: 0}, 5, now); got != nil {
		t.Error("self-hosted never flags")
	}
}

type recordingAlerts struct{ calls []int64 }

func (r *recordingAlerts) EnqueueUsageAlert(_ context.Context, _ string, count int64) error {
	r.calls = append(r.calls, count)
	return nil
}

func TestCleanupLuaKeepsTheNewlinesOfItsTemplateLiteral(t *testing.T) {
	if len(cleanupLua) == 0 || cleanupLua[0] != '\n' || cleanupLua[len(cleanupLua)-1] != '\n' {
		t.Fatal("the SHA covers the newlines")
	}
}
