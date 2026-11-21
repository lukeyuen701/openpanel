package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/flush"
)

const (
	lockTestTTL     = time.Second
	lockTestBackoff = 5 * time.Millisecond
	// keepaliveSettle is long enough for lossClock's 2 ms keepalive to run.
	keepaliveSettle = 30 * time.Millisecond
)

var errTransport = errors.New("connection refused")

// hookedRedis runs a hook after every LRANGE, which is the last step before a
// batch is inserted.
type hookedRedis struct {
	*memRedis
	afterLRange func()
}

func (h *hookedRedis) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	items, err := h.memRedis.LRange(ctx, key, start, stop)
	if h.afterLRange != nil {
		h.afterLRange()
	}
	return items, err
}

// lockRig is a session flush with two rows in two batches, so a lost lock that
// stops the drain shows as one row left.
type lockRig struct {
	*rig
	redis *hookedRedis
}

func newLockRig(t *testing.T) *lockRig {
	t.Helper()
	r := newRig()
	hooked := &hookedRedis{memRedis: r.redis}
	r.deps.Redis = hooked
	r.redis.Push(SessionKey, sessionRow("a", 1, 1), sessionRow("b", 1, 1))
	return &lockRig{rig: r, redis: hooked}
}

func (l *lockRig) run(t *testing.T) flush.Observation {
	t.Helper()
	cfg := buffersConfig(t, map[string]string{"SESSION_BUFFER_BATCH_SIZE": "1"})
	runner, err := flush.NewRunner(NewSession(cfg, l.deps, Overrides{}), flush.Deps{
		Redis: l.redis, Clock: lossClock{}, Observer: l.obs, Paused: flush.NeverPaused,
		LockTTL: lockTestTTL, LockRetryBackoff: lockTestBackoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.TryFlush(context.Background(), flush.TriggerCron); err != nil {
		t.Fatal(err)
	}
	return l.obs.last()
}

func (l *lockRig) takenOver() {
	l.redis.Set("lock:session", "lock_someone_else")
	time.Sleep(keepaliveSettle)
}

func TestSessionFlushLockExtendedKeepsDraining(t *testing.T) {
	l := newLockRig(t)
	l.redis.afterLRange = func() { time.Sleep(keepaliveSettle) }
	observation := l.run(t)
	if observation.Result != flush.ResultSuccess || len(l.ch.insertedLines()) != 2 || len(l.redis.List(SessionKey)) != 0 {
		t.Fatalf("observation %+v, inserted %d, left %d", observation, len(l.ch.insertedLines()), len(l.redis.List(SessionKey)))
	}
	if l.redis.CallCount("extend") == 0 {
		t.Fatal("the keepalive never extended the lock")
	}
}

func TestSessionFlushLockLostBeforeTheInsertInsertsNothing(t *testing.T) {
	l := newLockRig(t)
	l.redis.afterLRange = l.takenOver
	observation := l.run(t)
	if observation.Result != flush.ResultError || !errors.Is(observation.Err, flush.ErrLockLost) {
		t.Fatalf("observation %+v", observation)
	}
	if got := len(l.ch.insertedLines()); got != 0 {
		t.Fatalf("%d rows inserted by a flush without the lock", got)
	}
	if got := len(l.redis.List(SessionKey)); got != 2 {
		t.Fatalf("%d rows left, want 2", got)
	}
}

func TestSessionFlushLockLostAfterTheInsertTrimsWhatLandedAndStops(t *testing.T) {
	l := newLockRig(t)
	l.ch.onInsert = l.takenOver
	observation := l.run(t)
	if observation.Result != flush.ResultSuccess || observation.Rows != 1 {
		t.Fatalf("observation %+v", observation)
	}
	if got := len(l.ch.insertedLines()); got != 1 {
		t.Fatalf("%d rows inserted, want 1", got)
	}
	left := l.redis.List(SessionKey)
	if len(left) != 1 || left[0] != sessionRow("b", 1, 1) {
		t.Fatalf("left %v: the inserted row must be trimmed and the drain stopped", left)
	}
}

func TestSessionFlushTransportErrorOnExtendRecovers(t *testing.T) {
	l := newLockRig(t)
	l.redis.afterLRange = func() {
		l.redis.FailTimes("extend", 3, errTransport)
		time.Sleep(10 * keepaliveSettle)
	}
	observation := l.run(t)
	if observation.Result != flush.ResultSuccess || len(l.ch.insertedLines()) != 2 || len(l.redis.List(SessionKey)) != 0 {
		t.Fatalf("observation %+v, inserted %d, left %d", observation, len(l.ch.insertedLines()), len(l.redis.List(SessionKey)))
	}
}

func TestSessionFlushTransportErrorsUntilTheTTLCountAsLost(t *testing.T) {
	l := newLockRig(t)
	l.redis.afterLRange = func() {
		l.redis.FailTimes("extend", -1, errTransport)
		time.Sleep(lockTestTTL + 5*keepaliveSettle)
	}
	observation := l.run(t)
	if observation.Result != flush.ResultError || !errors.Is(observation.Err, flush.ErrLockLost) {
		t.Fatalf("observation %+v", observation)
	}
	if got := len(l.ch.insertedLines()); got != 0 {
		t.Fatalf("%d rows inserted without a confirmed lock", got)
	}
	if got := len(l.redis.List(SessionKey)); got != 2 {
		t.Fatalf("%d rows left, want 2", got)
	}
}

// Every core flush trims what landed after the lock is taken over, rather
// than inserting it again next tick.
func TestALostLockAfterTheInsertStillTrims(t *testing.T) {
	cases := []struct {
		name  string
		build func(r *rig) flush.Flusher
		key   string
		row   string
	}{
		{"event", func(r *rig) flush.Flusher { return NewEvent(buffersConfig(t, nil), r.deps, Overrides{}) }, EventKey, eventLine(1, "p")},
		{"profile", func(r *rig) flush.Flusher { return NewProfile(buffersConfig(t, nil), r.deps, Overrides{}) }, ProfileKey, profileLine("a", "A", "")},
		{"profile-backfill", func(r *rig) flush.Flusher { return NewProfileBackfill(buffersConfig(t, nil), r.deps, Overrides{}) }, ProfileBackfillKey, `{"projectId":"p","sessionId":"s","profileId":"u"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRig()
			r.redis.Push(c.key, c.row)
			takeOver := func() {
				r.redis.Set("lock:"+c.name, "lock_someone_else")
				time.Sleep(keepaliveSettle)
			}
			r.ch.onInsert = takeOver
			if c.name == "profile-backfill" {
				r.deps.CH = &commandHook{fakeCH: r.ch, hook: takeOver}
			}
			observation := r.flushWithClock(t, c.build(r), lossClock{})
			if observation.Result != flush.ResultSuccess {
				t.Fatalf("observation %+v", observation)
			}
			if got := r.redis.List(c.key); len(got) != 0 {
				t.Fatalf("%d rows left, want 0", len(got))
			}
		})
	}
}
