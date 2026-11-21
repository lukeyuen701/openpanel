package simple

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
	lockTestLockKey = "lock:" + BotName
)

var errTransport = errors.New("connection refused")

// hookedRedis runs a hook after every LRANGE, the last step before a batch is
// inserted.
type hookedRedis struct {
	*flush.MemoryRedis
	afterLRange func()
}

func (h *hookedRedis) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	items, err := h.MemoryRedis.LRange(ctx, key, start, stop)
	if h.afterLRange != nil {
		h.afterLRange()
	}
	return items, err
}

// lockRig is a bot flush with two rows in two batches, so a lost lock that
// stops the drain shows as one row left.
type lockRig struct {
	*rig
	redis *hookedRedis
}

func newLockRig() *lockRig {
	r := newRig()
	hooked := &hookedRedis{MemoryRedis: r.redis}
	r.deps.Redis = hooked
	r.redis.Push(BotKey, "{\"n\":1}", "{\"n\":2}")
	return &lockRig{rig: r, redis: hooked}
}

func (l *lockRig) run(t *testing.T) flush.Observation {
	t.Helper()
	cfg := testConfig()
	cfg.Bot.Batch = 1
	runner, err := flush.NewRunner(NewBot(cfg, l.deps, Overrides{}), flush.Deps{
		Redis: l.redis, Clock: lossClock{}, Observer: l.obs, Paused: flush.NeverPaused,
		LockTTL: lockTestTTL, LockRetryBackoff: lockTestBackoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.TryFlush(context.Background(), flush.TriggerCron); err != nil {
		t.Fatal(err)
	}
	l.obs.mutex.Lock()
	defer l.obs.mutex.Unlock()
	return l.obs.all[len(l.obs.all)-1]
}

func (l *lockRig) takenOver() {
	l.redis.Set(lockTestLockKey, "lock_someone_else")
	time.Sleep(keepaliveSettle)
}

func TestBotFlushLockExtendedKeepsDraining(t *testing.T) {
	l := newLockRig()
	l.redis.afterLRange = func() { time.Sleep(keepaliveSettle) }
	observation := l.run(t)
	if observation.Result != flush.ResultSuccess || len(l.inserter.inserted()) != 2 || len(l.redis.List(BotKey)) != 0 {
		t.Fatalf("observation %+v, inserted %d, left %d", observation, len(l.inserter.inserted()), len(l.redis.List(BotKey)))
	}
	if l.redis.CallCount("extend") == 0 {
		t.Fatal("the keepalive never extended the lock")
	}
}

func TestBotFlushLockLostBeforeTheInsertInsertsNothing(t *testing.T) {
	l := newLockRig()
	l.redis.afterLRange = l.takenOver
	observation := l.run(t)
	if observation.Result != flush.ResultError || !errors.Is(observation.Err, flush.ErrLockLost) {
		t.Fatalf("observation %+v", observation)
	}
	if got := len(l.inserter.inserted()); got != 0 {
		t.Fatalf("%d rows inserted by a flush without the lock", got)
	}
	if got := len(l.redis.List(BotKey)); got != 2 {
		t.Fatalf("%d rows left, want 2", got)
	}
}

func TestBotFlushLockLostAfterTheInsertTrimsWhatLandedAndStops(t *testing.T) {
	l := newLockRig()
	l.inserter.onInsert = l.takenOver
	observation := l.run(t)
	if observation.Result != flush.ResultSuccess || observation.Rows != 1 {
		t.Fatalf("observation %+v", observation)
	}
	if got := len(l.inserter.inserted()); got != 1 {
		t.Fatalf("%d rows inserted, want 1", got)
	}
	left := l.redis.List(BotKey)
	if len(left) != 1 || left[0] != "{\"n\":2}" {
		t.Fatalf("left %v: the inserted row must be trimmed and the drain stopped", left)
	}
}

func TestBotFlushTransportErrorOnExtendRecovers(t *testing.T) {
	l := newLockRig()
	l.redis.afterLRange = func() {
		l.redis.FailTimes("extend", 3, errTransport)
		time.Sleep(10 * keepaliveSettle)
	}
	observation := l.run(t)
	if observation.Result != flush.ResultSuccess || len(l.inserter.inserted()) != 2 || len(l.redis.List(BotKey)) != 0 {
		t.Fatalf("observation %+v, inserted %d, left %d", observation, len(l.inserter.inserted()), len(l.redis.List(BotKey)))
	}
}

func TestBotFlushTransportErrorsUntilTheTTLCountAsLost(t *testing.T) {
	l := newLockRig()
	l.redis.afterLRange = func() {
		l.redis.FailTimes("extend", -1, errTransport)
		time.Sleep(lockTestTTL + 5*keepaliveSettle)
	}
	observation := l.run(t)
	if observation.Result != flush.ResultError || !errors.Is(observation.Err, flush.ErrLockLost) {
		t.Fatalf("observation %+v", observation)
	}
	if got := len(l.inserter.inserted()); got != 0 {
		t.Fatalf("%d rows inserted without a confirmed lock", got)
	}
	if got := len(l.redis.List(BotKey)); got != 2 {
		t.Fatalf("%d rows left, want 2", got)
	}
}
