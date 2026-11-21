package flush

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/clock"
)

const (
	testLockTTL   = time.Second
	testBatchSize = 2
	testListKey   = "wparity:lock-test:queue"
	testLockName  = "wparity-lock-test"
)

// testFlusher is a buffer whose flush is whatever the test hands in.
type testFlusher struct {
	flush func(context.Context, *Run) error
}

func (f testFlusher) Name() string    { return testLockName }
func (f testFlusher) ListKey() string { return testListKey }
func (f testFlusher) Flush(ctx context.Context, run *Run) error {
	return f.flush(ctx, run)
}

type collector struct {
	mutex        sync.Mutex
	observations []Observation
}

func (c *collector) ObserveFlush(obs Observation) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.observations = append(c.observations, obs)
}

func (c *collector) results() []Result {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var out []Result
	for _, obs := range c.observations {
		out = append(out, obs.Result)
	}
	return out
}

func newRunner(t *testing.T, redis Redis, obs Observer, flush func(context.Context, *Run) error) *Runner {
	t.Helper()
	runner, err := NewRunner(testFlusher{flush: flush}, Deps{
		Redis: redis, Clock: clock.System{}, Observer: obs, Paused: NeverPaused, LockTTL: testLockTTL,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

// base-buffer.test.ts: a flush that outlives the lock TTL keeps the lock until it is done.
func TestFlushThatOutlivesTheLockTTLKeepsTheLock(t *testing.T) {
	redis := NewMemoryRedis()
	var ttlSamples []time.Duration
	other := &collector{}
	runner := newRunner(t, redis, nil, func(ctx context.Context, _ *Run) error {
		otherWorker := newRunner(t, redis, other, func(context.Context, *Run) error { return nil })
		for range 4 {
			time.Sleep(testLockTTL / 2)
			ttlSamples = append(ttlSamples, redis.TTL("lock:"+testLockName))
		}
		return otherWorker.TryFlush(ctx, TriggerCron)
	})
	if err := runner.TryFlush(context.Background(), TriggerCron); err != nil {
		t.Fatal(err)
	}
	for _, ttl := range ttlSamples {
		if ttl <= 0 {
			t.Fatalf("lock expired mid-flush: %v", ttlSamples)
		}
	}
	if got := other.results(); len(got) != 1 || got[0] != ResultLocked {
		t.Fatalf("second worker: %v", got)
	}
	if _, held := redis.Get("lock:" + testLockName); held {
		t.Fatal("lock still held after the flush")
	}
}

// base-buffer.test.ts: one instance runs one flush at a time.
func TestOneInstanceRunsOneFlushAtATime(t *testing.T) {
	redis := NewMemoryRedis()
	obs := &collector{}
	var flushes int
	var mutex sync.Mutex
	runner := newRunner(t, redis, obs, func(context.Context, *Run) error {
		mutex.Lock()
		flushes++
		mutex.Unlock()
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() { defer group.Done(); _ = runner.TryFlush(context.Background(), TriggerCron) }()
	}
	group.Wait()
	if flushes != 1 {
		t.Fatalf("%d flushes ran", flushes)
	}
	results := obs.results()
	locked := 0
	for _, result := range results {
		if result == ResultLocked {
			locked++
		}
	}
	if len(results) != 2 || locked != 1 {
		t.Fatalf("results %v", results)
	}
}

// base-buffer.test.ts: stops draining once the lock is no longer its own.
func TestDrainStopsOnceTheLockIsNoLongerItsOwn(t *testing.T) {
	redis := NewMemoryRedis()
	batches := 0
	runner := newRunner(t, redis, nil, func(ctx context.Context, run *Run) error {
		return run.Drain(ctx, testBatchSize, func(context.Context) (int, error) {
			batches++
			redis.Set("lock:"+testLockName, "someone-else")
			time.Sleep(testLockTTL * 8 / 10)
			return testBatchSize, nil
		})
	})
	if err := runner.TryFlush(context.Background(), TriggerCron); err != nil {
		t.Fatal(err)
	}
	if batches != 1 {
		t.Fatalf("%d batches ran after the lock was lost", batches)
	}
	if value, _ := redis.Get("lock:" + testLockName); value != "someone-else" {
		t.Fatalf("lock holder is %q: the release deleted someone else's lock", value)
	}
}

func TestAFailedExtensionStopsTheNextBatchButNotTheOneInFlight(t *testing.T) {
	redis := NewMemoryRedis()
	redis.FailOnce("extend", errors.New("connection refused"))
	batches := 0
	runner := newRunner(t, redis, nil, func(ctx context.Context, run *Run) error {
		return run.Drain(ctx, testBatchSize, func(context.Context) (int, error) {
			batches++
			time.Sleep(testLockTTL * 6 / 10)
			return testBatchSize, nil
		})
	})
	if err := runner.TryFlush(context.Background(), TriggerCron); err != nil {
		t.Fatal(err)
	}
	if batches != 1 {
		t.Fatalf("%d batches", batches)
	}
}

func TestPausedFlushTakesNoLock(t *testing.T) {
	redis := NewMemoryRedis()
	obs := &collector{}
	runner, _ := NewRunner(testFlusher{flush: func(context.Context, *Run) error { t.Fatal("flushed while paused"); return nil }},
		Deps{Redis: redis, Clock: clock.System{}, Observer: obs, Paused: func(context.Context) (bool, error) { return true, nil }})
	if err := runner.TryFlush(context.Background(), TriggerCron); err != nil {
		t.Fatal(err)
	}
	if got := obs.results(); len(got) != 1 || got[0] != ResultPaused || redis.CallCount("set") != 0 {
		t.Fatalf("results %v, lock attempts %d", got, redis.CallCount("set"))
	}
}

func TestErrorsThatFailTheCronJob(t *testing.T) {
	boom := errors.New("redis down")
	t.Run("pause check", func(t *testing.T) {
		runner, _ := NewRunner(testFlusher{}, Deps{Redis: NewMemoryRedis(), Clock: clock.System{}, Paused: func(context.Context) (bool, error) { return false, boom }})
		if err := runner.TryFlush(context.Background(), TriggerCron); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("lock acquisition", func(t *testing.T) {
		redis := NewMemoryRedis()
		redis.FailOnce("set", boom)
		obs := &collector{}
		runner := newRunner(t, redis, obs, func(context.Context, *Run) error { return nil })
		if err := runner.TryFlush(context.Background(), TriggerCron); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if len(obs.results()) != 0 {
			t.Fatalf("an observation for a flush that never started: %v", obs.results())
		}
	})
	t.Run("lock release", func(t *testing.T) {
		redis := NewMemoryRedis()
		redis.FailOnce("release", boom)
		obs := &collector{}
		runner := newRunner(t, redis, obs, func(context.Context, *Run) error { return nil })
		if err := runner.TryFlush(context.Background(), TriggerCron); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if got := obs.results(); len(got) != 1 || got[0] != ResultSuccess {
			t.Fatalf("results %v", got)
		}
	})
}

func TestAFlushErrorIsAnObservationAndTheLockIsReleased(t *testing.T) {
	redis := NewMemoryRedis()
	obs := &collector{}
	boom := errors.New("clickhouse down")
	runner := newRunner(t, redis, obs, func(_ context.Context, run *Run) error {
		run.Report(3, Phases{LRange: phase(time.Millisecond)})
		return boom
	})
	if err := runner.TryFlush(context.Background(), TriggerCron); err != nil {
		t.Fatalf("a flush error must not fail the job: %v", err)
	}
	got := obs.observations[0]
	if got.Result != ResultError || !errors.Is(got.Err, boom) || got.Rows != 3 || !got.Phases.OnFlush.Set {
		t.Fatalf("observation %+v", got)
	}
	if _, held := redis.Get("lock:" + testLockName); held {
		t.Fatal("lock not released after an error")
	}
}

func TestObservationCarriesTheListLengthAtStart(t *testing.T) {
	redis := NewMemoryRedis()
	redis.Push(testListKey, "a", "b", "c")
	obs := &collector{}
	runner := newRunner(t, redis, obs, func(context.Context, *Run) error { return nil })
	_ = runner.TryFlush(context.Background(), TriggerCron)
	if got := obs.observations[0]; !got.LLenKnown || got.LLenAtStart != 3 || got.Trigger != TriggerCron || got.Buffer != testLockName {
		t.Fatalf("observation %+v", got)
	}
	redis.FailOnce("llen", errors.New("x"))
	_ = runner.TryFlush(context.Background(), TriggerCron)
	if obs.observations[1].LLenKnown {
		t.Fatal("a failed LLEN must leave the length unknown")
	}
}

// Shutdown cancels the flush context; the lock must still be released so a
// rollback to the TypeScript flush is not blocked for a TTL.
func TestLockIsReleasedAfterTheContextIsCancelled(t *testing.T) {
	redis := NewMemoryRedis()
	ctx, cancel := context.WithCancel(context.Background())
	runner := newRunner(t, redis, nil, func(context.Context, *Run) error { cancel(); return ctx.Err() })
	if err := runner.TryFlush(ctx, TriggerCron); err != nil {
		t.Fatal(err)
	}
	if _, held := redis.Get("lock:" + testLockName); held {
		t.Fatal("lock left behind by a cancelled flush")
	}
}

func TestDrainLoopsUntilAShortBatchAndSumsTheReports(t *testing.T) {
	redis := NewMemoryRedis()
	obs := &collector{}
	sizes := []int{2, 2, 1}
	runner := newRunner(t, redis, obs, func(ctx context.Context, run *Run) error {
		next := 0
		return run.Drain(ctx, testBatchSize, func(context.Context) (int, error) {
			n := sizes[next]
			next++
			run.Report(n, Phases{LRange: phase(time.Millisecond), Trim: phase(2 * time.Millisecond)})
			return n, nil
		})
	})
	_ = runner.TryFlush(context.Background(), TriggerCron)
	got := obs.observations[0]
	if got.Rows != 5 || got.Phases.LRange.Duration != 3*time.Millisecond || got.Phases.Trim.Duration != 6*time.Millisecond || got.Phases.CHInsert.Set {
		t.Fatalf("observation %+v", got)
	}
}

func TestDrainStopsWhenTheBudgetIsSpent(t *testing.T) {
	batches := 0
	runner, err := NewRunner(testFlusher{flush: func(ctx context.Context, run *Run) error {
		return run.Drain(ctx, testBatchSize, func(context.Context) (int, error) {
			batches++
			time.Sleep(60 * time.Millisecond)
			return testBatchSize, nil
		})
	}}, Deps{Redis: NewMemoryRedis(), Clock: clock.System{}, Paused: NeverPaused, DrainBudget: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_ = runner.TryFlush(context.Background(), TriggerCron)
	if batches != 2 {
		t.Fatalf("%d batches in a 100 ms budget of 60 ms batches", batches)
	}
}

func TestStopPreventsTheNextBatch(t *testing.T) {
	redis := NewMemoryRedis()
	batches := 0
	var runner *Runner
	runner = newRunner(t, redis, nil, func(ctx context.Context, run *Run) error {
		return run.Drain(ctx, testBatchSize, func(context.Context) (int, error) {
			batches++
			runner.Stop()
			return testBatchSize, nil
		})
	})
	_ = runner.TryFlush(context.Background(), TriggerCron)
	if batches != 1 {
		t.Fatalf("%d batches after Stop", batches)
	}
}

// The lock scripts must hash to what the TypeScript worker computes from its source.
func TestLockScriptsMatchTheTypeScriptSource(t *testing.T) {
	raw, err := os.ReadFile("testdata/locks.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Release, Extend     struct{ Lua, SHA string }
		KeepaliveIntervalMs int
		LockTTLSeconds      int `json:"lockTtlSeconds"`
		Intervals           map[string]int
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if releaseLockScript.text != golden.Release.Lua || releaseLockScript.SHA() != golden.Release.SHA {
		t.Errorf("release script differs from the TypeScript text or SHA %s", golden.Release.SHA)
	}
	if extendLockScript.text != golden.Extend.Lua || extendLockScript.SHA() != golden.Extend.SHA {
		t.Errorf("extend script differs from the TypeScript text or SHA %s", golden.Extend.SHA)
	}
	if DefaultLockTTL != time.Duration(golden.LockTTLSeconds)*time.Second || DefaultLockTTL/lockKeepalivesPerTTL != time.Duration(golden.KeepaliveIntervalMs)*time.Millisecond {
		t.Errorf("lock TTL or keepalive interval differs")
	}
	for job, every := range golden.Intervals {
		got, ok := Interval(job)
		if !ok {
			continue // not a flush job
		}
		if got != time.Duration(every)*time.Millisecond {
			t.Errorf("%s interval %v, TypeScript %d ms", job, got, every)
		}
	}
	for _, job := range []string{JobFlushEvents, JobFlushBots, JobFlushProfiles, JobFlushProfileBackfill, JobFlushGroups, JobFlushAiEvents, JobFlushSessions, JobFlushReplay} {
		if _, ok := golden.Intervals[job]; !ok {
			t.Errorf("%s is not a TypeScript cron job with an interval", job)
		}
	}
}

func TestScriptFallsBackToEvalOnNoScript(t *testing.T) {
	redis := NewMemoryRedis()
	redis.Set("k", "id")
	got, err := releaseLockScript.Run(context.Background(), redis, []string{"k"}, []string{"id"})
	if err != nil || got != 1 {
		t.Fatalf("release: %d %v", got, err)
	}
	if _, held := redis.Get("k"); held {
		t.Fatal("not deleted")
	}
	if !redis.HasScript(releaseLockScript.SHA()) {
		t.Fatal("EVAL did not cache the script")
	}
	if got, _ := releaseLockScript.Run(context.Background(), redis, []string{"k"}, []string{"id"}); got != 0 {
		t.Fatalf("releasing a lock that is gone returned %d", got)
	}
}

func TestLockIDsLookLikeNanoidSecureIds(t *testing.T) {
	seen := map[string]bool{}
	for range 50 {
		id := newLockID()
		if len(id) != len(lockIDPrefix)+lockIDLength || id[:5] != lockIDPrefix || seen[id] {
			t.Fatalf("bad or repeated lock id %q", id)
		}
		seen[id] = true
	}
}

func TestCronPauseCheckKey(t *testing.T) {
	cases := []struct {
		namespace string
		cluster   bool
		key       string
	}{{"", false, "bull:cron:meta"}, {"ns", false, "bull:cron-ns:meta"}, {"", true, "bull:{cron}:meta"}, {"ns", true, "bull:{cron-ns}:meta"}}
	for _, tc := range cases {
		redis := NewMemoryRedis()
		redis.HSet(tc.key, "paused", "1")
		paused, err := CronPauseCheck(redis, tc.namespace, tc.cluster)(context.Background())
		if err != nil || !paused {
			t.Errorf("%+v: paused=%v err=%v", tc, paused, err)
		}
		redis.HDel(tc.key, "paused")
		if paused, _ := CronPauseCheck(redis, tc.namespace, tc.cluster)(context.Background()); paused {
			t.Errorf("%+v: a queue with no paused field is running", tc)
		}
	}
}
