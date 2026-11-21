package flush

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// manualClock only moves when a test advances it.
type manualClock struct {
	mutex   sync.Mutex
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	at   time.Time
	wake chan struct{}
}

func newManualClock(now time.Time) *manualClock { return &manualClock{now: now} }

func (c *manualClock) Now() time.Time { c.mutex.Lock(); defer c.mutex.Unlock(); return c.now }

func (c *manualClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mutex.Lock()
	w := &waiter{at: c.now.Add(d), wake: make(chan struct{})}
	c.waiters = append(c.waiters, w)
	c.mutex.Unlock()
	select {
	case <-w.wake:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *manualClock) Tick(time.Duration) (<-chan time.Time, func()) { return nil, func() {} }

func (c *manualClock) sleepers() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.waiters)
}

// advance moves time forward, waking sleepers one deadline at a time.
func (c *manualClock) advance(d time.Duration) {
	target := c.Now().Add(d)
	for {
		c.mutex.Lock()
		var next *waiter
		index := -1
		for i, w := range c.waiters {
			if !w.at.After(target) && (next == nil || w.at.Before(next.at)) {
				next, index = w, i
			}
		}
		if next == nil {
			c.now = target
			c.mutex.Unlock()
			return
		}
		c.waiters = append(c.waiters[:index], c.waiters[index+1:]...)
		if next.at.After(c.now) {
			c.now = next.at
		}
		c.mutex.Unlock()
		close(next.wake)
		time.Sleep(20 * time.Millisecond) // let the woken loop register its next sleep
	}
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func countingRunner(t *testing.T, name string, count *atomic.Int32, redis Redis, block func(context.Context)) *Runner {
	t.Helper()
	runner, err := NewRunner(namedFlusher{name: name, flush: func(ctx context.Context, _ *Run) error {
		count.Add(1)
		if block != nil {
			block(ctx)
		}
		return nil
	}}, Deps{Redis: redis, Clock: newManualClock(time.Unix(0, 0)), Paused: NeverPaused})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

type namedFlusher struct {
	name  string
	flush func(context.Context, *Run) error
}

func (f namedFlusher) Name() string                              { return f.name }
func (f namedFlusher) ListKey() string                           { return "wparity:" + f.name }
func (f namedFlusher) Flush(ctx context.Context, run *Run) error { return f.flush(ctx, run) }

func TestNextTickIsTheNextMultipleOfTheInterval(t *testing.T) {
	at := func(seconds float64) time.Time { return time.UnixMilli(int64(seconds * 1000)) }
	if got := nextTick(at(3), 10*time.Second); !got.Equal(at(10)) {
		t.Errorf("got %v", got)
	}
	if got := nextTick(at(10), 10*time.Second); !got.Equal(at(20)) {
		t.Errorf("an exact multiple waits a full interval: %v", got)
	}
	if got := nextTick(at(59.9), 30*time.Second); !got.Equal(at(60)) {
		t.Errorf("got %v", got)
	}
}

func TestSchedulerRunsOnlyTheEnabledJobsAtTheirIntervals(t *testing.T) {
	redis := NewMemoryRedis()
	var bots, events atomic.Int32
	botJob, _ := NewJob(JobFlushBots, countingRunner(t, "wparity-bot", &bots, redis, nil))
	eventJob, _ := NewJob(JobFlushEvents, countingRunner(t, "wparity-event", &events, redis, nil))
	clk := newManualClock(time.UnixMilli(3000))
	scheduler, err := NewScheduler([]Job{botJob, eventJob}, []string{JobFlushBots}, SchedulerDeps{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	if names := scheduler.JobNames(); len(names) != 1 || names[0] != JobFlushBots {
		t.Fatalf("jobs %v", names)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { scheduler.Run(ctx); close(done) }()
	waitFor(t, "the loop to sleep", func() bool { return clk.sleepers() == 1 })
	clk.advance(27 * time.Second) // to 30 s: the first 30 s multiple
	waitFor(t, "the first bot flush", func() bool { return bots.Load() == 1 })
	clk.advance(30 * time.Second)
	waitFor(t, "the second bot flush", func() bool { return bots.Load() == 2 })
	cancel()
	<-done
	if events.Load() != 0 {
		t.Fatalf("a job the Bun worker keeps ran %d times", events.Load())
	}
}

func TestSchedulerRefusesAJobNoOneImplements(t *testing.T) {
	redis := NewMemoryRedis()
	var n atomic.Int32
	botJob, _ := NewJob(JobFlushBots, countingRunner(t, "wparity-bot", &n, redis, nil))
	_, err := NewScheduler([]Job{botJob}, []string{JobFlushBots, JobFlushSessions, "salt"}, SchedulerDeps{Clock: newManualClock(time.Now())})
	if err == nil || !strings.Contains(err.Error(), "flushSessions, salt") || !strings.Contains(err.Error(), "flushBots") {
		t.Fatalf("err = %v", err)
	}
	if _, err := NewJob("salt", nil); err == nil {
		t.Fatal("salt is not a flush job")
	}
	scheduler, err := NewScheduler([]Job{botJob}, nil, SchedulerDeps{Clock: newManualClock(time.Now())})
	if err != nil || len(scheduler.JobNames()) != 0 {
		t.Fatalf("the default runs nothing: %v %v", scheduler, err)
	}
}

func TestShutdownWaitsForAFlushInProgressThenCancelsItAfterTheGrace(t *testing.T) {
	redis := NewMemoryRedis()
	var n atomic.Int32
	cancelled := make(chan struct{})
	runner := countingRunner(t, "wparity-bot", &n, redis, func(ctx context.Context) {
		<-ctx.Done()
		close(cancelled)
	})
	job, _ := NewJob(JobFlushBots, runner)
	clk := newManualClock(time.UnixMilli(1000))
	scheduler, _ := NewScheduler([]Job{job}, []string{JobFlushBots}, SchedulerDeps{Clock: clk, ShutdownGrace: 100 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { scheduler.Run(ctx); close(done) }()
	waitFor(t, "the loop to sleep", func() bool { return clk.sleepers() == 1 })
	clk.advance(29 * time.Second)
	waitFor(t, "the flush to start", func() bool { return n.Load() == 1 })
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned while a flush was in progress")
	case <-time.After(30 * time.Millisecond):
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the flush was never cancelled after the grace")
	}
	<-done
}

func TestShutdownLetsAFlushFinishWithinTheGrace(t *testing.T) {
	redis := NewMemoryRedis()
	var n atomic.Int32
	finish := make(chan struct{})
	var cancelledEarly atomic.Bool
	runner := countingRunner(t, "wparity-bot", &n, redis, func(ctx context.Context) {
		select {
		case <-finish:
		case <-ctx.Done():
			cancelledEarly.Store(true)
		}
	})
	job, _ := NewJob(JobFlushBots, runner)
	clk := newManualClock(time.UnixMilli(1000))
	scheduler, _ := NewScheduler([]Job{job}, []string{JobFlushBots}, SchedulerDeps{Clock: clk, ShutdownGrace: 5 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { scheduler.Run(ctx); close(done) }()
	waitFor(t, "the loop to sleep", func() bool { return clk.sleepers() == 1 })
	clk.advance(29 * time.Second)
	waitFor(t, "the flush to start", func() bool { return n.Load() == 1 })
	cancel()
	time.Sleep(50 * time.Millisecond)
	close(finish)
	<-done
	if cancelledEarly.Load() {
		t.Fatal("the flush's context was cancelled by the shutdown itself")
	}
	if _, held := redis.Get("lock:wparity-bot"); held {
		t.Fatal("lock left behind")
	}
}
