package consumer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"slices"
)

const laneEpoch = 3

func item(offset int64) Record { return Record{Offset: offset, LeaderEpoch: laneEpoch} }

func handleAll(_ context.Context, _ int32, items []Record, control *BatchControl) (Outcome, error) {
	for _, it := range items {
		control.ResolveOffset(it.Offset)
	}
	return Done, nil
}

type laneRig struct {
	lanes   *Lanes
	commits *commitLog
	paused  atomic.Int64
	resumed atomic.Int64
	logger  *recordingLogger
	clock   *fakeClock
}

func newLaneRig(t *testing.T, mutate func(*LanesConfig)) *laneRig {
	t.Helper()
	rig := &laneRig{commits: &commitLog{}, logger: newRecordingLogger(), clock: &fakeClock{}}
	cfg := LanesConfig{
		Concurrency: 2, MaxBatchSize: 10, MaxBuffered: 1000,
		RunBatch:     handleAll,
		Commit:       rig.commits.commit,
		PauseSource:  func() { rig.paused.Add(1) },
		ResumeSource: func() { rig.resumed.Add(1) },
		Logger:       rig.logger.Logger,
		Clock:        rig.clock,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	rig.lanes = NewLanes(context.Background(), cfg)
	t.Cleanup(rig.lanes.Stop)
	return rig
}

// push queues the offsets of one partition and schedules once, as one fetch.
func (r *laneRig) push(partition int32, offsets ...int64) {
	for _, o := range offsets {
		r.lanes.Push(partition, item(o))
	}
	r.lanes.Schedule()
}

func TestLanesCommitOnePastLastHandledWithLeaderEpoch(t *testing.T) {
	rig := newLaneRig(t, nil)
	rig.push(0, 5, 6)
	eventually(t, "commit", func() bool { return len(rig.commits.all()) == 1 })
	if got := rig.commits.all(); !slices.Equal(got, []commitCall{{0, 7, laneEpoch}}) {
		t.Fatalf("commits = %v", got)
	}
}

func TestLanesNeverRunMorePartitionsThanConcurrency(t *testing.T) {
	var running, peak atomic.Int64
	gate := make(chan struct{})
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			n := running.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			<-gate
			running.Add(-1)
			return handleAll(ctx, p, items, control)
		}
	})
	for p := int32(0); p < 4; p++ {
		rig.lanes.Push(p, item(0))
	}
	rig.lanes.Schedule()
	eventually(t, "two lanes", func() bool { return rig.lanes.ActiveCount() == 2 })
	never(t, "a third lane", func() bool { return rig.lanes.ActiveCount() > 2 })
	close(gate)
	eventually(t, "all committed", func() bool { return len(rig.commits.all()) == 4 })
	eventually(t, "idle", func() bool { return rig.lanes.ActiveCount() == 0 })
	if peak.Load() != 2 {
		t.Fatalf("peak = %d", peak.Load())
	}
}

func TestLanesRunOneBatchPerPartitionInOffsetOrder(t *testing.T) {
	var mu sync.Mutex
	var seen [][]int64
	gate := make(chan struct{})
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.MaxBatchSize = 2
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			mu.Lock()
			seen = append(seen, []int64{items[0].Offset, items[len(items)-1].Offset})
			mu.Unlock()
			<-gate
			return handleAll(ctx, p, items, control)
		}
	})
	rig.push(0, 0, 1, 2, 3, 4)
	eventually(t, "first batch", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 1 })
	never(t, "an overlapping batch", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) > 1 })
	close(gate)
	eventually(t, "three commits", func() bool { return len(rig.commits.all()) == 3 })
	mu.Lock()
	defer mu.Unlock()
	if want := [][]int64{{0, 1}, {2, 3}, {4, 4}}; !slices.EqualFunc(seen, want, slices.Equal) {
		t.Fatalf("batches = %v", seen)
	}
	if got := rig.commits.all(); !slices.Equal(got, []commitCall{{0, 2, laneEpoch}, {0, 4, laneEpoch}, {0, 5, laneEpoch}}) {
		t.Fatalf("commits = %v", got)
	}
}

func TestLanesBusyPartitionDoesNotStarveOthers(t *testing.T) {
	var mu sync.Mutex
	var order []int32
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.Concurrency = 1
		c.MaxBatchSize = 1
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			mu.Lock()
			order = append(order, p)
			mu.Unlock()
			return handleAll(ctx, p, items, control)
		}
	})
	rig.lanes.Push(0, item(0))
	rig.lanes.Push(0, item(1))
	rig.lanes.Push(0, item(2))
	rig.lanes.Push(1, item(0))
	rig.lanes.Schedule()
	eventually(t, "four batches", func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 4 })
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(order, []int32{0, 1, 0, 0}) {
		t.Fatalf("order = %v", order)
	}
}

func TestLanesIgnoreAnOffsetAlreadyQueued(t *testing.T) {
	var mu sync.Mutex
	var seen []int64
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			mu.Lock()
			for _, it := range items {
				seen = append(seen, it.Offset)
			}
			mu.Unlock()
			return handleAll(ctx, p, items, control)
		}
	})
	rig.push(0, 0, 1, 2, 1, 0)
	eventually(t, "commit", func() bool { return len(rig.commits.all()) >= 1 })
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(seen, []int64{0, 1, 2}) {
		t.Fatalf("seen = %v", seen)
	}
}

func TestLanesRunTheSameBatchAgainUntilDurable(t *testing.T) {
	var attempts atomic.Int64
	var commitsSeen []int
	var mu sync.Mutex
	var rig *laneRig
	rig = newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			n := attempts.Add(1)
			mu.Lock()
			commitsSeen = append(commitsSeen, len(rig.commits.all()))
			mu.Unlock()
			if n < 3 {
				return Redeliver, nil
			}
			return handleAll(ctx, p, items, control)
		}
	})
	rig.push(0, 0, 1)
	eventually(t, "commit", func() bool { return len(rig.commits.all()) == 1 })
	mu.Lock()
	defer mu.Unlock()
	if attempts.Load() != 3 || !slices.Equal(commitsSeen, []int{0, 0, 0}) {
		t.Fatalf("attempts = %d, commits seen = %v", attempts.Load(), commitsSeen)
	}
	if got := rig.commits.all(); !slices.Equal(got, []commitCall{{0, 2, laneEpoch}}) {
		t.Fatalf("commits = %v", got)
	}
}

func TestLanesAbandonAnUndurableBatchOnStop(t *testing.T) {
	var started atomic.Bool
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(context.Context, int32, []Record, *BatchControl) (Outcome, error) {
			started.Store(true)
			return Redeliver, nil
		}
	})
	rig.push(0, 0)
	eventually(t, "batch started", started.Load)
	rig.lanes.Stop()
	if len(rig.commits.all()) != 0 || rig.lanes.ActiveCount() != 0 {
		t.Fatalf("commits = %v, active = %d", rig.commits.all(), rig.lanes.ActiveCount())
	}
}

func TestLanesAbandonAnUndurableBatchWhenRevoked(t *testing.T) {
	var attempts atomic.Int64
	var rig *laneRig
	rig = newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(context.Context, int32, []Record, *BatchControl) (Outcome, error) {
			if attempts.Add(1) == 2 {
				rig.lanes.SetAssignment([]int32{1})
			}
			return Redeliver, nil
		}
	})
	rig.lanes.SetAssignment([]int32{0, 1})
	rig.push(0, 0)
	eventually(t, "lane idle after two runs", func() bool { return attempts.Load() == 2 && rig.lanes.ActiveCount() == 0 })
	if len(rig.commits.all()) != 0 {
		t.Fatalf("commits = %v", rig.commits.all())
	}
}

func TestLanesRetryABatchHandlerThatErrorsAfterAPause(t *testing.T) {
	var attempts atomic.Int64
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			if attempts.Add(1) == 1 {
				return Done, errors.New("bug")
			}
			return handleAll(ctx, p, items, control)
		}
	})
	rig.push(0, 0)
	eventually(t, "commit", func() bool { return len(rig.commits.all()) == 1 })
	if rig.logger.h.errors.Load() != 1 || !slices.Equal(rig.clock.waits(), []time.Duration{time.Second}) {
		t.Fatalf("errors = %d, waits = %v", rig.logger.h.errors.Load(), rig.clock.waits())
	}
}

func TestLanesTreatAPanickingBatchHandlerLikeAnErroringOne(t *testing.T) {
	var attempts atomic.Int64
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			if attempts.Add(1) == 1 {
				panic("boom")
			}
			return handleAll(ctx, p, items, control)
		}
	})
	rig.push(0, 0)
	eventually(t, "commit", func() bool { return len(rig.commits.all()) == 1 })
}

func TestLanesDropWhatIsQueuedForARevokedPartition(t *testing.T) {
	var mu sync.Mutex
	var seen []int32
	gate := make(chan struct{})
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.Concurrency = 1
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			mu.Lock()
			seen = append(seen, p)
			mu.Unlock()
			<-gate
			return handleAll(ctx, p, items, control)
		}
	})
	rig.lanes.SetAssignment([]int32{0, 1})
	rig.lanes.Push(0, item(0))
	rig.lanes.Push(1, item(0))
	rig.lanes.Schedule()
	eventually(t, "partition 0 running", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 1 })
	rig.lanes.SetAssignment([]int32{0})
	close(gate)
	eventually(t, "idle", func() bool { return rig.lanes.ActiveCount() == 0 && len(rig.commits.all()) == 1 })
	never(t, "partition 1 to run", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) > 1 })
}

func TestLanesAcceptNothingForAPartitionTheyDoNotOwn(t *testing.T) {
	var ran atomic.Bool
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			ran.Store(true)
			return handleAll(ctx, p, items, control)
		}
	})
	rig.lanes.SetAssignment([]int32{0})
	rig.push(1, 0)
	never(t, "a batch", ran.Load)
}

func TestLanesRunningBatchSeesTheRevocationAndCommitsWhatItFinished(t *testing.T) {
	var rig *laneRig
	rig = newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(_ context.Context, _ int32, items []Record, control *BatchControl) (Outcome, error) {
			for _, it := range items {
				if control.IsStale() {
					break
				}
				control.ResolveOffset(it.Offset)
				if it.Offset == 1 {
					rig.lanes.SetAssignment([]int32{})
				}
			}
			return Done, nil
		}
	})
	rig.lanes.SetAssignment([]int32{0})
	rig.push(0, 0, 1, 2, 3)
	eventually(t, "commit", func() bool { return len(rig.commits.all()) == 1 })
	eventually(t, "idle", func() bool { return rig.lanes.ActiveCount() == 0 })
	if got := rig.commits.all(); !slices.Equal(got, []commitCall{{0, 2, laneEpoch}}) {
		t.Fatalf("commits = %v", got)
	}
}

func TestLanesWaitPartitionsIdleReturnsAfterTheRevokedBatchCommitted(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(_ context.Context, _ int32, items []Record, control *BatchControl) (Outcome, error) {
			control.ResolveOffset(items[0].Offset)
			close(started)
			<-release
			return Done, nil
		}
	})
	rig.lanes.SetAssignment([]int32{0})
	rig.push(0, 0)
	<-started
	rig.lanes.SetAssignment([]int32{})
	done := make(chan struct{})
	go func() { rig.lanes.WaitPartitionsIdle([]int32{0}); close(done) }()
	select {
	case <-done:
		t.Fatal("returned while the batch ran")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	<-done
	if got := rig.commits.all(); !slices.Equal(got, []commitCall{{0, 1, laneEpoch}}) {
		t.Fatalf("commits = %v", got)
	}
}

func TestLanesPauseTheSourceWhenFullAndResumeOnceDrained(t *testing.T) {
	gate := make(chan struct{})
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.Concurrency = 1
		c.MaxBatchSize = 2
		c.MaxBuffered = 4
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			<-gate
			return handleAll(ctx, p, items, control)
		}
	})
	rig.push(0, 0, 1, 2, 3)
	if rig.paused.Load() != 1 || rig.resumed.Load() != 0 {
		t.Fatalf("paused = %d, resumed = %d", rig.paused.Load(), rig.resumed.Load())
	}
	close(gate)
	eventually(t, "resume", func() bool { return rig.resumed.Load() == 1 })
	if rig.paused.Load() != 1 {
		t.Fatalf("paused = %d", rig.paused.Load())
	}
}

func TestLanesStopLetsTheRunningBatchFinishItsPrefix(t *testing.T) {
	var rig *laneRig
	stopped := make(chan struct{})
	rig = newLaneRig(t, func(c *LanesConfig) {
		c.RunBatch = func(_ context.Context, _ int32, items []Record, control *BatchControl) (Outcome, error) {
			for _, it := range items {
				if !control.IsRunning() {
					break
				}
				control.ResolveOffset(it.Offset)
				if it.Offset == 1 {
					go func() { rig.lanes.Stop(); close(stopped) }()
					eventually(t, "stop to take effect", func() bool { return !control.IsRunning() })
				}
			}
			return Done, nil
		}
	})
	rig.push(0, 0, 1, 2, 3)
	<-stopped
	if got := rig.commits.all(); !slices.Equal(got, []commitCall{{0, 2, laneEpoch}}) {
		t.Fatalf("commits = %v", got)
	}
	rig.push(0, 4)
	never(t, "a commit after stop", func() bool { return len(rig.commits.all()) > 1 })
}

func TestLanesAFailedCommitIsLoggedAndThePartitionKeepsMoving(t *testing.T) {
	var failed atomic.Bool
	rig := newLaneRig(t, func(c *LanesConfig) { c.MaxBatchSize = 1 })
	rig.commits.fail = func(commitCall) error {
		if failed.CompareAndSwap(false, true) {
			return errors.New("coordinator moved")
		}
		return nil
	}
	rig.push(0, 0, 1)
	eventually(t, "both commits attempted", func() bool { return rig.commits.total.Load() == 2 })
	if rig.logger.h.warns.Load() != 1 {
		t.Fatalf("warns = %d", rig.logger.h.warns.Load())
	}
}

func TestLanesQuiesceWaitsForRunningBatchesAndResetForgetsTheQueue(t *testing.T) {
	gate := make(chan struct{})
	var runs atomic.Int64
	var started = make(chan struct{}, 4)
	rig := newLaneRig(t, func(c *LanesConfig) {
		c.Concurrency = 1
		c.MaxBatchSize = 1
		c.RunBatch = func(ctx context.Context, p int32, items []Record, control *BatchControl) (Outcome, error) {
			runs.Add(1)
			started <- struct{}{}
			<-gate
			return handleAll(ctx, p, items, control)
		}
	})
	rig.push(0, 0, 1)
	<-started
	quiesced := make(chan struct{})
	go func() { rig.lanes.Quiesce(); close(quiesced) }()
	select {
	case <-quiesced:
		t.Fatal("quiesced while a batch ran")
	case <-time.After(30 * time.Millisecond):
	}
	close(gate)
	<-quiesced
	if got := rig.commits.all(); !slices.Equal(got, []commitCall{{0, 1, laneEpoch}}) || runs.Load() != 1 {
		t.Fatalf("commits = %v, runs = %d", got, runs.Load())
	}
	rig.lanes.Reset()
	rig.push(0, 1)
	eventually(t, "second commit", func() bool { return len(rig.commits.all()) == 2 })
	if got := rig.commits.all(); !slices.Equal(got, []commitCall{{0, 1, laneEpoch}, {0, 2, laneEpoch}}) {
		t.Fatalf("commits = %v", got)
	}
}
