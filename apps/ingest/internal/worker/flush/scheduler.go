package flush

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"openpanel/ingest/internal/worker/clock"
)

// Cron job ids of the flushes, as in the Bun cron queue.
const (
	JobFlushEvents          = "flushEvents"
	JobFlushBots            = "flushBots"
	JobFlushProfiles        = "flushProfiles"
	JobFlushProfileBackfill = "flushProfileBackfill"
	JobFlushGroups          = "flushGroups"
	JobFlushAiEvents        = "flushAiEvents"
	JobFlushSessions        = "flushSessions"
	JobFlushReplay          = "flushReplay"
)

// Intervals of the flush jobs (the `cron: { every }` of each job definition).
const (
	FlushIntervalFast = 10 * time.Second
	FlushIntervalSlow = 30 * time.Second
)

// Interval is how often a flush job runs.
func Interval(job string) (time.Duration, bool) {
	switch job {
	case JobFlushEvents, JobFlushProfiles, JobFlushGroups, JobFlushAiEvents, JobFlushSessions, JobFlushReplay:
		return FlushIntervalFast, true
	case JobFlushBots, JobFlushProfileBackfill:
		return FlushIntervalSlow, true
	}
	return 0, false
}

// DefaultShutdownGrace is how long shutdown waits for flushes in progress
// (the Bun worker waits up to 60 s for its active cron jobs).
const DefaultShutdownGrace = 60 * time.Second

// Job is one cron job: its id, its schedule and what to run. A flush has a
// Runner; any other job (the session reaper and vacuum) has a Task.
type Job struct {
	Name     string
	Interval time.Duration
	Runner   *Runner
	// Task runs instead of Runner. Next, when set, picks the next tick instead
	// of the multiple of Interval.
	Task func(ctx context.Context) error
	Next func(now time.Time) time.Time

	taskRunning *atomic.Bool
	paused      PauseCheck
}

// NewTaskJob is a cron job that is not a buffer flush. A tick that finds the
// previous run still going does nothing, and a paused cron queue skips it,
// as for a flush.
func NewTaskJob(name string, interval time.Duration, next func(now time.Time) time.Time, paused PauseCheck, task func(ctx context.Context) error) Job {
	return Job{Name: name, Interval: interval, Task: task, Next: next, taskRunning: &atomic.Bool{}, paused: paused}
}

func (j Job) nextTick(now time.Time) time.Time {
	if j.Next != nil {
		return j.Next(now)
	}
	return nextTick(now, j.Interval)
}

func (j Job) stop() {
	if j.Runner != nil {
		j.Runner.Stop()
	}
}

func (j Job) tick(ctx context.Context) error {
	if j.Runner != nil {
		return j.Runner.TryFlush(ctx, TriggerCron)
	}
	paused, err := j.paused(ctx)
	if err != nil || paused {
		return err
	}
	if !j.taskRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer j.taskRunning.Store(false)
	return j.Task(ctx)
}

// NewJob pairs runner with the standard interval of the cron job name.
func NewJob(name string, runner *Runner) (Job, error) {
	interval, ok := Interval(name)
	if !ok {
		return Job{}, fmt.Errorf("flush: %q is not a flush job", name)
	}
	return Job{Name: name, Interval: interval, Runner: runner}, nil
}

// SchedulerDeps are what a Scheduler reads from outside.
type SchedulerDeps struct {
	Clock  clock.Clock
	Logger *slog.Logger
	// ShutdownGrace defaults to DefaultShutdownGrace.
	ShutdownGrace time.Duration
}

// Scheduler fires each enabled flush job at wall-clock multiples of its
// interval, as the BullMQ job schedulers did. A tick that finds the previous
// flush still running is a "locked" no-op, exactly as in the Bun worker, where
// the per-process guard and the Redis lock refuse it.
type Scheduler struct {
	jobs   []Job
	deps   SchedulerDeps
	logger *slog.Logger
}

// NewScheduler selects, from jobs, those named in enabled (GO_WORKER_CRON_JOBS).
// A name in enabled that no job implements fails: it would run nowhere, because
// the operator has handed it over in DISABLED_CRON_JOBS. A job outside enabled
// is left to the Bun worker.
func NewScheduler(jobs []Job, enabled []string, deps SchedulerDeps) (*Scheduler, error) {
	if deps.Clock == nil {
		return nil, errors.New("flush: a clock is required")
	}
	if deps.ShutdownGrace == 0 {
		deps.ShutdownGrace = DefaultShutdownGrace
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	byName := map[string]Job{}
	for _, job := range jobs {
		byName[job.Name] = job
	}
	var missing []string
	scheduler := &Scheduler{deps: deps, logger: logger}
	for _, name := range enabled {
		job, ok := byName[name]
		if !ok {
			missing = append(missing, name)
			continue
		}
		if slices.ContainsFunc(scheduler.jobs, func(existing Job) bool { return existing.Name == name }) {
			continue
		}
		scheduler.jobs = append(scheduler.jobs, job)
	}
	if len(missing) > 0 {
		var known []string
		for name := range byName {
			known = append(known, name)
		}
		sort.Strings(known)
		return nil, fmt.Errorf("GO_WORKER_CRON_JOBS lists %s, which this worker has no implementation of (it implements: %s)",
			strings.Join(missing, ", "), strings.Join(known, ", "))
	}
	return scheduler, nil
}

// JobNames are the jobs this scheduler runs.
func (s *Scheduler) JobNames() []string {
	names := make([]string, len(s.jobs))
	for i, job := range s.jobs {
		names[i] = job.Name
	}
	return names
}

// nextTick is floor(now/every)*every + every, BullMQ's getNextMillis.
func nextTick(now time.Time, every time.Duration) time.Time {
	millis := now.UnixMilli()
	step := every.Milliseconds()
	return time.UnixMilli((millis/step)*step + step)
}

// Run schedules until ctx is done, then stops the flushes: no further batch
// starts, the batch in flight gets ShutdownGrace to finish and release its
// lock, and only then is its context cancelled. What is still in Redis stays
// there for the next process.
func (s *Scheduler) Run(ctx context.Context) {
	flushCtx, cancelFlushes := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelFlushes()
	var flushes sync.WaitGroup
	var loops sync.WaitGroup
	for _, job := range s.jobs {
		loops.Add(1)
		go func() {
			defer loops.Done()
			s.loop(ctx, flushCtx, job, &flushes)
		}()
	}
	loops.Wait()
	for _, job := range s.jobs {
		job.stop()
	}
	finished := make(chan struct{})
	go func() { flushes.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(s.deps.ShutdownGrace):
		s.logger.Error("flushes still running after the shutdown grace; cancelling them", "grace", s.deps.ShutdownGrace)
		cancelFlushes()
		<-finished
	}
}

func (s *Scheduler) loop(ctx, flushCtx context.Context, job Job, flushes *sync.WaitGroup) {
	for {
		now := s.deps.Clock.Now()
		if err := s.deps.Clock.Sleep(ctx, job.nextTick(now).Sub(now)); err != nil {
			return
		}
		flushes.Add(1)
		go func() {
			defer flushes.Done()
			if err := job.tick(flushCtx); err != nil {
				s.logger.Error("cron job failed", "job", job.Name, "err", err)
			}
		}()
	}
}
