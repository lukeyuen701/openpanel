// Package flush is BaseBuffer: the lock, the keepalive, the drain loop and
// the observation of one buffer's flush, and the scheduler that runs the flush
// jobs the Bun cron queue used to.
//
// A TypeScript flush and a Go flush exclude each other through the same
// `lock:<name>` key and the same Lua, so a buffer can move from one process to
// the other tick by tick.
//
// The lock counts as lost only when the extend script answers that another
// owner holds it (or the key is gone), or when a transport error on extend
// could not be retried into an answer before the TTL, counted from the last
// confirmation, ran out. A flush that lost the lock inserts no further batch
// (Run.EnsureLock); a batch whose insert already landed is still trimmed, since
// leaving it would insert it twice, and the drain stops. The TypeScript flush
// trims regardless and checks the flag only between batches.
//
// A buffer plugs in by implementing Flusher (its name, its list and what to do
// under the lock; Run.Drain, Run.Report and ParallelLimit are the BaseBuffer
// helpers), wrapping it in a Runner, and handing the Runner to NewJob under the
// buffer's cron job id; NewScheduler then runs the jobs listed in
// GO_WORKER_CRON_JOBS. package simple has four complete examples.
package flush

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"openpanel/ingest/internal/worker/clock"
)

const (
	lockKeyPrefix = "lock:"
	lockIDPrefix  = "lock_"
	lockIDLength  = 18
	// DefaultLockTTL only has to outlive a crashed worker: a live flush keeps
	// extending it.
	DefaultLockTTL = 60 * time.Second
	// DefaultDrainBudget is how long one run keeps reading batches; the
	// deadline is checked before a batch starts.
	DefaultDrainBudget = 8 * time.Second
	// lockKeepalivesPerTTL: the lock is extended this many times per TTL.
	lockKeepalivesPerTTL = 3
	// releaseTimeout bounds the lock release that runs after shutdown cancelled the flush.
	releaseTimeout = 5 * time.Second
	// DefaultLockRetryBackoff is the first wait before an extend that failed
	// on the wire is sent again; it doubles up to maxLockRetryBackoff.
	DefaultLockRetryBackoff = 500 * time.Millisecond
	maxLockRetryBackoff     = 5 * time.Second
	secureIDAlphabet        = "_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

// ErrLockLost is what a flush returns instead of inserting a batch once the
// lock is lost. Rows that already landed are still trimmed.
var ErrLockLost = errors.New("lock lost")

// Trigger is what invoked a flush. Only the cron does; "add" is the
// vestigial value of the TypeScript type.
type Trigger string

// TriggerCron is the label value of every flush.
const TriggerCron Trigger = "cron"

// Result of one flush call.
type Result string

// The four results of buffer_flush_total.
const (
	ResultSuccess Result = "success"
	ResultError   Result = "error"
	ResultLocked  Result = "locked"
	ResultPaused  Result = "paused"
)

// Phase is one timed step of a flush; Set tells "took 0" from "did not run".
type Phase struct {
	Duration time.Duration
	Set      bool
}

func phase(d time.Duration) Phase { return Phase{Duration: d, Set: true} }

// Phases are the timings a flush reports (FlushPhaseTimings).
type Phases struct {
	LRange, CHFetch, Transform, CHInsert, Trim, OnFlush Phase
}

// Add sums the phases that are set in other into p.
func (p Phases) Add(other Phases) Phases {
	sum := func(a, b Phase) Phase {
		if !b.Set {
			return a
		}
		return Phase{Duration: a.Duration + b.Duration, Set: true}
	}
	return Phases{
		LRange: sum(p.LRange, other.LRange), CHFetch: sum(p.CHFetch, other.CHFetch),
		Transform: sum(p.Transform, other.Transform), CHInsert: sum(p.CHInsert, other.CHInsert),
		Trim: sum(p.Trim, other.Trim), OnFlush: sum(p.OnFlush, other.OnFlush),
	}
}

// Observation is what one tryFlush call produced (FlushObservation).
type Observation struct {
	Buffer  string
	Result  Result
	Trigger Trigger
	Total   time.Duration
	// Rows is how many rows were removed from Redis, when any were.
	Rows int
	// LLenAtStart is the list length when the flush began; LLenKnown is false
	// when LLEN failed.
	LLenAtStart int64
	LLenKnown   bool
	Phases      Phases
	Err         error
}

// Observer receives every observation.
type Observer interface{ ObserveFlush(Observation) }

// Flusher is one buffer: what to flush under the lock.
type Flusher interface {
	// Name is the buffer's name: the metric label and the lock key suffix.
	Name() string
	// ListKey is the Redis list whose length is the buffer's depth.
	ListKey() string
	// Flush moves rows to ClickHouse. It reads and trims through run's helpers
	// and reports what it did with run.Report. Any error leaves the rows in Redis
	// for the next tick.
	Flush(ctx context.Context, run *Run) error
}

// PauseCheck says whether the cron queue is paused, which halts every flush.
type PauseCheck func(ctx context.Context) (bool, error)

// Deps are what a Runner reads from outside. Logger and Observer may be nil.
type Deps struct {
	Redis    Redis
	Clock    clock.Clock
	Logger   *slog.Logger
	Observer Observer
	Paused   PauseCheck
	// LockTTL and DrainBudget default to DefaultLockTTL and DefaultDrainBudget.
	LockTTL     time.Duration
	DrainBudget time.Duration
	// LockRetryBackoff defaults to DefaultLockRetryBackoff.
	LockRetryBackoff time.Duration
}

// Runner runs one Flusher's tryFlush.
type Runner struct {
	flusher  Flusher
	deps     Deps
	lockKey  string
	inFlight atomic.Bool
	stopping atomic.Bool
}

// NewRunner builds the runner of f.
func NewRunner(f Flusher, deps Deps) (*Runner, error) {
	if deps.Redis == nil || deps.Clock == nil {
		return nil, errors.New("flush: Redis and Clock are required")
	}
	if deps.Paused == nil {
		return nil, errors.New("flush: a pause check is required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	deps.Logger = deps.Logger.With("buffer", f.Name())
	if deps.LockTTL == 0 {
		deps.LockTTL = DefaultLockTTL
	}
	if deps.DrainBudget == 0 {
		deps.DrainBudget = DefaultDrainBudget
	}
	if deps.LockRetryBackoff == 0 {
		deps.LockRetryBackoff = DefaultLockRetryBackoff
	}
	return &Runner{flusher: f, deps: deps, lockKey: lockKeyPrefix + f.Name()}, nil
}

// Name is the buffer's name.
func (r *Runner) Name() string { return r.flusher.Name() }

// LockKey is the Redis key shared with the TypeScript flush.
func (r *Runner) LockKey() string { return r.lockKey }

// Stop asks drain loops to start no further batch; the batch in flight
// finishes. Used on shutdown.
func (r *Runner) Stop() { r.stopping.Store(true) }

func newLockID() string {
	raw := make([]byte, lockIDLength)
	_, _ = rand.Read(raw)
	for i, b := range raw {
		raw[i] = secureIDAlphabet[int(b)&(len(secureIDAlphabet)-1)]
	}
	return lockIDPrefix + string(raw)
}

// TryFlush is BaseBuffer.tryFlush. The returned error is what made the
// TypeScript cron job fail: the pause check, the lock acquisition or the lock
// release failing. An error inside the flush itself is an observation with
// result "error" and a nil return, the rows staying in Redis for the next tick.
func (r *Runner) TryFlush(ctx context.Context, trigger Trigger) error {
	startedAt := r.deps.Clock.Now()
	obs := Observation{Buffer: r.flusher.Name(), Trigger: trigger}
	if length, err := r.deps.Redis.LLen(ctx, r.flusher.ListKey()); err == nil {
		obs.LLenAtStart, obs.LLenKnown = length, true
	}
	finish := func(result Result, err error) {
		obs.Result, obs.Err = result, err
		obs.Total = r.deps.Clock.Now().Sub(startedAt)
		r.emit(obs)
	}

	paused, err := r.deps.Paused(ctx)
	if err != nil {
		return fmt.Errorf("flush %s: cron pause check: %w", r.flusher.Name(), err)
	}
	if paused {
		finish(ResultPaused, nil)
		return nil
	}
	if !r.inFlight.CompareAndSwap(false, true) {
		finish(ResultLocked, nil)
		return nil
	}
	defer r.inFlight.Store(false)

	lockID := newLockID()
	acquiredAt := r.deps.Clock.Now()
	acquired, err := r.deps.Redis.SetNXEx(ctx, r.lockKey, lockID, r.deps.LockTTL)
	if err != nil {
		return fmt.Errorf("flush %s: acquire lock: %w", r.flusher.Name(), err)
	}
	if !acquired {
		finish(ResultLocked, nil)
		return nil
	}

	run := &Run{runner: r}
	stopKeepalive := r.keepLockAlive(ctx, lockID, run, acquiredAt)
	flushStarted := r.deps.Clock.Now()
	flushErr := r.flusher.Flush(ctx, run)
	obs.Rows, obs.Phases = run.rows, run.phases
	obs.Phases.OnFlush = phase(r.deps.Clock.Now().Sub(flushStarted))
	if flushErr != nil {
		finish(ResultError, flushErr)
	} else {
		finish(ResultSuccess, nil)
	}
	stopKeepalive()

	// Shutdown cancels ctx; the lock must still go, or a rollback to the
	// TypeScript flush waits out the TTL.
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	if _, err := releaseLockScript.Run(releaseCtx, r.deps.Redis, []string{r.lockKey}, []string{lockID}); err != nil {
		return fmt.Errorf("flush %s: release lock: %w", r.flusher.Name(), err)
	}
	return nil
}

// keepLockAlive extends the lock every TTL/3 until the returned function is
// called, which also waits for a renewal in flight so it cannot touch the next
// flush's lock. The lock is marked lost on the run only when the extend script
// answers that another owner holds it or the key is gone, or when ownership
// could not be confirmed before the TTL ran out (see extendLock); the batch in
// flight finishes, no new one starts.
func (r *Runner) keepLockAlive(ctx context.Context, lockID string, run *Run, acquiredAt time.Time) (stop func()) {
	ticks, stopTicker := r.deps.Clock.Tick(r.deps.LockTTL / lockKeepalivesPerTTL)
	keepaliveCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		confirmedAt := acquiredAt
		for {
			select {
			case <-keepaliveCtx.Done():
				return
			case <-ticks:
			}
			var lost bool
			confirmedAt, lost = r.extendLock(keepaliveCtx, lockID, confirmedAt)
			if lost {
				run.lockLost.Store(true)
				return
			}
		}
	}()
	return func() {
		cancel()
		stopTicker()
		<-done
	}
}

// extendLock sends the extend script once. An answer settles ownership: the
// lock is lost when the script says so. A transport error settles nothing, so
// the call is retried with backoff for as long as the TTL counted from the
// last confirmation has not run out; after that the key may have expired and
// been taken, and the lock counts as lost. It returns the time of the latest
// confirmation, and ok false (lost) otherwise.
func (r *Runner) extendLock(ctx context.Context, lockID string, confirmedAt time.Time) (time.Time, bool) {
	ttl := fmt.Sprint(int64(r.deps.LockTTL / time.Second))
	backoff := r.deps.LockRetryBackoff
	for {
		attemptedAt := r.deps.Clock.Now()
		extended, err := extendLockScript.Run(ctx, r.deps.Redis, []string{r.lockKey}, []string{lockID, ttl})
		if ctx.Err() != nil {
			return confirmedAt, false
		}
		if err == nil {
			if extended == 1 {
				return attemptedAt, false
			}
			r.deps.Logger.Error("flush lock lost mid-flush; stopping before the next batch", "lockKey", r.lockKey)
			return confirmedAt, true
		}
		remaining := confirmedAt.Add(r.deps.LockTTL).Sub(r.deps.Clock.Now())
		if remaining <= 0 {
			r.deps.Logger.Error("flush lock ownership not confirmed before its TTL ran out; treating it as lost", "err", err, "lockKey", r.lockKey)
			return confirmedAt, true
		}
		r.deps.Logger.Warn("flush lock could not be extended; retrying", "err", err, "lockKey", r.lockKey)
		if r.deps.Clock.Sleep(ctx, min(backoff, remaining)) != nil {
			return confirmedAt, false
		}
		backoff = min(backoff*2, maxLockRetryBackoff)
	}
}

func (r *Runner) emit(obs Observation) {
	if r.deps.Observer != nil {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					r.deps.Logger.Warn("flush observer panicked", "panic", recovered)
				}
			}()
			r.deps.Observer.ObserveFlush(obs)
		}()
	}
	fields := []any{
		"result", obs.Result, "trigger", obs.Trigger, "totalMs", obs.Total.Milliseconds(),
		"rowsProcessed", obs.Rows, "llenAtStart", obs.LLenAtStart,
	}
	switch obs.Result {
	case ResultError:
		r.deps.Logger.Error("Flush failed", append(fields, "err", obs.Err)...)
	case ResultPaused:
		r.deps.Logger.Debug("Flush skipped (cron paused)", fields...)
	case ResultLocked:
		r.deps.Logger.Debug("Flush skipped (locked)", fields...)
	default:
		r.deps.Logger.Debug("Flush completed", fields...)
	}
}

// Run is one flush in progress: what a Flusher uses to loop, time and report.
type Run struct {
	runner   *Runner
	lockLost atomic.Bool
	rows     int
	phases   Phases
}

// LockLost reports whether the keepalive found the lock expired or taken, or
// could not confirm it before its TTL ran out.
func (r *Run) LockLost() bool { return r.lockLost.Load() }

// EnsureLock is the guard before an insert: with the lock lost another flush
// may be writing the same rows, so none is sent and the batch stays for the
// next tick. A batch that already landed is trimmed regardless.
func (r *Run) EnsureLock() error {
	if r.LockLost() {
		return fmt.Errorf("flush %s: %w before the insert; leaving the batch for the next tick", r.runner.flusher.Name(), ErrLockLost)
	}
	return nil
}

// Report adds a batch's row count and timings to the flush's totals.
func (r *Run) Report(rows int, phases Phases) {
	r.rows += rows
	r.phases = r.phases.Add(phases)
}

// Now is the runner's clock, so a Flusher's timings follow it.
func (r *Run) Now() time.Time { return r.runner.deps.Clock.Now() }

// Since times a step against the runner's clock.
func (r *Run) Since(start time.Time) Phase { return phase(r.Now().Sub(start)) }

// Redis is the client the flush uses.
func (r *Run) Redis() Redis { return r.runner.deps.Redis }

// ShouldContinue is false once the lock is lost, the process is shutting
// down, or ctx is done: no further batch may start.
func (r *Run) ShouldContinue(ctx context.Context) bool {
	return !r.LockLost() && !r.runner.stopping.Load() && ctx.Err() == nil
}

// Deadline is the end of this run's drain budget, counted from start.
func (r *Run) Deadline(start time.Time) time.Time { return start.Add(r.runner.deps.DrainBudget) }

// Drain is drainBatches: read a batch, insert, trim, while the budget lasts and
// the lock is held, stopping as soon as a batch comes back smaller than
// batchSize. The deadline is checked before a batch starts, so a run lasts up
// to the budget plus one batch.
func (r *Run) Drain(ctx context.Context, batchSize int, processBatch func(context.Context) (int, error)) error {
	deadline := r.Deadline(r.Now())
	for r.Now().Before(deadline) && r.ShouldContinue(ctx) {
		processed, err := processBatch(ctx)
		if err != nil {
			return err
		}
		if processed < batchSize {
			return nil
		}
	}
	return nil
}
