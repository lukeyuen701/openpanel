package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

// Outcome is what a batch handler reports.
type Outcome int

const (
	// Done: whatever was resolved may be committed.
	Done Outcome = iota
	// Redeliver: nothing in the batch may be committed; run it again.
	Redeliver
)

const (
	// failedBatchRetryDelay keeps a partition from spinning on a batch
	// handler that errors.
	failedBatchRetryDelay = time.Second
	// resumeBelowShare is the share of MaxBuffered the backlog falls to
	// before a paused source resumes.
	resumeBelowShare = 0.5
)

// BatchControl is what a running batch sees of its lane.
type BatchControl struct {
	running func() bool
	stale   func() bool
	resolve func(offset int64)
}

// NewBatchControl builds a control from its three answers; Lanes does it for
// a real batch, tests do it to drive a BatchHandler directly.
func NewBatchControl(running, stale func() bool, resolve func(offset int64)) *BatchControl {
	return &BatchControl{running: running, stale: stale, resolve: resolve}
}

// IsRunning is false once the lanes are stopped.
func (c *BatchControl) IsRunning() bool { return c.running() }

// IsStale reports that a rebalance took the partition away.
func (c *BatchControl) IsStale() bool { return c.stale() }

// ResolveOffset marks an offset handled. Call it in ascending order from one
// goroutine; the highest one is committed.
func (c *BatchControl) ResolveOffset(offset int64) { c.resolve(offset) }

// BatchFunc handles one batch of consecutive records of one partition. An
// error is a bug in the handler: the batch is run again after a pause.
type BatchFunc func(ctx context.Context, partition int32, items []Record, control *BatchControl) (Outcome, error)

// LanesConfig configures Lanes.
type LanesConfig struct {
	Concurrency  int
	MaxBatchSize int
	// MaxBuffered is the queued plus in-flight count above which the source
	// is paused.
	MaxBuffered int
	RunBatch    BatchFunc
	// Commit gets the next offset to consume, as Kafka expects.
	Commit func(ctx context.Context, partition int32, offset int64, leaderEpoch int32) error
	// PauseSource and ResumeSource run under the lanes' lock: they must not
	// block or call back into Lanes.
	PauseSource  func()
	ResumeSource func()
	Logger       *slog.Logger
	Clock        Clock
}

// Lanes gives every partition a lane: one batch at a time, offsets committed
// in order, up to Concurrency partitions side by side.
type Lanes struct {
	cfg LanesConfig
	// ctx outlives a graceful stop, so a batch that is finishing can still
	// commit.
	ctx context.Context

	mu       sync.Mutex
	idle     *sync.Cond
	lanes    map[int32]*lane
	order    []int32
	assigned map[int32]struct{} // nil: not known yet, accept everything
	stopped  bool
	holding  bool
	paused   bool
	active   int
	buffered int
}

type lane struct {
	queue   []Record
	running bool
	// nextOffset is one past the highest accepted offset, so a source that
	// rewinds cannot queue a message twice.
	nextOffset int64
	hasNext    bool
}

// NewLanes builds lanes whose work runs under ctx.
func NewLanes(ctx context.Context, cfg LanesConfig) *Lanes {
	l := &Lanes{cfg: cfg, ctx: ctx, lanes: map[int32]*lane{}}
	l.idle = sync.NewCond(&l.mu)
	return l
}

func (l *Lanes) laneFor(partition int32) *lane {
	ln, ok := l.lanes[partition]
	if !ok {
		ln = &lane{}
		l.lanes[partition] = ln
		l.order = append(l.order, partition)
	}
	return ln
}

func (l *Lanes) isAssigned(partition int32) bool {
	if l.assigned == nil {
		return true
	}
	_, ok := l.assigned[partition]
	return ok
}

func (l *Lanes) dropQueue(ln *lane) {
	l.buffered -= len(ln.queue)
	ln.queue = nil
	ln.hasNext = false
}

func (l *Lanes) updateBackpressure() {
	if !l.paused && l.buffered >= l.cfg.MaxBuffered {
		l.paused = true
		l.cfg.PauseSource()
		return
	}
	if l.paused && float64(l.buffered) <= float64(l.cfg.MaxBuffered)*resumeBelowShare {
		l.paused = false
		l.cfg.ResumeSource()
	}
}

// Push queues a record. It is dropped when the lanes are stopped, the
// partition is not assigned, or its offset was queued before. Nothing runs
// until Schedule, so one fetch response becomes one batch and not a batch of
// one and a remainder.
func (l *Lanes) Push(partition int32, record Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped || !l.isAssigned(partition) {
		return
	}
	ln := l.laneFor(partition)
	if ln.hasNext && record.Offset < ln.nextOffset {
		return
	}
	ln.nextOffset, ln.hasNext = record.Offset+1, true
	ln.queue = append(ln.queue, record)
	l.buffered++
	l.updateBackpressure()
}

// Schedule starts every lane that has work while a slot is free.
func (l *Lanes) Schedule() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.scheduleLocked()
}

func (l *Lanes) scheduleLocked() {
	if l.stopped || l.holding {
		return
	}
	for _, partition := range slices.Clone(l.order) {
		if l.active >= l.cfg.Concurrency {
			return
		}
		ln := l.lanes[partition]
		if ln.running || len(ln.queue) == 0 {
			continue
		}
		take := min(l.cfg.MaxBatchSize, len(ln.queue))
		items := slices.Clone(ln.queue[:take])
		ln.queue = ln.queue[take:]
		ln.running = true
		l.active++
		go l.runLane(partition, ln, items)
	}
}

func (l *Lanes) runOnce(partition int32, items []Record, control *BatchControl) (outcome Outcome) {
	defer func() {
		if r := recover(); r != nil {
			l.cfg.Logger.Error("kafka batch handler panicked — running the batch again", "partition", partition, "panic", fmt.Sprint(r))
			_ = l.cfg.Clock.Sleep(l.ctx, failedBatchRetryDelay)
			outcome = Redeliver
		}
	}()
	outcome, err := l.cfg.RunBatch(l.ctx, partition, items, control)
	if err != nil {
		l.cfg.Logger.Error("kafka batch handler threw — running the batch again", "partition", partition, "err", err)
		_ = l.cfg.Clock.Sleep(l.ctx, failedBatchRetryDelay)
		return Redeliver
	}
	return outcome
}

func (l *Lanes) runLane(partition int32, ln *lane, items []Record) {
	var handled *Record
	byOffset := make(map[int64]int, len(items))
	for i, item := range items {
		byOffset[item.Offset] = i
	}
	control := NewBatchControl(
		func() bool { l.mu.Lock(); defer l.mu.Unlock(); return !l.stopped },
		func() bool { l.mu.Lock(); defer l.mu.Unlock(); return !l.isAssigned(partition) },
		func(offset int64) {
			if i, ok := byOffset[offset]; ok {
				handled = &items[i]
			}
		})

	// A failed durability gate resolves nothing and asks for the batch again;
	// the batch handler has waited out its backoff by then.
	outcome := l.runOnce(partition, items, control)
	for outcome == Redeliver && control.IsRunning() && !control.IsStale() {
		outcome = l.runOnce(partition, items, control)
	}

	unfinished := items
	if handled != nil {
		l.commitHandled(partition, *handled)
		last := handled.Offset
		unfinished = slices.DeleteFunc(slices.Clone(items), func(item Record) bool { return item.Offset <= last })
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.buffered -= len(items) - len(unfinished)
	if len(unfinished) > 0 {
		if !l.stopped && l.isAssigned(partition) {
			ln.queue = append(unfinished, ln.queue...)
		} else {
			// Shutdown or revocation: never committed, so the next owner reads them.
			l.buffered -= len(unfinished)
		}
	}
	ln.running = false
	l.active--
	if l.lanes[partition] == ln {
		l.order = append(slices.DeleteFunc(l.order, func(p int32) bool { return p == partition }), partition)
	}
	l.updateBackpressure()
	l.idle.Broadcast()
	l.scheduleLocked()
}

// commitHandled never fails the lane: the handled events are in Redis, and a
// later commit on the partition covers these offsets too. Without one they
// are redelivered, as duplicates.
func (l *Lanes) commitHandled(partition int32, last Record) {
	if err := l.cfg.Commit(l.ctx, partition, last.Offset+1, last.LeaderEpoch); err != nil {
		l.cfg.Logger.Warn("kafka offset commit failed", "err", err, "partition", partition, "offset", last.Offset)
	}
}

// SetAssignment replaces the partitions this member owns and drops what is
// queued for the others. A nil slice means "not known yet": accept everything.
// A running batch of a partition that left sees IsStale and stops between
// messages.
func (l *Lanes) SetAssignment(partitions []int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if partitions == nil {
		l.assigned = nil
	} else {
		l.assigned = make(map[int32]struct{}, len(partitions))
		for _, p := range partitions {
			l.assigned[p] = struct{}{}
		}
	}
	for partition, ln := range l.lanes {
		if !l.isAssigned(partition) {
			l.dropQueue(ln)
		}
	}
	l.updateBackpressure()
}

// WaitPartitionsIdle returns once no batch of the given partitions runs.
// Call it after SetAssignment took them away, so their batches have stopped
// and committed before the group moves the partitions on.
func (l *Lanes) WaitPartitionsIdle(partitions []int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.anyRunning(partitions) {
		l.idle.Wait()
	}
}

func (l *Lanes) anyRunning(partitions []int32) bool {
	for _, partition := range partitions {
		if ln, ok := l.lanes[partition]; ok && ln.running {
			return true
		}
	}
	return false
}

// Quiesce stops starting batches and waits for the running ones.
func (l *Lanes) Quiesce() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holding = true
	l.waitIdleLocked()
}

// Reset forgets everything queued, for a source that restarts from the
// committed offsets, and lets batches start again. A paused source is not
// resumed: the new one starts unpaused.
func (l *Lanes) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ln := range l.lanes {
		l.dropQueue(ln)
	}
	l.holding = false
	l.paused = false
	l.scheduleLocked()
}

// Stop lets running batches finish their prefix and commit it, then drops
// the queues.
func (l *Lanes) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = true
	l.waitIdleLocked()
	for _, ln := range l.lanes {
		l.dropQueue(ln)
	}
}

func (l *Lanes) waitIdleLocked() {
	for l.active > 0 {
		l.idle.Wait()
	}
}

// ActiveCount is the number of running batches.
func (l *Lanes) ActiveCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active
}
