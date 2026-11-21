package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/wire"
)

// fakeBroker is an in-memory source: tests feed it polls and drive the group hooks.
type fakeBroker struct {
	hooks    GroupHooks
	feed     chan fakePoll
	commits  *commitLog
	paused   atomic.Int64
	resumed  atomic.Int64
	allowed  atomic.Int64
	closed   atomic.Bool
	closeErr chan struct{}
}

type fakePoll struct {
	records []Record
	err     error
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{feed: make(chan fakePoll, 64), commits: &commitLog{}, closeErr: make(chan struct{})}
}

func (b *fakeBroker) Poll(ctx context.Context) ([]Record, error) {
	if b.closed.Load() {
		return nil, ErrBrokerClosed
	}
	select {
	case p := <-b.feed:
		return p.records, p.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.closeErr:
		return nil, ErrBrokerClosed
	}
}

func (b *fakeBroker) AllowRebalance() { b.allowed.Add(1) }
func (b *fakeBroker) PauseFetch()     { b.paused.Add(1) }
func (b *fakeBroker) ResumeFetch()    { b.resumed.Add(1) }
func (b *fakeBroker) Commit(ctx context.Context, partition int32, offset int64, epoch int32) error {
	return b.commits.commit(ctx, partition, offset, epoch)
}
func (b *fakeBroker) Close() {
	if b.closed.CompareAndSwap(false, true) {
		close(b.closeErr)
	}
}

type consumerRig struct {
	broker     *fakeBroker
	consumer   *Consumer
	clock      *fakeClock
	metrics    *recordingMetrics
	deadLetter *memoryDeadLetter
	durability *fakeDurability
	activity   atomic.Int64
	mu         sync.Mutex
	seen       []Delivery
	handle     func(ctx context.Context, d Delivery) error
}

func startRig(t *testing.T, mutate func(*Config)) *consumerRig {
	t.Helper()
	rig := &consumerRig{broker: newFakeBroker(), clock: &fakeClock{}, metrics: newRecordingMetrics(), deadLetter: &memoryDeadLetter{}, durability: &fakeDurability{}}
	cfg := Config{
		NewBroker: func(_ context.Context, hooks GroupHooks) (Broker, error) {
			rig.broker.hooks = hooks
			return rig.broker, nil
		},
		Handler: func(ctx context.Context, _ wire.IncomingEventPayload, d Delivery) error {
			rig.mu.Lock()
			rig.seen = append(rig.seen, d)
			handle := rig.handle
			rig.mu.Unlock()
			if handle != nil {
				return handle(ctx, d)
			}
			return nil
		},
		DeadLetter: rig.deadLetter, Durability: rig.durability, Logger: quietLogger(), Metrics: rig.metrics,
		Clock: rig.clock, OnActivity: func() { rig.activity.Add(1) },
		Topic: "events", PartitionsConcurrent: 2, MaxMessagesPerBatch: 10, PollTimeout: 5 * time.Millisecond,
		MaxAttempts: 3, InitialRetry: time.Millisecond, MaxRetry: 2 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	consumer, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	rig.consumer = consumer
	t.Cleanup(consumer.Stop)
	return rig
}

func (r *consumerRig) seenOffsets() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int64, len(r.seen))
	for i, d := range r.seen {
		out[i] = d.Offset
	}
	return out
}

func (r *consumerRig) deliver(partition int32, offsets ...int64) {
	records := make([]Record, len(offsets))
	for i, o := range offsets {
		records[i] = record(partition, o, fmt.Sprint("k", o), payload(o))
	}
	r.broker.feed <- fakePoll{records: records}
}

func TestStartFailsTheBootWhenTheBrokerCannotBeReached(t *testing.T) {
	want := errors.New("brokers unreachable")
	_, err := Start(context.Background(), Config{
		NewBroker: func(context.Context, GroupHooks) (Broker, error) { return nil, want },
		Logger:    quietLogger(), Clock: &fakeClock{},
	})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}

func TestConsumerHandsAPartitionsMessagesToTheHandlerAndCommitsPastTheLast(t *testing.T) {
	rig := startRig(t, nil)
	rig.broker.hooks.Assigned([]int32{0})
	rig.deliver(0, 4, 5, 6)
	eventually(t, "commit", func() bool { return len(rig.broker.commits.all()) == 1 })
	if got := rig.broker.commits.all(); !slices.Equal(got, []commitCall{{0, 7, 3}}) {
		t.Fatalf("commits = %v", got)
	}
	if got := rig.seenOffsets(); !slices.Equal(slices.Sorted(slices.Values(got)), []int64{4, 5, 6}) {
		t.Fatalf("handled = %v", got)
	}
	if rig.broker.allowed.Load() == 0 {
		t.Fatal("the group was never allowed to rebalance")
	}
}

func TestConsumerAFailedFlushRunsTheBatchAgainAndCommitsOnlyOnceDurable(t *testing.T) {
	var failures atomic.Int64
	rig := startRig(t, nil)
	rig.durability.gate = func() error {
		if failures.Add(1) <= 2 {
			return errRedisDown
		}
		return nil
	}
	rig.broker.hooks.Assigned([]int32{0})
	rig.deliver(0, 0, 1)
	eventually(t, "commit", func() bool { return len(rig.broker.commits.all()) == 1 })
	if len(rig.seenOffsets()) != 6 { // two messages, three laps
		t.Fatalf("handled = %v", rig.seenOffsets())
	}
	if got := rig.broker.commits.all(); !slices.Equal(got, []commitCall{{0, 2, 3}}) {
		t.Fatalf("commits = %v", got)
	}
}

func TestConsumerARevocationStopsHandingOverThatPartitionsMessages(t *testing.T) {
	rig := startRig(t, nil)
	rig.broker.hooks.Assigned([]int32{0, 1})
	rig.broker.hooks.Revoked([]int32{1})
	rig.deliver(1, 0, 1)
	rig.deliver(0, 0)
	eventually(t, "partition 0 committed", func() bool { return len(rig.broker.commits.all()) == 1 })
	never(t, "a commit for the revoked partition", func() bool { return len(rig.broker.commits.all()) > 1 })
	if rig.broker.commits.all()[0].Partition != 0 {
		t.Fatalf("commits = %v", rig.broker.commits.all())
	}
}

func TestConsumerRevokedWaitsForTheRunningBatchToStopAndCommit(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	rig := startRig(t, nil)
	rig.handle = func(context.Context, Delivery) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
		return nil
	}
	rig.broker.hooks.Assigned([]int32{0})
	rig.deliver(0, 0, 1, 2)
	<-entered
	done := make(chan struct{})
	go func() { rig.broker.hooks.Revoked([]int32{0}); close(done) }()
	select {
	case <-done:
		t.Fatal("Revoked returned while a batch of the partition ran")
	case <-time.After(30 * time.Millisecond):
	}
	close(gate)
	<-done
	// The messages are in distinct key groups, so all three had started; the
	// batch is stale only for what had not been checked yet, and whatever
	// finished and was durable is committed before Revoked returned.
	for _, c := range rig.broker.commits.all() {
		if c.Partition != 0 || c.Offset < 1 || c.Offset > 3 {
			t.Fatalf("commit = %v", c)
		}
	}
}

func TestConsumerLostDropsThePartitionsLikeARevocation(t *testing.T) {
	rig := startRig(t, nil)
	rig.broker.hooks.Assigned([]int32{0})
	rig.broker.hooks.Lost([]int32{0})
	rig.deliver(0, 0)
	never(t, "a batch for a lost partition", func() bool { return len(rig.seenOffsets()) > 0 })
}

func TestConsumerAFailingPollBacksOffAndTheBackoffResetsOnData(t *testing.T) {
	rig := startRig(t, nil)
	rig.broker.hooks.Assigned([]int32{0})
	errFetch := errors.New("fetch failed")
	rig.broker.feed <- fakePoll{err: errFetch}
	rig.broker.feed <- fakePoll{err: errFetch}
	eventually(t, "two backoffs", func() bool { return len(rig.clock.waits()) >= 2 })
	rig.deliver(0, 0)
	eventually(t, "commit", func() bool { return len(rig.broker.commits.all()) == 1 })
	rig.broker.feed <- fakePoll{err: errFetch}
	eventually(t, "third backoff", func() bool { return len(rig.clock.waits()) >= 3 })
	want := []time.Duration{time.Second, 2 * time.Second, time.Second}
	if got := rig.clock.waits()[:3]; !slices.Equal(got, want) {
		t.Fatalf("waits = %v", got)
	}
}

func TestConsumerAnIdlePollInTheGroupMarksTheLoopAlive(t *testing.T) {
	rig := startRig(t, nil)
	rig.broker.hooks.Assigned([]int32{0})
	eventually(t, "activity", func() bool { return rig.activity.Load() > 0 })
}

func TestConsumerAPollOutsideTheGroupDoesNotMarkTheLoopAlive(t *testing.T) {
	rig := startRig(t, nil)
	never(t, "activity", func() bool { return rig.activity.Load() > 0 })
	rig.broker.hooks.Assigned([]int32{0})
	rig.broker.hooks.Revoked([]int32{0})
	base := rig.activity.Load()
	never(t, "activity after leaving", func() bool { return rig.activity.Load() > base })
}

func TestConsumerAPollWhileABatchRunsDoesNotCoverForIt(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	rig := startRig(t, nil)
	rig.handle = func(context.Context, Delivery) error {
		once.Do(func() { close(entered) })
		<-gate
		return nil
	}
	rig.broker.hooks.Assigned([]int32{0})
	rig.deliver(0, 0)
	<-entered
	base := rig.activity.Load()
	never(t, "activity from idle polls while a lane runs", func() bool { return rig.activity.Load() > base })
	close(gate)
}

func TestConsumerBackpressurePausesAndResumesTheBroker(t *testing.T) {
	gate := make(chan struct{})
	rig := startRig(t, func(c *Config) {
		c.PartitionsConcurrent = 1
		c.MaxMessagesPerBatch = 2 // maxBuffered = 1 * 2 * 2 = 4
	})
	rig.handle = func(context.Context, Delivery) error { <-gate; return nil }
	rig.broker.hooks.Assigned([]int32{0})
	rig.deliver(0, 0, 1, 2, 3)
	eventually(t, "pause", func() bool { return rig.broker.paused.Load() == 1 })
	close(gate)
	eventually(t, "resume", func() bool { return rig.broker.resumed.Load() == 1 })
}

func TestConsumerStopConsumingWaitsForTheRunningBatchAndStopLeavesTheGroup(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	rig := startRig(t, nil)
	rig.handle = func(context.Context, Delivery) error {
		once.Do(func() { close(entered) })
		<-gate
		return nil
	}
	rig.broker.hooks.Assigned([]int32{0})
	// One message: with two, the batch may run them on parallel goroutines, and
	// when the second enters the gate first, StopConsuming stops the first before
	// it starts, leaving no handled prefix to commit.
	rig.deliver(0, 0)
	<-entered
	stopped := make(chan struct{})
	go func() { rig.consumer.StopConsuming(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("StopConsuming returned while a batch ran")
	case <-time.After(30 * time.Millisecond):
	}
	if rig.broker.closed.Load() {
		t.Fatal("the group was left before the batch finished")
	}
	close(gate)
	<-stopped
	if len(rig.broker.commits.all()) != 1 {
		t.Fatalf("what the batch handled must be committed: %v", rig.broker.commits.all())
	}
	rig.consumer.StopConsuming() // idempotent
	rig.consumer.Stop()
	if !rig.broker.closed.Load() {
		t.Fatal("Stop did not leave the group")
	}
}

func TestConsumerADeadLetteredMessageIsCommittedWithTheRest(t *testing.T) {
	rig := startRig(t, nil)
	rig.handle = func(_ context.Context, d Delivery) error {
		if d.Offset == 1 {
			return errors.New("poison")
		}
		return nil
	}
	rig.broker.hooks.Assigned([]int32{0})
	rig.deliver(0, 0, 1, 2)
	eventually(t, "commit", func() bool { return len(rig.broker.commits.all()) == 1 })
	if got := rig.broker.commits.all(); !slices.Equal(got, []commitCall{{0, 3, 3}}) {
		t.Fatalf("commits = %v", got)
	}
	if recorded := rig.deadLetter.recorded(); len(recorded) != 1 || recorded[0].Offset != 1 {
		t.Fatalf("dead letters = %+v", recorded)
	}
}
