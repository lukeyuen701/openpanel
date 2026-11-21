package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/wire"
)

const testTopic = "events"
const testPartition = 7

type batchRig struct {
	handler    *BatchHandler
	clock      *fakeClock
	metrics    *recordingMetrics
	deadLetter *memoryDeadLetter
	durability *fakeDurability
	logger     *recordingLogger
	activity   atomic.Int64
	mu         sync.Mutex
	handled    []string // "offset" in handling order
	failWith   func(delivery Delivery, attempt int) error
	attempts   map[int64]int
}

func newBatchRig(t *testing.T, mutate func(*BatchHandlerConfig)) *batchRig {
	t.Helper()
	rig := &batchRig{
		clock: &fakeClock{}, metrics: newRecordingMetrics(), deadLetter: &memoryDeadLetter{},
		durability: &fakeDurability{}, logger: newRecordingLogger(), attempts: map[int64]int{},
	}
	cfg := BatchHandlerConfig{
		Handler: func(_ context.Context, _ wire.IncomingEventPayload, d Delivery) error {
			rig.mu.Lock()
			rig.attempts[d.Offset]++
			attempt := rig.attempts[d.Offset]
			rig.handled = append(rig.handled, fmt.Sprint(d.Offset))
			fail := rig.failWith
			rig.mu.Unlock()
			if fail != nil {
				return fail(d, attempt)
			}
			return nil
		},
		DeadLetter: rig.deadLetter, Durability: rig.durability, Logger: rig.logger.Logger,
		Metrics: rig.metrics, Clock: rig.clock, OnActivity: func() { rig.activity.Add(1) },
		Topic: testTopic, MaxAttempts: 3, InitialRetry: 10 * time.Millisecond, MaxRetry: 25 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	rig.handler = NewBatchHandler(cfg)
	return rig
}

func (r *batchRig) handledCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.handled)
}

// control is a BatchControl whose answers the test sets.
type control struct {
	running  func() bool
	stale    func() bool
	mu       sync.Mutex
	resolved []int64
}

func (c *control) batchControl() *BatchControl {
	running, stale := c.running, c.stale
	if running == nil {
		running = func() bool { return true }
	}
	if stale == nil {
		stale = func() bool { return false }
	}
	return NewBatchControl(running, stale, func(offset int64) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.resolved = append(c.resolved, offset)
	})
}

func (c *control) offsets() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.resolved)
}

func payload(offset int64) string {
	return fmt.Sprintf(`{"projectId":"project-%d","event":{"name":"screen_view"}}`, offset)
}

func batchOf(keys ...string) []Record {
	items := make([]Record, len(keys))
	for i, key := range keys {
		items[i] = record(testPartition, int64(i), key, payload(int64(i)))
	}
	return items
}

func (r *batchRig) run(t *testing.T, items []Record, c *control) Outcome {
	t.Helper()
	outcome, err := r.handler.EachBatch(context.Background(), testPartition, items, c.batchControl())
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}

func TestBatchResolvesTheWholeContiguousPrefixInAscendingOrder(t *testing.T) {
	rig := newBatchRig(t, nil)
	c := &control{}
	if outcome := rig.run(t, batchOf("a", "b", "a", "c", "b"), c); outcome != Done {
		t.Fatalf("outcome = %v", outcome)
	}
	if got := c.offsets(); !slices.Equal(got, []int64{0, 1, 2, 3, 4}) {
		t.Fatalf("resolved = %v", got)
	}
}

func TestBatchStopsAtTheFirstGapEvenWhenLaterOffsetsFinished(t *testing.T) {
	var running atomic.Bool
	running.Store(true)
	flipped := make(chan struct{})
	rig := newBatchRig(t, nil)
	rig.handler.cfg.Handler = func(_ context.Context, _ wire.IncomingEventPayload, d Delivery) error {
		switch d.Offset {
		case 0:
			running.Store(false)
			close(flipped)
		case 2:
			<-flipped // group b is already past its own check when a stops
		}
		return nil
	}
	c := &control{running: running.Load}
	items := batchOf("a", "a", "b")
	// Offset 2 is checked before offset 0 flips the state, so it is handled.
	outcome, _ := rig.handler.EachBatch(context.Background(), testPartition, items, c.batchControl())
	if outcome != Done {
		t.Fatalf("outcome = %v", outcome)
	}
	if got := c.offsets(); !slices.Equal(got, []int64{0}) {
		t.Fatalf("resolved = %v (offset 1 never ran, so 2 must not be resolved)", got)
	}
}

func TestBatchResolvesNothingWhenStaleBeforeTheFirstMessage(t *testing.T) {
	rig := newBatchRig(t, nil)
	c := &control{stale: func() bool { return true }}
	rig.run(t, batchOf("a", "b"), c)
	if len(c.offsets()) != 0 || rig.handledCount() != 0 {
		t.Fatalf("resolved = %v, handled = %d", c.offsets(), rig.handledCount())
	}
}

func TestBatchMessagesOfOneKeyRunInOrderAndDifferentKeysConcurrently(t *testing.T) {
	var mu sync.Mutex
	perKey := map[string][]int64{}
	var concurrentSameKey atomic.Bool
	inFlight := map[string]int{}
	arrived := make(chan struct{}, 2)
	proceed := make(chan struct{})
	rig := newBatchRig(t, nil)
	items := batchOf("a", "b", "a", "b", "a")
	rig.handler.cfg.Handler = func(_ context.Context, _ wire.IncomingEventPayload, d Delivery) error {
		key := string(items[d.Offset].Key)
		mu.Lock()
		inFlight[key]++
		if inFlight[key] > 1 {
			concurrentSameKey.Store(true)
		}
		perKey[key] = append(perKey[key], d.Offset)
		first := d.Offset < 2
		mu.Unlock()
		if first {
			arrived <- struct{}{}
			<-proceed
		}
		mu.Lock()
		inFlight[key]--
		mu.Unlock()
		return nil
	}
	c := &control{}
	done := make(chan struct{})
	go func() { rig.run(t, items, c); close(done) }()
	// Both keys' first messages are inside the handler at the same time.
	<-arrived
	<-arrived
	close(proceed)
	<-done
	if concurrentSameKey.Load() {
		t.Fatal("two messages of one key ran at once")
	}
	if !slices.Equal(perKey["a"], []int64{0, 2, 4}) || !slices.Equal(perKey["b"], []int64{1, 3}) {
		t.Fatalf("per key = %v", perKey)
	}
}

func TestBatchANullKeyIsItsOwnGroupAndAnEmptyKeyIsOneGroup(t *testing.T) {
	items := []Record{record(1, 0, "-", payload(0)), record(1, 1, "-", payload(1)), record(1, 2, "", payload(2)), record(1, 3, "", payload(3))}
	groups := groupByKey(items)
	if len(groups) != 3 || !slices.Equal(groups[2], []int{2, 3}) {
		t.Fatalf("groups = %v", groups)
	}
}

func TestBatchRetriesWithBoundedBackoffBeforeGivingUp(t *testing.T) {
	rig := newBatchRig(t, nil)
	rig.failWith = func(Delivery, int) error { return errors.New("db is down") }
	c := &control{}
	rig.run(t, batchOf("a"), c)
	if rig.handledCount() != 3 {
		t.Fatalf("attempts = %d", rig.handledCount())
	}
	if got := rig.clock.waits(); !slices.Equal(got, []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}) {
		t.Fatalf("waits = %v", got)
	}
	if rig.metrics.failed["7"] != 3 || rig.metrics.deadLetters["7/handler_error"] != 1 {
		t.Fatalf("metrics = %+v", rig.metrics)
	}
	if got := c.offsets(); !slices.Equal(got, []int64{0}) {
		t.Fatalf("a dead-lettered message must still resolve: %v", got)
	}
}

func TestBatchTheRetryWaitIsCappedByTheMaximum(t *testing.T) {
	rig := newBatchRig(t, func(cfg *BatchHandlerConfig) { cfg.MaxAttempts = 5 })
	rig.failWith = func(Delivery, int) error { return errors.New("x") }
	rig.run(t, batchOf("a"), &control{})
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}
	if got := rig.clock.waits(); !slices.Equal(got, want) {
		t.Fatalf("waits = %v", got)
	}
}

func TestBatchStopsRetryingAsSoonAsTheHandlerSucceeds(t *testing.T) {
	rig := newBatchRig(t, nil)
	rig.failWith = func(_ Delivery, attempt int) error {
		if attempt < 2 {
			return errors.New("flaky")
		}
		return nil
	}
	rig.run(t, batchOf("a"), &control{})
	if rig.handledCount() != 2 || len(rig.deadLetter.recorded()) != 0 || rig.metrics.failed["7"] != 1 {
		t.Fatalf("handled = %d, recorded = %d, failed = %v", rig.handledCount(), len(rig.deadLetter.recorded()), rig.metrics.failed)
	}
}

func TestBatchDeadLettersTheRawMessage(t *testing.T) {
	rig := newBatchRig(t, nil)
	rig.failWith = func(Delivery, int) error { return errors.New("db is down") }
	items := batchOf("device-1", "other")
	items[0].Headers = []Header{{"x-h", []byte("hv")}}
	c := &control{}
	rig.run(t, items, c)
	var got DeadLetterMessage
	for _, m := range rig.deadLetter.recorded() {
		if m.Offset == 0 {
			got = m
		}
	}
	want := DeadLetterMessage{
		Key: []byte("device-1"), Value: []byte(payload(0)), Headers: []Header{{"x-h", []byte("hv")}},
		Topic: testTopic, Partition: testPartition, Offset: 0, Reason: ReasonHandlerError, Err: "db is down",
	}
	if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
		t.Fatalf("dead letter = %+v", got)
	}
	if got := c.offsets(); !slices.Equal(got, []int64{0, 1}) {
		t.Fatalf("resolved = %v", got)
	}
}

func TestBatchResolvesTheOffsetEvenWhenTheDeadLetterWriteFails(t *testing.T) {
	rig := newBatchRig(t, nil)
	rig.deadLetter.err = errRedisDown
	rig.failWith = func(Delivery, int) error { return errors.New("db is down") }
	c := &control{}
	rig.run(t, batchOf("a"), c)
	if got := c.offsets(); !slices.Equal(got, []int64{0}) {
		t.Fatalf("resolved = %v", got)
	}
	if rig.metrics.dropped["7"] != 1 || len(rig.metrics.deadLetters) != 0 {
		t.Fatalf("metrics = %+v", rig.metrics)
	}
}

func TestBatchUnparseableMessagesAreDeadLetteredWithoutRetry(t *testing.T) {
	rig := newBatchRig(t, nil)
	items := []Record{record(7, 0, "a", `{"bad":`), record(7, 1, "b", "")}
	items[1].Value = nil
	c := &control{}
	rig.run(t, items, c)
	if rig.handledCount() != 0 || len(rig.clock.waits()) != 0 || rig.metrics.failed["7"] != 0 {
		t.Fatalf("handler must not run or retry: handled = %d", rig.handledCount())
	}
	messages := rig.deadLetter.recorded()
	slices.SortFunc(messages, func(a, b DeadLetterMessage) int { return int(a.Offset - b.Offset) })
	if len(messages) != 2 || messages[0].Reason != ReasonParseError || messages[1].Reason != ReasonParseError {
		t.Fatalf("messages = %+v", messages)
	}
	if messages[1].Err != "kafka message has no value" || messages[1].Value != nil {
		t.Fatalf("no-value message = %+v", messages[1])
	}
	if rig.metrics.deadLetters["7/parse_error"] != 2 || !slices.Equal(c.offsets(), []int64{0, 1}) {
		t.Fatalf("metrics = %+v, resolved = %v", rig.metrics, c.offsets())
	}
}

func TestBatchAValueThatIsNotAnObjectFailsTheHandlerNotTheParse(t *testing.T) {
	for _, value := range []string{"null", "5", "[1]", `"x"`} {
		rig := newBatchRig(t, nil)
		items := []Record{record(7, 0, "a", value)}
		c := &control{}
		rig.run(t, items, c)
		if rig.handledCount() != 0 || rig.metrics.failed["7"] != 3 || rig.metrics.deadLetters["7/handler_error"] != 1 {
			t.Fatalf("%s: handled = %d, metrics = %+v", value, rig.handledCount(), rig.metrics)
		}
		if got := rig.deadLetter.recorded()[0].Err; got != wire.ErrNotObject.Error() {
			t.Fatalf("%s: error = %q", value, got)
		}
	}
}

func TestBatchARedeliveredBatchIsHandledAndResolvedAgain(t *testing.T) {
	rig := newBatchRig(t, nil)
	first, second := &control{}, &control{}
	rig.run(t, batchOf("a", "b"), first)
	rig.run(t, batchOf("a", "b"), second)
	if !slices.Equal(second.offsets(), []int64{0, 1}) || rig.handledCount() != 4 {
		t.Fatalf("resolved = %v, handled = %d", second.offsets(), rig.handledCount())
	}
}

func TestBatchFlushesBeforeItResolvesAnyOffset(t *testing.T) {
	rig := newBatchRig(t, nil)
	c := &control{}
	var resolvedWhenFlushed, handledWhenFlushed = -1, -1
	rig.durability.gate = func() error {
		resolvedWhenFlushed = len(c.offsets())
		handledWhenFlushed = rig.handledCount()
		return nil
	}
	rig.run(t, batchOf("a", "b"), c)
	if resolvedWhenFlushed != 0 || handledWhenFlushed != 2 || !slices.Equal(c.offsets(), []int64{0, 1}) {
		t.Fatalf("resolved at flush = %d, handled at flush = %d, resolved = %v", resolvedWhenFlushed, handledWhenFlushed, c.offsets())
	}
}

func TestBatchResolvesNothingWhenTheFlushFails(t *testing.T) {
	rig := newBatchRig(t, nil)
	rig.durability.gate = func() error { return errRedisDown }
	c := &control{}
	outcome := rig.run(t, batchOf("a", "b", "a"), c)
	if outcome != Redeliver || len(c.offsets()) != 0 || rig.handledCount() != 3 {
		t.Fatalf("outcome = %v, resolved = %v, handled = %d", outcome, c.offsets(), rig.handledCount())
	}
	if rig.logger.h.errors.Load() != 1 || rig.activity.Load() == 0 {
		t.Fatalf("errors = %d, activity = %d", rig.logger.h.errors.Load(), rig.activity.Load())
	}
}

func TestBatchARedeliveryAfterAFailedFlushCommits(t *testing.T) {
	rig := newBatchRig(t, nil)
	var down atomic.Bool
	down.Store(true)
	rig.durability.gate = func() error {
		if down.Load() {
			return errRedisDown
		}
		return nil
	}
	first := &control{}
	rig.run(t, batchOf("a", "b"), first)
	down.Store(false)
	second := &control{}
	rig.run(t, batchOf("a", "b"), second)
	if len(first.offsets()) != 0 || !slices.Equal(second.offsets(), []int64{0, 1}) {
		t.Fatalf("first = %v, second = %v", first.offsets(), second.offsets())
	}
}

func TestBatchOpensTheDurabilityWindowBeforeTheFirstHandler(t *testing.T) {
	rig := newBatchRig(t, nil)
	handledWhenOpened := -1
	rig.durability.onOpen = func() { handledWhenOpened = rig.handledCount() }
	rig.run(t, batchOf("a", "b"), &control{})
	if rig.durability.opened.Load() != 1 || rig.durability.closed.Load() != 1 || handledWhenOpened != 0 {
		t.Fatalf("opened = %d, closed = %d, handled when opened = %d", rig.durability.opened.Load(), rig.durability.closed.Load(), handledWhenOpened)
	}
}

func TestBatchBacksOffBeforeARedeliveryAndTheWaitGrowsThenCaps(t *testing.T) {
	rig := newBatchRig(t, func(cfg *BatchHandlerConfig) { cfg.InitialRetry = 100 * time.Millisecond; cfg.MaxRetry = time.Second })
	rig.durability.gate = func() error { return errRedisDown }
	const laps = 8
	for range laps {
		rig.run(t, batchOf("a"), &control{})
	}
	want := []time.Duration{100, 200, 400, 800, 1600, 3200, 5000, 5000}
	for i := range want {
		want[i] *= time.Millisecond
	}
	if got := rig.clock.waits(); !slices.Equal(got, want) {
		t.Fatalf("waits = %v", got)
	}
}

func TestBatchForgetsTheBackoffOnceDurableAgain(t *testing.T) {
	rig := newBatchRig(t, func(cfg *BatchHandlerConfig) { cfg.InitialRetry = 100 * time.Millisecond })
	var down atomic.Bool
	down.Store(true)
	rig.durability.gate = func() error {
		if down.Load() {
			return errRedisDown
		}
		return nil
	}
	for range 3 {
		rig.run(t, batchOf("a"), &control{})
	}
	down.Store(false)
	rig.run(t, batchOf("a"), &control{})
	down.Store(true)
	rig.run(t, batchOf("a"), &control{})
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 100 * time.Millisecond}
	if got := rig.clock.waits(); !slices.Equal(got, want) {
		t.Fatalf("waits = %v", got)
	}
}

func TestBatchResetDurabilityBackoffStartsTheWaitOver(t *testing.T) {
	rig := newBatchRig(t, func(cfg *BatchHandlerConfig) { cfg.InitialRetry = 100 * time.Millisecond })
	rig.durability.gate = func() error { return errRedisDown }
	rig.run(t, batchOf("a"), &control{})
	rig.run(t, batchOf("a"), &control{})
	rig.handler.ResetDurabilityBackoff()
	rig.run(t, batchOf("a"), &control{})
	if got := rig.clock.waits(); !slices.Equal(got, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 100 * time.Millisecond}) {
		t.Fatalf("waits = %v", got)
	}
}

func TestBatchDoesNotHoldThePartitionWhileShuttingDown(t *testing.T) {
	rig := newBatchRig(t, nil)
	rig.durability.gate = func() error { return errRedisDown }
	c := &control{running: func() bool { return false }}
	rig.run(t, batchOf("a"), c)
	if len(rig.clock.waits()) != 0 || len(c.offsets()) != 0 {
		t.Fatalf("waits = %v, resolved = %v", rig.clock.waits(), c.offsets())
	}
}

func TestBatchFlushesWhatAShutdownTruncatedBatchHandledBeforeResolvingIt(t *testing.T) {
	rig := newBatchRig(t, nil)
	c := &control{running: func() bool { return rig.handledCount() < 2 }}
	rig.run(t, batchOf("a", "a", "a"), c)
	if !slices.Equal(c.offsets(), []int64{0, 1}) || rig.durability.closed.Load() != 1 {
		t.Fatalf("resolved = %v, flushes = %d", c.offsets(), rig.durability.closed.Load())
	}
}

func TestBatchMarksActivityEverySixteenMessagesAndAfterTheBatch(t *testing.T) {
	rig := newBatchRig(t, nil)
	keys := make([]string, 40)
	for i := range keys {
		keys[i] = fmt.Sprint("k", i)
	}
	rig.run(t, batchOf(keys...), &control{})
	if got := rig.activity.Load(); got != 3 { // messages 16 and 32, then the end of the batch
		t.Fatalf("activity = %d", got)
	}
}

func TestBatchAPanickingHandlerCountsAsAFailedAttempt(t *testing.T) {
	rig := newBatchRig(t, nil)
	rig.handler.cfg.Handler = func(context.Context, wire.IncomingEventPayload, Delivery) error { panic("boom") }
	c := &control{}
	rig.run(t, batchOf("a"), c)
	if rig.metrics.failed["7"] != 3 || !strings.Contains(rig.deadLetter.recorded()[0].Err, "boom") {
		t.Fatalf("metrics = %+v, dead letters = %+v", rig.metrics, rig.deadLetter.recorded())
	}
}

func TestBatchAnEmptyBatchIsDoneWithoutOpeningAWindow(t *testing.T) {
	rig := newBatchRig(t, nil)
	if outcome := rig.run(t, nil, &control{}); outcome != Done || rig.durability.opened.Load() != 0 {
		t.Fatalf("outcome = %v, opened = %d", outcome, rig.durability.opened.Load())
	}
}
