package events

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newBuffer(redis *fakeRedis, size int) *EventBuffer {
	return NewEventBuffer(redis, EventBufferConfig{MaxSize: size, MaxWait: 5 * time.Millisecond}, nil, nil)
}

func TestFlushPendingPushesTheMicroBatchInOneRPush(t *testing.T) {
	redis := newFakeRedis()
	buffer := newBuffer(redis, 100)
	buffer.Add("a", true)
	buffer.Add("b", true)
	if err := buffer.FlushPendingOrThrow(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got := redis.list(EventQueueKey); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("list = %v", got)
	}
	if len(redis.rpushCalls) != 1 {
		t.Fatalf("%d RPUSH calls, want one carrying both rows", len(redis.rpushCalls))
	}
	if buffer.PendingLocalCount() != 0 {
		t.Fatal("rows still pending locally")
	}
}

func TestMicroBatchTimerFlushesWithoutAGate(t *testing.T) {
	redis := newFakeRedis()
	buffer := newBuffer(redis, 100)
	buffer.Add("a", true)
	deadline := time.Now().Add(2 * time.Second)
	for len(redis.list(EventQueueKey)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the timer never wrote the row")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReachingTheBatchSizeWritesAtOnce(t *testing.T) {
	redis := newFakeRedis()
	buffer := NewEventBuffer(redis, EventBufferConfig{MaxSize: 3, MaxWait: time.Hour}, nil, nil)
	for _, line := range []string{"a", "b", "c"} {
		buffer.Add(line, true)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(redis.list(EventQueueKey)) < 3 {
		if time.Now().After(deadline) {
			t.Fatal("a full batch was not written without the timer")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFlushFailureIsReturnedAndPlainRowsAreKept(t *testing.T) {
	redis := newFakeRedis()
	var failing atomic.Bool
	failing.Store(true)
	redis.failRPush = func(string, []string) error {
		if failing.Load() {
			return errRedisDown
		}
		return nil
	}
	buffer := newBuffer(redis, 100)
	buffer.Add("kept", false)
	err := buffer.FlushPendingOrThrow(context.Background(), 0)
	if !errors.Is(err, errRedisDown) {
		t.Fatalf("err = %v, want the Redis failure", err)
	}
	if buffer.PendingLocalCount() != 1 {
		t.Fatalf("pending = %d: a row nothing redelivers must be requeued", buffer.PendingLocalCount())
	}
	failing.Store(false)
	if err := buffer.FlushPendingOrThrow(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got := redis.list(EventQueueKey); len(got) != 1 || got[0] != "kept" {
		t.Fatalf("list = %v", got)
	}
}

func TestWaitsForTheWriteInFlightThenPushesWhatArrivedDuringIt(t *testing.T) {
	redis := newFakeRedis()
	redis.rpushGate = make(chan struct{})
	buffer := newBuffer(redis, 1)
	buffer.Add("first", true) // size 1: the write starts and blocks on the gate
	time.Sleep(20 * time.Millisecond)

	buffer.Add("second", true)
	done := make(chan error, 1)
	go func() { done <- buffer.FlushPendingOrThrow(context.Background(), 0) }()
	select {
	case <-done:
		t.Fatal("the gate resolved before the in-flight write ended")
	case <-time.After(30 * time.Millisecond):
	}
	close(redis.rpushGate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := redis.list(EventQueueKey)
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("list = %v, want first then second", got)
	}
}

func TestConcurrentCallersAllResolve(t *testing.T) {
	redis := newFakeRedis()
	buffer := newBuffer(redis, 100)
	var wait sync.WaitGroup
	errs := make(chan error, 40)
	for i := range 40 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			buffer.Add(string(rune('a'+i%26)), true)
			errs <- buffer.FlushPendingOrThrow(context.Background(), 0)
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("spurious rejection: %v", err)
		}
	}
	if got := len(redis.list(EventQueueKey)); got != 40 {
		t.Fatalf("%d rows in Redis, want 40", got)
	}
}

func TestEveryConcurrentCallerRejectsWhenRedisRefuses(t *testing.T) {
	redis := newFakeRedis()
	redis.failRPush = func(string, []string) error { return errRedisDown }
	buffer := newBuffer(redis, 100)
	var wait sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		buffer.Add("x", true)
	}
	for range 10 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs <- buffer.FlushPendingOrThrow(context.Background(), 0)
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err == nil {
			t.Fatal("a caller was told its rows are safe")
		}
	}
}

func TestRedeliverableRowsAreDroppedNotRequeued(t *testing.T) {
	redis := newFakeRedis()
	var failing atomic.Bool
	failing.Store(true)
	redis.failRPush = func(string, []string) error {
		if failing.Load() {
			return errRedisDown
		}
		return nil
	}
	buffer := newBuffer(redis, 100)
	for range 3 { // the same message redelivered through an outage
		buffer.Add("same", true)
		if err := buffer.FlushPendingOrThrow(context.Background(), 0); err == nil {
			t.Fatal("expected the gate to fail")
		}
	}
	failing.Store(false)
	gate := buffer.OpenWindow()
	buffer.Add("same", true)
	if err := gate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := redis.list(EventQueueKey); len(got) != 1 {
		t.Fatalf("%d copies in Redis, want exactly one", len(got))
	}
}

func TestAProducerNothingRedeliversKeepsItsRows(t *testing.T) {
	redis := newFakeRedis()
	var failing atomic.Bool
	failing.Store(true)
	redis.failRPush = func(string, []string) error {
		if failing.Load() {
			return errRedisDown
		}
		return nil
	}
	buffer := newBuffer(redis, 100)
	buffer.Add("held", false)
	_ = buffer.FlushPendingOrThrow(context.Background(), 0)
	failing.Store(false)
	if err := buffer.FlushPendingOrThrow(context.Background(), buffer.DroppedThrough()); err != nil {
		t.Fatal(err)
	}
	if got := redis.list(EventQueueKey); len(got) != 1 || got[0] != "held" {
		t.Fatalf("list = %v", got)
	}
}

func TestAWindowRejectsWhatAWriteItNeverAwaitedDropped(t *testing.T) {
	redis := newFakeRedis()
	var failing atomic.Bool
	failing.Store(true)
	redis.failRPush = func(string, []string) error {
		if failing.Load() {
			return errRedisDown
		}
		return nil
	}
	buffer := newBuffer(redis, 1)
	gate := buffer.OpenWindow()
	buffer.Add("dropped", true) // the size-1 write fails on its own
	time.Sleep(30 * time.Millisecond)
	failing.Store(false)
	if err := gate(context.Background()); err == nil {
		t.Fatal("the window's row was dropped but the gate said it was durable")
	}
}

func TestAWindowOpenedAfterTheDropIsNotPoisoned(t *testing.T) {
	redis := newFakeRedis()
	var failing atomic.Bool
	failing.Store(true)
	redis.failRPush = func(string, []string) error {
		if failing.Load() {
			return errRedisDown
		}
		return nil
	}
	buffer := newBuffer(redis, 1)
	buffer.Add("dropped", true)
	time.Sleep(30 * time.Millisecond)
	failing.Store(false)
	gate := buffer.OpenWindow()
	buffer.Add("fine", true)
	if err := gate(context.Background()); err != nil {
		t.Fatalf("a new window failed for a drop before it opened: %v", err)
	}
}

func TestAddCountsEachRowOnce(t *testing.T) {
	redis := newFakeRedis()
	metrics := &recordingMetrics{}
	buffer := NewEventBuffer(redis, EventBufferConfig{}, metrics, nil)
	buffer.Add("a", true)
	buffer.Add("b", true)
	if metrics.added["event"] != 2 {
		t.Fatalf("buffer_add_total{event} = %d", metrics.added["event"])
	}
}
