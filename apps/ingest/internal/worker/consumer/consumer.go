package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// bufferedBatchesPerLane is how many batches each partition may have
	// waiting behind the one it runs.
	bufferedBatchesPerLane = 2
	// pollRetryInitial and pollRetryMax bound the wait after a poll that
	// failed and returned nothing, doubling per failure in a row.
	pollRetryInitial = time.Second
	pollRetryMax     = 30 * time.Second
)

// ErrBrokerClosed is what Broker.Poll returns once the client is closed.
var ErrBrokerClosed = errors.New("kafka client closed")

// GroupHooks are the group membership events a Broker reports. Each runs on
// the broker's group goroutine and blocks the rebalance until it returns.
type GroupHooks interface {
	// Assigned is a completed join: the partitions of the events topic this
	// member now owns.
	Assigned(partitions []int32)
	// Revoked is a rebalance taking partitions away. Offsets can still be
	// committed from inside it.
	Revoked(partitions []int32)
	// Lost is a fatal group error: the partitions are gone and commits will
	// likely fail.
	Lost(partitions []int32)
}

// Broker is the Kafka client as the consumer uses it: one consumer-group
// member subscribed to the events topic, from the committed offsets (the
// earliest for a partition with none), with manual commits.
type Broker interface {
	// Poll returns the next records, or none once ctx is done. The group
	// cannot rebalance until AllowRebalance, so the records of one Poll are
	// pushed to the lanes before a revocation can run.
	Poll(ctx context.Context) ([]Record, error)
	AllowRebalance()
	PauseFetch()
	ResumeFetch()
	// Commit commits the next offset to consume for one partition.
	Commit(ctx context.Context, partition int32, offset int64, leaderEpoch int32) error
	// Close leaves the group.
	Close()
}

// BrokerFactory connects the broker. An error fails the boot: a wrong broker
// list or credential has to stop the deploy, not show up later as a stale probe.
type BrokerFactory func(ctx context.Context, hooks GroupHooks) (Broker, error)

// Config wires a Consumer.
type Config struct {
	NewBroker  BrokerFactory
	Handler    Handler
	DeadLetter DeadLetterSink
	Durability Durability
	Logger     *slog.Logger
	Metrics    Metrics
	Clock      Clock
	// OnActivity is the readiness heartbeat (markEventsActivity).
	OnActivity func()

	Topic                string
	PartitionsConcurrent int
	MaxMessagesPerBatch  int
	// PollTimeout is how long one Poll waits: the fetch max wait. An idle
	// consumer still ticks at this rate.
	PollTimeout  time.Duration
	MaxAttempts  int
	InitialRetry time.Duration
	MaxRetry     time.Duration
}

// Consumer runs the events consumer: broker, lanes and batch handler.
type Consumer struct {
	cfg     Config
	broker  Broker
	lanes   *Lanes
	batches *BatchHandler

	workCtx    context.Context
	cancelWork context.CancelFunc
	loopCtx    context.Context
	stopLoop   context.CancelFunc
	loopDone   chan struct{}

	// joined is true between a join and the next revoke or loss: a poll
	// outside the group does not prove the consumer healthy.
	joined atomic.Bool

	mu    sync.Mutex
	owned map[int32]struct{}

	stopOnce sync.Once
}

// Start connects, joins the group and begins consuming. Group membership,
// reconnects and re-joins after a lost session are the Kafka client's; a
// failing poll is retried here with the stream-restart backoff.
func Start(ctx context.Context, cfg Config) (*Consumer, error) {
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	c := &Consumer{cfg: cfg, workCtx: workCtx, cancelWork: cancelWork, owned: map[int32]struct{}{}, loopDone: make(chan struct{})}
	c.loopCtx, c.stopLoop = context.WithCancel(workCtx)
	c.batches = NewBatchHandler(BatchHandlerConfig{
		Handler: cfg.Handler, DeadLetter: cfg.DeadLetter, Durability: cfg.Durability,
		Logger: cfg.Logger, Metrics: cfg.Metrics, Clock: cfg.Clock, OnActivity: cfg.OnActivity,
		Topic: cfg.Topic, MaxAttempts: cfg.MaxAttempts, InitialRetry: cfg.InitialRetry, MaxRetry: cfg.MaxRetry,
	})
	c.lanes = NewLanes(workCtx, LanesConfig{
		Concurrency:  cfg.PartitionsConcurrent,
		MaxBatchSize: cfg.MaxMessagesPerBatch,
		MaxBuffered:  cfg.PartitionsConcurrent * cfg.MaxMessagesPerBatch * bufferedBatchesPerLane,
		RunBatch:     c.batches.EachBatch,
		Commit: func(ctx context.Context, partition int32, offset int64, epoch int32) error {
			return c.broker.Commit(ctx, partition, offset, epoch)
		},
		PauseSource:  func() { c.broker.PauseFetch() },
		ResumeSource: func() { c.broker.ResumeFetch() },
		Logger:       cfg.Logger,
		Clock:        cfg.Clock,
	})
	broker, err := cfg.NewBroker(ctx, c)
	if err != nil {
		cancelWork()
		return nil, err
	}
	c.broker = broker
	cfg.Logger.Info("kafka events consumer running", "topic", cfg.Topic, "partitionsConsumedConcurrently", cfg.PartitionsConcurrent)
	go c.pollLoop()
	return c, nil
}

// Assigned implements GroupHooks.
func (c *Consumer) Assigned(partitions []int32) {
	// New partitions may have moved between members, so a failure count
	// carried over from the old assignment would pick the wrong backoff.
	c.batches.ResetDurabilityBackoff()
	c.mu.Lock()
	for _, p := range partitions {
		c.owned[p] = struct{}{}
	}
	owned := c.ownedLocked()
	c.mu.Unlock()
	c.lanes.SetAssignment(owned)
	c.joined.Store(true)
	c.cfg.Logger.Info("kafka consumer joined group (rebalance complete)", "assignments", partitions)
}

// Revoked implements GroupHooks. It returns once the batches of the revoked
// partitions stopped between messages and committed what was durable, so the
// next owner starts where they left off.
func (c *Consumer) Revoked(partitions []int32) {
	c.release(partitions)
}

// Lost implements GroupHooks.
func (c *Consumer) Lost(partitions []int32) {
	c.cfg.Logger.Error("kafka consumer lost its group membership", "partitions", partitions)
	c.release(partitions)
}

func (c *Consumer) release(partitions []int32) {
	c.mu.Lock()
	for _, p := range partitions {
		delete(c.owned, p)
	}
	owned := c.ownedLocked()
	c.mu.Unlock()
	c.lanes.SetAssignment(owned)
	c.lanes.WaitPartitionsIdle(partitions)
	if len(owned) == 0 {
		c.joined.Store(false)
	}
}

// ownedLocked is never nil: a nil assignment would mean "not known yet" and
// accept every partition.
func (c *Consumer) ownedLocked() []int32 {
	owned := make([]int32, 0, len(c.owned))
	for p := range c.owned {
		owned = append(owned, p)
	}
	return owned
}

func (c *Consumer) pollLoop() {
	defer close(c.loopDone)
	failures := 0
	for c.loopCtx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(c.loopCtx, c.cfg.PollTimeout)
		records, err := c.broker.Poll(pollCtx)
		cancel()
		// A busy lane proves its own progress; only an idle consumer that is
		// in the group vouches for itself here.
		idleInGroup := c.lanes.ActiveCount() == 0 && c.joined.Load()
		if errors.Is(err, ErrBrokerClosed) {
			return
		}
		if idleInGroup {
			c.cfg.OnActivity()
		}
		for _, record := range records {
			c.lanes.Push(record.Partition, record)
		}
		c.lanes.Schedule()
		c.broker.AllowRebalance()
		if len(records) > 0 {
			failures = 0
		}
		if err == nil || c.loopCtx.Err() != nil || isPollTimeout(err) {
			continue
		}
		c.cfg.Logger.Error("kafka consumer crashed", "err", err)
		if len(records) == 0 {
			failures = c.waitBeforePolling(failures)
		}
	}
}

func isPollTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func (c *Consumer) waitBeforePolling(failures int) int {
	delay := pollRetryInitial
	for i := 0; i < failures && delay < pollRetryMax; i++ {
		delay *= retryBackoffFactor
	}
	delay = min(delay, pollRetryMax)
	c.cfg.Logger.Warn("reopening the kafka events stream", "delayMs", delay.Milliseconds(), "attempt", failures+1)
	_ = c.cfg.Clock.Sleep(c.loopCtx, delay)
	return failures + 1
}

// StopConsuming stops fetching and waits for the running batches, which stop
// between messages, make what they handled durable and commit it. Safe to
// call more than once.
func (c *Consumer) StopConsuming() {
	c.stopOnce.Do(func() {
		c.stopLoop()
		<-c.loopDone
		c.lanes.Stop()
	})
}

// Stop is StopConsuming, then leaving the group. Without the leave the other
// members wait out the session timeout before the partitions move.
func (c *Consumer) Stop() {
	c.StopConsuming()
	c.broker.Close()
	c.cancelWork()
}
