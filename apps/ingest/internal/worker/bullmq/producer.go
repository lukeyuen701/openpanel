package bullmq

import (
	"context"
	"crypto/sha1" //nolint:gosec // BullMQ addresses scripts by SHA-1.
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"openpanel/ingest/internal/jsjson"
)

//go:embed scripts/addStandardJob.lua
var addStandardJobScript string

const (
	// ioredis's default retryStrategy, which the TS queue connection keeps:
	// 50 ms per attempt up to 2 s.
	retryStep = 50 * time.Millisecond
	retryCap  = 2 * time.Second

	standardJobKeys = 9
	// ARGV[1] of addStandardJob holds this many msgpack values.
	standardJobArgs = 10
)

// Clock is the wall clock and the waiting between retries.
type Clock interface {
	Now() time.Time
	// Sleep waits d or until ctx ends, returning ctx's error in that case.
	Sleep(ctx context.Context, d time.Duration) error
}

// SystemClock is the real clock.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// Sleep implements Clock.
func (SystemClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Config is what NewProducer needs. Redis and Queues are required.
type Config struct {
	Redis Redis
	// Queues are the queues Add accepts.
	Queues []Queue
	// Namespace and Cluster are QUEUE_NAMESPACE and QUEUE_CLUSTER.
	Namespace string
	Cluster   bool
	Clock     Clock
	Logger    *slog.Logger
	// Registerer receives the producer's metrics; nil registers none.
	Registerer prometheus.Registerer
	// RetryDelay is the wait before retry number attempt (1-based) while
	// Redis is unavailable; nil uses ioredis's schedule.
	RetryDelay func(attempt int) time.Duration
}

// Producer adds jobs the way BullMQ's Queue.add does.
type Producer struct {
	redis      Redis
	queues     map[string]Queue
	namespace  string
	cluster    bool
	clock      Clock
	logger     *slog.Logger
	retryDelay func(int) time.Duration
	retries    *prometheus.CounterVec
	scriptSHA  string

	metaMu   sync.Mutex
	metaDone map[string]bool
}

// NewProducer builds a Producer. It does no I/O.
func NewProducer(cfg Config) (*Producer, error) {
	if cfg.Redis == nil {
		return nil, errors.New("bullmq: Redis is required")
	}
	queues := make(map[string]Queue, len(cfg.Queues))
	for _, queue := range cfg.Queues {
		queues[queue.Name] = queue
	}
	p := &Producer{
		redis:      cfg.Redis,
		queues:     queues,
		namespace:  cfg.Namespace,
		cluster:    cfg.Cluster,
		clock:      cfg.Clock,
		logger:     cfg.Logger,
		retryDelay: cfg.RetryDelay,
		scriptSHA:  scriptSHA(addStandardJobScript),
		metaDone:   map[string]bool{},
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bullmq_enqueue_retries_total",
			Help: "Enqueue attempts repeated because Redis was unavailable.",
		}, []string{"queue"}),
	}
	if p.clock == nil {
		p.clock = SystemClock{}
	}
	if p.logger == nil {
		p.logger = slog.New(slog.DiscardHandler)
	}
	if p.retryDelay == nil {
		p.retryDelay = defaultRetryDelay
	}
	if cfg.Registerer != nil {
		if err := cfg.Registerer.Register(p.retries); err != nil {
			return nil, fmt.Errorf("bullmq: register metrics: %w", err)
		}
	}
	return p, nil
}

func defaultRetryDelay(attempt int) time.Duration {
	return min(time.Duration(attempt)*retryStep, retryCap)
}

func scriptSHA(script string) string {
	sum := sha1.Sum([]byte(script)) //nolint:gosec // see import
	return hex.EncodeToString(sum[:])
}

// ScriptSHA is the SHA-1 EVALSHA addresses addStandardJob by.
func (p *Producer) ScriptSHA() string { return p.scriptSHA }

// Add enqueues one job and returns its id: opts.JobID, or the queue's
// counter. data is the job's JSON text (see Envelope). While Redis is
// unavailable Add waits and retries until ctx ends, as the TS producer's
// offline queue does; a refusal from Redis is returned at once. A retry after
// a connection that died mid-command can enqueue a job without an id twice,
// as ioredis's resend can.
func (p *Producer) Add(ctx context.Context, queueName, jobName string, data []byte, opts AddOptions) (string, error) {
	queue, ok := p.queues[queueName]
	if !ok {
		return "", fmt.Errorf("bullmq: queue %q is not registered", queueName)
	}
	if opts.JobID != "" {
		if err := refusesCustomID(opts.JobID); err != nil {
			return "", err
		}
	}
	key := QueueKey(queue.Name, p.namespace, p.cluster)
	keys := newQueueKeys(key)
	timestamp := p.clock.Now().UnixMilli()
	args := p.arguments(keys, opts.JobID, jobName, timestamp, data, queue.Defaults.merged(opts))

	for attempt := 1; ; attempt++ {
		id, err := p.addOnce(ctx, keys, args)
		if err == nil || !isTransient(err) {
			return id, err
		}
		p.retries.WithLabelValues(queue.Name).Inc()
		p.logger.Warn("redis unavailable, retrying enqueue", "queue", queue.Name, "job", jobName, "attempt", attempt, "err", err)
		if sleepErr := p.clock.Sleep(ctx, p.retryDelay(attempt)); sleepErr != nil {
			return "", fmt.Errorf("bullmq: add %s/%s: %w (last error: %v)", queue.Name, jobName, sleepErr, err)
		}
	}
}

// Init writes every registered queue's meta hash, as BullMQ's Queue
// constructor does for all queues at boot, waiting out an outage like Add.
// Calling it is optional: Add writes a queue's meta before its first job.
func (p *Producer) Init(ctx context.Context) error {
	for _, queue := range p.queues {
		keys := newQueueKeys(QueueKey(queue.Name, p.namespace, p.cluster))
		for attempt := 1; ; attempt++ {
			err := p.ensureMeta(ctx, keys)
			if err == nil {
				break
			}
			if !isTransient(err) {
				return err
			}
			if sleepErr := p.clock.Sleep(ctx, p.retryDelay(attempt)); sleepErr != nil {
				return fmt.Errorf("bullmq: init %s: %w (last error: %v)", queue.Name, sleepErr, err)
			}
		}
	}
	return nil
}

func (p *Producer) addOnce(ctx context.Context, keys queueKeys, args []string) (string, error) {
	if err := p.ensureMeta(ctx, keys); err != nil {
		return "", err
	}
	reply, err := p.redis.EvalSHA(ctx, p.scriptSHA, keys.addStandardJob(), args)
	if isNoScript(err) {
		reply, err = p.redis.Eval(ctx, addStandardJobScript, keys.addStandardJob(), args)
	}
	if err != nil {
		return "", err
	}
	if reply.IsInteger {
		if reply.Integer < 0 {
			return "", fmt.Errorf("bullmq: addStandardJob failed with code %d", reply.Integer)
		}
		return strconv.FormatInt(reply.Integer, 10), nil
	}
	return reply.Text, nil
}

// ensureMeta writes what BullMQ's Queue constructor writes at boot, once per
// queue; the script itself only fills in opts.maxLenEvents.
func (p *Producer) ensureMeta(ctx context.Context, keys queueKeys) error {
	p.metaMu.Lock()
	defer p.metaMu.Unlock()
	if p.metaDone[keys.base] {
		return nil
	}
	err := p.redis.HSet(ctx, keys.meta(), metaMaxLenEventsField, metaMaxLenEvents, metaVersionField, metaVersion)
	if err != nil {
		return err
	}
	p.metaDone[keys.base] = true
	return nil
}

// arguments builds ARGV for addStandardJob: the msgpack values of
// redis-queue-backend.js addJobToTransaction (not an array), the job data,
// and the packed options.
func (p *Producer) arguments(keys queueKeys, jobID, jobName string, timestamp int64, data []byte, defaults Defaults) []string {
	header := make([]byte, 0, 128)
	// A msgpack array header followed by the values is what the script's
	// cmsgpack.unpack(ARGV[1]) reads as the args table.
	header = appendPacked(header, []any{keys.base, jobID, jobName, timestamp, nil, nil, nil, nil, nil, nil})
	return []string{string(header), string(data), string(pack(packedOptions(defaults, jobID)))}
}

// Envelope wraps payload (JSON text) as {"payload":…,"meta":{…}}, the shape
// every job's data has (core/jobs/envelope.ts). An empty requestID gives an
// empty meta, as a scope without a request does.
func Envelope(payload []byte, requestID string) []byte {
	out := make([]byte, 0, len(payload)+len(requestID)+48)
	out = append(out, `{"payload":`...)
	out = append(out, payload...)
	out = append(out, `,"meta":{`...)
	if requestID != "" {
		out = append(out, `"requestId":`...)
		out = append(out, jsjson.StringifyString(requestID)...)
	}
	return append(out, "}}"...)
}
