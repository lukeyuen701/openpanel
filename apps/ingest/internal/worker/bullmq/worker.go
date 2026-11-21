package bullmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"openpanel/ingest/internal/jsjson"
)

// Handler processes one job. A nil return completes it; an error fails the
// attempt, which BullMQ retries while attempts remain, unless the error is
// an Unrecoverable one. The context ends only when Run's shutdown grace
// period runs out.
type Handler func(ctx context.Context, job Job) error

// BullMQ 6.7.8 Worker defaults.
const (
	defaultConcurrency     = 1
	defaultLockDuration    = 30 * time.Second
	defaultStalledInterval = 30 * time.Second
	defaultMaxStalledCount = 1
	defaultDrainDelay      = 5 * time.Second
	defaultRunRetryDelay   = 15 * time.Second
	// maxBlockTimeout is the longest a BZPOPMIN blocks (see #1658 in BullMQ).
	maxBlockTimeout = 10 * time.Second
	// minBlockTimeout is the shortest BZPOPMIN timeout Redis accepts.
	minBlockTimeout = time.Millisecond
	// maxRateLimitDelay is maximumRateLimitDelay.
	maxRateLimitDelay = 30 * time.Second
	// replyErrorDelay paces fetch retries after a refusal from Redis, which
	// waiting for the connection cannot fix.
	replyErrorDelay = time.Second
	tokenBytes      = 16
)

// Script return codes of moveToFinished and its siblings (finishedErrors).
const (
	codeMissingKey     = -1
	codeMissingLock    = -2
	codeNotInActive    = -3
	codePendingKids    = -4
	codeLockMismatch   = -6
	codeFailedChildren = -9
)

// ScriptError is a negative return code of a BullMQ script: the job is gone,
// finished, or no longer locked by this worker.
type ScriptError struct {
	Command string
	JobID   string
	Code    int64
}

func (e *ScriptError) Error() string {
	return fmt.Sprintf("bullmq: %s on job %s failed: %s (code %d)", e.Command, e.JobID, scriptCodeText(e.Code), e.Code)
}

func scriptCodeText(code int64) string {
	switch code {
	case codeMissingKey:
		return "missing key"
	case codeMissingLock:
		return "missing lock"
	case codeNotInActive:
		return "not in active set"
	case codePendingKids:
		return "pending children"
	case codeLockMismatch:
		return "lock is not owned by this worker"
	case codeFailedChildren:
		return "failed children"
	default:
		return "unknown code"
	}
}

// WorkerOptions are BullMQ's WorkerOptions the Go worker honours. Zero values
// mean BullMQ's defaults.
type WorkerOptions struct {
	Concurrency   int
	LockDuration  time.Duration
	LockRenewTime time.Duration
	// StalledInterval is how often the stalled checker runs; the Lua also
	// refuses a second check inside this window across all workers.
	StalledInterval time.Duration
	// MaxStalledCount is how many times a job may stall before it fails;
	// only read when MaxStalledCountSet (0 is a valid setting).
	MaxStalledCount    int
	MaxStalledCountSet bool
	SkipStalledCheck   bool
	// DrainDelay is the blocking wait while the queue is empty.
	DrainDelay    time.Duration
	RunRetryDelay time.Duration
	// ShutdownGrace bounds how long Run waits for running handlers after
	// shutdown starts before cancelling their context; zero waits forever.
	ShutdownGrace time.Duration
	// Name is stored as the job's worker name by the scripts.
	Name string
	// Namespace and Cluster are QUEUE_NAMESPACE and QUEUE_CLUSTER.
	Namespace string
	Cluster   bool
	Clock     Clock
	Logger    *slog.Logger
	// OnError receives errors BullMQ's Worker emits as 'error' events.
	OnError func(error)
	// OnStalled receives the ids a stalled check moved back to wait.
	OnStalled func(jobIDs []string)
}

func (o WorkerOptions) withDefaults() (WorkerOptions, error) {
	if o.Concurrency == 0 {
		o.Concurrency = defaultConcurrency
	}
	if o.Concurrency < 1 {
		return o, errors.New("bullmq: concurrency must be at least 1")
	}
	if o.LockDuration == 0 {
		o.LockDuration = defaultLockDuration
	}
	if o.LockRenewTime == 0 {
		o.LockRenewTime = o.LockDuration / 2
	}
	if o.StalledInterval == 0 {
		o.StalledInterval = defaultStalledInterval
	}
	if o.StalledInterval < 0 || o.DrainDelay < 0 {
		return o, errors.New("bullmq: stalledInterval and drainDelay must be greater than 0")
	}
	if !o.MaxStalledCountSet {
		o.MaxStalledCount = defaultMaxStalledCount
	}
	if o.MaxStalledCount < 0 {
		return o, errors.New("bullmq: maxStalledCount must be greater or equal than 0")
	}
	if o.DrainDelay == 0 {
		o.DrainDelay = defaultDrainDelay
	}
	if o.RunRetryDelay == 0 {
		o.RunRetryDelay = defaultRunRetryDelay
	}
	if o.Clock == nil {
		o.Clock = SystemClock{}
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return o, nil
}

type trackedLock struct {
	token string
	ts    int64
}

// Worker consumes one BullMQ queue with BullMQ 6.7.8's own Lua scripts, so it
// can run beside TypeScript workers on the same queue.
type Worker struct {
	redis   BlockingRedis
	handler Handler
	opts    WorkerOptions
	keys    queueKeys
	id      string

	mu         sync.Mutex
	drained    bool
	blockUntil int64
	limitUntil int64
	tokenSeq   int
	locks      map[string]*trackedLock
	running    bool
}

// NewWorker builds a worker for queueName (the unscoped name; opts carries
// the namespace). It does no I/O; call Run.
func NewWorker(redis BlockingRedis, queueName string, handler Handler, opts WorkerOptions) (*Worker, error) {
	if redis == nil || handler == nil {
		return nil, errors.New("bullmq: worker needs Redis and a handler")
	}
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}
	raw := make([]byte, tokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("bullmq: worker id: %w", err)
	}
	return &Worker{
		redis:   redis,
		handler: handler,
		opts:    opts,
		keys:    newQueueKeys(QueueKey(queueName, opts.Namespace, opts.Cluster)),
		id:      hex.EncodeToString(raw),
		locks:   map[string]*trackedLock{},
	}, nil
}

// Run processes jobs until ctx ends, then stops fetching, waits for the jobs
// being processed (each is finished with its lock still held), and returns.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return errors.New("bullmq: worker is already running")
	}
	w.running = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.running = false
		w.mu.Unlock()
	}()

	// Handlers, and the calls that finish their jobs, outlive a shutdown.
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()
	// Lock renewal must keep running while jobs drain.
	renewCtx, stopRenewing := context.WithCancel(context.WithoutCancel(ctx))
	defer stopRenewing()

	var helpers sync.WaitGroup
	helpers.Go(func() { w.renewLoop(renewCtx) })
	if !w.opts.SkipStalledCheck {
		helpers.Go(func() { w.stalledLoop(ctx) })
	}

	var jobs sync.WaitGroup
	w.fetchLoop(ctx, jobCtx, &jobs)
	w.waitForJobs(&jobs, cancelJobs)

	stopRenewing()
	helpers.Wait()
	return nil
}

func (w *Worker) waitForJobs(jobs *sync.WaitGroup, cancelJobs context.CancelFunc) {
	finished := make(chan struct{})
	go func() {
		jobs.Wait()
		close(finished)
	}()
	if w.opts.ShutdownGrace <= 0 {
		<-finished
		return
	}
	select {
	case <-finished:
	case <-time.After(w.opts.ShutdownGrace):
		cancelJobs()
		<-finished
	}
}

// fetchLoop is BullMQ's mainLoop: one fetch at a time, at most Concurrency
// jobs in flight. A job's goroutine keeps its slot while it chains into the
// job the finishing script hands back.
func (w *Worker) fetchLoop(ctx, jobCtx context.Context, jobs *sync.WaitGroup) {
	slots := make(chan struct{}, w.opts.Concurrency)
	for ctx.Err() == nil {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		job, err := w.nextJob(ctx)
		if err != nil {
			<-slots
			w.fetchFailed(ctx, err)
			continue
		}
		if job == nil {
			<-slots
			continue
		}
		jobs.Go(func() {
			defer func() { <-slots }()
			for current := job; current != nil; {
				current = w.process(jobCtx, ctx, current)
			}
		})
	}
}

func (w *Worker) fetchFailed(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	w.report(err)
	delay := replyErrorDelay
	if isTransient(err) {
		delay = w.opts.RunRetryDelay
	}
	_ = w.opts.Clock.Sleep(ctx, delay)
}

func (w *Worker) report(err error) {
	if w.opts.OnError != nil {
		w.opts.OnError(err)
		return
	}
	w.opts.Logger.Error("bullmq worker", "err", err)
}

func (w *Worker) now() int64 { return w.opts.Clock.Now().UnixMilli() }

func (w *Worker) newToken() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	token := w.id + ":" + strconv.Itoa(w.tokenSeq)
	w.tokenSeq++
	return token
}

// updateDelays is Worker.updateDelays.
func (w *Worker) updateDelays(limitDelayMs, delayUntil int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.limitUntil = 0
	if limitDelayMs > 0 {
		w.limitUntil = w.now() + limitDelayMs
	}
	w.blockUntil = max(delayUntil, 0)
}

// nextJob is Worker._getNextJob with block = true.
func (w *Worker) nextJob(ctx context.Context) (*Job, error) {
	token := w.newToken()
	w.mu.Lock()
	drained, limitUntil, blockUntil := w.drained, w.limitUntil, w.blockUntil
	w.mu.Unlock()

	if limitUntil > w.now() {
		return nil, w.waitForRateLimit(ctx, limitUntil)
	}
	if !drained {
		return w.moveToActive(ctx, token)
	}
	until, err := w.waitForJob(ctx, blockUntil)
	w.mu.Lock()
	w.blockUntil = until
	w.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if until <= 0 || until-w.now() < 1 {
		return w.moveToActive(ctx, token)
	}
	return nil, nil
}

func (w *Worker) waitForRateLimit(ctx context.Context, limitUntil int64) error {
	delay := min(time.Duration(limitUntil-w.now())*time.Millisecond, maxRateLimitDelay)
	if err := w.opts.Clock.Sleep(ctx, delay); err != nil {
		return nil
	}
	w.mu.Lock()
	w.drained = false
	w.limitUntil = 0
	w.mu.Unlock()
	return nil
}

// blockedForever is waitForJob's answer when it must not fetch: BullMQ's
// Infinity.
const blockedForever = int64(1) << 62

// waitForJob blocks on the marker key until a job may be available and
// returns the time a delayed job is due (0 when jobs are ready now).
func (w *Worker) waitForJob(ctx context.Context, blockUntil int64) (int64, error) {
	timeout := w.blockTimeout(blockUntil)
	if timeout <= 0 {
		return 0, nil
	}
	w.updateDelays(0, 0)
	score, ok, err := w.redis.BZPopMin(ctx, w.keys.key("marker"), timeout)
	if err != nil {
		if ctx.Err() != nil {
			return blockedForever, nil
		}
		return blockedForever, err
	}
	if !ok {
		return 0, nil
	}
	if blockUntil > 0 && score > blockUntil {
		return blockUntil, nil
	}
	return score, nil
}

// blockTimeout is Worker.getBlockTimeout; zero means do not block.
func (w *Worker) blockTimeout(blockUntil int64) time.Duration {
	if blockUntil == 0 {
		return max(w.opts.DrainDelay, minBlockTimeout)
	}
	delay := time.Duration(blockUntil-w.now()) * time.Millisecond
	switch {
	case delay <= 0:
		return 0
	case delay < minBlockTimeout:
		return minBlockTimeout
	default:
		return min(delay, maxBlockTimeout)
	}
}

func (w *Worker) moveToActive(ctx context.Context, token string) (*Job, error) {
	lockOptions := packedMap{{"token", token}, {"lockDuration", int(w.opts.LockDuration.Milliseconds())}}
	if w.opts.Name != "" {
		lockOptions = append(lockOptions, member{"name", w.opts.Name})
	}
	args := []string{w.keys.base, strconv.FormatInt(w.now(), 10), string(pack(lockOptions))}
	reply, err := moveToActiveScript.run(ctx, w.redis, w.keys.moveToActive(), args)
	if err != nil {
		return nil, err
	}
	return w.nextFromReply(reply, token)
}

func replyInt(r Reply) int64 {
	if r.IsInteger {
		return r.Integer
	}
	return parseInt(r.Text)
}

// nextFromReply reads the [jobData, id, limitDelay, delayUntil] tuple the
// fetching scripts answer with (raw2NextJobData + nextJobFromJobData).
func (w *Worker) nextFromReply(reply Reply, token string) (*Job, error) {
	const tupleLen = 4
	if !reply.IsArray || len(reply.Array) < tupleLen {
		return nil, nil
	}
	w.updateDelays(replyInt(reply.Array[2]), replyInt(reply.Array[3]))
	fields := reply.Array[0]
	if !fields.IsArray || len(fields.Array) == 0 {
		w.mu.Lock()
		w.drained = true
		w.mu.Unlock()
		return nil, nil
	}
	w.mu.Lock()
	w.drained = false
	w.mu.Unlock()
	id := reply.Array[1].Text
	if reply.Array[1].IsInteger {
		id = strconv.FormatInt(reply.Array[1].Integer, 10)
	}
	job, err := jobFromHash(fields.Array, id)
	if err != nil {
		return nil, err
	}
	job.Token = token
	return &job, nil
}

// process runs the handler and finishes the job, returning the next job the
// finishing script fetched, if any.
func (w *Worker) process(jobCtx, stopCtx context.Context, job *Job) *Job {
	w.track(job)
	var failure error
	if job.deferredFailure != "" {
		failure = Unrecoverable(errors.New(job.deferredFailure))
	} else {
		failure = w.call(jobCtx, *job)
	}
	w.untrack(job.ID)

	fetchNext := stopCtx.Err() == nil
	var next *Job
	var err error
	if failure == nil {
		next, err = w.complete(jobCtx, job, fetchNext)
	} else {
		next, err = w.fail(jobCtx, job, failure, fetchNext)
	}
	if err != nil {
		w.report(fmt.Errorf("job %s: %w", job.ID, err))
	}
	return next
}

func (w *Worker) call(ctx context.Context, job Job) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return w.handler(ctx, job)
}

func (w *Worker) track(job *Job) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.locks[job.ID] = &trackedLock{token: job.Token, ts: job.ProcessedOn}
}

func (w *Worker) untrack(jobID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.locks, jobID)
}

// renewLoop is LockManager's timer: every half renew time it extends the
// locks of jobs not extended for half a renew time.
func (w *Worker) renewLoop(ctx context.Context) {
	if w.opts.LockRenewTime <= 0 {
		return
	}
	tick := w.opts.LockRenewTime / 2
	for w.opts.Clock.Sleep(ctx, tick) == nil {
		w.renewLocks(ctx)
	}
}

func (w *Worker) renewLocks(ctx context.Context) {
	now := w.now()
	halfRenew := w.opts.LockRenewTime.Milliseconds() / 2
	var ids, tokens []any
	w.mu.Lock()
	for id, lock := range w.locks {
		if lock.ts == 0 {
			lock.ts = now
			continue
		}
		if lock.ts+halfRenew < now {
			lock.ts = now
			ids = append(ids, id)
			tokens = append(tokens, lock.token)
		}
	}
	w.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	args := []string{
		w.keys.base, string(pack(tokens)), string(pack(ids)),
		strconv.FormatInt(w.opts.LockDuration.Milliseconds(), 10),
	}
	reply, err := extendLocksScript.run(ctx, w.redis, []string{w.keys.key("stalled")}, args)
	if err != nil {
		if ctx.Err() == nil {
			w.report(err)
		}
		return
	}
	for _, failed := range reply.Array {
		w.report(fmt.Errorf("could not renew lock for job %s", failed.Text))
	}
}

// stalledLoop is Worker.stalledChecker.
func (w *Worker) stalledLoop(ctx context.Context) {
	for ctx.Err() == nil {
		w.checkStalled(ctx)
		if w.opts.Clock.Sleep(ctx, w.opts.StalledInterval) != nil {
			return
		}
	}
}

func (w *Worker) checkStalled(ctx context.Context) {
	args := []string{
		strconv.Itoa(w.opts.MaxStalledCount), w.keys.base,
		strconv.FormatInt(w.now(), 10), strconv.FormatInt(w.opts.StalledInterval.Milliseconds(), 10),
	}
	reply, err := moveStalledJobsToWaitScript.run(ctx, w.redis, w.keys.moveStalledJobsToWait(), args)
	if err != nil {
		if ctx.Err() == nil {
			w.report(err)
		}
		return
	}
	if len(reply.Array) == 0 || w.opts.OnStalled == nil {
		return
	}
	ids := make([]string, 0, len(reply.Array))
	for _, id := range reply.Array {
		ids = append(ids, id.Text)
	}
	w.opts.OnStalled(ids)
}

// finishing repeats op while Redis is unavailable, as retryIfFailed does with
// runRetryDelay, and gives up on a refusal from Redis or a script code.
func (w *Worker) finishing(ctx context.Context, op func() (*Job, error)) (*Job, error) {
	for {
		next, err := op()
		var scriptErr *ScriptError
		if err == nil || errors.As(err, &scriptErr) || !isTransient(err) {
			return next, err
		}
		if w.opts.Clock.Sleep(ctx, w.opts.RunRetryDelay) != nil {
			return nil, err
		}
	}
}

func (w *Worker) finishReply(command string, job *Job, reply Reply) (*Job, error) {
	if reply.IsInteger && reply.Integer < 0 {
		return nil, &ScriptError{Command: command, JobID: job.ID, Code: reply.Integer}
	}
	return w.nextFromReply(reply, job.Token)
}

func flag(on bool) string {
	if on {
		return "1"
	}
	return "0"
}

// finishArgs is moveToFinishedArgs.
func (w *Worker) finishArgs(job *Job, property, value, target string, retention json.RawMessage, fetchNext bool, fields string) []string {
	options := packedMap{{"token", job.Token}}
	if w.opts.Name != "" {
		options = append(options, member{"name", w.opts.Name})
	}
	options = append(options,
		member{"keepJobs", keepJobs(retention)},
		member{"lockDuration", int(w.opts.LockDuration.Milliseconds())},
		member{"attempts", job.Options.Attempts},
		member{"maxMetricsSize", ""},
		member{"fpof", false}, member{"cpof", false}, member{"idof", false}, member{"rdof", false},
	)
	return []string{
		job.ID, strconv.FormatInt(w.now(), 10), property, value, target,
		flag(fetchNext), w.keys.base, string(pack(options)), fields,
	}
}

// completedReturnValue is JSON.stringify(undefined) as the script receives
// it: handlers return no value.
const completedReturnValue = "null"

func (w *Worker) complete(ctx context.Context, job *Job, fetchNext bool) (*Job, error) {
	return w.finishing(ctx, func() (*Job, error) {
		args := w.finishArgs(job, "returnvalue", completedReturnValue, "completed", job.Options.removeOnComplete, fetchNext, "")
		reply, err := moveToFinishedScript.run(ctx, w.redis, w.keys.moveToFinished("completed", job.ID), args)
		if err != nil {
			return nil, err
		}
		return w.finishReply("moveToFinished", job, reply)
	})
}

// fail is Job.moveToFailed: retry now, retry after a backoff, or fail.
func (w *Worker) fail(ctx context.Context, job *Job, cause error, fetchNext bool) (*Job, error) {
	message := cause.Error()
	retry, delayMs, backoffErr := job.shouldRetry(cause)
	if backoffErr != nil {
		w.report(fmt.Errorf("job %s: %w", job.ID, backoffErr))
	}
	fields := string(pack([]any{
		"failedReason", message,
		"stacktrace", stacktraceJSON(append(job.stacktrace[:len(job.stacktrace):len(job.stacktrace)], stackLine(cause))),
	}))
	return w.finishing(ctx, func() (*Job, error) {
		switch {
		case retry && delayMs > 0:
			return w.moveToDelayed(ctx, job, delayMs, fields, fetchNext)
		case retry:
			return w.retryJob(ctx, job, fields)
		default:
			args := w.finishArgs(job, "failedReason", message, "failed", job.Options.removeOnFail, fetchNext, fields)
			reply, err := moveToFinishedScript.run(ctx, w.redis, w.keys.moveToFinished("failed", job.ID), args)
			if err != nil {
				return nil, err
			}
			return w.finishReply("moveToFinished", job, reply)
		}
	})
}

// stackLine stands for err.stack: Go has no stack to report, only the
// "Name: message" line JavaScript starts one with.
func stackLine(err error) string {
	if isUnrecoverable(err) {
		return "UnrecoverableError: " + err.Error()
	}
	return "Error: " + err.Error()
}

func stacktraceJSON(entries []string) string {
	out := []byte{'['}
	for i, entry := range entries {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, jsjson.StringifyString(entry)...)
	}
	return string(append(out, ']'))
}

func (w *Worker) moveToDelayed(ctx context.Context, job *Job, delayMs int64, fields string, fetchNext bool) (*Job, error) {
	const skipAttempt = "0"
	lockOptions := packedMap{{"token", job.Token}, {"lockDuration", int(w.opts.LockDuration.Milliseconds())}}
	if w.opts.Name != "" {
		lockOptions = append(lockOptions, member{"name", w.opts.Name})
	}
	nextOptions := ""
	if fetchNext {
		nextOptions = string(pack(lockOptions))
	}
	args := []string{
		w.keys.base, strconv.FormatInt(w.now(), 10), job.ID, job.Token,
		strconv.FormatInt(delayMs, 10), skipAttempt, fields, flag(fetchNext), nextOptions,
	}
	reply, err := moveToDelayedScript.run(ctx, w.redis, w.keys.moveToDelayed(job.ID), args)
	if err != nil {
		return nil, err
	}
	return w.finishReply("moveToDelayed", job, reply)
}

func (w *Worker) retryJob(ctx context.Context, job *Job, fields string) (*Job, error) {
	pushCommand := "LPUSH"
	if job.Options.Lifo {
		pushCommand = "RPUSH"
	}
	args := []string{w.keys.base, strconv.FormatInt(w.now(), 10), pushCommand, job.ID, job.Token, fields}
	reply, err := retryJobScript.run(ctx, w.redis, w.keys.retryJob(job.ID), args)
	if err != nil {
		return nil, err
	}
	if reply.IsInteger && reply.Integer < 0 {
		return nil, &ScriptError{Command: "retryJob", JobID: job.ID, Code: reply.Integer}
	}
	return nil, nil
}
