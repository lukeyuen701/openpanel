package bullmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
)

// Job is a job a Worker fetched. Data is the raw JSON text the producer
// stored (the envelope, for OpenPanel's queues).
type Job struct {
	ID              string
	Name            string
	Data            json.RawMessage
	Timestamp       int64
	ProcessedOn     int64
	Delay           int64
	Priority        int
	AttemptsMade    int
	AttemptsStarted int
	StalledCounter  int
	Options         JobOptions
	// Token is the lock token this worker holds the job under.
	Token string

	stacktrace      []string
	deferredFailure string
}

// JobOptions are the stored job options a worker acts on.
type JobOptions struct {
	Attempts         int
	Lifo             bool
	Backoff          backoffSpec
	removeOnComplete json.RawMessage
	removeOnFail     json.RawMessage
}

type rawOptions struct {
	Attempts         int             `json:"attempts"`
	Lifo             bool            `json:"lifo"`
	Backoff          json.RawMessage `json:"backoff"`
	RemoveOnComplete json.RawMessage `json:"removeOnComplete"`
	RemoveOnFail     json.RawMessage `json:"removeOnFail"`
}

func parseOptions(text string) (JobOptions, error) {
	if text == "" {
		return JobOptions{}, nil
	}
	var raw rawOptions
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return JobOptions{}, fmt.Errorf("bullmq: job opts %q: %w", text, err)
	}
	backoff, err := parseBackoff(raw.Backoff)
	if err != nil {
		return JobOptions{}, err
	}
	return JobOptions{
		Attempts:         raw.Attempts,
		Lifo:             raw.Lifo,
		Backoff:          backoff,
		removeOnComplete: raw.RemoveOnComplete,
		removeOnFail:     raw.RemoveOnFail,
	}, nil
}

// jobFromHash decodes HGETALL's flat field/value list (rawToJobJson).
func jobFromHash(fields []Reply, id string) (Job, error) {
	hash := make(map[string]string, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		hash[fields[i].Text] = fields[i+1].Text
	}
	options, err := parseOptions(hash["opts"])
	if err != nil {
		return Job{}, err
	}
	data := hash["data"]
	if data == "" {
		data = "{}"
	}
	atm := hash["atm"]
	if hash["attemptsMade"] != "" {
		atm = hash["attemptsMade"]
	}
	job := Job{
		ID:              id,
		Name:            hash["name"],
		Data:            json.RawMessage(data),
		Timestamp:       parseInt(hash["timestamp"]),
		ProcessedOn:     parseInt(hash["processedOn"]),
		Delay:           parseInt(hash["delay"]),
		Priority:        int(parseInt(hash["priority"])),
		AttemptsMade:    int(parseInt(atm)),
		AttemptsStarted: int(parseInt(hash["ats"])),
		StalledCounter:  int(parseInt(hash["stc"])),
		Options:         options,
		deferredFailure: hash["defa"],
	}
	// A stacktrace that does not parse is dropped, as getTraces does.
	_ = json.Unmarshal([]byte(hash["stacktrace"]), &job.stacktrace)
	return job, nil
}

// parseInt is JavaScript's parseInt(x, 10) for the values BullMQ stores,
// with NaN read as 0.
func parseInt(text string) int64 {
	text = strings.TrimSpace(text)
	end := 0
	for end < len(text) && (text[end] >= '0' && text[end] <= '9' || end == 0 && (text[end] == '-' || text[end] == '+')) {
		end++
	}
	value, err := strconv.ParseInt(text[:end], 10, 64)
	if err != nil {
		return 0
	}
	return value
}

// keepJobs is RedisQueueBackend.getKeepJobs for a job's own removeOnComplete / removeOnFail (a worker-level default is not
// supported): unset keeps everything, true removes at once, a number keeps
// that many, an object is passed through.
func keepJobs(raw json.RawMessage) packedMap {
	const keepAllCount, removeNowCount = -1, 0
	text := strings.TrimSpace(string(raw))
	switch text {
	case "", "null", "false":
		return packedMap{{"count", keepAllCount}}
	case "true":
		return packedMap{{"count", removeNowCount}}
	}
	var count float64
	if err := json.Unmarshal(raw, &count); err == nil {
		return packedMap{{"count", count}}
	}
	var object struct {
		Age   *float64 `json:"age"`
		Count *float64 `json:"count"`
		Limit *float64 `json:"limit"`
	}
	if err := json.Unmarshal(raw, &object); err != nil {
		return packedMap{{"count", keepAllCount}}
	}
	out := packedMap{}
	if object.Age != nil {
		out = append(out, member{"age", *object.Age})
	}
	if object.Count != nil {
		out = append(out, member{"count", *object.Count})
	}
	if object.Limit != nil {
		out = append(out, member{"limit", *object.Limit})
	}
	return out
}

// Backoff strategy names BullMQ ships.
const (
	backoffFixed       = "fixed"
	backoffExponential = "exponential"
	// noRetryDelay is the sentinel a backoff strategy returns to stop retrying.
	noRetryDelay = -1
)

var errUnknownBackoff = errors.New("bullmq: unknown backoff strategy")

type backoffSpec struct {
	set    bool
	kind   string
	delay  float64
	jitter float64
}

// parseBackoff reads Backoffs.normalize: a finite number is a fixed delay.
func parseBackoff(raw json.RawMessage) (backoffSpec, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" || text == "false" {
		return backoffSpec{}, nil
	}
	var delay float64
	if err := json.Unmarshal(raw, &delay); err == nil {
		return backoffSpec{set: true, kind: backoffFixed, delay: delay}, nil
	}
	var object struct {
		Type   string  `json:"type"`
		Delay  float64 `json:"delay"`
		Jitter float64 `json:"jitter"`
	}
	if err := json.Unmarshal(raw, &object); err != nil {
		return backoffSpec{}, fmt.Errorf("bullmq: job backoff %s: %w", text, err)
	}
	return backoffSpec{set: true, kind: object.Type, delay: object.Delay, jitter: object.Jitter}, nil
}

// retryDelay is Backoffs.calculate for the built-in strategies, in
// milliseconds. attemptsMade counts the attempt that just failed. An unset
// backoff retries at once (delay 0).
func (b backoffSpec) retryDelay(attemptsMade int) (int64, error) {
	if !b.set {
		return 0, nil
	}
	switch b.kind {
	case backoffFixed:
		if b.jitter > 0 {
			return int64(math.Floor(rand.Float64()*b.delay*b.jitter + b.delay*(1-b.jitter))), nil
		}
		return int64(b.delay), nil
	case backoffExponential:
		maxDelay := jsRound(math.Pow(2, float64(attemptsMade-1)) * b.delay)
		if b.jitter > 0 {
			return int64(math.Floor(rand.Float64()*maxDelay*b.jitter + maxDelay*(1-b.jitter))), nil
		}
		return int64(maxDelay), nil
	default:
		return 0, fmt.Errorf("%w %q", errUnknownBackoff, b.kind)
	}
}

// jsRound is Math.round: halves go up.
func jsRound(v float64) float64 { return math.Floor(v + 0.5) }

// UnrecoverableError wraps an error a handler returns to fail the job
// without using up its remaining attempts (BullMQ's UnrecoverableError).
type UnrecoverableError struct{ Err error }

// Unrecoverable marks err as one a retry cannot fix.
func Unrecoverable(err error) error { return &UnrecoverableError{Err: err} }

func (e *UnrecoverableError) Error() string { return e.Err.Error() }
func (e *UnrecoverableError) Unwrap() error { return e.Err }

func isUnrecoverable(err error) bool {
	var target *UnrecoverableError
	return errors.As(err, &target)
}

// shouldRetry is Job.shouldRetryJob: whether the failed attempt is retried
// and after what delay (0 means straight back to wait).
func (j Job) shouldRetry(err error) (retry bool, delayMs int64, backoffErr error) {
	if j.AttemptsMade+1 >= j.Options.Attempts || isUnrecoverable(err) {
		return false, 0, nil
	}
	delay, backoffErr := j.Options.Backoff.retryDelay(j.AttemptsMade + 1)
	if backoffErr != nil {
		return false, 0, backoffErr
	}
	if delay == noRetryDelay {
		return false, 0, nil
	}
	return true, delay, nil
}
