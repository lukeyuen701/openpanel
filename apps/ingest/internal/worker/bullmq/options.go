package bullmq

import (
	"errors"
	"strconv"
)

var (
	errCustomIDInteger = errors.New("bullmq: custom id cannot be integers")
	errCustomIDColon   = errors.New("bullmq: custom id cannot contain :")
)

// maxPlainNumberDigits is where a decimal integer stops printing back unchanged
// through a JavaScript number (it switches to exponent notation at 1e21).
const maxPlainNumberDigits = 21

// looksLikeInteger is `${parseInt(id, 10)}` === id.
func looksLikeInteger(id string) bool {
	if id == "" || id == "0" {
		return id == "0"
	}
	digits := id
	if digits[0] == '-' {
		digits = digits[1:]
	}
	if digits == "" || digits[0] == '0' || len(digits) >= maxPlainNumberDigits {
		return false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return false
		}
	}
	parsed, err := strconv.ParseFloat(id, 64)
	if err != nil {
		return false
	}
	return strconv.FormatFloat(parsed, 'f', -1, 64) == id
}

// Retention is removeOnComplete / removeOnFail. The zero value leaves the
// option out, which BullMQ treats as keep everything.
type Retention struct {
	mode  retentionMode
	count int
	age   int
}

type retentionMode int

const (
	retentionUnset retentionMode = iota
	retentionRemove
	retentionKeep
	retentionLast
	retentionTrim
)

// RemoveImmediately is `true`: the record goes when the job ends.
func RemoveImmediately() Retention { return Retention{mode: retentionRemove} }

// KeepAll is `false`.
func KeepAll() Retention { return Retention{mode: retentionKeep} }

// KeepLast is a number: the newest count records stay.
func KeepLast(count int) Retention { return Retention{mode: retentionLast, count: count} }

// KeepFor is `{age, count}`: records older than ageSeconds go, and so do all
// but the newest count (count 0 leaves it out).
func KeepFor(ageSeconds, count int) Retention {
	return Retention{mode: retentionTrim, age: ageSeconds, count: count}
}

func (r Retention) packed() any {
	switch r.mode {
	case retentionRemove:
		return true
	case retentionKeep:
		return false
	case retentionLast:
		return r.count
	default:
		m := packedMap{{"age", r.age}}
		if r.count != 0 {
			m = append(m, member{"count", r.count})
		}
		return m
	}
}

// Backoff is the retry delay BullMQ applies between attempts.
type Backoff struct {
	// Type is "fixed" or "exponential"; empty means no backoff.
	Type    string
	DelayMs int
}

// Defaults are a queue's defaultJobOptions (core/jobs.registry.ts).
type Defaults struct {
	Attempts         int
	Backoff          Backoff
	RemoveOnComplete Retention
	RemoveOnFail     Retention
}

// Queue names a queue and the defaults every job on it starts from.
type Queue struct {
	Name     string
	Defaults Defaults
}

// SessionsQueue is the queue of session-end jobs (jobs.registry.ts).
func SessionsQueue() Queue {
	return Queue{Name: "sessions", Defaults: Defaults{
		RemoveOnComplete: RemoveImmediately(),
		RemoveOnFail:     KeepFor(secondsPerDay, 1000),
	}}
}

// NotificationQueue is the queue of notification jobs (jobs.registry.ts).
func NotificationQueue() Queue {
	return Queue{Name: "notification", Defaults: Defaults{
		RemoveOnComplete: KeepLast(10),
		RemoveOnFail:     KeepFor(secondsPerDay, 100),
	}}
}

const secondsPerDay = 86400

// Job names of the two queues.
const (
	SessionJob      = "session"
	NotificationJob = "sendNotification"
)

// AddOptions override the queue's defaults for one job. The zero value uses
// them.
type AddOptions struct {
	// JobID makes the add idempotent: while any record with the id exists
	// the add changes nothing and returns the id.
	JobID string
	// Attempts replaces the default when AttemptsSet.
	Attempts    int
	AttemptsSet bool
	// Backoff replaces the default when its Type is set.
	Backoff Backoff
}

func (d Defaults) merged(o AddOptions) Defaults {
	if o.AttemptsSet {
		d.Attempts = o.Attempts
	}
	if o.Backoff.Type != "" {
		d.Backoff = o.Backoff
	}
	return d
}

// packedOptions is optsAsJSON in the key order BullMQ's Job constructor
// builds it: attempts, the two retentions, then jobId and backoff.
func packedOptions(d Defaults, jobID string) packedMap {
	m := packedMap{{"attempts", d.Attempts}}
	if d.RemoveOnComplete.mode != retentionUnset {
		m = append(m, member{"removeOnComplete", d.RemoveOnComplete.packed()})
	}
	if d.RemoveOnFail.mode != retentionUnset {
		m = append(m, member{"removeOnFail", d.RemoveOnFail.packed()})
	}
	if jobID != "" {
		m = append(m, member{"jobId", jobID})
	}
	if d.Backoff.Type != "" {
		m = append(m, member{"backoff", packedMap{{"type", d.Backoff.Type}, {"delay", d.Backoff.DelayMs}}})
	}
	return m
}
