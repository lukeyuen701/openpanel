package config

import (
	"strings"
	"time"
)

// GoWorkerQueueValues are the BullMQ queues this worker can consume.
var GoWorkerQueueValues = []string{"sessions"}

const (
	defaultSessionReaperBatch = 5000
	defaultSessionVacuumBatch = 1000
	defaultVacuumStale        = 7 * 24 * time.Hour
	defaultQueueConcurrency   = 1
)

// SessionsConfig is the session lifecycle's environment (P3.9).
type SessionsConfig struct {
	// Timeout is SESSION_TIMEOUT_MS.
	Timeout time.Duration
	// ReaperEnabled and VacuumEnabled are off only for "0".
	ReaperEnabled bool
	ReaperBatch   int
	// ReaperDeadman is SESSION_REAPER_WALLCLOCK_DEADMAN_MS, the timeout when unset.
	ReaperDeadman time.Duration
	VacuumEnabled bool
	VacuumBatch   int
	// VacuumStale is SESSION_VACUUM_STALE_THRESHOLD_MS.
	VacuumStale time.Duration
	// ProfileBackfill is EXPERIMENTAL_PROFILE_BACKFILL=1; ProfileBackfillProjects
	// is its allowlist, empty meaning every project.
	ProfileBackfill         bool
	ProfileBackfillProjects []string
}

// ConcurrencyEnvKey is concurrencyEnvKey of env.ts.
func ConcurrencyEnvKey(queue string) string {
	var key strings.Builder
	for _, r := range strings.ToUpper(queue) {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			key.WriteRune(r)
		} else {
			key.WriteByte('_')
		}
	}
	return key.String() + "_CONCURRENCY"
}

// goWorkerQueues is GO_WORKER_QUEUES: the BullMQ queues this process consumes.
// The operator removes the same names from the Bun worker's ENABLED_QUEUES, or
// leaves them in to share the queue.
func (l *loader) goWorkerQueues() []string {
	tokens := l.tokens("GO_WORKER_QUEUES")
	for _, token := range tokens {
		if !contains(GoWorkerQueueValues, token) {
			l.problem("GO_WORKER_QUEUES: unknown queue %q. Accepted values: %s.", token, strings.Join(GoWorkerQueueValues, ", "))
		}
	}
	return tokens
}

func (l *loader) queueConcurrency(queues []string) map[string]int {
	out := map[string]int{}
	for _, queue := range queues {
		out[queue] = orDefault(l.optionalPositiveInt(ConcurrencyEnvKey(queue)), defaultQueueConcurrency)
	}
	return out
}

func (l *loader) sessions(timeout time.Duration) SessionsConfig {
	explicitTimeout := l.optionalPositiveInt("SESSION_TIMEOUT_MS")
	deadmanMs := l.optionalPositiveInt("SESSION_REAPER_WALLCLOCK_DEADMAN_MS")
	staleMs := l.optionalPositiveInt("SESSION_VACUUM_STALE_THRESHOLD_MS")
	cfg := SessionsConfig{
		Timeout:         timeout,
		ReaperEnabled:   l.getenvTrimmed("SESSION_REAPER") != "0",
		ReaperBatch:     orDefault(l.optionalPositiveInt("SESSION_REAPER_BATCH_SIZE"), defaultSessionReaperBatch),
		ReaperDeadman:   timeout,
		VacuumEnabled:   l.getenvTrimmed("SESSION_VACUUM") != "0",
		VacuumBatch:     orDefault(l.optionalPositiveInt("SESSION_VACUUM_BATCH_SIZE"), defaultSessionVacuumBatch),
		VacuumStale:     defaultVacuumStale,
		ProfileBackfill: l.getenvTrimmed("EXPERIMENTAL_PROFILE_BACKFILL") == "1",
	}
	cfg.ProfileBackfillProjects = l.tokens("EXPERIMENTAL_PROFILE_BACKFILL_PROJECTS")
	if deadmanMs > 0 {
		cfg.ReaperDeadman = millis(deadmanMs)
	}
	if staleMs > 0 {
		cfg.VacuumStale = millis(staleMs)
	}
	if deadmanMs > 0 && explicitTimeout > 0 && deadmanMs < explicitTimeout {
		l.problem("SESSION_REAPER_WALLCLOCK_DEADMAN_MS (%d) is below SESSION_TIMEOUT_MS (%d) — the reaper would close sessions that are still live.", deadmanMs, explicitTimeout)
	}
	deadmanForVacuum := deadmanMs
	if deadmanForVacuum == 0 {
		deadmanForVacuum = explicitTimeout
	}
	if staleMs > 0 && deadmanForVacuum > 0 && staleMs <= deadmanForVacuum {
		l.problem("SESSION_VACUUM_STALE_THRESHOLD_MS (%d) must exceed the reaper deadman (%d) — the vacuum would drop sessions the reaper is still closing.", staleMs, deadmanForVacuum)
	}
	if len(cfg.ProfileBackfillProjects) > 0 && !cfg.ProfileBackfill {
		l.problem("EXPERIMENTAL_PROFILE_BACKFILL_PROJECTS is set while EXPERIMENTAL_PROFILE_BACKFILL is not 1 — the allowlist has no effect.")
	}
	return cfg
}

func (l *loader) getenvTrimmed(name string) string { return strings.TrimSpace(l.getenv(name)) }

func (l *loader) selfHosted() bool {
	v := l.getenvTrimmed("SELF_HOSTED")
	return v == "true" || v == "1"
}
