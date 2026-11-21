// Package sessions is the session lifecycle of the Go worker: the sessions
// queue job that emits session_end (createSessionEnd and the usage counters
// after it), the reaper and the vacuum (docs/GO_WORKER_CONTRACTS_FLUSH.md P3).
//
// It keeps the Redis state, the BullMQ jobs and the rows the TypeScript worker
// writes byte for byte, so the two can share the sessions queue and the cron
// jobs can move between them by configuration. Faithful includes the TypeScript
// hazards listed in P3.11, with one exception: nothing here drops a row the
// TypeScript would have kept.
package sessions
