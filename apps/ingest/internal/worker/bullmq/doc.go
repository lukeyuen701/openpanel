// Package bullmq is a BullMQ 6.7.8 producer: it adds jobs to the queues the
// TypeScript worker consumes, with the same Redis keys, script, and msgpack
// arguments that `Queue.add` sends, so a job added here is indistinguishable
// from one the Bun API added (docs/GO_WORKER_CONTRACTS.md P1.9, P3.3).
//
// The Lua is vendored by `bun run gen` (scripts/gen-tables.ts) from the bullmq
// package the worker runs; `gen:check` fails when the copy is stale. Only
// standard jobs are supported (no delay, priority, deduplication, parent):
// every producer the worker needs enqueues plain jobs.
//
// TODO(consumer): when the Go worker consumes these queues, vendor and wire
// moveToActive-11, moveToFinished-14, extendLock-2 and moveStalledJobsToWait-9
// the same way (add them to BULLMQ_SCRIPTS in gen-tables.ts), plus the
// promote/retry scripts the backoff path calls. Not implemented here.
package bullmq
