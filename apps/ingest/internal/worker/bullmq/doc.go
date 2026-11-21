// Package bullmq is a BullMQ 6.7.8 producer and worker for the queues the
// TypeScript worker consumes. The producer adds jobs with the same Redis keys,
// script, and msgpack arguments that `Queue.add` sends, so a job added here is
// indistinguishable from one the Bun API added (docs/GO_WORKER_CONTRACTS.md
// P1.9, P3.3). The Worker fetches, renews, finishes, retries and recovers jobs
// with BullMQ's own Lua scripts and Worker's timings, so Go and Bun workers can
// consume one queue at once and moving a queue between them is configuration.
//
// The Lua is vendored by `bun run gen` (scripts/gen-tables.ts) from the bullmq
// package the worker runs; `gen:check` fails when the copy is stale. Only
// standard jobs are supported (no priority, deduplication, parent or
// repeatable jobs, rate limiter, or custom backoff strategy).
package bullmq
