# Go worker contracts

What the Go worker must reproduce, written from the TypeScript worker's code and from
observing it. The plan and the phases are in [GO_WORKER.md](GO_WORKER.md); this file is
the specification the port is checked against. Each section is one phase.

- [P1: the Kafka consumer path](#p1-the-kafka-consumer-path) (this file)
- P2 buffer flushes and P3 session lifecycle: [GO_WORKER_CONTRACTS_FLUSH.md](GO_WORKER_CONTRACTS_FLUSH.md)

---

## P1: the Kafka consumer path

Everything one worker does for one Kafka message when started as
`ROLE=worker ENABLED_QUEUES=events`: from the broker to the Redis lists the flush phase (P2) reads, the
Postgres rows, the BullMQ jobs and the metrics. Flushing those lists into ClickHouse is P2 and is
not described here, except where an effect of P1 is only meaningful because of it.

### P1.0 How to read this section

**Path abbreviations** (all relative to the repository root):

| Short | Path |
|---|---|
| `core/` | `packages/core/src/` |
| `ingest/` | `packages/core/src/modules/ingest/src/` |
| `buf/` | `packages/core/src/buffers/` |
| `redis/` | `packages/redis/` |
| `env.ts` | `apps/api/src/config/env.ts` |
| `main.ts` | `apps/api/src/main.ts` |

Citations are `file:line` against branch `feat/go-worker`; the code they point to is identical at `27ec5f1a` and at `3055fb49` (HEAD when this was last checked). `main.ts` and `env.ts` moved while `DISABLED_CRON_JOBS` was being added, so their line numbers were re-derived from the final file.

**Tags.**
**[SHARED]** an effect the Bun API (or another TS role that stays) also performs, so TS and Go must keep
reading and writing the same bytes with the same meaning for as long as both exist.
**[WORKER]** only the events consumer performs it today.
**[NONDET]** the value depends on the wall clock or on randomness; goldens must mask it (section P1.15).
**[QUIRK]** behaviour that looks accidental but is observable; the port must copy it or the decision to
diverge must be taken explicitly (section P1.17 lists them).

**How this was verified.** Besides reading the code, the effect timelines and the encodings below were
**captured** by running the real TypeScript modules against the local Redis (db index 11, every key
namespaced `wparity` or removed afterwards, `MONITOR` on) and the real Prisma client
against the local Postgres (every write inside a rolled-back transaction). The recipes are in Appendix A. Where the text says
"captured", the bytes are real output; where it says "derived", they come from the code only.
Values written to the dev databases by other work (db 0) were not used as goldens.

### P1.1 Process: what starts, in which order

With `ROLE=worker` (`env.ts:14`, default `api`) and `ENABLED_QUEUES=events`:

1. `process.env.TZ = 'UTC'` is the first statement (`main.ts:6`). Anything that formats a local time depends on it.
2. `loadConfig()` (zod, `env.ts`); a bad value stops the process (`main.ts:110-120`). `ROLE=worker` with an empty
   `ENABLED_QUEUES` is rejected (`env.ts:632-640`).
3. `configureRedisCache({commandTimeoutMs: REDIS_CACHE_COMMAND_TIMEOUT_MS})` (`main.ts:123`).
4. `buildDeps()` (`main.ts:176-198`): BullMQ `Queue` objects for **all seven** registry queues (`createProducers`,
   `core/jobs/producers.ts:69-138`, regardless of `ENABLED_QUEUES`; each one writes its `meta` hash on first use, see P1.9), the
   eight buffers (`buf/create-buffers.ts:22-32`), Prisma, the ClickHouse client, `getRedisCache()`.
5. Metrics: default collectors, then because the role consumes: buffer gauges and observers, `<queue>_*_count`
   gauges, session scrape gauges (`main.ts:477-480`, `main.ts:316-325`).
6. HTTP: ops routes (`/healthz/live`, `/healthz/ready`, `/metrics`) are mounted for every role; the public API, dashboard and
   tRPC surfaces are **not** mounted for `ROLE=worker` (`main.ts:355-397`). The server listens *before* the consumer starts
   (`main.ts:487-505`); readiness answers 503 while `booting` is set.
7. `workersEnabled = ROLE != api && !DISABLE_WORKERS` (`main.ts:139`). `consumedQueues()` filters the registry by `ENABLED_QUEUES`; `events`
   is not a registry key, so `startWorkers` gets an empty set and no BullMQ worker and no cron scheduler start (`main.ts:206-222`,
   `main.ts:536-570`).
8. `startIngestConsumer` (`main.ts:245-286`, called at `main.ts:572-574`): `assertKafkaConfigured` (throws when `KAFKA_BROKERS` is empty or an
   unreadable `KAFKA_SSL_CA_PATH`, `ingest/kafka.ts:76-85`), `enableEventsHeartbeat()`, then `startKafkaEventsConsumer` (P1.3). A broker that
   cannot be reached fails the boot (`consumer.ts:720-722`).
9. `createInitialSalts(deps)` runs for any worker with workers enabled (`main.ts:576`): it reads `salts` in Postgres and
   creates two rows when the table is empty (`core/modules/salt/salt.service.ts:54-82`). It is boot work, not part of a message;
   the Go worker does not need it while a TS process keeps doing it.
10. Fatal handlers, signal handlers, `setBooting(false)` (`main.ts:581-646`).

**Readiness** (`core/modules/health/src/readiness.ts:26-55`): not ready when shutting down, when booting, or when the events
heartbeat is older than `EVENTS_HEARTBEAT_STALE_MS = 60_000` (line 12). The heartbeat is
`markEventsActivity()` = `lastActivityAt = Date.now()` (`ingest/heartbeat.ts:11-28`); the places that call it are listed in P1.3.7.

**Shutdown** (`main.ts:584-636`, `apps/api/src/shutdown.ts:52-97`), in order: stop HTTP; drain cron (only if cron is consumed); then concurrently close
BullMQ workers and `consumer.stopConsuming()`; then `buffers.event.flushPendingOrThrow()` — a failure here **aborts the shutdown with exit
code 1 without leaving the group**, so the broker redelivers; then `consumer.stop()` (leave group), close producers, `disconnectKafka`
(closes consumers, flushes the events producer batcher, closes the producer, `ingest/kafka.ts:440-472`). The force-exit timer is
`SHUTDOWN_FORCE_EXIT_MS` (default 20000, exits 1). Exit 0 only when every step succeeded.
The Go worker must handle SIGTERM and leave the group explicitly: without it the members wait for the 30 s session timeout before partitions move
(see `GO_WORKER.md` progress table, 2026-11-22).

### P1.2 The input: `IncomingEventPayload`

Declared in `ingest/incoming-event.ts:13-33` and marked there as a wire format. The record on the topic:

| Part | Value |
|---|---|
| topic | `KAFKA_EVENTS_TOPIC` (default `events`) |
| key | UTF-8 bytes of the partition key: the deviceId, or `${projectId}:${profileId}` for server events with a profile (producer side, README of `apps/ingest`). May be null. |
| value | UTF-8 JSON of one `IncomingEventPayload` |
| headers | not read by the consumer except as dead-letter context (P1.3.6); request id travels **inside** the value (`headers` field) |

JSON fields (the order is the producer's; the consumer does not depend on it, but see P1.5.1 and P1.9.1 for what is copied into job data):

| Field | Type | Notes |
|---|---|---|
| `id` | string, optional | producer-minted event id; absent on records written before the field existed. Absent ⇒ `createEvent` mints `crypto.randomUUID()` [NONDET] |
| `projectId` | string | |
| `event` | object | `name` string, `properties?` object, `profileId?` string or number, `groups?` string[] or null, `timestamp` string or number, `isTimestampFromThePast` boolean (`ingest.constants.ts:157-162`) |
| `uaInfo` | object | `isServer`, `os`, `osVersion`, `browser`, `browserVersion`, `device`, `brand`, `model` (any may be absent/undefined) |
| `geo` | object | `city`, `country`, `region`, `longitude`, `latitude` |
| `headers` | object string→string | read keys: `openpanel-sdk-name`, `openpanel-sdk-version`, `request-id` (`REQUEST_ID_HEADER`, `core/logger.ts:29`) |
| `deviceId`, `sessionId` | string | |
| `ai?` | object | `AiEventRowDraft` (`core/modules/ai/src/row.ts:8-41`); its presence alone routes the message down the server branch (P1.5.2) |

**Producers [SHARED]**: the Go receiver and the Bun API (forwarded requests, `identify` carrying a track, the AI draft); also records replayed from a
dead-letter value, and old records. A consumer must accept: `id` missing, numeric `profileId` (stringified with `String()`, `incoming-event-handler.ts:348`). A record with **no `headers` object** is *not* accepted by the TS handler: `ingest/consumer-handler.ts:20` optional-chains it, but `ingestIncomingEvent` then evaluates `headers['openpanel-sdk-name']` and throws `TypeError` (captured, P1.17 #9), so such a record is retried three times and dead-lettered.

**Parsing** (`ingest/consumer.ts:292-306`): `JSON.parse(message.value.toString())`; a null value throws `Error('kafka message has no value')`.
Both are `parse_error` dead letters with **no retry**. `Buffer#toString()` decodes UTF-8 with U+FFFD replacement (no BOM stripping, so a BOM makes
the parse fail). A value that parses to a non-object (`null`, a number, an array) is *not* a parse error: it reaches the handler and fails there
(`null.headers` → TypeError), so it takes the three attempts and ends as `handler_error`.

### P1.3 The consumer: lanes, batches, retries, durability, commit

Files: `ingest/consumer.ts` (752 lines), `ingest/partition-lanes.ts` (304), `ingest/consumer-handler.ts` (47). The executable specification is
`ingest/consumer.test.ts` (42 tests), `ingest/partition-lanes.test.ts` (16), `ingest/consumer-group.test.ts`, and the durability tests in
`buf/event-buffer.test.ts`; a Go implementation should port those cases one to one.

#### P1.3.1 Client and group configuration

Built by `createKafkaEventsConsumer` (`ingest/kafka.ts:293-324`) from `clientOptions` (`kafka.ts:95-109`):

| Setting | Value |
|---|---|
| bootstrap brokers | `KAFKA_BROKERS` (comma list) |
| client id | `KAFKA_CLIENT_ID` (default `openpanel`) |
| connect timeout | `KAFKA_CONNECTION_TIMEOUT_MS` (2000) |
| request timeout | `KAFKA_REQUEST_TIMEOUT_MS + KAFKA_REBALANCE_TIMEOUT_MS` = 5000 + 60000 for the consumer (`kafka.ts:48-49, 301`), so a JoinGroup the broker holds is not abandoned |
| TLS / SASL | `KAFKA_SSL` (defaults to on when SASL credentials are set), `KAFKA_SSL_CA_PATH`, `KAFKA_SSL_REJECT_UNAUTHORIZED`; SASL mechanism `KAFKA_SASL_MECHANISM` default `scram-sha-512` (`env.ts:668-742, 834-852`) |
| group id | `KAFKA_CONSUMER_GROUP` (default `openpanel-events`) |
| group protocol | one entry: name `RoundRobinAssigner`, version `0` (`EVENTS_GROUP_PROTOCOLS`, `kafka.ts:60-62`). The Go member must advertise the same name or the broker answers `INCONSISTENT_GROUP_PROTOCOL`; `apps/ingest/internal/kafka/balancer.go` already does (`EventsGroupBalancer`) and the mixed-group spike passed (GO_WORKER.md §7) |
| assignor | the leader computes the assignment with the client's round-robin: members in JoinGroup order, partition `i` of the topic goes to member `i mod n` (@platformatic/kafka `partitions-assigners.js` (`roundRobinAssigner`)). Whichever member is leader decides, TS or Go |
| session / heartbeat / rebalance timeout | `KAFKA_SESSION_TIMEOUT_MS` 30000 / `KAFKA_HEARTBEAT_INTERVAL_MS` 3000 / `KAFKA_REBALANCE_TIMEOUT_MS` 60000; boot refuses heartbeat ≥ session (`env.ts:654-666`) |
| fetch | min bytes = `KAFKA_MIN_MESSAGES × 1024` (1024), max wait `KAFKA_MAX_WAIT_MS` (500), max bytes per partition = `KAFKA_MAX_MESSAGES_PER_PARTITION × 1024` (262144, 256 KiB) (`kafka.ts:40, 309-313`); client defaults not overridden: max bytes 50 MiB per fetch, isolation `READ_COMMITTED` (`@platformatic/kafka dist/clients/consumer/options.js:251-255`) |
| topics | `autocreateTopics: true` (a worker may start before the topic exists) |
| offsets | `autocommit: false`; the consumer commits itself. Start position: `MessagesStreamModes.COMMITTED` with fallback `EARLIEST` — a partition with no committed offset starts at the beginning (`consumer.ts:623-633`) |

**Commit.** `consumer.commit({ offsets: [{ topic, partition, offset, leaderEpoch }] })` with `offset = lastResolvedOffset + 1` and `leaderEpoch` of
that last message (`consumer.ts:553-554`, `partition-lanes.ts:146`); one partition per commit request, awaited, no commit metadata. A failed commit is logged
(`'kafka offset commit failed'`) and **ignored**: a later commit on the partition covers it, otherwise the messages are redelivered (`partition-lanes.ts:141-156`).

#### P1.3.2 Lanes: concurrency and ordering

One lane per partition (`partition-lanes.ts:1-5`). Constants and their sources:

| Quantity | Value | Source |
|---|---|---|
| concurrent lanes | `KAFKA_PARTITIONS_CONCURRENT` (8) | `consumer.ts:536` |
| max messages per batch | `KAFKA_MAX_MESSAGES_PER_PARTITION` (256) | `consumer.ts:537` |
| `maxBuffered` | `concurrency × maxBatch × BUFFERED_BATCHES_PER_LANE (2)` = 4096 | `consumer.ts:38, 538-541` |
| resume threshold | `maxBuffered × 0.5` = 2048 | `partition-lanes.ts:67` |
| failed-handler wait | 1000 ms | `partition-lanes.ts:65` |

Behaviour, exactly:

1. **push(partition, item)** (`partition-lanes.ts:260-273`): dropped when the lanes are stopped or the partition is not in the current assignment (a `null` assignment
   accepts everything). Dropped when `item.offset < lane.nextOffset` — a source that rewinds cannot queue a message twice. Otherwise `nextOffset = offset + 1`, the item is queued,
   `buffered++`, backpressure is re-evaluated, and a scheduling pass is queued as a microtask (`scheduleSoon`) so one fetch response (delivered message by message in one synchronous run) becomes one batch, not a batch of one and a remainder (`:245-257`).
2. **schedule** (`:217-243`): iterate lanes in map order; start a lane that is not running and has queued items while `active < concurrency`. The lane that just finished is moved to the end of the map (round robin).
3. **runLane** (`:175-215`): `items = queue.splice(0, maxBatchSize)` (the next ≤256 consecutive items of that partition). One batch per partition at a time; batches of the same partition never overlap.
4. **Batch outcome loop**: `runOnce` calls the batch handler. If the handler *throws* it is logged (`'kafka batch handler threw — running the batch again'`), the lane sleeps 1000 ms and the outcome becomes `redeliver` (`:158-173`).
   While the outcome is `redeliver` and the lanes are running and the partition is still assigned, **the same `items` are run again** (`:189-196`). There is no attempt limit on this loop.
5. After the loop: if any offset was resolved, commit `highestResolved + 1`. `unfinished` = items with offset greater than the committed one (all of them if nothing was resolved);
   `buffered -= (items − unfinished)`. If `unfinished` is non-empty and the lanes are still running and the partition still assigned they are put back at the front of the queue; otherwise (shutdown or revocation)
   they are dropped without commit, so the next owner re-reads them (`:197-215`).
6. **Backpressure** (`:111-121`): `paused = true` and `stream.pause()` as soon as `buffered ≥ maxBuffered`; `stream.resume()` when `buffered ≤ maxBuffered × 0.5`. Re-evaluated on every push, every lane completion and every assignment change. `buffered` counts queued **and** in-flight items.
7. **Assignment change** (`:274-282`): queues of partitions no longer assigned are dropped (`buffered` adjusted); a running batch of a revoked partition sees `isStale() = true` and stops between messages.
8. `quiesce()` sets `holding` (no lane starts) and waits for idle (`:283`); `reset()` drops every queue, clears `holding` and `paused` (`:287-294`); `stop()` sets `running=false`, waits for idle, drops queues (`:295-301`).

#### P1.3.3 One batch (`eachBatch`, `consumer.ts:367-460`)

1. Empty batch ⇒ `done`.
2. `closeDurabilityWindow = openDurabilityWindow()` **before any handler runs** (`:381`; implementation P1.6.1).
3. **Group by key** (`:387-396`): `key = message.key ? message.key.toString() : '__no_key__:' + offset`. (A zero-length non-null key is a truthy Buffer and forms group `''`; a null key is its own singleton group.)
4. **Run groups concurrently, messages within a group strictly in order** (`:409-430`): `Promise.all` over groups, no cap other than the batch size. Before each message: if `!isRunning() || isStale()` the group stops (the remaining
   messages of the group stay unprocessed). A message is "processed" when `processMessage` returns (it never throws); every `ACTIVITY_EVERY = 16` processed messages call `onActivity()` (`:35, 425`).
   A retrying message therefore holds back the rest of **its own key** only.
5. **Durability gate** (`:437-441`): `await closeDurabilityWindow()`. If it throws, `'buffered events could not be made durable — batch left unresolved for redelivery'` is logged and the batch is **not durable**.
6. **Durable**: `durabilityFailures.delete(partitionKey)`, then resolve offsets in strict ascending order over `batch.messages` and **stop at the first message that was not processed** (`:446-451`). Returns `done`.
7. **Not durable**: nothing is resolved. If still running and not stale, `waitForRedelivery` (`:347-365`): `failures = count + 1` per `topic-partition` key, `delay = min(KAFKA_HANDLER_RETRY_INITIAL_MS × 2^(failures−1), 5000)` (`DURABILITY_RETRY_MAX_MS`, line 57),
   `onActivity()` is called first (a waiting member must not look dead), then sleep. Not while shutting down. Returns `redeliver`; the lane then re-runs **all 256 messages** through the handlers again (P1.17 #2 lists what a rerun repeats).
8. `onActivity()` after the gate, either way (`:458`).
9. `resetDurabilityBackoff()` (clears the failure counts) runs on every group join (`consumer.ts:667-672`, `:464-466`).

#### P1.3.4 One message: parse, retry, dead-letter

`processMessage` (`:288-317`): parse (P1.2) → on failure dead-letter `parse_error` and return. Else `runHandlerWithRetry` (`:246-281`):

- attempts `1..KAFKA_HANDLER_MAX_ATTEMPTS` (default 3); the handler is `incomingEvent` (P1.5) bound to a per-message context (P1.4).
- On each failed attempt: `metrics.handlerFailed(partition)`, a `warn` log (`'kafka incomingEvent handler failed'` with `err, partition, offset, projectId, attempt, maxAttempts`), and unless it was the last attempt: sleep `delay`, `delay = min(delay × 2, KAFKA_HANDLER_RETRY_MAX_MS)` starting from `KAFKA_HANDLER_RETRY_INITIAL_MS`. Defaults: sleeps 100 ms then 200 ms; no sleep after the last attempt.
- All attempts failed ⇒ dead-letter `handler_error` with the last error's `message` (`String(err)` when it is not an `Error`).
- A dead-lettered message **counts as processed**: its offset is resolved with the rest (the event is dropped). A failed dead-letter write does not change that (`:204-244`).

#### P1.3.5 What a successful attempt may have left behind

The handler is re-run from scratch on a retry; side effects of the failed attempt stay (they are not rolled back). See the failure table in P1.5.5.

#### P1.3.6 The dead-letter list [WORKER]

Written by `createDeadLetterRecorder({ client: getRedisCache(), maxEntries: INGEST_DEAD_LETTER_MAX_ENTRIES })` (`main.ts:269-272`, `redis/dead-letter.ts:121-143`). The cache client is deliberately the fail-fast one (P1.6.7).

Redis commands, captured, one pipelined write (ioredis `multi()`):

```
MULTI
LPUSH dead_letter:events <json>
LTRIM dead_letter:events 0 <INGEST_DEAD_LETTER_MAX_ENTRIES - 1>
EXEC
```

- Key `dead_letter:events` is fixed (`redis/dead-letter.ts:16`), newest at the head, capped at `INGEST_DEAD_LETTER_MAX_ENTRIES` (default 1000). No TTL.
- `<json>` is `JSON.stringify` of, in this key order (`redis/dead-letter.ts:95-108`; captured):
  `recordedAt` (`new Date().toISOString()`, e.g. `2026-11-21T17:30:22.556Z`) [NONDET], `topic` (string), `partition` (number), `offset` (**string**), `reason` (`parse_error` | `handler_error`), `error` (string),
  `key` (string via `Buffer.toString()`, or `null`), `value` (string via `Buffer.toString()`, or `null`), `headers` (object name→string; a value that is an array is joined with `,`, null/undefined become `''`, anything else `String(v)`; in the consumer
  path `message.headers` is a map of the record headers with `name.toString()` keys and Buffer values, so each value is `Buffer.toString()`).
  Captured: `{"recordedAt":"2026-11-21T17:30:22.556Z","topic":"events","partition":3,"offset":"42","reason":"parse_error","error":"Unexpected end","key":"wparity-dev","value":"{\"bad\":","headers":{"x-h":"hv"}}`
- The `error` text is the JS engine's message (JavaScriptCore's for JSON errors) and is not reproducible in Go; it is a debugging sample, not a contract. `topic` here is `KAFKA_EVENTS_TOPIC` (the consumer passes its own topic).
- A MULTI that returns `null` or contains a per-command error (e.g. `WRONGTYPE`) throws; the consumer then calls `metrics.deadLetterFailed`, logs `'kafka message DROPPED without being recorded — dead-letter write failed'` and drops the message anyway (`consumer.ts:222-238`). On success: `metrics.deadLettered(partition, reason)` and an `error` log (`:239-243`).
- The DLQ **topic** (`KAFKA_EVENTS_DLQ_TOPIC`, `produceDeadLetterEvent`) is not wired into the worker; only the Redis list is used on this path.

#### P1.3.7 Heartbeat activity

`markEventsActivity` is called: every 16th processed message of a batch (`consumer.ts:425`), after each batch (`:458`), at the start of a durability wait (`:359`), and on every `'fetch'` event of the stream **only when** `lanes.activeCount() === 0 && consumer.isActive()` (`:645-649`) — an idle consumer still fetches every `KAFKA_MAX_WAIT_MS`, a busy lane must prove its own progress, and a stream that fetches from outside the group does not count. In Go: the equivalent of a fetch returning (empty or not) while no lane runs.

#### P1.3.8 Rebalance, stream failure, stop

- **Group join** (`consumer.ts:667-683`): `resetDurabilityBackoff`; `lanes.setAssignment(partitions of the subscribed topic)`; log `'kafka consumer joined group (rebalance complete)'`. **Rebalance** start is only logged (`:684-689`). Heartbeat stall is only logged (`:690-695`).
- **Membership lost** (`consumer:group:lost`, `:696-706`) and consumer `error` (failed rejoin, `:709-715`) and stream `error` (`:650-659`): `restartStream()`.
- **restartStream** (`:584-621`): single flight (`restarting` flag). `closeStream` = detach the stream, `lanes.quiesce()` (running batches finish), close the stream, `lanes.reset()`. Then loop until success or stopping:
  `delay = min(1000 × 2^consecutiveRestarts, 30000)` ms, `consecutiveRestarts++`, sleep, `openStream()` (a fresh `consume` from the **committed** offsets), `ensureMembership()` (`joinGroup()` when the consumer lost its member id), fail the attempt if the new stream already failed. `consecutiveRestarts` resets to 0 on the first `data` message (`:635`).
- **Stop** (`:732-751`): `stopConsuming` (once): `stopping = true`, pause the stream, `lanes.stop()` (running batches see `isRunning() = false`, stop between messages, **commit what they finished**, which is only done after the durability gate for the messages handled), close the stream. `stop()` = `stopConsuming` then `consumer.close(true)` (leave group).
- A batch interrupted by a stop that handled a prefix: the gate still runs for that prefix and the prefix is resolved when durable (test `'flushes what a shutdown-truncated batch handled, before resolving it'`).

### P1.4 The per-message scope

`createIncomingEventHandler` (`ingest/consumer-handler.ts:24-47`), built once at boot:

- `consumerDeps` = boot deps with `buffers.event = buffers.event.asRedeliverable()` (a view whose `add` is `addRedeliverable`, P1.6.1).
- Per message: `requestId = resolveRequestId(payload.headers?.['request-id'])` (`core/shared/request-id.ts:12-29`): delete every character outside `[A-Za-z0-9_-]`, truncate to 64; if nothing is left, a fresh random id of 21 characters (`REQUEST_ID_LENGTH`, `generateId`) [NONDET]. A non-string header value is not guarded (`candidate.replace` on a number throws; see P1.17 #9).
- `ctx = createCtx(consumerDeps, { requestId, logger: logger.child({ requestId }) })` (`core/context.ts:115-133`): `ctx.queues = producers.scope({ requestId })`, so **every BullMQ job added while handling this message carries `meta: { requestId }`**. Services are built lazily from `ctx` (`installLazyServices`).
- Delivery coordinates `kafkaPartition`, `kafkaOffset` are added to the logger (`incoming-event-handler.ts:272-277`). Logs are not a contract.

### P1.5 What `incomingEvent` does

Entry: `incomingEvent(jobPayload, deps, meta)` (`ingest/incoming-event-handler.ts:263-283`) = `ingestIncomingEvent` then, if `jobPayload.ai` and the result is a stored event (an object with an object `document`), `storeAiEvent` (P1.5.7).

#### P1.5.1 Derivations (`ingestIncomingEvent`, `:317-403`)

All of this is plain JavaScript semantics that a port has to copy exactly (P1.16 lists the traps).

- `properties = body.properties ?? {}`.
- `getProperty(name) = (properties[name] || properties[name.replace('__','')]) ?? undefined` — a **falsy** prefixed value (`''`, `0`, `false`) falls through to the un-prefixed key (`__path` → `path`; `String.replace` with a string pattern replaces the first occurrence only).
- `profileId = body.profileId ? String(body.profileId) : ''` (`0` and `''` become `''`).
- `createdAt = new Date(body.timestamp)` (string or number); `isTimestampFromThePast` from the body.
- `url = getProperty('__path')`; `{ path, hash, query, origin } = parsePath(url)` (P1.16.1).
- `referrer = isSameDomain(getProperty('__referrer'), url) ? null : parseReferrer(getProperty('__referrer'))`; `utmReferrer = getReferrerWithQuery(query)` (P1.16.2).
- `clientQuery = properties.__query`; `eventProperties = { ...properties, __hash: hash, __query: (clientQuery || query) ? { ...clientQuery, ...query } : undefined }` — JS spread: the key order is `properties`' own order, with `__hash` and `__query` either replacing an existing key **in place** or appended last; `query` keys overwrite `clientQuery` keys.
- `sdkName = headers['openpanel-sdk-name']`, `sdkVersion = headers['openpanel-sdk-version']` (`undefined` when absent).
- `baseEvent` (JSON key order matters because `baseEvent` is copied into BullMQ job data, P1.9; captured):

```
name, profileId, projectId, deviceId, sessionId,
properties,            // omit(['__path','__referrer','__timestamp','__revenue'], eventProperties)  (own keys removed; __hash/__query keep their undefined)
groups,                // body.groups ?? []   (null becomes [])
createdAt,             // Date
duration,              // 0
sdkName, sdkVersion,   // may be undefined
city, country, region, longitude, latitude,                 // geo.* as received
path, origin,          // from parsePath
referrer,              // referrer?.url || ''
referrerName,          // utmReferrer?.name || referrer?.name || referrer?.url      (may be undefined)
referrerType,          // utmReferrer?.type || referrer?.type || ''
os, osVersion, browser, browserVersion, device, brand, model,   // uaInfo.*
revenue                // body.name === 'revenue' && '__revenue' in properties ? parseRevenue(properties.__revenue) : undefined
```

  `parseRevenue(v)`: falsy ⇒ `undefined`; a number ⇒ itself; a string ⇒ `parseFloat`, NaN ⇒ `undefined`; anything else ⇒ `undefined` (`:249-261`).
  JSON.stringify drops `undefined` members, so `sdkName`, `revenue` and friends are simply absent from serialized copies (observed: no `revenue` key in the captured job data).

#### P1.5.2 Branch A: server, past or AI event (`:405-450`)

Condition: `uaInfo.isServer || isTimestampFromThePast || ai` (an AI event is server-side even when it forwards a browser UA). Session state is **read, never written**.

1. `session = (profileId !== '' && !isTimestampFromThePast) ? getExistingSession({ profileId, projectId }) : null`. (`buf/session-buffer.ts:123-142`): `GET session:profile:<projectId>:<profileId>`; if absent or empty, no session; else `GET session:<projectId>:<deviceIdFromPointer>`; the value goes through `getSafeJson` (invalid JSON ⇒ no session, no error). A Redis error **propagates** (handler failure).
2. Payload (JS spread of `baseEvent`, so the key order is `baseEvent`'s, overrides in place):

```
deviceId       = session?.device_id || deviceId            // falsy (including '') falls back
sessionId      = session?.id        || sessionId
properties     = session ? inheritSessionUtm(baseEvent.properties, session) : baseEvent.properties
referrer       = session?.referrer       ?? undefined      // NOTE: undefined (not baseEvent.referrer) when there is no session
referrerName   = session?.referrer_name  ?? undefined
referrerType   = session?.referrer_type  ?? undefined
path           = session?.exit_path   ?? baseEvent.path    // '' is not nullish: an empty exit_path wins
origin         = session?.exit_origin ?? baseEvent.origin
os, osVersion(session.os_version), browserVersion(session.browser_version), browser, device, brand, model,
city, country, region, longitude, latitude   = session?.<same name> ?? baseEvent.<same name>
```

   `inheritSessionUtm(properties, session)` (`:56-68`): `query = { ...properties.__query }`; for `utm_source, utm_medium, utm_campaign, utm_content, utm_term` in that order: `if (!query[key] && session[key]) query[key] = session[key]`; returns `{ ...properties, __query: query }` — `__query` is always set to an object (possibly empty).
3. `createEventAndNotify({ ...payload, id: eventId })` (`id` appended last; `eventId` may be `undefined` ⇒ random UUID later).

No `session_start` is created, no session is created or extended, `enqueueSessionEnd` is never called.

#### P1.5.3 Branch B: client event (`:452-528`)

1. `isEventExcludedByProjectFilter(baseEvent)` (P1.10.1). Excluded ⇒ return `null`: nothing is written, not even the session.
2. `session = await sessions.ingest(baseEvent)` (P1.6.2). Returns `null` for: no `projectId`/`deviceId`, an event named `session_start` or `session_end`, **or any error inside** (logged, swallowed).
3. `kind === 'boundary'`: `await enqueueSessionEnd({ payload: baseEvent, closedSession: session.closed })` (P1.9.1); success ⇒ `session_ends_enqueued_total{source="boundary"}`; **failure is logged and swallowed**. This happens *before* the new `session_start` is created.
4. `kind ∈ {new, boundary}`: `sessions_started_total{kind}` ++, then `createEventAndNotify({ ...baseEvent, name: 'session_start', createdAt: new Date(baseEvent.createdAt.getTime() − 100) })` — **no `id`** (random UUID [NONDET]); uses `baseEvent` (no UTM inheritance); keeps `baseEvent.revenue` (so the `session_start` of a `revenue` event carries its `revenue`). A failure here is logged and **rethrown** (handler failure).
5. `finalPayload` = `baseEvent` when `session` is `null`; else `{ ...mergeDeepRight(baseEvent, reject(isEmpty|isNil)({ referrer: s.current.referrer, referrerName: s.current.referrer_name, referrerType: s.current.referrer_type })), properties: inheritSessionUtm(baseEvent.properties, s.current) }` — i.e. the session's referrer fields override the event's own **only when non-empty**, and UTM tags are inherited.
6. `return createEventAndNotify({ ...finalPayload, id: eventId })`.

#### P1.5.4 `createEventAndNotify` (`:218-247`)

1. `isEventExcludedByProjectFilter(payload)` again (with the payload's own `name`, so a filter on `session_start` would exclude it). Excluded ⇒ log, return `null`.
2. `Promise.all([ createEvent(payload), checkNotificationRulesForEvent(payload).catch(() => null) ])`. Both are *started* synchronously in this order. `createEvent` runs its whole synchronous prefix first (P1.5.5); the notification check reaches its first `await` afterwards. They share the **same `payload` object**: `createEvent` sets `payload.profileId = payload.deviceId` when the profile id is empty, and the notification check may later attach `payload.profile` (P1.10.3). Consequences: rules match against the already-mutated payload (so `has_profile` differs between the project filter, evaluated before, and rule matching, evaluated after), and the notification's stored `payload.event` contains the fallback `profileId`, the `id` and possibly `profile` (captured).
3. After both settle: `markFirstEvent(projectId)` with errors swallowed (P1.10.2). If `createEvent` rejected, `Promise.all` rejects first and `markFirstEvent` is not reached.
4. Returns `createEvent`'s result `{ document }`.

#### P1.5.5 `createEvent` (`core/modules/event/event.service.ts:452-544`)

Synchronous prefix, in order:
1. `if (!payload.profileId && payload.deviceId) payload.profileId = payload.deviceId` (mutates the caller's object).
2. Build the row `IClickhouseEvent` (below). 3. `deps.buffers.event.add(event)` — **not awaited**; with the consumer's view this is `addRedeliverable` (P1.6.1).
4. If `payload.profileId` is truthy **and** `name ∈ {session_start, session_end}`: `upsertProfile(deps, profile, true)` (P1.6.3), awaited via `Promise.all`.

The row — the JSON line pushed to Redis is `JSON.stringify(row)` with exactly this key order (captured, `event.service.ts:460-498`):

| # | Key | Value |
|---|---|---|
| 1 | `id` | `payload.id ?? crypto.randomUUID()` [NONDET when absent] |
| 2 | `name` | `payload.name` |
| 3 | `device_id` | `payload.deviceId` |
| 4 | `profile_id` | `payload.profileId ? String(payload.profileId) : ''` (after the fallback in step 1) |
| 5 | `project_id` | `payload.projectId` |
| 6 | `session_id` | `payload.sessionId` |
| 7 | `properties` | `toDots(payload.properties)` (P1.16.3): object string→string |
| 8 | `path` | `payload.path ?? ''` |
| 9 | `origin` | `payload.origin ?? ''` |
| 10 | `created_at` | Luxon `DateTime.fromJSDate(createdAt).setZone('UTC').toFormat('yyyy-MM-dd HH:mm:ss.SSS')` — e.g. `2026-01-01 00:00:00.000`. An invalid `Date` yields the literal `Invalid DateTime` (P1.17 #7) |
| 11–13 | `country`, `city`, `region` | `?? ''` |
| 14–15 | `longitude`, `latitude` | `?? null` (JSON number or `null`; JS number formatting, P1.16.5) |
| 16–22 | `os`, `os_version`, `browser`, `browser_version`, `device`, `brand`, `model` | `?? ''` |
| 23 | `duration` | `payload.duration ?? 0` |
| 24–26 | `referrer`, `referrer_name`, `referrer_type` | `?? ''` |
| 27 | `imported_at` | `null` |
| 28 | `inserted_at` | `DateTime.utc().toFormat('yyyy-MM-dd HH:mm:ss.SSS')` [NONDET] — stamped here, not by the column default |
| 29–30 | `sdk_name`, `sdk_version` | `?? ''` |
| 31 | `revenue` | `payload.revenue` — **key omitted** when `undefined` |
| 32 | `groups` | `payload.groups ?? []` |

Captured example (a `session_start`; random id and `inserted_at` masked):
`{"id":"<uuid>","name":"session_start","device_id":"wparity-dev","profile_id":"wparity-user","project_id":"wparity-proj","session_id":"wparity-sess-1","properties":{"plan":"pro","n":"5","nested.a.b":"1","nested.arr.0":"1","nested.arr.1":"x","__hash":"#top","__query.utm_source":"google","__query.x":"1"},"path":"/pricing","origin":"https://example.com","created_at":"2025-12-31 23:59:59.900","country":"GB","city":"London","region":"ENG","longitude":-0.1,"latitude":51.5,"os":"Mac OS","os_version":"10.15.7","browser":"Chrome","browser_version":"128.0","device":"desktop","brand":"Apple","model":"Mac","duration":0,"referrer":"https://www.google.com","referrer_name":"Google","referrer_type":"search","imported_at":null,"inserted_at":"<now>","sdk_name":"web","sdk_version":"1.0.0","groups":[]}`

Return value `{ document: row }` (the object, with `profile_id` etc.). `AiEventIdentitySource` reads `id, project_id, profile_id, device_id, session_id, groups, created_at` from it.

#### P1.5.6 Effect timelines (captured)

The captured sequences below are the Redis commands of the *real code* for each branch, in server order; `MULTI` blocks are single ioredis pipelines (`MULTI … EXEC`). Commands issued by cached lookups that the probe stubbed (project row, project filters) are marked *(derived)*; they are the `GET cachable:…` / `SETEX` pairs of P1.6.5 and run in the order the code calls them. Project and rule reads are served by the in-process L1 (60 s) after the first one, so those `GET` lines appear only on an L1 miss.

**T1. Client event, new session** (`screen_view`, profile `u` ≠ device `d`, no live session):
```
GET cachable:getProjectById:[P]                                  (derived: project-filter check, P1.10.1)
GET session:P:d
MULTI
  SET session:P:d <session json>                                 (no TTL)
  ZADD session:wallclock:P <Date.now()> d
  SADD session:projects P
  SET session:profile:P:u d PX 3600000                           (only when profile_id != device_id)
  RPUSH session-buffer <session json>                            (1 row, sign=1, version=1)
EXEC
-- session_start: createEventAndNotify --
GET cachable:getProjectById:[P]                                  (derived)
EXISTS profile-cache:P:u                                         (createEvent → upsertProfile(isFromEvent=true))
GET cachable:getProjectById:[P]                                  (notification check; SETEX on a miss)
GET cachable:getNotificationRulesByProjectId:[P]                 (SETEX … 1440 on a miss)
RPUSH profile-buffer <profile json>                              (only when EXISTS returned 0)
RPUSH event_buffer:queue <session_start row> <event row>         (micro-batch timer, 10 ms; may be 1 or 2 RPUSH calls)
-- main event: createEventAndNotify, same pattern, no profile upsert --
GET cachable:getProjectById:[P]                                  (derived, markFirstEvent twice)
```
**T2. Client event, existing session, same window ("extend")**: `GET session:P:d`; `MULTI SET session; ZADD; SADD; [SET pointer]; RPUSH session-buffer <current(+1)> <previous(−1)> EXEC` (two rows, current first); then only the main event's `createEventAndNotify` (no `session_start`, no profile upsert). Notification jobs, when a rule matches, are added **before** the event's `RPUSH` (the micro-batch timer fires later).
**T3. Client event, idle gap > timeout ("boundary")**: as T1 but the MULTI writes the **new** session (one row) and, between the MULTI and the `session_start`, the BullMQ `add` of the `sessions` queue (P1.9.1) is issued. The old session's blob is overwritten in place by the same `SET`; its profile pointer is deleted only if the profile id changed (P1.6.2).
**T4. Server event with a profile and a live session**: `GET session:profile:P:u`, `GET session:P:<d'>`, then the event `RPUSH`. **No session writes, no profile upsert** (the event is not a `session_start`).
**T5. Server event, no live session**: `GET session:profile:P:u` (null) then the event `RPUSH`. With an empty `profileId` **no read at all**.
**T6. Past event** (`isTimestampFromThePast`): no session read; only the event `RPUSH`.
**T7. AI event**: T4/T5 first (server branch), then `RPUSH ai-event-buffer <ai row>` **before** the event's own `RPUSH` lands (the event waits for the micro-batch timer; the AI row is written directly), preceded by the *(derived)* `GET cachable:getProjectAiStoreContent:[P]` (P1.6.4).
**T8. Rule match**: the rule cache GET, up to three `sendNotification` BullMQ adds per matching rule (created integrations, app, email — `notification-dispatch.ts:151-177` — but the app delivery awaits its Postgres insert before it enqueues, so BullMQ receives integrations, then email, then app; captured job ids 1, 2, 3 were integration, email, app), one Postgres `INSERT INTO notifications` for the app delivery (before its add), all before the event `RPUSH`.
**T9. Dead letter**: `MULTI / LPUSH / LTRIM / EXEC` (P1.3.6).

Timing contract: only two orderings are guaranteed — within one message the session `MULTI` precedes the `session_start` row precedes the event row (sequential `await`s), and **every buffer, session and job write of a batch (event, session, profile, AI rows; BullMQ adds; the dead-letter write) has completed before its offsets are committed** (P1.3.3); the cache `SETEX` calls are fire-and-forget and are not part of that promise. Interleaving of independent commands across keys of one batch, and the batching of the `RPUSH` into lists, is not a contract. Order of rows **within one key** (same device) in `event_buffer:queue` is.

#### P1.5.7 AI events (`:285-315`)

After the event row is produced (`isStoredEvent(result)`: an event excluded by the project filter returns `null` and leaves no AI row): `storeContent = (await getAiStoreContent(projectId)) !== false` then `aiEvents.add(buildAiEventRow(jobPayload.ai, document, { storeContent }))` (P1.6.4). An error from the `getAiStoreContent` lookup propagates (handler failure); an error from the Redis write is swallowed.

#### P1.5.8 Failure behaviour by step

| Step | Redis/Postgres error | Retried by the handler loop? |
|---|---|---|
| project-filter read `getProjectByIdCached` (twice per event, plus notification and first-event reads) | Redis `GET` rejection or Prisma error **propagates** | yes |
| `getExistingSession` (branch A) | propagates | yes |
| `sessions.ingest` (branch B) | **swallowed** (logged `'Failed to ingest session'`), returns `null` ⇒ the event is written as if it had no session, no `session_start` | no |
| `enqueueSessionEnd` | swallowed (logged) | no |
| `session_start` `createEventAndNotify` | rethrown | yes |
| `createEvent` (event add, profile add) | cannot fail in practice: `buffers.event.add` is in-memory, `profileBuffer.add` swallows | — |
| notification check (rules read, Postgres insert, BullMQ add) | swallowed by `.catch(() => null)`; rules processed after the failing one in the same `Promise.all` still run (already started) | no |
| `markFirstEvent` | swallowed | no |
| `getAiStoreContent` | propagates | yes |
| `aiEvents.add` | swallowed (logged `'Failed to add ai event'`) | no |
| matcher `TypeError` (P1.10.1, P1.17 #4) | project filter: **propagates**; notification rules: swallowed | filter: yes |
| durability gate | batch redelivered (P1.3.3) | n/a |

**What a rerun repeats** (handler retry, or a redelivered batch): every step again, with the state the first run left. Specifically: the event row is buffered again with the **same `id`** (the worker does not deduplicate; `buf/event-buffer.ts:225-231` records duplicates seen in ClickHouse); `session_start` gets a **new random id**; `sessions.ingest` now finds the session the first run wrote and returns `extend`, so `event_count`/`screen_view_count` and `version` advance a second time and `session_start` is **not** emitted again (P1.17 #2); notifications are created and enqueued again (no dedup, P1.17 #5).

### P1.6 Redis

All keys are literal (no ioredis `keyPrefix`); the logical database is whatever `REDIS_URL` selects (default `redis://localhost:23379`, `redis/redis.ts:45`). Values are UTF-8 strings. Everything below uses the **cache client** (`getRedisCache()`, P1.6.7) except the BullMQ adds (P1.9).

| Key | Type | Written by P1 | TTL | Tags |
|---|---|---|---|---|
| `event_buffer:queue` | list | `RPUSH` of event rows | none | [SHARED] read/drained by the flush (P2); `getActiveVisitorCount` etc. read ClickHouse, not this |
| `session:<projectId>:<deviceId>` | string | `SET` | none | [SHARED] read by Go `/track` (`apps/ingest/internal/identity/redis.go`), `device-id.ts`, reaper, vacuum |
| `session:wallclock:<projectId>` | zset | `ZADD score=Date.now() member=deviceId` | none | [SHARED] reaper / metrics |
| `session:projects` | set | `SADD projectId` | none | [SHARED] reaper / metrics |
| `session:profile:<projectId>:<profileId>` | string = deviceId | `SET … PX 2×SESSION_TIMEOUT_MS` | 3,600,000 ms by default | [SHARED] read by the server branch |
| `session-buffer` | list | `RPUSH` of session rows | none | drained by the session flush (P2) |
| `profile-buffer` | list | `RPUSH` of profile rows | none | [SHARED] the Bun API's `identify` writes it too |
| `profile-cache:<projectId>:<profileId>` | string | **read only** (`EXISTS`, `GET`) | set by the profile flush (P2) | [SHARED] |
| `ai-event-buffer` | list | `RPUSH` | none | AI rows; no other writer |
| `dead_letter:events` | list | `LPUSH` + `LTRIM` | none | [WORKER] (P1.3.6) |
| `cachable:getProjectById:[<id>]` | string | `SETEX` on miss, `DEL` after first event | 86400 s | [SHARED] |
| `cachable:getProjectAiStoreContent:[<id>]` | string | `SETEX` on miss | 86400 s | [SHARED] |
| `cachable:getNotificationRulesByProjectId:[<id>]` | string | `SETEX` on miss | **1440 s** | [SHARED] |
| `bull:<queue>:*` | BullMQ | via `Queue.add` | see P1.9 | [SHARED] |

Not on this path: **no application Lua script runs** (the session `cleanupScript` is session close, P3; the flush lock scripts are P2; BullMQ's own script is P1.9), **no Redis pub/sub** is published (`events:batch` is published by the event flush, `notification:created` by the notification *job*, `redis/publisher.ts`), `session:hwm:*` is only read by a metric and not written anywhere in `core/`.

#### P1.6.1 Event buffer (`buf/event-buffer.ts`) [WORKER]; `createEvent` is also used by the session-end job and imports [SHARED]

Constants: `EVENT_BUFFER_MICRO_BATCH_SIZE` 100 and `EVENT_BUFFER_MICRO_BATCH_MS` 10 (`:48-49, 57-60`; a malformed env value is ignored and the default kept). State: `pendingEvents`, `lastQueuedSeq`, `lastDurableSeq`, `droppedThroughSeq`, `lastDropFailure`, `inFlightWrite {coversSeq, done}`, `flushTimer`, a set of "redeliverable" event objects.

- `add(event)` (`:129-151`): `lastQueuedSeq++`; push to `pendingEvents`; if `len ≥ 100` ⇒ `flushLocalBuffer()` at once, else if no timer is armed ⇒ arm one for 10 ms that calls it. Then the add observer fires (`buffer_add_total{buffer="event"}`, `buffer_add_duration_ms`, P1.11). Not awaited by callers.
- `flushLocalBuffer()` (`:291-318`): if a write is in flight, return it; if nothing is pending, return `null`; clear the timer; take **the whole** pending array (so a write can carry more than 100 events when adds arrived during a write) and set `coversSeq = lastQueuedSeq`; start the write; when it ends clear `inFlightWrite` and, if events are pending and no timer, arm the 10 ms timer again (`:373-381`). Only one write is in flight at a time.
- `writeToRedis` (`:321-370`): **one** `RPUSH event_buffer:queue <JSON.stringify(e1)> <JSON.stringify(e2)> …` (captured: both rows of one `session_start`+event pair arrived in a single RPUSH). Success ⇒ `lastDurableSeq = max(lastDurableSeq, coversSeq)`, `flushRetryCount = 0`. Failure (reject, timeout, `WRONGTYPE`, OOM): events that are **not** redeliverable are put back at the **front** of `pendingEvents`; events that are redeliverable are **dropped**, `droppedThroughSeq = max(…, coversSeq)` and `lastDropFailure = error` (it over-approximates which sequences were dropped on purpose); `flushRetryCount++`, warn log; the failure is the resolved value, never a rejection.
- `addRedeliverable(event)` (`:236-239`) marks the object and calls `add`. The consumer's view (`asRedeliverable()`, a `Proxy` whose `add` is `addRedeliverable`, `:251-259`) means **every event created while handling a Kafka message is redeliverable**: a failed write drops it and relies on the batch being redelivered. Events created by other producers in the process (session-end job, import) use plain `add` and are re-queued.
- `openDurabilityWindow()` (`:272-275`): `sinceSeq = lastQueuedSeq`; returns the gate `() => flushPendingOrThrow(sinceSeq)`.
- `flushPendingOrThrow(sinceSeq = droppedThroughSeq)` (`:178-219`): `target = lastQueuedSeq`; `while lastDurableSeq < target`: if no write is in flight — if nothing is pending ⇒ break (everything queued is settled: written or dropped), else start a write and throw on its failure; if a write is in flight ⇒ await it and throw its failure **only if `inFlight.coversSeq ≥ target`** (another write that did not carry our events is not our failure). After the loop: `if droppedThroughSeq > sinceSeq⇒ throw lastDropFailure`. A failure that is not an `Error` is wrapped in one. Note the gate is **process-wide**: events of other lanes are in the sequence space, so a drop caused by another lane's write also fails this lane's gate (a redundant redelivery, never a loss), and `lastDropFailure` is never cleared.
- Shutdown calls the same function with the default `sinceSeq` (`main.ts:624`).
- Port note: what is observable is (a) every event of a batch is in `event_buffer:queue` before the batch's offsets are committed, (b) list order of the events of one key, (c) a failed write ⇒ no commit + backoff (P1.3.3), (d) shutdown refuses to complete if events are still local. The micro-batch sizes are not observable.

#### P1.6.2 Session buffer (`buf/session-buffer.ts`) [SHARED: blob format is read by the Go receiver, `device-id.ts`, reaper, vacuum, session-end]

**Constants**: idle timeout `SESSION_TIMEOUT_MS` default 1,800,000 (`:11, 24-26`); profile pointer TTL = 2 × timeout (`:16, 104`); `MAX_SESSION_GROUPS = 100` (`:29`). Keys: `session:<p>:<d>`, `session:profile:<p>:<profile>`, `session:wallclock:<p>`, `session:projects`, list `session-buffer` (`:31-36, 107`).

**Read** `getExistingSession` (`:123-142`): by device ⇒ `GET session:<p>:<d>`; by profile ⇒ `GET session:profile:<p>:<profile>` and, if truthy, `GET session:<p>:<that device>`. `getSafeJson` = `JSON.parse` in a try (`null` on error).

**Decision** `ingest(payload)` (`:166-205`):
```
if !(projectId && deviceId)                         return null
if name in {session_start, session_end}             return null
try:
  existing   = getExistingSession({projectId, deviceId})
  eventMs    = payload.createdAt.getTime()
  isBoundary = existing && (eventMs - fromClickhouseDate(existing.ended_at).getTime()) > idleTimeoutMs   // strictly greater; event time, not wall clock
  if existing && !isBoundary:  (current, rows) = extendSession(existing, payload); persist(current, rows, existing); return {extend, current}
  current = newSession(payload); persist(current, [current])
  return existing ? {boundary, current, closed: existing} : {new, current}
catch e: log 'Failed to ingest session'; return null
```
`fromClickhouseDate` = Luxon `fromFormat(s, 'yyyy-MM-dd HH:mm:ss.SSS', zone UTC)`; an unparseable string gives an invalid date and `NaN` comparisons (`isBoundary` false, the session is "extended"). `toClickhouseDate` = Luxon UTC `'yyyy-MM-dd HH:mm:ss.SSS'`.

**`newSession(payload)`** (`:333-379`), JSON key order = this order (captured; `sign` and `version` last):
`id` = `payload.sessionId`; `project_id`; `device_id`; `profile_id` = `payload.profileId || payload.deviceId`; `is_bounce` = `true`; `created_at` = `ended_at` = clickhouse date of `createdAt`; `event_count` = `name === 'screen_view' ? 0 : 1`; `screen_view_count` = `name === 'screen_view' ? 1 : 0`; `entry_path`, `entry_origin`, `exit_path`, `exit_origin` = `path ?? ''`, `origin ?? ''` (twice); `revenue` = `name === 'revenue' ? (payload.revenue ?? 0) : 0`; `referrer`, `referrer_name`, `referrer_type`, `os`, `os_version`, `browser`, `browser_version`, `device`, `brand`, `model`, `country`, `region`, `city` (each `?? ''`); `longitude`, `latitude` (`?? null`); `duration` = `payload.duration ?? 0`; `utm_medium`, `utm_source`, `utm_campaign`, `utm_content`, `utm_term` = `String(properties.__query[key])` when truthy else `''` (key order here: medium, source, campaign, content, term; `pickUtm` `:79-88`); `groups` = `(payload.groups ?? []).slice(0, 100)`; `sign` = 1; `version` = 1.

**`extendSession(existing, payload)`** (`:249-327`): `old = {...existing, sign: -1}`; `current = {...existing, sign: 1, version: existing.version + 1}` (key order of the stored blob is preserved; keys the blob lacks are appended). Then, with `eventMs`, `startMs = ms(current.created_at)`, `endMs = ms(current.ended_at)`, `eventCh = toClickhouseDate(createdAt)`:
```
if eventMs >= endMs:  current.ended_at = eventCh; if payload.path   exit_path   = payload.path;   if payload.origin exit_origin = payload.origin
if eventMs <  startMs: current.created_at = eventCh; if payload.path entry_path = payload.path;   if payload.origin entry_origin = payload.origin
else:                  if !current.entry_path   && payload.path   → entry_path   = payload.path
                       if !current.entry_origin && payload.origin → entry_origin = payload.origin
current.duration = ms(current.ended_at) - ms(current.created_at)
if name == 'revenue':                         current.revenue = (current.revenue ?? 0) + (payload.revenue ?? 0)
if name == 'screen_view' && payload.path:     current.screen_view_count += 1
else:                                         current.event_count += 1
if current.screen_view_count > 1:             current.is_bounce = false
if payload.profileId && payload.profileId != payload.deviceId:  current.profile_id = payload.profileId
if payload.groups?.length:  current.groups = unique([...(current.groups ?? []), ...payload.groups]).slice(0, 100)
rows = [current, old]                           // this order
```
Attribution fields (referrer, utm, os, geo…) are **not** touched on extend. (`is_bounce` is never set back to true; a screen_view without a path counts as an event.)

**`persist(current, rows, previous?)`** (`:392-434`), one `MULTI/EXEC` pipeline:
```
SET session:<p>:<d> JSON.stringify(current)                                  // no expiry
ZADD session:wallclock:<p> <Date.now()> <d>                                   // wall clock [NONDET]
SADD session:projects <p>
DEL session:profile:<p>:<previous.profile_id>        // only if previous.profile_id && != d && != current.profile_id
SET session:profile:<p>:<current.profile_id> <d> PX <2 × idleTimeoutMs>       // only if current.profile_id && != d
RPUSH session-buffer <JSON.stringify(row)> …                                  // 1 row (new/boundary), 2 rows (extend)
```
Error handling: `exec()` `null` ⇒ throw `'session write-back transaction was aborted'`; the first per-command error is thrown (ioredis resolves a MULTI with per-command errors). Both are caught in `ingest` and become `null`.

Race note: `GET` then `MULTI` is **not** atomic; correctness relies on one message per device key being in flight (P1.3.3 key serialization) and on a device's messages being in one partition (key = device or `project:profile`). Two members must never consume the same partition concurrently.

Captured blob (extend, after one `screen_view` then a `purchase` 60 s later, `created_at` untouched):
`{"id":"wparity-sess-1","project_id":"wparity-proj","device_id":"wparity-dev","profile_id":"wparity-user","is_bounce":true,"created_at":"2026-01-01 00:00:00.000","ended_at":"2026-01-01 00:01:00.000","event_count":1,"screen_view_count":1,"entry_path":"/pricing","entry_origin":"https://example.com","exit_path":"/order","exit_origin":"https://example.com","revenue":0,"referrer":"https://www.google.com","referrer_name":"Google","referrer_type":"search","os":"Mac OS","os_version":"10.15.7","browser":"Chrome","browser_version":"128.0","device":"desktop","brand":"Apple","model":"Mac","country":"GB","region":"ENG","city":"London","longitude":-0.1,"latitude":51.5,"duration":60000,"utm_medium":"","utm_source":"google","utm_campaign":"","utm_content":"","utm_term":"","groups":[],"sign":1,"version":2}`
(Out-of-order, captured: an event at 00:02:10 arriving after a session whose `created_at` and `ended_at` are 00:32:00 moves `created_at` back to 00:02:10, sets `entry_path` to its path, keeps `ended_at`, and `duration` becomes 1790000.)

#### P1.6.3 Profile (`core/modules/profile/profile.service.ts:334-368`, `buf/profile-buffer.ts:74-129`) [SHARED: `identify`/`increment` in the Bun API call the same `upsertProfile`]

Triggered from `createEvent` only for `name ∈ {session_start, session_end}` and a truthy `profileId`. The row (JSON key order, captured):

`{"id":<String(profileId)>,"first_name":"","last_name":"","email":"","avatar":"","properties":<strip(props)>,"project_id":<projectId>,"created_at":"<YYYY-MM-DD HH:mm:ss>","last_seen_at":"<same>","is_external":<profileId !== deviceId>,"groups":[]}`

- `properties` before stripping, in this order: `path, country, city, region, longitude, latitude, os, os_version, browser, browser_version, device, brand, model, referrer, referrer_name, referrer_type` taken from the event payload (`path` from `payload.path`, `os_version` from `osVersion`, …). `strip` = ramda `reject(anyPass([isEmpty, isNil]))`: removes keys whose value is `undefined`, `null`, `''`, `[]` or `{}` (a `0` or `false` stays). **Numbers stay numbers** (`"longitude":-0.1`), strings stay strings.
- `created_at` and `last_seen_at`: `formatClickhouseDate(new Date())` = ISO string with `T`→space and the `.mmmZ` suffix removed: `2026-11-21 17:30:22` (seconds, **wall clock**, both equal) [NONDET].
- `is_external` is evaluated **after** the `profileId = deviceId` fallback, so anonymous visitors are `false`.
- `add(profile, isFromEvent = true)`: `EXISTS profile-cache:<p>:<profileId>`; if the result is exactly `1⇒ skip` (counted `buffer_add_skipped_total{buffer="profile",reason="cached"}`); else `RPUSH profile-buffer <JSON.stringify(row)>`. Errors are caught and logged (`'Failed to add profile'`). Called through `timeAdd` (metrics P1.11). The `profile-cache:` entries are written by the flush (P2, TTL `PROFILE_BUFFER_TTL_IN_SECONDS` default 3600), which is why a profile re-upserts at most once per hour from events.

#### P1.6.4 AI buffer and its cache [WORKER]

- `ai-event-buffer` (`buf/ai-event-buffer.ts:31-39`): `RPUSH ai-event-buffer JSON.stringify(row)`, errors caught and logged, no add metrics (the buffer is not in `allBuffers`, P1.11).
- Row (`core/modules/ai/src/row.ts:70-89`): `{...draft, span_id: draft.span_id || event.id, input/output/tools: storeContent ? draft.X : '', project_id, id, profile_id, device_id, session_id, groups, created_at}` with the last seven from the stored event row (`id` = event id, `created_at` = the event's `created_at` string). Key order: **the draft's own order** (the keys `span_id`, `input`, `output`, `tools` keep their draft positions), then `project_id`, `id`, `profile_id`, `device_id`, `session_id`, `groups`, `created_at` for any of them the draft did not already contain. Captured ends: `…"properties":{"a":"b"},"project_id":"wparity-proj","id":"wparity-ev-7","profile_id":"wparity-user","device_id":"wparity-dev","session_id":"wparity-sess-2","groups":[],"created_at":"2026-01-01 00:03:20.000"`.
- `getProjectAiStoreContentCached`: `cachable:getProjectAiStoreContent:[<id>]`, TTL 86400, **no in-process copy** (so an opt-out reaches all processes at once, `project.service.ts:61-76`), value `true`/`false`; the loader is `SELECT "public"."projects"."id", "public"."projects"."aiStoreContent" FROM "public"."projects" WHERE ("public"."projects"."id" = $1 AND 1=1) LIMIT $2 OFFSET $3` (captured) and a missing project yields `null` (not cached). `storeContent = value !== false`.

#### P1.6.5 The shared `cachable` read-through (`redis/cachable.ts:153-285`, `core/cacheable-per-deps.ts:82-111`)

- **Key**: `cachable:<name>:<stringify(args)>` with `replaceAll(/\s/g, '')` over the **whole key** (so an id containing whitespace loses it). `stringify([id])` is `[` + the string `id` unquoted + `]`, e.g. `cachable:getProjectById:[acme-web]` (`:217-219, 70-102`). Object arguments would be key-sorted with `localeCompare`; none are used on this path.
- **L1**: per-process LRU, max 1000 entries, TTL 60 s, one instance per Prisma client (`cacheablePerDb`), so it is process-wide. `AiStoreContent` has none.
- **Read** (`:229-262`): L1 hit and `shouldCache(hit)` ⇒ return. Else `GET key`; a non-empty reply goes through `parseCache` (`JSON.parse` with a reviver that turns any string matching `/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*Z$/` into a `Date`; a parse error logs and yields `null` ⇒ treated as a miss); `shouldCache(parsed)⇒ L1 set, return`. Else run the loader; `if shouldCache(result)` ⇒ L1 set and **fire-and-forget** `SETEX key ttl JSON.stringify(result)` (errors ignored). A Redis `GET` rejection is **not** caught here.
- `shouldCache(x)` (`:114-132`): `undefined/null⇒false`; string ⇒ non-empty; array ⇒ non-empty, or any array when `cacheEmptyArray`; object ⇒ at least one key; number/boolean ⇒ true. Hence: an **unknown project is never cached** (every message for it costs Postgres reads); `false` is cached; rules `[]` is cached.
- **Clear** (`:265-269`): L1 delete + `DEL key` (other processes' L1 copies live up to 60 s).
- Project row cached JSON = Prisma's row, scalar columns in schema order (captured from the real client): `{"id":…,"name":…,"organizationId":…,"eventsCount":0,"types":[],"domain":null,"cors":[],"crossDomain":false,"allowUnsafeRevenueTracking":false,"aiStoreContent":true,"filters":[],"firstEventAt":null|"2026-11-07T00:41:21.000Z","noDataNotifiedAt":null,"dataStoppedNotifiedAt":null,"deleteAt":null,"createdAt":"…Z","updatedAt":"…Z"}`. The P1 handler reads only `firstEventAt`, `filters` and `name`, but **other TS code reads the same key and needs the full row** [SHARED]: a Go writer must write the full row, or never write.
- Rules cached JSON: `[{"id","name","sendToApp","sendToEmail","config","template","integrations":[{"id"}]}]` — scalars in `select` order, relation last; `config` is a Postgres `jsonb` so its object keys come back **length-then-bytewise sorted** (captured: `{"id","name","filters","segment"}` for an event, `{"id","name","value","operator"}` for a filter). Only structure, not byte order, is a contract for cached values.

#### P1.6.6 What the Go receiver already does

`apps/ingest/internal/kv` offers `GET, SET NX PX, MGET, SETEX, PING` only; `lookups` reads `cachable:op:salt:[]` and `cachable:getOrganizationByProjectId:[<id>]` without writing; `identity` reads `session:<p>:<d>` with `MGET`. The worker additionally needs `RPUSH, LPUSH, LTRIM, EXISTS, DEL, ZADD, SADD, SET … PX, MULTI/EXEC, EVALSHA/EVAL` (the last only for the BullMQ bridge, P1.9). `kv.Redis` has no pipeline or transaction surface yet.

#### P1.6.7 How the TS clients behave (matters for failure parity)

- **Cache client** `getRedisCache()` (`redis/redis.ts:100-154`): ioredis, `connectTimeout` 10 s, `commandTimeout = REDIS_CACHE_COMMAND_TIMEOUT_MS` (5000), reconnect delay `min(attempt × 50, 500)` ms, and **the offline queue is disabled after the first `ready`**: a command on a down connection rejects immediately instead of queueing. Every `await redis.x()` is its own round trip; `multi()` sends `MULTI … EXEC` as one pipeline; there is no auto-pipelining.
- **Queue client** `getRedisQueue()` (BullMQ producers, `redis/redis.ts:36-41, 179-191`): `maxRetriesPerRequest: null`, offline queue **on**, `enableReadyCheck: false`. During a Redis outage a `Queue.add` waits for the reconnect with **no timeout**, so a notification or session-end enqueue can hold a message's handler (and its key group) until Redis returns (P1.17 #10).

### P1.7 Postgres

Client: Prisma 7 with the `pg` adapter (`packages/db/src/prisma-client.ts`, `postgres-adapter.ts`). Pool: `max = connection_limit (URL) || availableParallelism × 2 + 1`, wait/connect timeout `pool_timeout` (URL) default 10 s. `DATABASE_URL` (the `schema` query parameter selects the schema, `public`). Dates are bound as ISO-8601 strings (`toISOString()`, UTC, ms) to `timestamp without time zone` columns (`timestamp(3)` on `notifications`), i.e. they store the UTC wall clock — a Go driver must send UTC wall-clock values.

All statements below were **captured from the real Prisma client** (reads on the dev database, writes inside a transaction that was rolled back); `$n` are bind parameters.

| # | Call | SQL | Params |
|---|---|---|---|
| 1 | `project.findUnique({where:{id}})` (`getProjectById`, on cache miss) | `SELECT "public"."projects"."id", "public"."projects"."name", "public"."projects"."organizationId", "public"."projects"."eventsCount", "public"."projects"."types"::text[], "public"."projects"."domain", "public"."projects"."cors", "public"."projects"."crossDomain", "public"."projects"."allowUnsafeRevenueTracking", "public"."projects"."aiStoreContent", "public"."projects"."filters", "public"."projects"."firstEventAt", "public"."projects"."noDataNotifiedAt", "public"."projects"."dataStoppedNotifiedAt", "public"."projects"."deleteAt", "public"."projects"."createdAt", "public"."projects"."updatedAt" FROM "public"."projects" WHERE ("public"."projects"."id" = $1 AND 1=1) LIMIT $2 OFFSET $3` | `[id, 1, 0]` |
| 2 | `project.findUnique({select:{aiStoreContent}})` | `SELECT "public"."projects"."id", "public"."projects"."aiStoreContent" FROM "public"."projects" WHERE (… = $1 AND 1=1) LIMIT $2 OFFSET $3` | `[id, 1, 0]` |
| 3a | `notificationRule.findMany({where:{projectId}, select:{…, integrations:{select:{id}}}})` | `SELECT "public"."notification_rules"."id", "…"."name", "…"."sendToApp", "…"."sendToEmail", "…"."config", "…"."template" FROM "public"."notification_rules" WHERE "public"."notification_rules"."projectId" = $1 OFFSET $2` | `[projectId, 0]` |
| 3b | (relation; issued even when there are no rule rows, then with `IN (NULL)`) | `SELECT "public"."integrations"."id", "t0"."B" AS "IntegrationToNotificationRule@NotificationRule" FROM "public"."integrations" INNER JOIN "public"."_IntegrationToNotificationRule" AS "t0" ON "t0"."A" = "public"."integrations"."id" WHERE (1=1 AND "t0"."B" IN ($1,…)) OFFSET $n` | rule ids |
| 4 | `project.updateMany({where:{id, firstEventAt:null}, data:{firstEventAt:new Date()}})` (markFirstEvent) | `UPDATE "public"."projects" SET "firstEventAt" = $1, "updatedAt" = $2 WHERE ("public"."projects"."id" = $3 AND "public"."projects"."firstEventAt" IS NULL)` | `[now₁, now₂, id]` — **two** separate `new Date()` values (`@updatedAt` is filled client-side) [NONDET] |
| 5 | `notification.create({data})` (app delivery only) | `INSERT INTO "public"."notifications" ("projectId","title","message","createdAt","updatedAt","sendToApp","sendToEmail","integrationId","notificationRuleId","payload") VALUES ($1,…,$10) RETURNING <all columns>` — the `notificationRuleId` column is **omitted** when `undefined` | `[projectId, title, message, now, now, true, false, null, ruleId, payloadJSON]` — `createdAt` and `updatedAt` are the same `new Date()` computed client-side; `id` is the column default `gen_random_uuid()` (not client-side) |

Details of #5 (captured): `payload` is the `JSON.stringify` of the (null-char-stripped) object bound to a `jsonb` column — `Date` ⇒ ISO string, `undefined⇒key omitted`, `null⇒null`; the schema for `notifications`: `id uuid default gen_random_uuid()`, `createdAt/updatedAt timestamp(3) not null default CURRENT_TIMESTAMP`, `integrationId uuid`, `notificationRuleId uuid` (FKs `ON DELETE CASCADE`, so an unknown rule/integration id fails the insert). The insert is awaited **before** the job is enqueued.

### P1.8 ClickHouse, pub/sub and scripts

- **ClickHouse reads on this path: exactly one, conditional.** `getProfileById(projectId, profileId)` runs only when (`payload.profileId` is truthy **and** some cached rule's `template` matches `/{{profile\.[^}]*}}/`) (`notification-dispatch.ts:116-131`). It first tries `GET profile-cache:<p>:<profileId>` (a cached `IClickhouseProfile` JSON), otherwise (`profile.service.ts:213-236`, `profile/src/sql.ts:187-198`):
  `SELECT id, first_name, last_name, email, avatar, properties, project_id, is_external, created_at, last_seen_at, groups FROM profiles WHERE id = {p1:String} AND project_id = {p2:String} ORDER BY last_seen_at DESC LIMIT 1` via `chQuery` (JSON format, `ch.query` with `query_params`, no `session_timezone` setting, `core/ch-query.ts:28-83`; integer-typed columns would be coerced to numbers). The row becomes `transformProfile` (`profile.service.ts:142-163`): `{firstName, lastName, isExternal, properties: toObject(properties), createdAt, lastSeenAt, projectId, id, email, avatar, groups}` (dates parsed from `YYYY-MM-DD HH:mm:ss` as UTC) and is attached as `payload.profile`. A read error rejects the whole notification check (swallowed).
- **Pub/sub: none** on P1. **Lua: none** of the application's. **ClickHouse writes: none** (events reach ClickHouse only through the flush, P2).

### P1.9 BullMQ producers

The consumer path adds jobs to two queues. Everything here was captured with `MONITOR` against BullMQ **6.7.8** (the version in `packages/core`'s lockfile entry) using the real `createProducers` and the real `notificationQueueJobs` schema; the exact commands are in Appendix A.

**Queue name and key prefix.** `queueKey(name, { cluster: QUEUE_CLUSTER, namespace: QUEUE_NAMESPACE })` (`core/jobs/naming.ts:20-23`): `<name>`, or `<name>-<namespace>` when `QUEUE_NAMESPACE` is set, wrapped in braces `{…}` when `QUEUE_CLUSTER` is set (any non-blank value). BullMQ's key prefix is its default `bull`, so keys are `bull:<queueKey>:<suffix>` (`bull:sessions:wait`, `bull:sessions-wparity:wait`, `bull:{sessions}:wait`). Both variables are read at boot (`main.ts:142-149`); changing either orphans existing jobs (`env.ts:416-427`).

**What one `Queue.add(jobName, data, opts)` does** (captured. The script is not reproduced here: it is `bullmq@6.7.8`'s `addStandardJob-9` — 26,076 characters with its includes inlined, exported by `node_modules/bullmq/dist/cjs/scripts/addStandardJob-9.js` (sources in `dist/cjs/commands/addStandardJob-9.lua` and `includes/`). `sha1(content)` of that export is `5ab432e88830d573e2fa33392ffbd4ad25a9b100`, the SHA ioredis sent in `EVALSHA`; checked):

1. When a `Queue` object initialises (at boot, for all seven queues): `HSET bull:<q>:meta opts.maxLenEvents 10000 version bullmq:6.7.8` per queue, and a version-check `INFO` (seen twice in the probe, which opened two queues on one connection; not asserted per queue).
2. One script call, `EVALSHA <sha> 9 KEYS… ARGV1 ARGV2 ARGV3` (the first call on a connection is `EVAL` with the script text; `NOSCRIPT` falls back to `EVAL`):
   - `KEYS`: `bull:<q>:wait`, `…:paused`, `…:meta`, `…:id`, `…:completed`, `…:delayed`, `…:active`, `…:events`, `…:marker`.
   - `ARGV1`: msgpack values **concatenated, not an array**: `"bull:<q>:"`, custom job id or `""`, job name, `timestamp` (ms, `Date.now()` [NONDET]), then six `nil` (parent key, parent dependencies key, parent, repeat job key, deduplication key, defer-parent flag). `ARGV2`: `JSON.stringify(data)`. `ARGV3`: msgpack of the options map: `attempts`, `removeOnComplete`, `removeOnFail`, and `jobId` / `backoff` when given (captured: `{"attempts":3,"removeOnComplete":true,"removeOnFail":{"age":86400,"count":1000},"jobId":"sessionEnd:v2:sess1","backoff":{"type":"exponential","delay":200}}` and `{"attempts":0,"removeOnComplete":10,"removeOnFail":{"age":86400,"count":100}}`). Map order is irrelevant: the script reads keys and re-encodes `opts` with `cjson` for the stored hash, whose key order is not a contract.
   - Effects inside the script (captured, in order): `INCR bull:<q>:id`; `HGET meta opts.maxLenEvents` (`HSET` it to 10000 when missing); for a custom id `EXISTS bull:<q>:<jobId>` — **if it exists**: `HGET bull:<q>:<jobId> parentKey`, `XADD events MAXLEN ~ 10000 * event duplicated jobId <id>` and the existing id is returned, nothing else is written; otherwise `HMSET bull:<q>:<jobId> name <jobName> data <json> opts <cjson> timestamp <ms> delay 0 priority 0`; `XADD bull:<q>:events * event added jobId <id> name <jobName>`; `HMGET meta paused concurrency`; `LPUSH bull:<q>:wait <jobId>` (to `paused` instead when the queue is paused, see the script); `ZADD bull:<q>:marker 0 0`; `XADD events MAXLEN ~ 10000 * event waiting jobId <id>`.
   - The reply is the job id: the `INCR` counter as a decimal string (`"1"`) for default ids, the custom id otherwise. TS throws if there is none (`producers.ts:116-120`).
3. The data stored for every job is the **envelope** `{"payload":<validated payload>,"meta":{"requestId":"<id>"}}` (`core/jobs/envelope.ts:28-36`, `producers.ts:104-122`): the payload is parsed by the job's zod schema first (a bad payload throws at the call site) and `undefined`/functions are dropped by `JSON.stringify`; `Date` ⇒ ISO-8601 with milliseconds and `Z`; `meta.requestId` comes from the message scope (P1.4).

#### P1.9.1 `sessions` queue, job `session` [SHARED: `session-reaper` enqueues the same job; the TS worker consumes it]

- Trigger: a session `boundary` in branch B (P1.5.3 step 3): `ctx.services.session.enqueueSessionEnd(input)` ⇒ `queues.sessions.session.add(sessionEndJobPayload(input), sessionEndEnqueueOptions(closedSession.id))` (`core/modules/session/session.service.ts:480-487`).
- Queue defaults (`core/jobs.registry.ts:55-65`): `removeOnComplete: true`, `removeOnFail: { age: 86400, count: 1000 }`; worker concurrency 1. Job options (`session-end.ts:25-53`): `jobId = "sessionEnd:v2:" + closedSession.id`, `attempts: 3`, `backoff: { type: "exponential", delay: 200 }`. No `delay`, no priority.
- Data (`session-end.ts:78-92`): `{ "payload": { "event": { ...baseEvent, "projectId": closed.project_id, "deviceId": closed.device_id, "sessionId": closed.id, "profileId": closed.profile_id || baseEvent.profileId }, "snapshot": <closed session blob> }, "meta": { "requestId": … } }`. `baseEvent` (P1.5.1) is the event that *caused* the boundary, **without** an `id` and with its own `createdAt`; the four overrides keep the `baseEvent` key positions; `snapshot` is the stored blob as parsed (its stored key order). Captured (abridged):
  `{"payload":{"event":{"name":"screen_view","profileId":"d1","projectId":"p1","deviceId":"d1","sessionId":"sess1","properties":{"a":"b"},"createdAt":"2026-01-01T00:00:00.123Z","groups":[],"duration":0,"path":"/x","origin":""},"snapshot":{"id":"sess1","project_id":"p1","device_id":"d1","profile_id":"d1","ended_at":"2026-01-01 00:00:00.000"}},"meta":{"requestId":"wparity-req-1"}}`
  Validation at enqueue (`session.jobs.ts:35-49`): `event.projectId`, `event.deviceId` strings and `snapshot.{id, project_id, device_id, ended_at}` strings, else the add throws (swallowed by the caller).
- Idempotency: the custom job id. A second add while the record exists is a no-op (captured: `EXISTS` then `XADD … duplicated`, returns the same id). With `removeOnComplete: true` the record disappears after the job finishes, so the same id can be enqueued again later; `createSessionEnd` then relies on the `SET session:end:emitted:<p>:<d>:<sessionId> 1 EX 7200 NX` claim (P3). BullMQ refuses a custom id containing `:` unless it splits into exactly three parts (`bullmq/dist/cjs/classes/job.js:910-919`, ids that are plain integers are refused too), so a `sessionId` containing `:` makes `add` throw; the caller logs it and the reaper is the only way that session closes.
- Metric: `session_ends_enqueued_total{source="boundary"}` after the add resolves.

#### P1.9.2 `notification` queue, job `sendNotification` [SHARED: session-end funnel rules enqueue it too; the TS worker consumes it]

- Trigger: one add per (matching rule × delivery), P1.10.3. `core/modules/notification/notification.jobs.ts:13-28`.
- Queue defaults: `removeOnComplete: 10`, `removeOnFail: { age: 86400, count: 100 }`; concurrency 1. **No job id, no attempts, no backoff**: BullMQ stores `attempts: 0`; ids are the `INCR` counter.
- Data: `{ "payload": { "notification": N }, "meta": { "requestId": … } }` where `N` is the object **after the job's zod parse**, hence in the schema's key order, with `undefined` dropped and unknown keys removed: `projectId, title, message, sendToApp, sendToEmail, integrationId, notificationRuleId, payload` (`id` never present on this path). Values: `sendToApp`/`sendToEmail`/`integrationId` per P1.10.3; `payload` is `{"type":"event","event":<event payload>}`. Captured (rule with one integration, app and email; `event` abridged):
  `{"payload":{"notification":{"projectId":"wparity-proj","title":"Bought purchase by wparity-user","message":"Project: WParity Project","sendToApp":true,"sendToEmail":false,"integrationId":null,"notificationRuleId":"00000000-0000-0000-0000-0000000000a1","payload":{"type":"event","event":{"name":"purchase","profileId":"wparity-user","projectId":"wparity-proj","deviceId":"wparity-dev","sessionId":"wparity-sess-1","properties":{"value":"99","__query":{"utm_source":"google"}},"groups":[],"createdAt":"2026-01-01T00:01:00.000Z","duration":0,"sdkName":"web","sdkVersion":"1.0.0","city":"London","country":"GB","region":"ENG","longitude":-0.1,"latitude":51.5,"path":"/order","origin":"","referrer":"https://www.google.com","referrerName":"Google","referrerType":"search","os":"Mac OS","osVersion":"10.15.7","browser":"Chrome","browserVersion":"128.0","device":"desktop","brand":"Apple","model":"Mac","id":"wparity-ev-2"}}}},"meta":{"requestId":"wparity-req"}}`
  The event object is the **final payload as mutated** by `createEvent` and the notification check: `profileId` filled from `deviceId`, `id` appended, `profile` appended when a rule template needs it, and the `properties` are the **nested** (not dotted) object.
- No idempotency. A redelivered batch enqueues the same notifications again.
- A Go process that does not speak BullMQ can write the same logical job to a transit stream as GO_WORKER.md proposes; the logical contract is: queue, job name, the data envelope above, the options above, "written before the offset commit", "sessions: dedupe on job id; notification: none".

### P1.10 Project filters, first event, notification rules

#### P1.10.1 Project event filters (`incoming-event-handler.ts:185-198`)

`project = getProjectByIdCached(projectId)`; `filters = (project?.filters ?? []).filter(f => f.type === 'event')`; excluded ⇔ `filters.some(f => matchEvent(payload, f))`. `ip` and `profile_id` filter types are applied at receive time (`/track`), not here. The row's `filters` column is `jsonb` (default `[]`). Evaluated **twice for a client event** (once on `baseEvent` before the session is touched, again inside `createEventAndNotify` on each created event, including `session_start`) and once for branch A. The predicate is `matchEvent`:

```
matchEvent(payload, ev):
  if payload.name != ev.name && ev.name != '*':   false
  if ev.filters.length > 0:                       matchEventFilters(payload, ev.filters)
  else                                            true
matchEventFilters(payload, filters) = filters.every(f =>
  if f.value.length == 0                          true
  if f.name == 'has_profile':  f.value.includes('true') ? payload.profileId != payload.deviceId : payload.profileId == payload.deviceId
  v = (f.name startsWith 'properties.' ? pathOr('', f.name.split('.'), payload) : pathOr('', [f.name], payload)).trim()
  switch f.operator
    is             f.value.includes(v)                       // SameValueZero against the array elements: a number element never equals a string
    isNot          !f.value.includes(v)
    contains       f.value.some(x => v.includes(String(x)))
    doesNotContain !f.value.some(x => v.includes(String(x)))
    startsWith / endsWith   f.value.some(x => v.startsWith/endsWith(String(x)))
    regex          f.value.some(x => testFilterPattern(stripLeadingAndTrailingSlashes(String(x)), v))
    isNull         v == ''
    isNotNull      v != ''
    gt/lt/gte/lte  f.value.some(x => Number(v) <op> Number(x))
    default        false)
```
(`core/modules/notification/notification.service.ts:93-167`). `pathOr('', path, payload)` is ramda's: the default is returned when the value at the path is `null`/`undefined` (or an intermediate step is not an object). The `payload` is the camelCase `IServiceCreateEventPayload` (`createdAt` is a `Date`, `properties` nested). **If the value at the path is not a string — a number, boolean, array, object, `Date` — `.trim()` throws `TypeError`** (verified for `properties.value = 5`, `true`, `['a']` and `createdAt`; a missing path and an absent key are fine). In the project filter that error propagates (handler failure ×3 ⇒ dead letter); in rule matching it is swallowed and aborts the whole notification check of that event (P1.17 #4).
Regexes: `RE2JS.compile(pattern)` and `.test(input)` = find anywhere (unanchored); a pattern RE2 cannot compile **never matches** and is logged once as `warn`; compiled patterns are cached (LRU 1000) (`…/src/filter-regex.ts:36-70`). `stripLeadingAndTrailingSlashes` removes all leading and trailing `/` (`/^[/]+|[/]+$/g`). RE2J and Go's `regexp` are both RE2 syntax but are different implementations (P1.17 #11).
JS `Number(string)`: trims whitespace, `''` ⇒ 0, `0x…`/`0b…`/`0o…` and `1e3` and `Infinity` accepted, anything else `NaN` (all comparisons false); `Number(null)` = 0, `Number(true)` = 1.

#### P1.10.2 First event (`incoming-event-handler.ts:166-172, 205-216`)

After every successful `createEventAndNotify` (each of `session_start` and the event): `project = getProjectByIdCached(projectId)`; if there is no project or `project.firstEventAt` is truthy ⇒ nothing. Else: Postgres #4 of P1.7 (conditional, so concurrent workers are idempotent), then `getProjectByIdCached.clear` (= L1 delete + `DEL cachable:getProjectById:[<id>]`), then a log line. Errors anywhere in it are swallowed. Other processes keep their L1 copy up to 60 s and therefore repeat the (no-op) `UPDATE` and the `DEL` until it expires.

#### P1.10.3 Notification rules for an event (`core/modules/notification/src/notification-dispatch.ts:109-185`)

Input: the final event payload of `createEventAndNotify` (after the `profileId` fallback mutation). The whole function is wrapped by the caller in `.catch(() => null)`.

1. `project = getProjectByIdCached`, `rules = getNotificationRulesByProjectId` (P1.6.5; the project is only used for `name`).
2. If `payload.profileId` is truthy and any rule's `template` matches `/{{profile\.[^}]*}}/`: `getProfileById(profileId, projectId)` (P1.8); if found, `payload.profile = profile` (mutating the shared payload).
3. `Promise.all(rules.flatMap(rule => …))` — rules are processed concurrently, in order of the array; only `rule.config.type === 'events'` (funnel rules are P3). The matching step is **synchronous inside the `flatMap`**: `match = rule.config.events.find(ev => matchEvent(payload, ev))` (first matching definition; a `TypeError` here rejects the whole function and rules after it are not evaluated).
4. No match ⇒ nothing. Match ⇒ `notification = { title, message, projectId, payload: { type: 'event', event: payload } }` with `title = notificationTemplateEvent(payload, rule)` and `message = project?.name ? "Project: " + project.name : ''`.
5. Deliveries, **created in this order**: one per `rule.integrations[i]` with `integrationId = integration.id`; then `sendToApp` ⇒ `integrationId: 'app'`; then `sendToEmail` ⇒ `integrationId: 'email'` (captured enqueue order: integrations, email, app, because the app delivery waits for its insert). Each is `createNotification({ ...notification, integrationId, notificationRuleId: rule.id })` (`:74-92`):
   - `data = { title, message, projectId, payload: stripNullChars(payload) || undefined, ...getIntegration(integrationId), notificationRuleId }` where `getIntegration`: `'app'⇒{integrationId:null,sendToApp:true,sendToEmail:false}`, `'email'⇒{integrationId:null,sendToApp:false,sendToEmail:true}`, otherwise `{sendToApp:false,sendToEmail:false,integrationId}`. `stripNullChars` removes U+0000 from every string **value** (recursively; keys untouched, `Date` kept) because `jsonb` cannot store it.
   - If `data.sendToApp`: `await db.notification.create({ data })` (P1.7 #5) **first**.
   - Then `queues.notification.sendNotification.add({ notification: data })` (P1.9.2) — always, for all three kinds.
   - Email and integration deliveries have **no Postgres row**; a retried job would resend.
6. Template (`notification.service.ts:182-211`): no template ⇒ `You received a new "<name>" event`. Else `replaceAll('$EVENT_NAME', name).replaceAll('$RULE_NAME', rule.name).replaceAll('{{rule_name}}', rule.name)`; then for every distinct-or-repeated match of `/{{[^}]+}}/g` in the **already substituted** string: `path = match.slice(2,-2).split('.')`, `value = pathOr('', path, payload)`; if `value` is truthy: `template = template.replaceAll(match, typeof value === 'object' ? JSON.stringify(value) : value)`. Falsy values (`''`, `0`, `false`, `null`) leave the placeholder in place. `String.prototype.replaceAll` with a string argument interprets the dollar patterns of `replace` (dollar-ampersand, dollar-dollar, dollar-quote, dollar-backtick) in the **replacement** (P1.17 #8).

### P1.11 Metrics

Names, types and labels are a contract (`docs/SCALING.md` and the e2e load scripts read them). Exposed on `GET /metrics` of the worker's HTTP port (`core/rest.routes.ts:117`) from one `prom-client` registry (`core/metrics.ts`).

| Metric | Type | Labels | Incremented by | Source |
|---|---|---|---|---|
| `kafka_events_handler_failures_total` | counter | `partition` | every failed handler attempt (retries included) | `ingest/ingest.metrics.ts:10-15` |
| `kafka_events_dead_lettered_total` | counter | `partition`, `reason` (`parse_error`, `handler_error`) | dead letter recorded and dropped | `:21-26` |
| `kafka_events_dead_letter_failed_total` | counter | `partition` | dead-letter write failed, message dropped unrecorded | `:32-37` |
| `sessions_started_total` | counter | `kind` (`new`, `boundary`) | before creating `session_start` | `:39-44` |
| `session_ends_enqueued_total` | counter | `source` (`boundary`; the reaper uses `reaper`) | after the `sessions` add resolves | `core/modules/session/src/session.metrics.ts:29-34` |
| `buffer_add_total` | counter | `buffer` | every `add` that enqueued: `event` (direct observer in `EventBuffer.add`), `profile` (via `timeAdd`) | `buf/buffer.metrics.ts:149-154, 206-217` |
| `buffer_add_duration_ms` | histogram, buckets 0.5 1 2.5 5 10 25 50 100 250 500 1000 5000 | `buffer` | same (not for skipped adds) | `:141-147` |
| `buffer_add_skipped_total` | counter | `buffer`, `reason` | profile add skipped, `reason="cached"` | `:156-161` |
| `buffer_event_pending_local_count` | gauge (collect) | — | `pendingEvents.length` | `:69-81` |
| `buffer_<name>_count` for `event, profile, bot, session, replay, group, profile_backfill` | gauge (collect) | — | `LLEN` of the buffer's list at scrape time | `:27-65` |
| `<queue>_{active,delayed,failed,completed,waiting}_count` for each of the 7 queues | gauge (collect) | — | BullMQ counts per scrape (queue name with `-`→`_`, braces removed) | `core/jobs/jobs.metrics.ts:21-113` |
| `sessions_active_total`, `sessions_projects_active`, `sessions_hwm_lag_ms` | gauges (collect) | — | `SMEMBERS session:projects` + `ZCARD session:wallclock:*` per scrape; `GET session:hwm:*` | `session.metrics.ts:100-172` |
| prom-client default process/GC metrics | — | — | `registerDefaultMetrics()` | `core/metrics.ts:15-21` |

Not emitted on P1: the AI buffer has **no** gauge and **no** add metrics (it is not in `allBuffers`, `buffer.metrics.ts:27-37`); `session.ingest` does not go through `timeAdd` (no `buffer_add_*{buffer="session"}`); `job_duration_ms` is the failure-only BullMQ worker histogram (no worker runs on `events`-only); there is **no Kafka consumer-lag gauge** in the worker (lag sampling exists only in `apps/api/e2e/lag-monitor.ts` and `ingest-load.ts` via the admin API). The `buffer_*` flush metrics belong to P2.

### P1.12 Environment variables

Parsing rules (`env.ts:102-290`), which a Go config loader must reproduce for any variable it adopts:

| Rule | Behaviour |
|---|---|
| blank | an unset **or whitespace-only** value counts as unset (`blankToUndefined`) |
| `positiveIntWithDefault(d)` | `z.coerce.number().int().positive()`; unset ⇒ `d`; **a malformed value (`0`, `-1`, `5000junk`, `1.5`) fails the boot** |
| `optionalPositiveInt` | a positive integer, or `undefined` when unset **or malformed** (the module default applies; never a boot failure) |
| `definedIsTrue` | **any** non-blank value is true, including `false` and `0` (`DISABLE_WORKERS`, `QUEUE_CLUSTER`, `BUFFER_ASYNC_INSERTS`) |
| `trueOrOne` | true only for `true` or `1` (`SELF_HOSTED`, `AUTOMATIC_MIGRATIONS`, `DISABLE_BULLBOARD`) |
| `strictOptionalBoolean` | `true`/`1`/`false`/`0`, anything else fails the boot (`KAFKA_SSL`, `KAFKA_SSL_REJECT_UNAUTHORIZED`) |
| token list | comma-separated, trimmed, empties dropped (`KAFKA_BROKERS`, `ENABLED_QUEUES`) |

Variables the consumer path reads:

| Variable | Default | Meaning on this path | Source |
|---|---|---|---|
| `ROLE` | `api` | `worker` or `all` consume; `api` never does; unknown value fails the boot | `env.ts:14, 296-312` |
| `ENABLED_QUEUES` | all | tokens `events` + the seven registry names (`sessions, cron, notification, import, insights, gsc, cohortCompute`); `events` starts the Kafka consumer; an unknown token or the old `events_kafka` fails the boot; empty for a consuming role fails | `env.ts:74-91, 318-345, 632-640` |
| `DISABLE_WORKERS` | off | any value ⇒ the process starts no queue worker **and no Kafka consumer** (the consumer is started inside the `workersEnabled` block) | `main.ts:139, 536-574` |
| `QUEUE_CLUSTER` / `QUEUE_NAMESPACE` | off / unset | BullMQ queue naming (P1.9) | `env.ts:421-427` |
| `SHUTDOWN_FORCE_EXIT_MS` | 20000 | forced exit(1) timer | `env.ts:408-411` |
| `REDIS_CACHE_COMMAND_TIMEOUT_MS` | 5000 | `commandTimeout` of the cache client | `env.ts:412` |
| `INGEST_DEAD_LETTER_MAX_ENTRIES` | 1000 | `LTRIM` bound of `dead_letter:events` | `env.ts:67, 567` |
| `KAFKA_BROKERS` | — (required) | bootstrap list; empty ⇒ boot error | `env.ts:504` |
| `KAFKA_CLIENT_ID` | `openpanel` | client id | `env.ts:30` |
| `KAFKA_EVENTS_TOPIC` | `events` | consumed topic | `env.ts:31` |
| `KAFKA_CONSUMER_GROUP` | `openpanel-events` | group id | `env.ts:32` |
| `KAFKA_PARTITIONS_CONCURRENT` | 8 | concurrent lanes | `env.ts:33` |
| `KAFKA_MIN_MESSAGES` | 1 | fetch min bytes = × 1024 | `env.ts:34` |
| `KAFKA_MAX_WAIT_MS` | 500 | fetch max wait | `env.ts:35` |
| `KAFKA_MAX_MESSAGES_PER_PARTITION` | 256 | batch size and fetch bytes per partition (× 1024) | `env.ts:36` |
| `KAFKA_SESSION_TIMEOUT_MS` | 30000 | group session timeout | `env.ts:37` |
| `KAFKA_HEARTBEAT_INTERVAL_MS` | 3000 | must be < session timeout (boot check) | `env.ts:38, 654-666` |
| `KAFKA_REBALANCE_TIMEOUT_MS` | 60000 | rebalance timeout; also added to the request timeout | `env.ts:40` |
| `KAFKA_REQUEST_TIMEOUT_MS` | 5000 | request timeout | `env.ts:41` |
| `KAFKA_CONNECTION_TIMEOUT_MS` | 2000 | connect timeout | `env.ts:42` |
| `KAFKA_HANDLER_MAX_ATTEMPTS` | 3 | attempts per message | `env.ts:61` |
| `KAFKA_HANDLER_RETRY_INITIAL_MS` | 100 | first retry wait; also the base of the durability backoff | `env.ts:68` |
| `KAFKA_HANDLER_RETRY_MAX_MS` | 1000 | cap of the handler retry wait (the durability backoff has its own 5000 cap) | `env.ts:69` |
| `KAFKA_SSL`, `KAFKA_SSL_CA_PATH`, `KAFKA_SSL_REJECT_UNAUTHORIZED`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SASL_MECHANISM` | unset (`scram-sha-512`) | TLS and SASL; SASL needs both credentials; TLS defaults to on when credentials are set | `env.ts:559-565, 668-742` |
| `EVENT_BUFFER_MICRO_BATCH_SIZE` / `_MS` | 100 / 10 | event buffer micro-batch | `env.ts:576-577`, `event-buffer.ts:48-60` |
| `SESSION_TIMEOUT_MS` | 1,800,000 | idle window: boundary detection and ×2 for the profile pointer TTL | `env.ts:594`, `session-buffer.ts:24-26` |
| `REDIS_URL` | `redis://localhost:23379` | read directly by `redis/redis.ts:45` (db index from the URL path) | |
| `DATABASE_URL` | — | read directly by `db/src/prisma-client.ts:16`; `connection_limit` / `pool_timeout` query parameters tune the pool | |
| `CLICKHOUSE_URL` (comma list) and `CLICKHOUSE_CLUSTER`, `CLICKHOUSE_SETTINGS`, `CLICKHOUSE_REQUEST_COMPRESSION`, `CLICKHOUSE_MAX_OPEN_CONNECTIONS`, `CLICKHOUSE_REQUEST_TIMEOUT_MS`, `CLICKHOUSE_UNHEALTHY_MARK_MS` | — | read directly by `db/src/clickhouse/client.ts:88-195`; used on P1 only by the conditional profile read (P1.8) | |

Read by the process but outside the message path: `LOG_LEVEL` (`info`), `NODE_ENV`, `API_PORT` (3000, the worker still serves `/healthz/*` and `/metrics` there), `API_HOST`, `AUTOMATIC_MIGRATIONS` (applies pending ClickHouse code migrations once listening — also in a worker, `main.ts:509-531`), `SELF_HOSTED`, `ADMIN_USERNAME`/`ADMIN_PASSWORD`/`DISABLE_BULLBOARD` (bull-board is mounted for consuming roles), `DISABLED_CRON_JOBS` (cron only). Flush/TTL variables (`BUFFER_ASYNC_INSERTS`, `*_BUFFER_BATCH_SIZE`, `PROFILE_BUFFER_TTL_IN_SECONDS` …) belong to P2; `SESSION_REAPER*`, `SESSION_VACUUM*`, `EXPERIMENTAL_PROFILE_BACKFILL*` to P3.

### P1.13 What the Go module has and lacks for P1

| Need | Exists in `apps/ingest` | Missing |
|---|---|---|
| group join under the TS protocol name | `internal/kafka/balancer.go` `EventsGroupBalancer` (mixed TS+Go group passed the spike) | the consumer itself (franz-go `kgo.ConsumerGroup`, manual commit with leader epoch, `AtStart` fallback, pause/resume, revoke/lost handling), lanes, retry loop, durability gate |
| JS-ordered JSON | `internal/jsjson` (`Obj` with index-keys-first order, `Stringify`, `FormatNumber`, `JSString`, `StableMD5`, UTF-16 compare) | `toDots`, JS `trim`, `parseFloat`/`Number()`, Luxon-format dates, ramda `omit/mergeDeepRight/strip` equivalents |
| session blob | `internal/identity` reads `session:<p>:<d>` | full `IClickhouseSession` with JS key order; `ingest/extend/persist` |
| project cache | `internal/lookups` pattern (Redis then Postgres, 60 s local LRU) for salts/organization | `cachable:getProjectById`, `…AiStoreContent`, `…NotificationRulesByProjectId` with the exact key rules (P1.6.5) |
| URL / referrer | none (the receiver forwards what it cannot parse) | WHATWG `URL` and `URLSearchParams`, the 2,584-entry referrer table (generate from `core/shared/referrers/index.ts`, order significant; all keys unique, none integer-like) |
| Redis commands | `kv.Store`: `GET, SET NX PX, MGET, SETEX, PING` | `RPUSH, LPUSH, LTRIM, EXISTS, DEL, ZADD, SADD, SET PX, MULTI/EXEC` |
| Postgres | `pgxpool` reads in `lookups` | the 5 statements of P1.7 |
| BullMQ | none | job writes (or the transit stream) |
| metrics | `internal/server/metrics.go` | the names of P1.11 |

### P1.14 Effects the Bun API also performs (shared contracts)

| Effect | Also done by | So that |
|---|---|---|
| `event_buffer:queue` rows from `createEvent` | the TS `session-end` job (`session.jobs.ts`) and imports, until P3; the flush (P2) reads all of them | the row JSON stays identical |
| `session:<p>:<d>` blob, `session:wallclock:<p>`, `session:projects`, `session:profile:*` | read by Bun `/track/device-id` (`ingest/device-id.ts`, `ingest.service.ts:847`) and the Go receiver; written, id-gated-deleted and reaped by the TS session-end, reaper and vacuum | blob shape and key names; `sign/version` semantics |
| `session-buffer` rows | drained by the TS session flush | row JSON; extend ordering (current first, then the −1 row) |
| `profile-buffer` rows | the Bun API: `identify` (`ingest.service.ts:461, 527`), `identifyProfile`/`adjustProfileProperty` (`profile.service.ts:692, 743`) via the same `upsertProfile` | row JSON and `isFromEvent` skip rule |
| `profile-cache:*` | written by the profile flush, read by `getProfileById` in the API | |
| `cachable:getProjectById:[id]` | read and cleared by the Bun API (`project.service.ts:363, 430, 459, 502-506`) | full-row JSON, key rule, DEL on first event |
| `cachable:getProjectAiStoreContent:[id]` | cleared on project update (`project.service.ts:431`) | |
| `cachable:getNotificationRulesByProjectId:[id]` | cleared on rule changes (`notification.rpc.ts:58, 75`) | TTL 1440 s, `[]` cached |
| `bull:sessions-*` job `session` | the reaper enqueues the same job id scheme; the TS worker consumes | job id, data shape, options |
| `bull:notification-*` job `sendNotification` | the session-end funnel check; TS worker consumes | data shape (zod order), no id |
| `notifications` rows (`sendToApp`) | listed / marked read by the tRPC procedures; created by the funnel check too | columns, `jsonb` payload |
| `projects.firstEventAt` | read by the dashboard (activation) | conditional `UPDATE`, `updatedAt` set |
| `dead_letter:events` | ops tooling only | record format |
| Prometheus names | dashboards and `SCALING.md` | names, labels |

### P1.15 Non-determinism (what goldens must mask or control)

- **Wall clock**: event `inserted_at`; profile `created_at`/`last_seen_at`; `ZADD session:wallclock:<p>` score; dead-letter `recordedAt`; Prisma `createdAt`/`updatedAt` of the notification and `firstEventAt`/`updatedAt` of the project (two different clock reads); BullMQ `timestamp`; every heartbeat; retry sleeps.
- **Random**: `id` of an event whose record has none; `id` of **every** `session_start` (UUID v4); the request id when the record's `request-id` is missing or empty after sanitising (21 random chars); Postgres `gen_random_uuid()` notification ids; BullMQ numeric job ids (counter state).
- **Order**: the interleaving of Redis commands issued concurrently for different keys of one batch and for one message's parallel branches (event add / notification check); how rows are grouped into `RPUSH` calls (micro-batch timing); the position of rows of different keys relative to each other in a list; the order in which the three deliveries of a rule reach BullMQ (the app delivery awaits its Postgres insert first). Not random: per-key order, and the commit-after-durable rule.
- **Environment**: `process.env.TZ` is forced to UTC; `Date` parsing of non-ISO timestamp strings depends on the engine (JavaScriptCore on Bun), see `apps/ingest/README.md`'s list of timestamp forms the receiver forwards for that reason.
- A deterministic comparison replays recorded Kafka values with: fixed clock, seeded UUID source, `request-id` present, per-key ordering, and a normaliser that sorts rows across keys.

### P1.16 JavaScript semantics the port has to emulate

**1. `parsePath(path?)`** (`packages/shared/src/url.ts:11-49`). `!path⇒{path:'',origin:''}` (no `query`/`hash` keys). `hasOrigin = path.startsWith('http')` (so `httpfoo` counts). Not starting with `/` and not `hasOrigin⇒{path, origin:''}` (returned verbatim, no query, no hash — a "named route"). Otherwise `new URL(path, hasOrigin ? undefined : 'http://localhost')` and return `{ query: parseSearchParams(url.searchParams), path: url.pathname, hash: url.hash || undefined, origin: hasOrigin ? url.origin : '' }`; a `URL` parse error ⇒ `{path, origin:''}` verbatim. This is the **WHATWG URL parser** (percent-encoding sets, dot-segment removal, `//host` network-path references resolving against `localhost`, default-port elision, IDNA lowercasing) and the **`application/x-www-form-urlencoded` parser** (`+⇒space`, UTF-8 percent decoding with U+FFFD for invalid bytes). `parseSearchParams` copies `entries()` into a plain object (a repeated key keeps the first position and the **last** value; integer-like keys sort first on enumeration) and returns `undefined` for no parameters. `url.hash` is `''` for a lone `#`. `url.origin` of a non-special scheme is the string `"null"`.
  `getProperty` values are typed `string` but are whatever JSON the SDK sent: a non-string truthy `__path` (number, object) makes `path.startsWith` throw `TypeError⇒ handler failure`.

**2. Referrers** (`core/shared/parse-referrer.ts`, table `core/shared/referrers/index.ts`, a plain object literal `Record<string,{type,name}>`). `isSameDomain(a,b)`: both truthy, and `new URL(a).hostname === new URL(b).hostname` (both must parse as absolute URLs; a relative `__path` ⇒ `false`). `parseReferrer(url)`: `hostname = new URL(url).hostname` or `''`; `match = referrers[hostname] ?? referrers[hostname.replace('www.','')]` (**first** occurrence of `www.` anywhere, not a prefix strip); result `{ name: match?.name ?? '', type: match?.type ?? '', url: stripTrailingSlash(url ?? '') }` (`/\/+$/`). A non-string `__referrer` makes `stripTrailingSlash` throw. `getReferrerWithQuery(query)`: `source = (query.utm_source ?? query.ref ?? query.utm_referrer ?? '').toLowerCase()` (`??` takes the first **defined** one, so an empty `utm_source` shadows `ref`); `source === ''⇒null`; `match = referrers[source] || referrers[source + '.com'] || Object.values(referrers).find(r => r.name.toLowerCase() === source)` (table order); a match ⇒ `{name, type, url:''}`, else `{name: source, type:'', url:''}`. `referrers[x]` is an ordinary property lookup, so inherited names (`constructor`, `toString`, `__proto__`, …) hit `Object.prototype` (P1.17 #14).

**3. `toDots(obj)`** (`packages/shared/src/object.ts:3-136`; limits `TO_DOTS_MAX_KEYS = 1000`, `TO_DOTS_MAX_KEYS_PER_OBJECT = 50`):
```
out = {} ; count = 0
visit(value, key):
  if count >= 1000: return
  if value is undefined | null | '': return
  if string:
      kind = first char is '{' or '[' ? (last char is '}' / ']' respectively ? 'json' : 'malformed') : 'text'
      malformed: return
      json:      try visit(JSON.parse(value), key) catch: return          // same key, parsed value
      text:      out[key] = removeLoneSurrogates(value).trim(); count++   // an all-blank string is stored as ''
      return
  if object:
      if it is {type:'Buffer', data:[…]}: visit(UTF-8 decode of Uint8Array.from(data), key); return   // bytes taken mod 256, invalid sequences U+FFFD
      visitEntries(value, key + '.', 50); return
  out[key] = String(value); count++                                          // numbers, booleans
visitEntries(value, prefix, max):
  if Array: for i in 0 .. min(len, max)-1 while count < 1000:  visit(value[i], prefix + i)
  else:     seen = 0; for each own enumerable key k in JS order (index keys first):
              if seen >= max or count >= 1000: return;  seen++;  visit(value[k], prefix + k)    // 'seen' counts skipped (empty) entries too
visitEntries(root, path = '', 1000)           // the per-object cap of 50 does not apply to the root
```
Output order is insertion order with JS's index-keys-first rule applied to `out` (a key such as `"7"` is serialized first); a repeated key overwrites in place and still advances `count`. `String(number)` is the JS Number-to-string conversion (P1.16.5). `trim` removes ECMAScript WhiteSpace and LineTerminator (U+0009–000D, 0020, 00A0, 1680, 2000–200A, 2028, 2029, 202F, 205F, 3000, FEFF), **not** Go's `unicode.IsSpace` set (which lacks U+FEFF and includes U+0085). `removeLoneSurrogates` applies only if the string is not well-formed UTF-16 (`isWellFormed()` false) and deletes unpaired `U+D800–DFFF`; keys are not sanitised.

**4. Strings**: all lengths, indexes, `slice`, `includes`, `startsWith` are UTF-16 code units; `toLowerCase` is full Unicode lower-casing (`'İ'.toLowerCase()` is two code points); `localeCompare` is used only in the unused object-argument branch of the cache key.

**5. Numbers**: `JSON.stringify(number)` and `String(number)` use ECMAScript Number::toString (shortest round-trip digits, exponent form when ≥ 1e21 or < 1e-6, `-0⇒"0"`); `NaN` and ±`Infinity` serialize as `null` in JSON. `jsjson.FormatNumber` already implements it. `parseFloat`: skips leading whitespace, takes the longest valid decimal prefix (`Infinity` accepted, hex not), `NaN` if none. `Number(string)`: see P1.10.1.

**6. Dates**: `new Date(string|number)`; `toISOString()` always has ms and `Z` (years outside 0000–9999 use ±YYYYYY and an invalid date **throws** `RangeError`); `JSON.stringify(Date)` = `toISOString()` or `null` for an invalid date; Luxon `toFormat` on an invalid date returns the string `Invalid DateTime`; `formatClickhouseDate` = `toISOString().replace('T',' ').replace(/(\.\d{3})?Z+$/,'')`.

**7. Objects and JSON**: `JSON.parse` keeps the last duplicate value at the first position; `JSON.stringify` omits `undefined`/function members (`null` inside arrays), escapes `"`, `\` and control characters (`\b \t \n \f \r`, else `\u00xx`), writes lone surrogates as `\udxxx` escapes, does **not** escape `/`, `<`, `U+2028/2029` or non-ASCII, and has no whitespace. A Go decoder that turns an unpaired `\ud800` escape into U+FFFD changes bytes (P1.17 #15).

### P1.17 Quirks and open questions

Items that look like bugs but are observable behaviour. Tags: **[captured]** reproduced by a probe of Appendix A; **[derived]** read from the code only, not executed; **[observed]** seen in the shared dev stack's state. For each: **faithful** keeps the byte-for-byte contract; the alternative needs a decision from the owner (it changes data).

1. **[captured]** **A rerun duplicates rows.** Every handler retry and every redelivered batch buffers the event again with the same `id`; `session_start` gets a new random id. Rows already written to ClickHouse are not retracted; `buf/event-buffer.ts:225-231` records that duplicates were seen in ClickHouse (4,759 rows for 230 events).
2. **[captured]** **Session state is advanced before the events are durable, and a redelivery sees the advanced state.** On a failed durability gate the events (including the `session_start` that opened a session) are *dropped* from the buffer, but the session blob and `session-buffer` rows are already written. The redelivered batch finds the session and takes the `extend` path: `session_start` is never emitted for that session, `event_count`/`screen_view_count`/`version` advance twice, and two `session-buffer` row pairs are written. Same after a handler retry whose first attempt failed after `ingest` (`session_start` createEvent rethrow). Captured with `p1b-probe.ts`: the same message handled twice leaves the session at `{event_count:0, screen_view_count:2, version:2}` and the list with **two** `screen_view` rows (same id) and one `session_start`; with a simulated `RPUSH` failure, the failed run leaves the session at `version:1` and an **empty** list (its `session_start` was dropped), and the redelivery gives `version:2`, `screen_view_count:2` and **no `session_start` row at all**. Faithful = Go has the same defect; fixing it (e.g. derive the session effect idempotently from the event id) is a deliberate divergence.
3. **[derived]** **Unknown project ids are never cached** (`shouldCache(null)`), so each message for one costs several Postgres reads (filter ×2–3, first-event, notification). Faithful = same; a negative cache in Go would be a (safe) divergence in load only.
4. **[captured]** **Non-string values break filter matching.** `pathOr(...).trim()` throws for numbers, booleans, arrays, objects and `Date`. A project `event` filter that reads such a property **dead-letters the event** after 3 attempts (reason `handler_error`); a notification rule that does is **silently skipped for the event and aborts the evaluation of the remaining rules** of that event. Properties are raw JSON (a numeric `value: 5` is common). Faithful vs fix (coerce to string) is an owner decision.
5. **[derived]** **Notifications are not idempotent.** Redelivery or retry creates a second `notifications` row (app) and second jobs (all kinds); email and integration deliveries have no row to dedupe on. The `notification` job has no job id.
6. **[derived]** **Two members on one partition.** The lanes learn about a revocation only at the next group join (`consumer:group:join` ⇒ `setAssignment`), and a running batch stops between messages only once `isStale()` flips. During a rebalance in a mixed TS+Go group the old owner can still be inside a batch while the new owner starts the same offsets; the `GET session → MULTI` in `ingest` is not atomic, so the same device can be extended from two processes at once. (This is a reading of the client's event order in `consumer.ts`, not something the probes observed.) The window is bounded by the batch time (< `KAFKA_REBALANCE_TIMEOUT_MS`). The Go consumer must stop handing messages of a revoked partition promptly and commit only what is durable.
7. **[derived]** **An invalid `timestamp`** yields `created_at: "Invalid DateTime"` in the row (Luxon does not throw), which ClickHouse rejects, failing the whole insert chunk at flush time (P2); BullMQ job data serialises the date as `null`. The receiver validates timestamps before producing, so only foreign producers can trigger it.
8. **[derived]** **Template substitution uses `String.replaceAll(match, value)` with a string replacement**, so the special patterns of `String.prototype.replace` (dollar-ampersand, dollar-dollar, dollar-quote, dollar-backtick) inside a property value are interpreted, not copied.
9. **[captured]** **Records without a `headers` object** (old or replayed) throw `TypeError` at `headers['openpanel-sdk-name']` ⇒ 3 attempts ⇒ dead letter. A non-string `request-id` value throws in `sanitizeRequestId` the same way (`ingest/consumer-handler.ts:20` guards only the object, not the value type). Captured: `TypeError: undefined is not an object (evaluating 'headers["openpanel-sdk-name"]')` and `TypeError: candidate.replace is not a function`; a request id that is absent or sanitises to nothing (`"!!!"`) yields a fresh 21-character id, `"a b/c"` yields `abc`.
10. **[derived]** **BullMQ adds have no timeout and wait out a Redis outage** (queue client keeps its offline queue), while the Redis writes on the cache client fail fast. A notification or session-end enqueue can therefore hold a key group (and, after 60 s without batch progress, flip readiness) during an outage. Go must choose an explicit timeout and decide whether an unwritten notification job is retried, dropped, or fails the message.
11. **[derived]** **Regex dialect.** Rule and project filters run on RE2J (`re2js`); Go's `regexp` is RE2 but a different implementation (differences in flags, `\pX` classes, `\Z`, Unicode case folding, and what each rejects). A pattern one engine cannot compile never matches. Needs golden cases from real saved patterns.
12. **[derived]** **A `session_start` made from a `revenue` event carries that event's `revenue`** (it is `{...baseEvent, name:'session_start'}`); the session row's own `revenue` is only incremented by the `revenue` event itself.
13. **[derived]** **Server/past/AI events with no live session lose their referrer** (`referrer: session?.referrer ?? undefined` overrides `baseEvent.referrer` with `undefined` ⇒ `''`), and take `path`/`origin` from the session's *exit* page, not their own.
14. **[derived]** **Prototype keys in referrer lookups**: `utm_source=constructor` finds `Object` in `referrers` and produces `{name:'Object', type: undefined⇒''}`; hosts named `constructor`, `toString` behave alike. Only a golden written for it would catch a port that uses a Go map.
15. **[derived]** **Lone surrogates**: a JSON string with `\ud800` survives in JS and is re-serialised as `\ud800`; `toDots` deletes it from property **values** only. Go's decoder replaces it with U+FFFD. Any field other than property values (names, ids, paths) would differ byte for byte.
16. **[observed]** **Cached project rows can be partial.** A cached object lacking fields is accepted as is (any non-empty object passes `shouldCache`); the P1 handler uses `firstEventAt`, `filters`, `name` and treats missing as absent. (A partial `{"firstEventAt":…,"filters":[]}` value exists under `cachable:getProjectById:[proj_ingest]` in the shared dev Redis.)

**Open questions for the owner**: (a) per #2, #4, #13: copy the TS behaviour or fix it in Go and accept divergent rows; (b) BullMQ strategy (transit stream vs Go BullMQ writes), which fixes how #10 and #5 are handled; (c) whether the Go worker may *write* the shared `cachable:*` keys (it must then write the full Prisma-shaped row) or only read them; (d) the regex engine (RE2J port vs Go `regexp` with a compatibility test corpus), and the WHATWG URL implementation; (e) the language of this document (English here; `GO_WORKER.md` is Chinese).

---

## Appendix A: how the captures were produced

Everything below is reproducible on a checkout with the dev stack running, **without touching shared state**: Redis db index 11 (empty beforehand; removed afterwards), a BullMQ namespace `wparity`, and Postgres writes only inside rolled-back transactions. Replace `<repo>` with the checkout path. Work from `packages/core` for the Redis/BullMQ probes and `packages/db` for the Prisma probes (Bun 1.3.x here; the repo pins 1.4.4 and its `undici` import needs the preload below on 1.3.x).

```bash
# 1. Watch db 11 while a probe runs (MONITOR shows every command with its arguments, bytes as \xNN)
redis-cli -h 127.0.0.1 -p 23379 monitor > monitor.txt &     # stop it with: pkill -f 'redis-cli.*monitor'
# 2. Redis / BullMQ probes
cd <repo>/packages/core && REDIS_URL=redis://127.0.0.1:23379/11 bun --preload ./preload.ts ./p1-probe.ts
# 3. Prisma probes (reads on the dev DB; writes in a rolled-back transaction)
cd <repo>/packages/db && DATABASE_URL='postgresql://postgres:postgres@localhost:23432/postgres?schema=public' bun ./prisma-probe.ts
# 4. Clean up. Only when db 11 was empty before you started (other work may use indexes 11-15):
redis-cli -h 127.0.0.1 -p 23379 -n 11 dbsize        # must have been 0 before step 2
redis-cli -h 127.0.0.1 -p 23379 -n 11 --scan | xargs -n1 redis-cli -h 127.0.0.1 -p 23379 -n 11 del
redis-cli -h 127.0.0.1 -p 23379 -n 11 dbsize        # 0 again
```

To decode `ARGV1/ARGV3` of the BullMQ script call, take the `EVAL`/`EVALSHA` arguments 13 and 15 (`ARGV1`, `ARGV3`; the \xNN escapes are raw bytes) and run them through `msgpackr` (`unpackMultiple` for `ARGV1`, `unpack` for `ARGV3`), which ships inside the `bullmq` install.

**preload.ts** — stubs the `undici/index.js` import that `packages/shared/src/server/safe-fetch.ts` makes (only needed to import `incoming-event-handler` and `notification-dispatch` on Bun 1.3.x):
```ts
Bun.plugin({
  name: 'wparity-undici-stub',
  setup(build) {
    build.module('undici/index.js', () => ({ exports: { Agent: class {}, fetch: globalThis.fetch }, loader: 'object' }));
  },
});
```

**p1-probe.ts** — drives the real `EventBuffer`, `SessionBuffer`, `ProfileBuffer`, `AiEventBuffer`, `createEvent`, `incomingEvent`, `checkNotificationRulesForEvent`, the real `createProducers` with the real `notificationQueueJobs` schema (the `sessions` queue uses the same `z.custom` shape as `session.jobs.ts`), and `createDeadLetterRecorder`; only Postgres (a stub `db` returning a fixed project and one rule), the project-filter lookups and `markFirstEvent` are stubbed. One message per scenario, each bracketed by `ECHO ### BEGIN/END <name>` so `MONITOR` output can be cut per scenario:
```ts
import { createProducers } from '<repo>/packages/core/src/jobs/producers.ts';
import { defineQueue, defineJob } from '<repo>/packages/core/src/jobs/define.ts';
import { z } from 'zod';
import { notificationQueueJobs } from '<repo>/packages/core/src/modules/notification/notification.jobs.ts';
import { sessionEndJobPayload, sessionEndEnqueueOptions } from '<repo>/packages/core/src/modules/session/src/session-end.ts';
import { getRedisQueue, getRedisCache } from '<repo>/packages/redis/redis.ts';
import { createDeadLetterRecorder } from '<repo>/packages/redis/dead-letter.ts';
import { EventBuffer } from '<repo>/packages/core/src/buffers/event-buffer.ts';
import { SessionBuffer } from '<repo>/packages/core/src/buffers/session-buffer.ts';
import { ProfileBuffer } from '<repo>/packages/core/src/buffers/profile-buffer.ts';
import { AiEventBuffer } from '<repo>/packages/core/src/buffers/ai-event-buffer.ts';
import { createEvent } from '<repo>/packages/core/src/modules/event/event.service.ts';
import { incomingEvent } from '<repo>/packages/core/src/modules/ingest/src/incoming-event-handler.ts';
import { checkNotificationRulesForEvent } from '<repo>/packages/core/src/modules/notification/src/notification-dispatch.ts';

const silent: any = { info() {}, warn() {}, error(...a: any[]) { console.log('LOGERR', JSON.stringify(a).slice(0, 300)); }, debug() {}, fatal() {}, child() { return silent; } };
const config: any = { buffers: { event: {}, profile: {}, session: { squash: true }, aiEvent: {}, bot: {}, group: {}, replay: {}, profileBackfill: {} }, session: {} };
const bufferDeps: any = { createLogger: () => silent, ch: {}, isCronPaused: async () => false, config };
const eventBuf = new EventBuffer(bufferDeps);
const buffers: any = { event: eventBuf.asRedeliverable(), profile: new ProfileBuffer(bufferDeps), session: new SessionBuffer(bufferDeps), aiEvent: new AiEventBuffer(bufferDeps) };
const redis = getRedisCache();

const sessionEndWire = z.custom((v: any) => true).transform((v) => v as any);
const sessions = defineQueue('sessions', { defaults: { removeOnComplete: true, removeOnFail: { age: 86400, count: 1000 } }, jobs: { session: defineJob({ payload: sessionEndWire as any, handler: async () => {} }) } });
const notification = defineQueue('notification', { defaults: { removeOnComplete: 10, removeOnFail: { age: 86400, count: 100 } }, jobs: { ...notificationQueueJobs } });
const handle = createProducers({ sessions, notification } as any, { connection: getRedisQueue(), namespace: 'wparity' });

const RULES = [
  { id: '00000000-0000-0000-0000-0000000000a1', name: 'Purchases', sendToApp: true, sendToEmail: true, config: { type: 'events', events: [{ name: 'purchase', segment: 'event', filters: [{ name: 'properties.value', operator: 'gt', value: ['10'] }] }] }, template: 'Bought $EVENT_NAME by {{profileId}}', integrations: [{ id: '00000000-0000-0000-0000-0000000000b1' }] },
];
const notifyRows: any[] = [];
const db: any = {
  project: { findUnique: async ({ where }: any) => ({ id: where.id, name: 'WParity Project', organizationId: 'wparity-org', eventsCount: 0, types: [], domain: null, cors: [], crossDomain: false, allowUnsafeRevenueTracking: false, aiStoreContent: true, filters: [], firstEventAt: null, noDataNotifiedAt: null, dataStoppedNotifiedAt: null, deleteAt: null, createdAt: new Date('2026-01-01T00:00:00Z'), updatedAt: new Date('2026-01-01T00:00:00Z') }), updateMany: async (a: any) => { console.log('PRISMA project.updateMany', JSON.stringify(a)); return { count: 1 }; } },
  notificationRule: { findMany: async () => RULES },
  notification: { create: async (a: any) => { notifyRows.push(a); return { id: 'x', ...a.data }; } },
};
const mark = (s: string) => redis.echo('### ' + s);

function ctxFor(requestId: string) {
  return { db, buffers, logger: silent, queues: handle.scope({ requestId }), config: {}, redis, prisma: {}, ch: {}, requestId } as any;
}
function depsFor(requestId: string, excludeFilters = false) {
  const ctx = ctxFor(requestId);
  return {
    logger: silent,
    sessions: buffers.session,
    createEvent: (p: any) => createEvent(ctx, p),
    checkNotificationRulesForEvent: (p: any) => checkNotificationRulesForEvent(ctx, p),
    projects: {
      getCached: async () => ({ firstEventAt: null, filters: [] }),
      markFirstEvent: async (id: string) => { console.log('markFirstEvent', id); },
      getAiStoreContent: async () => true,
    },
    enqueueSessionEnd: (input: any) => ctx.queues.sessions.session.add(sessionEndJobPayload(input), sessionEndEnqueueOptions(input.closedSession.id)),
    metrics: { sessionStarted() {}, sessionEndEnqueued() {} },
    aiEvents: { add: (row: any) => buffers.aiEvent.add(row) },
  } as any;
}
const uaClient = { isServer: false, os: 'Mac OS', osVersion: '10.15.7', browser: 'Chrome', browserVersion: '128.0', device: 'desktop', brand: 'Apple', model: 'Mac' };
const uaServer = { ...uaClient, isServer: true, device: 'server', browser: '', browserVersion: '' };
const geo = { city: 'London', country: 'GB', region: 'ENG', longitude: -0.1, latitude: 51.5 };
function payload(o: any) {
  return { id: o.id, projectId: o.projectId, event: { name: o.name, timestamp: o.ts, isTimestampFromThePast: !!o.past, properties: o.properties ?? {}, profileId: o.profileId, groups: o.groups }, uaInfo: o.ua ?? uaClient, geo, headers: { 'openpanel-sdk-name': 'web', 'openpanel-sdk-version': '1.0.0', 'request-id': 'wparity-req' }, deviceId: o.deviceId, sessionId: o.sessionId, ai: o.ai };
}
async function run(label: string, p: any) {
  await mark('BEGIN ' + label);
  const since = (eventBuf as any).lastQueuedSeq;
  const gate = eventBuf.openDurabilityWindow();
  const res = await incomingEvent(p, depsFor('wparity-req'), { partition: 0, offset: '1' });
  await gate();
  await mark('END ' + label);
  return res;
}
const T0 = Date.parse('2026-01-01T00:00:00.000Z');
const P = 'wparity-proj';
await run('S3 client new', payload({ id: 'wparity-ev-1', projectId: P, name: 'screen_view', ts: new Date(T0).toISOString(), properties: { __path: 'https://example.com/pricing?utm_source=google&x=1#top', __referrer: 'https://www.google.com/', plan: 'pro', n: 5, nested: { a: { b: 1 }, arr: [1, 'x'] } }, profileId: 'wparity-user', deviceId: 'wparity-dev', sessionId: 'wparity-sess-1' }));
await run('S4 client extend', payload({ id: 'wparity-ev-2', projectId: P, name: 'purchase', ts: new Date(T0 + 60000).toISOString(), properties: { __path: '/order', value: '99', __revenue: 99 }, profileId: 'wparity-user', deviceId: 'wparity-dev', sessionId: 'wparity-sess-1' }));
await run('S2 server event with live session', payload({ id: 'wparity-ev-3', projectId: P, name: 'server_thing', ts: new Date(T0 + 120000).toISOString(), properties: { k: 'v' }, profileId: 'wparity-user', deviceId: 'wparity-srv', sessionId: 'wparity-sess-srv', ua: uaServer }));
await run('S1 server event no session', payload({ id: 'wparity-ev-4', projectId: P, name: 'server_thing', ts: new Date(T0 + 120000).toISOString(), properties: { k: 'v' }, profileId: 'wparity-nouser', deviceId: 'wparity-srv2', sessionId: 'wparity-sess-srv2', ua: uaServer }));
await run('S6 past event', payload({ id: 'wparity-ev-5', projectId: P, name: 'old', ts: new Date(T0 - 86400000).toISOString(), past: true, properties: {}, profileId: 'wparity-user', deviceId: 'wparity-dev', sessionId: 'wparity-sess-1' }));
await run('S5 client boundary', payload({ id: 'wparity-ev-6', projectId: P, name: 'screen_view', ts: new Date(T0 + 31 * 60000 + 60000).toISOString(), properties: { __path: '/again' }, profileId: 'wparity-user', deviceId: 'wparity-dev', sessionId: 'wparity-sess-2' }));
await run('S7 ai event', payload({ id: 'wparity-ev-7', projectId: P, name: 'ai.generation', ts: new Date(T0 + 200000).toISOString(), properties: {}, profileId: 'wparity-user', deviceId: 'wparity-dev', sessionId: 'wparity-sess-2', ua: uaServer, ai: { type: 'generation', trace_id: 't1', span_id: '', parent_id: '', conversation_id: '', name: 'n', model: 'm', provider: 'p', input_tokens: 1, output_tokens: 2, cache_read_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0, input_cost: 0.1, output_cost: 0.2, total_cost: 0.3, cost_source: 'table', latency_ms: 5, time_to_first_token_ms: 0, is_error: false, http_status: 200, stop_reason: 'stop', is_stream: false, model_parameters: '{}', input: 'IN', output: 'OUT', tools: '[]', error: '', properties: { a: 'b' } } }));
await run('S8 notification rule', payload({ id: 'wparity-ev-8', projectId: P, name: 'purchase', ts: new Date(T0 + 130000).toISOString(), properties: { __path: '/order', value: '99', __revenue: 99 }, profileId: 'wparity-user', deviceId: 'wparity-dev', sessionId: 'wparity-sess-2' }));
await mark('BEGIN S9 dead letter');
await createDeadLetterRecorder({ client: redis as any, maxEntries: 1000, key: 'dead_letter:events' })({ key: Buffer.from('wparity-dev'), value: Buffer.from('{"bad":'), headers: { 'x-h': Buffer.from('hv') }, topic: 'events', partition: 3, offset: '42', reason: 'parse_error', error: 'Unexpected end' });
await mark('END S9 dead letter');
await sleep(300);
console.log('NOTIFY_ROWS', JSON.stringify(notifyRows));
await handle.close();
process.exit(0);
function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }
```

**prisma-probe.ts** — the statements of P1.7 (reads real; writes inside a `$transaction` that throws to roll back):
```ts
import { PrismaClient } from '<repo>/packages/db/src/generated/prisma/client';
import { createPostgresAdapter } from '<repo>/packages/db/src/postgres-adapter';
const adapter = createPostgresAdapter(process.env.DATABASE_URL!, (e: unknown) => console.error('pgerr', e));
const client = new PrismaClient({ adapter, log: [{ emit: 'event', level: 'query' }] } as any);
(client as any).$on('query', (e: any) => console.log('SQL:', e.query, '| params:', e.params));
const PID = "acme-web";
console.log('-- project.findUnique');
await client.project.findUnique({ where: { id: PID } });
console.log('-- project aiStoreContent');
await client.project.findUnique({ where: { id: PID }, select: { aiStoreContent: true } });
console.log('-- notificationRule.findMany');
await client.notificationRule.findMany({ where: { projectId: PID }, select: { id: true, name: true, sendToApp: true, sendToEmail: true, config: true, template: true, integrations: { select: { id: true } } } });
console.log('-- project.updateMany (rolled back)');
try { await client.$transaction(async (tx: any) => { await tx.project.updateMany({ where: { id: 'wparity-none', firstEventAt: null }, data: { firstEventAt: new Date() } }); throw new Error('rollback'); }); } catch (e: any) { console.log('rolled back:', e.message); }
console.log('-- notification.create (rolled back)');
try { await client.$transaction(async (tx: any) => { await tx.notification.create({ data: { title: 't', message: 'm', projectId: PID, payload: { type: 'event' }, sendToApp: true, sendToEmail: false, integrationId: null, notificationRuleId: undefined } }); throw new Error('rollback'); }); } catch (e: any) { console.log('rolled back:', e.message); }
await client.$disconnect();
process.exit(0);
```

**Matcher check** (`TypeError` on non-string values, P1.10.1), run from `packages/core` with the same preload:
```ts
import { matchEvent } from '<repo>/packages/core/src/modules/notification/notification.service.ts';
const base: any = { name: 'purchase', profileId: 'p', deviceId: 'd', createdAt: new Date(0), properties: { value: 5, s: ' x ', arr: ['a'], obj: { a: 1 }, flag: true, __query: {} }, path: '/a' };
const t = (label: string, f: any) => { try { console.log(label, matchEvent(base, { name: 'purchase', filters: [f] } as any)); } catch (e: any) { console.log(label, 'THROWS', e.constructor.name, e.message); } };
t('number is', { name: 'properties.value', operator: 'is', value: ['5'] });
t('number gt', { name: 'properties.value', operator: 'gt', value: ['1'] });
t('string trim', { name: 'properties.s', operator: 'is', value: ['x'] });
t('bool', { name: 'properties.flag', operator: 'isNotNull', value: ['x'] });
t('array', { name: 'properties.arr', operator: 'is', value: ['a'] });
t('missing', { name: 'properties.zzz', operator: 'isNull', value: ['x'] });
t('createdAt', { name: 'createdAt', operator: 'isNotNull', value: ['x'] });
t('empty value array', { name: 'properties.value', operator: 'is', value: [] });
t('has_profile true', { name: 'has_profile', operator: 'is', value: ['true'] });
t('unknown op', { name: 'path', operator: 'zzz', value: ['x'] });
t('gt non-numeric', { name: 'path', operator: 'gt', value: ['0'] });
t('contains num val', { name: 'path', operator: 'contains', value: [1, null, true] });
t('regex', { name: 'path', operator: 'regex', value: ['/^\\/a$/'] });
t('regex bad', { name: 'path', operator: 'regex', value: ['(?=a)'] });
t('is with number in value', { name: 'properties.s', operator: 'is', value: [5] });
process.exit(0);
```

**p1b-probe.ts** — the reruns behind P1.17 #2 and #9. It starts with the **same setup as `p1-probe.ts` up to (not including) `const T0 =`** and continues with:
```ts
const T0 =Date.parse('2026-01-01T00:00:00.000Z');
const P = 'wparity-proj';
const msg = (id: string, dev: string, sess: string, ts: number, name = 'screen_view') => payload({ id, projectId: P, name, ts: new Date(ts).toISOString(), properties: { __path: '/a' }, profileId: 'wparity-user2', deviceId: dev, sessionId: sess });
const rows = async () => (await redis.lrange('event_buffer:queue', 0, -1)).map((l) => { const o = JSON.parse(l); return o.name + ':' + o.id.slice(0, 12); });
const sess = async (dev: string) => { const s = JSON.parse((await redis.get('session:' + P + ':' + dev)) as string); return { ev: s.event_count, sv: s.screen_view_count, version: s.version }; };
const consumerLike = async (label: string, p: any) => {
  const gate = eventBuf.openDurabilityWindow();
  let handlerErr = '';
  try { await incomingEvent(p, depsFor('wparity-req'), { partition: 0, offset: '1' }); } catch (e: any) { handlerErr = e.constructor.name + ': ' + String(e.message).slice(0, 60); }
  let gateErr = '';
  try { await gate(); } catch (e: any) { gateErr = String(e.message).slice(0, 60); }
  console.log(label, JSON.stringify({ handlerErr, gateErr, list: await rows() }));
};

console.log('--- #2a: same message handled twice (handler retry)');
await consumerLike('run1', msg('wparity-r1', 'wparity-dev2', 'wparity-sess-a', T0));
console.log('session after run1', JSON.stringify(await sess('wparity-dev2')));
await consumerLike('run2', msg('wparity-r1', 'wparity-dev2', 'wparity-sess-a', T0));
console.log('session after run2', JSON.stringify(await sess('wparity-dev2')));

console.log('--- #2b: failed durability write, then redelivery');
await redis.del('event_buffer:queue');
const realRpush = redis.rpush.bind(redis);
let fail = true;
(redis as any).rpush = (...a: any[]) => (fail && a[0] === 'event_buffer:queue' ? Promise.reject(new Error('simulated rpush failure')) : (realRpush as any)(...a));
await consumerLike('run1 (rpush fails)', msg('wparity-r2', 'wparity-dev3', 'wparity-sess-b', T0));
console.log('session after failed run', JSON.stringify(await sess('wparity-dev3')));
fail = false;
await consumerLike('redelivery', msg('wparity-r2', 'wparity-dev3', 'wparity-sess-b', T0));
console.log('session after redelivery', JSON.stringify(await sess('wparity-dev3')));

console.log('--- #9: record without headers / non-string request-id');
const noHeaders = msg('wparity-r3', 'wparity-dev4', 'wparity-sess-c', T0); delete (noHeaders as any).headers;
try { await incomingEvent(noHeaders as any, depsFor('x'), { partition: 0, offset: '1' }); console.log('no headers: no error'); } catch (e: any) { console.log('no headers:', e.constructor.name, e.message); }
const { resolveRequestId } = await import('<repo>/packages/core/src/shared/request-id.ts');
try { console.log('numeric request-id ->', resolveRequestId(5 as any)); } catch (e: any) { console.log('numeric request-id:', e.constructor.name, e.message); }
console.log('missing request-id ->', resolveRequestId(undefined).length, 'chars; "!!!" ->', resolveRequestId('!!!').length, 'chars; "a b/c" ->', resolveRequestId('a b/c'));
await sleep(100);
await handle.close();
process.exit(0);
function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }
```

**bull-probe.ts** — the `sessions` add, the duplicate-id path and a plain `notification` add against `createProducers` (`MONITOR` output of this run is where the `duplicated` event and the msgpack arguments in P1.9 come from):
```ts
import { createProducers } from '<repo>/packages/core/src/jobs/producers.ts';
import { defineQueue, defineJob } from '<repo>/packages/core/src/jobs/define.ts';
import { sessionEndJobPayload, sessionEndEnqueueOptions } from '<repo>/packages/core/src/modules/session/src/session-end.ts';
import { getRedisQueue } from '<repo>/packages/redis/redis.ts';
import { z } from 'zod';
const sessions = defineQueue('sessions', { defaults: { removeOnComplete: true, removeOnFail: { age: 86400, count: 1000 } }, jobs: { session: defineJob({ payload: z.any(), handler: async () => {} }) } });
const notification = defineQueue('notification', { defaults: { removeOnComplete: 10, removeOnFail: { age: 86400, count: 100 } }, jobs: { sendNotification: defineJob({ payload: z.any(), handler: async () => {} }) } });
const handle = createProducers({ sessions, notification } as any, { connection: getRedisQueue(), namespace: 'wparity' });
const scoped: any = handle.scope({ requestId: 'wparity-req-1' });
const closed = { id:'sess1', project_id:'p1', device_id:'d1', profile_id:'d1', ended_at:'2026-01-01 00:00:00.000' };
const payload = { name:'screen_view', profileId:'d1', projectId:'p1', deviceId:'d1', sessionId:'s2', properties:{a:'b'}, createdAt:new Date('2026-01-01T00:00:00.123Z'), groups:[], duration:0, path:'/x', origin:'', referrer:undefined, referrerName:undefined, referrerType:undefined, sdkName:undefined, sdkVersion:undefined } as any;
const a = await scoped.sessions.session.add(sessionEndJobPayload({ payload, closedSession: closed as any }), sessionEndEnqueueOptions('sess1'));
const a2 = await scoped.sessions.session.add(sessionEndJobPayload({ payload, closedSession: closed as any }), sessionEndEnqueueOptions('sess1'));
const b = await scoped.notification.sendNotification.add({ notification: { title:'t', message:'m', projectId:'p1', payload:{type:'event', event: payload}, integrationId:null, sendToApp:true, sendToEmail:false, notificationRuleId:'r1' } as any });
console.log(JSON.stringify({a,a2,b}));
await handle.close();
process.exit(0);
```

**prisma-notif.ts** — the `notifications` INSERT with a rule id (column order, `payload` as JSON text, a `Date` inside it), rolled back:
```ts
import { PrismaClient } from '<repo>/packages/db/src/generated/prisma/client';
import { createPostgresAdapter } from '<repo>/packages/db/src/postgres-adapter';
const adapter = createPostgresAdapter(process.env.DATABASE_URL!, (e: unknown) => console.error('pgerr', e));
const client: any = new PrismaClient({ adapter, log: [{ emit: 'event', level: 'query' }] } as any);
client.$on('query', (e: any) => console.log('SQL:', e.query.replace(/RETURNING.*/, 'RETURNING …'), '| params:', e.params));
try {
  await client.$transaction(async (tx: any) => {
    const rule = await tx.notificationRule.create({ data: { name: 'wparity-rule', projectId: 'acme-web', sendToApp: true, config: { type: 'events', events: [] } } });
    console.log('-- create with rule id, payload with Date and nul char');
    await tx.notification.create({ data: { title: 't', message: 'm', projectId: 'acme-web', payload: { type: 'event', event: { createdAt: new Date('2026-01-01T00:00:00.123Z'), a: undefined, b: null } }, integrationId: null, sendToApp: true, sendToEmail: false, notificationRuleId: rule.id } });
    throw new Error('rollback');
  });
} catch (e: any) { console.log('done:', e.message); }
await client.$disconnect(); process.exit(0);
```

**prisma-rule.ts** — creates a rule inside a transaction, reads it back with the exact `select` of `getNotificationRulesByProjectId` (this is where the `jsonb` key order and the real `JSON.stringify` of the result come from), reads a project row, rolls back:
```ts
import { PrismaClient } from '<repo>/packages/db/src/generated/prisma/client';
import { createPostgresAdapter } from '<repo>/packages/db/src/postgres-adapter';
const adapter = createPostgresAdapter(process.env.DATABASE_URL!, (e: unknown) => console.error('pgerr', e));
const client: any = new PrismaClient({ adapter } as any);
try {
  await client.$transaction(async (tx: any) => {
    const rule = await tx.notificationRule.create({ data: { name: 'wparity-rule', projectId: 'acme-web', sendToApp: true, config: { type: 'events', events: [{ id: 'a', name: 'purchase', segment: 'event', filters: [{ id: 'f', name: 'properties.value', operator: 'gt', value: ['10'] }] }] }, template: null } });
    const rows = await tx.notificationRule.findMany({ where: { projectId: 'acme-web' }, select: { id: true, name: true, sendToApp: true, sendToEmail: true, config: true, template: true, integrations: { select: { id: true } } } });
    console.log(JSON.stringify(rows));
    const p = await tx.project.findUnique({ where: { id: 'acme-web' } });
    console.log(JSON.stringify(p));
    throw new Error('rollback');
  });
} catch (e: any) { console.log('done:', e.message); }
await client.$disconnect(); process.exit(0);
```

**Decoding `MONITOR` and the BullMQ msgpack arguments** (`msgpack-decode.ts`). The `MONITOR` line parser used to split a line into its quoted arguments (`\xNN` ⇒ byte; apply it to lines containing `[11 `):
```ts
function parseMon(line: string): string[] | null {
  const m = line.match(/^\S+ \[(\d+) [^\]]+\] (.*)$/s);
  if (!m) return null;
  const s = m[2]!, out: string[] = [];
  let i = 0;
  while (i < s.length) {
    if (s[i] === ' ') { i++; continue; }
    if (s[i] !== '"') { i++; continue; }
    i++;
    let v = '';
    while (i < s.length && s[i] !== '"') {
      if (s[i] === '\\') {
        const c = s[i + 1];
        if (c === 'n') v += '\n'; else if (c === 't') v += '\t';
        else if (c === 'x') { v += String.fromCharCode(parseInt(s.substr(i + 2, 2), 16)); i += 2; }
        else v += c;
        i += 2;
      } else { v += s[i]; i++; }
    }
    i++;
    out.push(v);
  }
  return out;
}
// ARGV1 = parsed[12], ARGV3 = parsed[14] (0-based, parsed[0] is the command) -> latin1 bytes -> base64 -> the script below
```
```ts
import { unpackMultiple, unpack } from '<repo>/node_modules/.bun/bullmq@6.7.8+8637724a9e2a69ac/node_modules/bullmq/../msgpackr/index.js';
const items = JSON.parse(await Bun.file('/private/tmp/claude-502/-Volumes-Lexar2TB-Dev-openpanel/020ec628-1386-49c4-9c0d-b8c3ebae0df4/scratchpad/wparity-mp.json').text());
for (const it of items) console.log(JSON.stringify([unpackMultiple(Buffer.from(it.argv1,'base64')), unpack(Buffer.from(it.argv3,'base64'))]));
```
