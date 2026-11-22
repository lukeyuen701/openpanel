# Go worker contracts: buffer flushes (P2) and session lifecycle (P3)

This is the specification the Go port must match. It describes what the
TypeScript worker (`apps/api` with `ROLE=worker`, code in `packages/core`) does today,
read from the code and, where marked `[ran]`, checked by running something.
It complements `docs/GO_WORKER.md` (sections 2, P2 and P3), which is the plan;
this file is the behaviour. Where the TypeScript is arguably wrong, the item is
marked **HAZARD** and listed again in the closing "Hazards and open questions"
of each section: the port must decide to copy it or fix it, knowingly.

Snapshot: branch `feat/go-worker`, HEAD `933bc74af753` ("test(ingest): a differential rig for the events worker"). Working tree at generation: modified or untracked: `?? .agents/`. Citations refer to the working tree at that moment.

## Conventions

- `path:line` points at the line where the cited construct starts (`path:a-b` is a
  range). Every citation was produced by searching the file for an anchor
  string at generation time and then re-verified by a script, so a drifting
  line is detectable (see "How this was checked" at the end).
- **ND-clock** and **ND-random** mark values that differ between two runs of the
  same input (wall clock, random ids). A golden test must mask them (`inserted_at`,
  random event ids, lock ids, `Date.now()` scores) rather than compare them.
- `[ran]` means I executed it against the local stack (ClickHouse 26.3.67.30,
  Redis, Bun 1.3.11), not just read it. Everything else is read-only analysis.
- "Redis" means the cache client `getRedisCache()` (``packages/redis/redis.ts:146``):
  ioredis, fail-fast (no offline queue once connected), 5000 ms command
  timeout (`REDIS_CACHE_COMMAND_TIMEOUT_MS`), URL `REDIS_URL` (default
  `redis://localhost:23379`). Queue traffic uses a different client
  (`getRedisQueue`, BullMQ's blocking options).

---

# P2: buffer flushes

## P2.1 Inventory

Eight buffers, created once per process by `packages/core/src/buffers/create-buffers.ts:22`.
"Flush" below always means `BaseBuffer.tryFlush` (section P2.3) called from a
BullMQ cron job (section P2.2). Nothing else calls it.

| buffer (`name`) | Redis list key | flush lock key | cron job → interval | batch size (env) | chunk size | ClickHouse target | row source in Redis |
|---|---|---|---|---|---|---|---|
| event (`event`) | `event_buffer:queue` `packages/core/src/buffers/event-buffer.ts:112` | `lock:event` | `flushEvents` 10 s | 4000 (`EVENT_BUFFER_BATCH_SIZE`) | 1000 (`EVENT_BUFFER_CHUNK_SIZE`) | `events` | JSONEachRow line |
| session (`session`) | `session-buffer` `packages/core/src/buffers/session-buffer.ts:107` | `lock:session` | `flushSessions` 10 s | 10000 (`SESSION_BUFFER_BATCH_SIZE`) | = batch (`SESSION_BUFFER_CHUNK_SIZE`) | `sessions` | JSON object, squashed |
| profile (`profile`) | `profile-buffer` `packages/core/src/buffers/profile-buffer.ts:59` | `lock:profile` | `flushProfiles` 10 s | 200 (`PROFILE_BUFFER_BATCH_SIZE`) | 1000 (`PROFILE_BUFFER_CHUNK_SIZE`) | `profiles` | JSON object, merged with existing |
| profile-backfill (`profile-backfill`) | `profile-backfill-buffer` `packages/core/src/buffers/profile-backfill-buffer.ts:22` | `lock:profile-backfill` | `flushProfileBackfill` 30 s | 1000 (`PROFILE_BACKFILL_BUFFER_BATCH_SIZE`) | 500 per UPDATE (constant) | `UPDATE events` (no insert) | JSON entry |
| bot (`bot`) | `bot-events-buffer` `packages/core/src/buffers/bot-buffer.ts:12` | `lock:bot` | `flushBots` 30 s | 1000 (`BOT_BUFFER_BATCH_SIZE`) | none (one insert) | `events_bots` | JSONEachRow line |
| replay (`replay`) | `replay-buffer` `packages/core/src/buffers/replay-buffer.ts:34` | `lock:replay` | `flushReplay` 10 s | 500 + 8 MiB cap (`REPLAY_BUFFER_BATCH_SIZE`) | 500 (`REPLAY_BUFFER_CHUNK_SIZE`) | `session_replay_chunks` | JSONEachRow line |
| group (`group`) | `group-buffer` `packages/core/src/buffers/group-buffer.ts:49` | `lock:group` | `flushGroups` 10 s | 200 (`GROUP_BUFFER_BATCH_SIZE`) | 1000 (`GROUP_BUFFER_CHUNK_SIZE`) | `groups` | JSONEachRow line |
| ai-event (`ai-event`) | `ai-event-buffer` `packages/core/src/buffers/ai-event-buffer.ts:18` | `lock:ai-event` | `flushAiEvents` 10 s | 1000 (`AI_EVENT_BUFFER_BATCH_SIZE`) | 500 (`AI_EVENT_BUFFER_CHUNK_SIZE`) | `ai_events` | JSONEachRow line |

Lock key = `lock:` + buffer name (`packages/core/src/buffers/base-buffer.ts:212`). Defaults and env
names: batch/chunk constants at the top of each buffer file; env parsing in
section P2.8. Table names are the string literals in the buffer files
(`'events'` is a literal in `packages/core/src/buffers/event-buffer.ts:432`, the others come from
`TABLE_NAMES` in `packages/core/src/shared/ch-tables.ts:9`).

## P2.2 What triggers a flush

1. **Only the cron queue.** `tryFlush` is called from eight cron handlers and
   nowhere else; its `trigger` argument is therefore always `'cron'` (the
   `'add'` value in `packages/core/src/buffers/base-buffer.ts:94` is vestigial: no producer path flushes any more;
   `enableParallelProcessing` is never set, so the branch at
   `packages/core/src/buffers/base-buffer.ts:569` is dead).
2. **Schedules** (BullMQ job schedulers on the `cron` queue, scheduler id = job
   name, `{ every: ms }`, payload `{"payload":null,"meta":{}}` from
   `packages/core/src/jobs/schedulers.ts:192`):

   | job id | interval | handler |
   |---|---|---|
   | `flushEvents` | 10 000 ms `packages/core/src/modules/event/event.jobs.ts:7` | `packages/core/src/modules/event/event.jobs.ts:16` |
   | `flushBots` | 30 000 ms `packages/core/src/modules/event/event.jobs.ts:8` | `packages/core/src/modules/event/event.jobs.ts:23` |
   | `flushProfiles` | 10 000 ms `packages/core/src/modules/profile/profile.jobs.ts:8` | `packages/core/src/modules/profile/profile.jobs.ts:17` |
   | `flushProfileBackfill` | 30 000 ms `packages/core/src/modules/profile/profile.jobs.ts:9` | `packages/core/src/modules/profile/profile.jobs.ts:24` |
   | `flushGroups` | 10 000 ms `packages/core/src/modules/group/group.jobs.ts:6` | `packages/core/src/modules/group/group.jobs.ts:14` |
   | `flushAiEvents` | 10 000 ms `packages/core/src/modules/ai/ai.jobs.ts:6` | `packages/core/src/modules/ai/ai.jobs.ts:14` |
   | `flushSessions` | 10 000 ms `packages/core/src/modules/session/session.jobs.ts:27` | `packages/core/src/modules/session/session.jobs.ts:113` |
   | `flushReplay` | 10 000 ms `packages/core/src/modules/session/session.jobs.ts:28` | `packages/core/src/modules/session/session.jobs.ts:123` |

   The scheduler fires at wall-clock multiples of the interval: BullMQ computes
   `floor(now/every)*every + every` (bullmq 6.7.8, `getNextMillis` in
   `dist/esm/classes/job-scheduler.js`; library behaviour, not a repo fact).
   The Go port does not need BullMQ for this: a timer plus the same Redis
   lock (P2.3) is equivalent. Alignment to epoch multiples is not observable
   in any output.
3. **Queue settings that shape flush behaviour** (`packages/core/src/jobs.registry.ts:70`):
   `removeOnComplete: 10`, `removeOnFail: {age: 86400 s, count: 100}`, no retries
   (BullMQ default one attempt), worker concurrency 4 (`packages/core/src/jobs.registry.ts:44`, override `CRON_CONCURRENCY`,
   key rule `apps/api/src/config/env.ts:98`). With concurrency 4 and 8 flush jobs, two
   ticks of the same buffer can overlap (a slow flush plus the next tick);
   the per-process guard and the Redis lock turn the second into a no-op
   (`result="locked"`).
4. **The pause switch.** Before taking the lock, every flush asks
   `isCronPaused()` (`apps/api/src/main.ts:161`): BullMQ `Queue.isPaused()` on the `cron` queue.
   In bullmq 6.7.8 that is `hasQueueMetaField('paused')` (`dist/esm/classes/queue.js:304`),
   i.e. `HEXISTS bull:<queueKey>:meta paused`. `[ran]` locally the key is
   `bull:cron:meta` with fields `opts.maxLenEvents`, `version` and no `paused` field
   while running. `<queueKey>` is `cron`, `cron-<QUEUE_NAMESPACE>` when a namespace
   is set, and `{cron}` / `{cron-<ns>}` under `QUEUE_CLUSTER`
   (`packages/core/src/jobs/naming.ts:20`); the namespaced/braced forms were read, not run.
   Pausing `cron` in bull-board therefore halts **all** flushing and all other
   scheduled jobs; the code calls this an accepted trade-off
   (`packages/core/src/buffers/base-buffer.ts:25`). A paused flush emits `result="paused"`
   and returns. The Go worker must honour the same flag, or "pause cron" stops
   meaning what operators think it means.
5. **Handing a flush to another process.** `DISABLED_CRON_JOBS` (comma list of
   cron job ids, unknown names fail boot) makes this process complete the
   named jobs without running them while their schedulers stay registered
   (`apps/api/src/config/env.ts:352`, `packages/core/src/jobs/workers.ts:140`, applied only to the cron
   queue at `apps/api/src/main.ts:221`). At the snapshot this exists in the repository;
   it is the switch `GO_WORKER.md` P2 asks for. Mutual exclusion between a TS
   flush and a Go flush is the `lock:<name>` key and nothing else, so the Go
   worker **must** use the same key and the same Lua (below).
6. **Shutdown.** SIGTERM waits up to 60 s (500 ms poll) for the active cron
   jobs (`apps/api/src/main.ts:97`), then closes workers; a force-exit timer of
   `SHUTDOWN_FORCE_EXIT_MS` (default 20 000, `apps/api/src/config/env.ts:26`) is armed first. The event
   buffer's in-process micro-batch is flushed to Redis on the way out
   (`apps/api/src/main.ts:624`). No buffer is flushed to ClickHouse on shutdown: what
   is in Redis stays in Redis for the next process.


## P2.3 The flush procedure (`BaseBuffer`)

All eight buffers share one procedure; only `onFlush` differs. Steps in order
(`tryFlush` `packages/core/src/buffers/base-buffer.ts:545`, `flushUnderLock` `packages/core/src/buffers/base-buffer.ts:621`):

1. `startedAt` = monotonic clock. `llenAtStart` = `LLEN <list key>` (errors
   swallowed, value then absent from the observation).
2. `isCronPaused()` (above). Paused → observation `paused`, return. An error
   here propagates and **fails the cron job** (nothing catches it).
3. Per-process guard `flushInFlight`: if this process is already flushing this
   buffer, observation `locked`, return.
4. **Acquire**: `SET lock:<name> <lockId> EX 60 NX`. `lockId` = `lock_` + 18 chars
   of nanoid (`packages/shared/src/id.ts:22`, **ND-random**). Not acquired (reply ≠ `OK`) →
   observation `locked`, return. A Redis error here propagates (job fails).
5. `lockLost = false`; start the keepalive: every `lockTimeout*1000/3` = **20 000 ms**
   (`packages/core/src/buffers/base-buffer.ts:53`) run the extend script (below) with TTL 60. Reply ≠ 1 (key
   expired or someone else owns it) → `lockLost = true`, error log. A thrown
   error from the call → `lockLost = true` too (ownership unknown, keeps
   renewing for the batch in flight). `lockLost` is only looked at between
   batches (`drainBatches`); the batch in flight is never cancelled.
6. Run `onFlush` (the buffer-specific part, P2.5). Success → observation
   `success`. **Any error thrown by `onFlush` is caught** and becomes observation
   `error` (logged at error level); the cron job still completes successfully
   and the rows stay in Redis for the next tick (10 s or 30 s later). There is no
   backoff, no attempt counter and no dead-letter.
7. `finally`: stop the keepalive, run the release script. An error from the
   release call propagates and fails the cron job (the lock then expires on
   its own within 60 s).

Lock scripts, verbatim from `packages/core/src/buffers/base-buffer.ts:55` and `packages/core/src/buffers/base-buffer.ts:63` (`KEYS[1]` = lock key,
`ARGV[1]` = lockId, `ARGV[2]` = TTL seconds as a string):

```lua
-- release (KEYS[1], ARGV[1])
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
```

```lua
-- extend (KEYS[1], ARGV[1], ARGV[2])
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("expire", KEYS[1], ARGV[2])
else
  return 0
end
```

Scripts are sent as `EVALSHA <sha1 of the exact text>`, falling back to `EVAL` on
a reply starting with `NOSCRIPT` (`packages/core/src/buffers/redis-script.ts:8`). Any other error propagates.
Go must use the same script text (the text is part of the contract, not just
the effect: a mixed TS/Go fleet should hash to one script).

**How long a flush runs.** `drainBatches` (`packages/core/src/buffers/base-buffer.ts:383`) loops "read a batch, insert,
trim" while `now < start + 8000 ms` (`packages/core/src/buffers/base-buffer.ts:46`) and `lockLost` is false, and stops
as soon as a batch comes back smaller than the batch size. The deadline is
checked **before** a batch starts, so a run lasts up to 8 s plus one batch. A
batch has no overall timeout: it is bounded by Redis 5 s per command and, per
ClickHouse attempt, the 30 s request timeout, up to three attempts (P2.4).
Typical run: milliseconds (empty list) to about 8–9 s (backlog). The lock TTL
(60 s) only matters after a crash or hang, because a live flush renews it every
20 s. `profile-backfill` does not drain (one batch per tick); `replay` has its own
loop with the same 8 s budget (P2.5).

**Durability model.** Rows are removed from Redis (`LTRIM key n -1`) only after
the ClickHouse insert for the whole batch returned success. A failure anywhere
before that leaves the batch in place and it is retried whole by the next
tick: **at-least-once**. With several chunks per batch, chunks that already
landed are inserted again on retry (harmless for the replacing engines
`profiles` and `groups`; duplicate rows in `events`, `events_bots`,
`session_replay_chunks`, `ai_events`). `session` avoids this by default by using
one chunk per batch (comment at
`packages/core/src/buffers/session-buffer.ts:96`: a duplicated sign row corrupts the collapse).

**Concurrency inside a flush.** `parallelLimit` (`packages/core/src/buffers/base-buffer.ts:311`) runs the chunk inserts with at
most `BUFFER_CH_INSERT_CONCURRENCY` in flight (default 5, `packages/core/src/buffers/base-buffer.ts:43`). After
the first failure no new chunk starts; the call rejects with the first error
only after the running ones settled (so nothing writes after the lock is
released). Concurrency ≤ 1 or a single chunk runs sequentially. Result order
is preserved (irrelevant to ClickHouse).

**Observation per flush** (feeds metrics, P2.7, and a debug log line with the
same fields): `buffer, result (success|error|locked|paused), trigger, totalMs,
rowsProcessed, llenAtStart` and phase timings `lrangeMs, chFetchMs, transformMs,
chInsertMs, trimMs, onFlushMs` (`packages/core/src/buffers/base-buffer.ts:96`). `rowsProcessed` is the number of
Redis rows removed by `LTRIM` (raw count, not the post-transform insert count).

## P2.4 The ClickHouse client contract

The buffers use the boot scope's `ch`: `packages/db/src/clickhouse/client.ts:282`, a proxy whose `insert`, `command` and `query`
go through the round-robin retry wrapper; everything else falls to the first
node. Go must reproduce the wire-visible parts below (HTTP interface).

**Nodes.** `CLICKHOUSE_URL` is one URL or a comma-separated list; the database
is the URL path (the Node client reads it from there). One client per URL,
picked round-robin (`packages/db/src/clickhouse/round-robin.ts:203`). A node that fails with a
connection-level error is skipped for `CLICKHOUSE_UNHEALTHY_MARK_MS` (default
5000, min 0); if every node is marked, the next one is tried anyway.

**Retry loop** (`packages/db/src/clickhouse/round-robin.ts:294`): attempts = `max(3, nodes+1)` (3 for one or two
nodes); on failure, delay `min(200 * 2^attempt, 2000)` ms (200, 400) then the next
node from the picker. Error classes (`packages/db/src/clickhouse/round-robin.ts:139`):
- `node-down` (`ECONNREFUSED, EHOSTUNREACH, EHOSTDOWN, ENETUNREACH, ETIMEDOUT, EAI_AGAIN, UND_ERR_CONNECT_TIMEOUT` and
  message forms): retry on the next node **and** mark the node unhealthy.
- `transient` (`ECONNRESET, EPIPE, UND_ERR_SOCKET, UND_ERR_*_TIMEOUT`, "socket hang
  up", "Premature close", header/body timeouts, and server error **202**
  `TOO_MANY_SIMULTANEOUS_QUERIES`): retry on the next node, node not marked.
- `ch-server` (message starts with `Code: <n>`, or `type` starts with `DB::`: SQL
  errors, `TOO_MANY_PARTS`, memory limit, timeouts reported by the server) and
  `other`: **no retry**, the error propagates (the flush fails, rows stay).

The two-node case is exercised by `packages/core/src/buffers/clickhouse-failover.test.ts:116` (first node a refused port,
`CLICKHOUSE_UNHEALTHY_MARK_MS=0`, one `ch.insert` of one row and one `chQuery`).

**HAZARD, reproduced `[ran]`: a retried one-shot stream insert loses its rows.** Fixed in TS by `0be53c2f` (each retry replays a fresh stream; regression test `packages/core/src/buffers/clickhouse-reset-retry.test.ts`); the Go client rebuilds its body on every attempt.
Event, bot, replay, group and ai-event inserts pass `values: Readable` built once
(`packages/core/src/buffers/base-buffer.ts:284`). The retry wrapper reuses the same `params` on the next node. If the
first node resets the connection after the body started to flow, the second
attempt sends an already-consumed stream and ClickHouse accepts an empty body:
the call **succeeds with zero rows**, and the buffer then trims the Redis rows.
Repro (scratch script, a TCP server that destroys the socket after the first
bytes as node 0, the real local ClickHouse as node 1, the repository's own
picker and retry code, 5 JSONEachRow lines): stream values → `rows landed = 0,
error = null`; array values (what session and profile pass) → `rows landed = 5`.
The failover test passes an array, which is why it does not see this. A refused
connection (`ECONNREFUSED`, nothing consumed) is safe. **Go must build the request
body again for every attempt** (keep the lines in memory and re-serialise), and a
chaos test should kill a node mid-insert. Whether production hits this depends
on how often a keep-alive socket resets mid-body; I did not measure that.

**Request** for `ch.insert` (Node client 1.25.0):
`POST /?query_id=<uuid, ND-random>&database=<db>&query=INSERT+INTO+<table>+FORMAT+JSONEachRow&<settings>`,
body = one JSON object per line, each line terminated by `\n` (the client appends
it to every string or object it is given), request body gzip-compressed
(`Content-Encoding: gzip`) unless `CLICKHOUSE_REQUEST_COMPRESSION` is `false` or `0`
(`packages/db/src/clickhouse/client.ts:122`). An object row is serialised with `JSON.stringify`; a string
is sent as is (`packages/db/src/clickhouse/client.ts:174`), so the Redis lines of event, bot,
replay, group and ai-event reach ClickHouse byte for byte.
Per-request timeout `CLICKHOUSE_REQUEST_TIMEOUT_MS` default 30 000 (min 1000,
`packages/db/src/clickhouse/client.ts:137`), pool `CLICKHOUSE_MAX_OPEN_CONNECTIONS` default 50, keep-alive idle
socket TTL 7000 ms (must stay under the server's 10 s).

**Settings on the wire**, lowest to highest precedence (client-level defaults,
then the per-call object; a per-call key wins):

| layer | settings |
|---|---|
| client default (`packages/db/src/clickhouse/client.ts:100`) | `distributed_product_mode=allow`, `date_time_input_format=best_effort`, `async_insert=0`, `query_plan_convert_any_join_to_semi_or_anti_join=0` (omitted if `CLICKHOUSE_SETTINGS_REMOVE_CONVERT_ANY_JOIN` is set), then every key of the JSON object in `CLICKHOUSE_SETTINGS` spread over all of these |
| insert defaults (`packages/db/src/clickhouse/client.ts:251`, **inserts only**, not `command` or `query`) | `max_execution_time=300`, `max_insert_block_size=500000`, `max_http_get_redirects=0`, `input_format_parallel_parsing=1`, `send_progress_in_http_headers=1`, `http_headers_progress_interval_ms=50000`, `wait_end_of_query=1` |
| per-buffer (`getClickhouseSettings()`, `packages/core/src/buffers/base-buffer.ts:182`) | only when `BUFFER_ASYNC_INSERTS` is set: `async_insert=1`, `wait_for_async_insert=0`, `parallel_view_processing=1`; otherwise nothing |

Which inserts use the per-buffer layer: event, profile, bot, replay, group,
ai-event: **yes**. session: **no** (its `ch.insert` has no `clickhouse_settings`,
`packages/core/src/buffers/session-buffer.ts:579`), so sessions are always synchronous. `profile-backfill` is a
`ch.command` with its own settings (P2.5).

Server facts `[ran]` on 26.3.67.30: the server default for `async_insert` is **1**
and for `wait_for_async_insert` **1**, and for `date_time_input_format` **basic**.
The client therefore must send `async_insert=0` explicitly to stay synchronous
and `date_time_input_format=best_effort` for parity. `input_format_skip_unknown_fields`
is 1 and `input_format_defaults_for_omitted_fields` is 1 on the server, so extra or
missing JSON keys do not fail an insert (an extra key in a Go row is silently
dropped, a missing key takes the column default).

With `BUFFER_ASYNC_INSERTS` the insert returns when the server has queued the
data, not when it is in a part; the buffer then trims Redis. A server crash in
that window loses the rows (the accepted trade-off of the flag).

**Clustered deployments** (`CLICKHOUSE_CLUSTER` = `true` or `1`; config
`apps/api/src/config/env.ts:464`, runtime helper `packages/db/src/clickhouse/client.ts:86`): the migrations create
`<table>_replicated` (Replicated* engine, on every node) plus `<table>` as a
`Distributed` table over it (`packages/db/src/clickhouse/migration.ts:82`).
- All inserts and all reads in the buffers use the **plain table name**
  (`events`, `sessions`, `profiles`, `groups`, ...), i.e. the Distributed table;
  the insert is acknowledged by the node that received it and forwarded to the
  shards asynchronously (Distributed default).
- The **only** place a buffer uses the replicated name is the profile-backfill
  `UPDATE`: `UPDATE events_replicated ON CLUSTER '{cluster}' ...` (`replicatedTarget`,
  `packages/core/src/shared/ch-tables.ts:47`).
- Sharding keys that matter here (all `cityHash64(...)`): sessions
  `(project_id, toString(toStartOfHour(created_at)))` (`packages/db/src/code-migrations/28-sessions-sort-key-id.ts:59`),
  profiles `(project_id)` (`packages/db/src/code-migrations/16-restructure-profiles.ts:95`), groups `(project_id, id)`
  (`packages/db/src/code-migrations/11-add-groups.ts:42`), replay `(project_id, session_id)`
  (`packages/db/src/code-migrations/10-add-session-replay.ts:30`). A session's +1 and −1 rows
  share `created_at` and so a shard, except when an out-of-order event moves
  `created_at` (then the pair can straddle shards until a merge; same in TS).
- Not verifiable on the single-node local stack: the `ON CLUSTER` lightweight
  UPDATE and the Distributed forwarding. Flagged for staging.


## P2.5 Buffer by buffer

"Row" below is what sits in Redis; "flush" is what `onFlush` does. Every flush
starts with `LRANGE <key> 0 <batch-1>` (head of the list, oldest first, since
producers `RPUSH`) and ends with `LTRIM <key> <n> -1` where `n` is the number
of rows read; an empty read returns 0 and ends the drain.

### event

**Row** (one JSON object per list element, written by `JSON.stringify(event)` of
an `IClickhouseEvent`, `packages/core/src/modules/event/event.service.ts:460`):
`id` (the producer-minted id, else `crypto.randomUUID()` **ND-random**), `name`, `device_id`,
`profile_id` (empty → `device_id`, `packages/core/src/modules/event/event.service.ts:456`), `project_id`, `session_id`,
`properties` (`toDots`-flattened to string values, caps 50 keys per object and 1000
total, `packages/shared/src/object.ts:56`), `path`, `origin`, `created_at` (`yyyy-MM-dd HH:mm:ss.SSS` UTC),
`country`, `city`, `region`, `longitude`, `latitude` (null if absent), `os`, `os_version`,
`browser`, `browser_version`, `device`, `brand`, `model`, `duration`, `referrer`,
`referrer_name`, `referrer_type`, `imported_at` (null), `inserted_at` (**ND-clock**, `now`
UTC, same format), `sdk_name`, `sdk_version`, `revenue` (key **omitted** when
undefined), `groups`. Key order is the literal's order; ClickHouse does not care.

**Writers.** Only `createEvent` (`packages/core/src/modules/event/event.service.ts:500`), reached from the Kafka consumer's
per-message handler (`packages/core/src/modules/ingest/src/incoming-event-handler.ts:159`, through the `asRedeliverable()`
view of the buffer, `packages/core/src/modules/ingest/src/consumer-handler.ts:36`) and from the session-end job
(`packages/core/src/modules/session/session.jobs.ts:68`). `add` appends to an in-process array; a timer (10 ms,
`EVENT_BUFFER_MICRO_BATCH_MS`) or reaching 100 events (`EVENT_BUFFER_MICRO_BATCH_SIZE`)
triggers a single `RPUSH event_buffer:queue <json>...` of the whole array
(`packages/core/src/buffers/event-buffer.ts:321`). On failure the events are put back at the front, except
those marked redeliverable (the Kafka path), which are dropped from memory
because the consumer's offsets are not committed and Kafka redelivers them
(`packages/core/src/buffers/event-buffer.ts:236`, the durability-window gate `flushPendingOrThrow`). That is
P1 territory; it is listed because the session-end job writes through the **plain**
`add` (no redelivery safety net) and a Go session-end must decide how to
await its write (P3).

**Flush** (`packages/core/src/buffers/event-buffer.ts:392`):
1. `LRANGE event_buffer:queue 0 3999`.
2. Count rows per project without parsing: `extractProjectId(line)`
   (`packages/core/src/buffers/event-buffer.ts:17`): needle `"project_id":"`; no hit → none; **two or more**
   hits → `JSON.parse(line).project_id` if it is a string (a user property named
   `project_id` produces a second hit), else none; exactly one hit → the text up
   to the next `"` (no unescaping; empty → none). Counts keep first-seen order.
3. Chunks of 1000 → up to 5 concurrent `INSERT INTO events FORMAT JSONEachRow` with
   the **original strings** as body lines (no re-serialisation), async layer if
   enabled (P2.4).
4. After **all** chunks succeeded, for each project in first-seen order:
   `PUBLISH events:batch <superjson>` on the pub connection (`getRedisPub`), payload
   `{"json":{"projectId":"<id>","count":<n>}}` (`[ran]` superjson 2.2.2 output for
   a plain object has no `meta` key; `packages/redis/publisher.ts:29`). The call is **not awaited**
   and its rejection is not handled. The dashboard's live view subscribes
   (`subscribeToPublishedEvent('events','batch')`).
5. `LTRIM event_buffer:queue n -1`.
6. Return n; the drain loops while n = 4000 and time remains.

**HAZARD (poison pill).** The body is the raw lines, so one line that is not a
valid row fails the whole insert with a server error (class `ch-server`, not
retried by the wrapper, retried every tick by cron) and the head of the list never
advances: there is no quarantine. Same for bot, replay, group and ai-event.

**Active visitors** (`getActiveVisitorCount`, `packages/core/src/buffers/event-buffer.ts:455`) is an API read helper on this class, not part
of flushing.

### session

**Row**: a full `IClickhouseSession` JSON (field list at `packages/core/src/modules/session/session.service.ts:37`; column types in
`test/clickhouse-schema.sql:331`, engine `VersionedCollapsingMergeTree(sign, version)`). Dates are
`yyyy-MM-dd HH:mm:ss.SSS` UTC strings. Writers: only `SessionBuffer.persist` inside
the ingest MULTI (P3.2 lists the other keys written in the same transaction,
`packages/core/src/buffers/session-buffer.ts:392`). A new session appends one row (`sign=1, version=1`); an extension
appends **two** rows in the order `[current(sign=1, version+1), old(sign=-1, old version)]`
(`packages/core/src/buffers/session-buffer.ts:326`).

**Flush** (`packages/core/src/buffers/session-buffer.ts:537`):
1. `LRANGE session-buffer 0 9999`.
2. Parse every element with `JSON.parse`; an unparsable element is **skipped** (still
   counted in the trim). On each parsed row: `duration = max(0, duration || 0)`
   (null, undefined, NaN → 0).
3. If `SESSION_BUFFER_SQUASH` is not `false`/`0`: `squashSessionsByVersion(rows)`:
   - Fewer than two rows → unchanged.
   - Group by `id` only (not by project), groups in first-appearance order. A
     group with one row is emitted as is (even if `sign=-1`).
   - Within a group, accumulate per `version` (versions in first-appearance order):
     `net += sign`; remember the **last** row seen with `sign=1` (`pos`) and the last
     with `sign=-1` (`neg`).
   - For each version with `net ≠ 0` emit `|net|` copies of `pos` (if `net > 0`) or `neg`
     (if `net < 0`); `net = 0` emits nothing. (Copies are the same object, so the same
     serialisation.)
   - Output = concatenation in that order. Rationale and the counter-example it
     protects against: the comment at `packages/core/src/buffers/session-buffer.ts:437`.
   Disabled → all parsed rows are inserted.
4. Chunks of `chunkSize` (default = the whole batch, 10 000) → `INSERT INTO sessions
   FORMAT JSONEachRow`, objects serialised with `JSON.stringify`, **no async
   settings ever** (P2.4). Rows are sent as parsed (key order preserved, `null`
   kept, numbers as JS prints them, `is_bounce` as JSON boolean).
5. `LTRIM session-buffer n -1` with `n` = rows read (not rows inserted).
Memory note from the code: the trim happens after the insert, "let it propagate"
(an error leaves rows and retries next tick).

Correctness link with P3: the squash must never drop a surviving row's
partner; a (+1, V) and (−1, V) in the same batch cancel, the net rows are what
ClickHouse needs to collapse (see the 6-line explanation in the cited comment).

### profile

**Row** (`IClickhouseProfile`, `packages/core/src/modules/profile/profile.service.ts:114`): `id, first_name, last_name, email, avatar,
properties (string map), project_id, is_external (bool), created_at, last_seen_at, groups`.
Both dates are `formatClickhouseDate(new Date())`, i.e. `yyyy-MM-dd HH:mm:ss`
without milliseconds, **ND-clock** (`packages/core/src/modules/profile/profile.service.ts:334`; empty-ish property values stripped by `strip`).

**Writers** (all through `upsertProfile` → `ProfileBuffer.add`): `createEvent` on
`session_start` and `session_end` (`isFromEvent=true`,
`packages/core/src/modules/event/event.service.ts:535`), identify and increment/decrement (`packages/core/src/modules/ingest/ingest.service.ts:461`,
`packages/core/src/modules/profile/profile.service.ts:692`), assign_group (`packages/core/src/modules/ingest/ingest.service.ts:518`). `add` with `isFromEvent` first runs
`EXISTS profile-cache:<project>:<id>` and skips the push when the key exists
(metric `buffer_add_skipped_total{reason="cached"}`); otherwise `RPUSH profile-buffer
<json>` (`packages/core/src/buffers/profile-buffer.ts:109`). The cache key is **only written by the flush** (below), so
until the first flush of a profile every session boundary event pushes another
row; the in-batch merge folds them.

**Flush** (`packages/core/src/buffers/profile-buffer.ts:226`):
1. `LRANGE profile-buffer 0 199`.
2. **In-batch merge**: for each element in order, `JSON.parse` (invalid skipped; still
   trimmed), key `<project_id>:<id>`, `merged[key] = mergeProfiles(merged[key] ?? null,
   profile)` (a Map, so first-appearance order).
3. `MGET profile-cache:<project_id>:<id> ...` for all unique profiles (one call; a
   value is parsed with `JSON.parse`, invalid → treated as a miss). Hits become the
   "existing" profile.
4. **ClickHouse lookup for the misses only**, in chunks of 50
   (`PROFILE_BUFFER_FETCH_CHUNK_SIZE`), at most 2 concurrent (constant
   `FETCH_CONCURRENCY`), exact query below. A failing chunk is **swallowed**
   (warning) and its profiles are merged against nothing. The result is keyed
   `<project_id>:<id>` from the returned row.
5. **Final merge**: for each unique profile in order, `merged = mergeProfiles(existing ?? null, profile)`;
   append to the insert list; queue `SET profile-cache:<project>:<id> <JSON.stringify(merged)> EX 3600`
   (`PROFILE_BUFFER_TTL_IN_SECONDS`) on a MULTI.
6. Chunks of 1000 → `INSERT INTO profiles FORMAT JSONEachRow` (objects, async layer
   if enabled).
7. After the inserts succeeded: add `LTRIM profile-buffer n -1` to the same MULTI and
   `EXEC`. The cache `SET`s and the trim are therefore applied together, **only if
   the insert succeeded**. The `EXEC` result (per-command errors) is not
   inspected (`packages/core/src/buffers/profile-buffer.ts:345`).

The lookup query, rendered by the repository's own `sql` builder (`[ran]`, stub client,
`p1..p4` are `String` parameters bound by the client as `param_p1=...` URL
parameters; two profiles shown, chunk size 50 gives `p1..p100`):

```sql
SELECT id, project_id, argMax(first_name, p.last_seen_at) AS first_name, argMax(last_name, p.last_seen_at) AS last_name, argMax(email, p.last_seen_at) AS email, argMax(avatar, p.last_seen_at) AS avatar, argMax(properties, p.last_seen_at) AS properties, argMax(is_external, p.last_seen_at) AS is_external, argMax(groups, p.last_seen_at) AS groups, min(created_at) AS created_at, max(p.last_seen_at) AS last_seen_at
FROM profiles AS p
WHERE (p.id, p.project_id) IN (({p1:String}, {p2:String}), ({p3:String}, {p4:String}))
GROUP BY p.id, p.project_id
```

`[ran]` against the local database on 26.3.67.30 with real profile ids: it executes
and returns `id, project_id, first_name, last_name, email, avatar, properties
(Map(String,String) → JSON object), is_external (Bool → true/false), groups
(Array(String)), created_at, last_seen_at (DateTime64(3) → "yyyy-MM-dd HH:mm:ss.SSS")`.
It is sent with the client-default settings only (no INSERT layer), format JSON.
Rationale in the file comment (argMax by `last_seen_at` instead of `FINAL`,
`packages/core/src/buffers/profile-buffer.ts:9`). There is deliberately no recency window.

`mergeProfiles(existing, incoming)` (`packages/core/src/buffers/profile-buffer.ts:131`):
- `existing = null` → `incoming` unchanged (no stripping, no defaults).
- If `existing.properties.device ≠ 'server'` and `incoming.properties.device = 'server'`:
  incoming's `properties` lose the keys `city, country, region, longitude, latitude, os,
  osVersion, browser, device, isServer, os_version, browser_version` before merging.
- Result = `deepMergeObjects(existing, incoming without created_at and groups)` with
  `groups = uniq([...existing.groups ?? [], ...incoming.groups ?? []])` (ramda `uniq`:
  equality-based, first occurrence wins, order kept). So `created_at` comes from
  existing, `last_seen_at` from incoming (so the ReplacingMergeTree version
  advances), every other scalar from incoming unless the rule below keeps
  existing.
- `deepMergeObjects(target, source)` (`packages/shared/src/object.ts:183`), key set = target keys then new source keys:
  1. source is null/undefined and target is not null/undefined → target; or source is
     `''` and target is a non-empty string → target;
  2. else if source is not undefined and both are plain objects (non-null, not arrays) →
     recurse;
  3. else if source is not undefined → source (arrays and scalars replace, `null`
     replaces a null/undefined target);
  4. else if source is undefined and target has the key → target.
  Used on `properties` (merged key by key), so an incoming property overwrites,
  an incoming empty string or null does not erase.
The incoming profile from `upsertProfile` has already had empty and nil properties
removed (`strip`), and ClickHouse "existing" has every column present, so rule 1
mostly matters for names/emails sent as `''`.

**Cache semantics**: `profile-cache:*` holds the **last merged row written by a
flush**, TTL 1 h; it is also read by the API (`fetchFromCache`,
`packages/core/src/modules/profile/profile.service.ts:222`). A TS flush and a Go flush must write the same JSON shape (the
merged IClickhouseProfile) and the same key.

### profile-backfill

**Row** `{"projectId","sessionId","profileId"}`, written only by the session-end
job when `EXPERIMENTAL_PROFILE_BACKFILL=1` (exactly `1`; optionally restricted to
`EXPERIMENTAL_PROFILE_BACKFILL_PROJECTS`) and `profileId ≠ session.device_id`
(`packages/core/src/modules/session/src/session-end.ts:216`, P3), via `RPUSH profile-backfill-buffer` (errors swallowed,
logged).

**Flush** (`packages/core/src/buffers/profile-backfill-buffer.ts:53`): **one batch per tick, no drain loop**, so a backlog above 1000
entries grows (30 s ticks).
1. `LRANGE profile-backfill-buffer 0 999`.
2. Deduplicate by `sessionId` in a Map (a later entry replaces the earlier one's
   value, position stays that of the first), keep `projectId` of the surviving entry.
3. Chunks of 500, **sequential**, one statement per chunk via `ch.command` (not
   `insert`: no INSERT default settings). Per-call settings `mutations_sync=0` and
   `allow_experimental_lightweight_update=1` on top of the client defaults.
   Rendered statements (`[ran]` the unclustered form against a throwaway table on
   26.3.67.30 in database `openpanel_wparity_flush`, since dropped: rows newer than
   6 h were updated, older untouched; the `ON CLUSTER` form could not be run on
   one node):

```sql
-- not clustered
UPDATE events
SET profile_id = CASE session_id
  WHEN {p1:String} THEN {p2:String}
WHEN {p3:String} THEN {p4:String}
END
WHERE (project_id, session_id) IN (({p5:String}, {p6:String}), ({p7:String}, {p8:String}))
  AND created_at > now() - INTERVAL 6 HOURS

-- CLICKHOUSE_CLUSTER=true
UPDATE events_replicated ON CLUSTER '{cluster}'
SET profile_id = CASE session_id
  WHEN {p1:String} THEN {p2:String}
WHEN {p3:String} THEN {p4:String}
END
WHERE (project_id, session_id) IN (({p5:String}, {p6:String}), ({p7:String}, {p8:String}))
  AND created_at > now() - INTERVAL 6 HOURS
```

   Parameters: `p1,p2` = (sessionId, profileId) pairs for the `CASE`, then `p` = (projectId,
   sessionId) pairs for the `IN` list, all `String`. The `6 HOURS` window uses the
   server clock (**ND-clock**) and the table needs `enable_block_number_column` and
   `enable_block_offset_column` (set on `events` by migration 8,
   `packages/db/src/code-migrations/8-order-keys.ts:60`).
4. `LTRIM profile-backfill-buffer n -1` **only if every chunk statement returned**; an
   error leaves the whole batch for the next tick (the UPDATE is idempotent).
   `mutations_sync=0` means the call returns before the mutation applies, and the
   trim does not wait for it.

### bot

**Row**: `{"id": uuid (ND-random), "name", "type", "project_id", "path", "created_at": "yyyy-MM-dd HH:mm:ss"}`
(`packages/core/src/modules/event/event.service.ts:764`), written by the ingest service on a bot user agent
(`packages/core/src/modules/ingest/ingest.service.ts:961`); the Go receiver forwards such requests to the Bun API
instead, README "What it handles").
**Flush** (`packages/core/src/buffers/bot-buffer.ts:42`): `LRANGE bot-events-buffer 0 999`, **one** `INSERT INTO events_bots
FORMAT JSONEachRow` of all lines (no chunking, raw strings, async layer if enabled),
`LTRIM` n.

### replay

**Row** `IClickhouseSessionReplayChunk` (`packages/core/src/buffers/replay-buffer.ts:5`): `project_id, session_id, chunk_index, started_at,
ended_at, events_count, is_full_snapshot, payload`. `payload` is a string, average
~270 KB, up to 1 MB (comment at `packages/core/src/buffers/replay-buffer.ts:19`). Writer: `handleReplay`
(`packages/core/src/modules/ingest/ingest.service.ts:478`) → `RPUSH replay-buffer <json>`; the Go receiver forwards `replay`
to the Bun API.
**Flush** (`packages/core/src/buffers/replay-buffer.ts:66`): its own loop with the same 8 s budget but **not** consulting
`lockLost`. Per round: read a batch with `LRANGE` in slices of **10 items** (`packages/core/src/buffers/replay-buffer.ts:23`) until
500 items or 8 MiB of UTF-8 bytes (`MAX_BATCH_BYTES`, checked before each slice so
a batch can exceed it by up to nine items), stopping early when a slice is short
(`reachedEnd`); insert the items in chunks of 500 (raw strings, async layer if
enabled), `LTRIM replay-buffer n -1`, add to totals; stop the loop when
`reachedEnd`. Phase timings are summed over rounds, `rowsProcessed` is the total.
The small slices exist because one count-sized `LRANGE` once asked Redis for
260 MB, timed out, and let the list grow (comment at the constants).
A round that read zero items does not call insert or trim.

### group

**Row** (`IGroupBufferEntry`: `project_id, id, type, name, properties (Map(String,String)),
created_at, version (string of epoch ms), deleted (0)`, `packages/core/src/buffers/group-buffer.ts:9`). The merging happens
**on the write side**, in `GroupBuffer.add` (called by the Bun API, `packages/core/src/modules/ingest/ingest.service.ts:503`), not by
the flush): read `group-cache:<project>:<id>` else the ClickHouse lookup below; merged
properties = `toDots({...existing.properties, ...input.properties})`; `created_at` =
existing's or now (**ND-clock**, `yyyy-MM-dd HH:mm:ss`); `version = String(Date.now())`
(**ND-clock**, the ReplacingMergeTree version); if type, name and properties are
unchanged versus existing, **nothing is written**; otherwise one `MULTI`:
`SET group-cache:<project>:<id> <json> EX 3600` (`GROUP_BUFFER_TTL_IN_SECONDS`) and
`RPUSH group-buffer <json>` (the cache is written at add time, unlike profile).
Lookup (`[ran]`-rendered; engine `ReplacingMergeTree(version, deleted)`):

```sql
SELECT project_id, id, type, name, properties, created_at
  FROM groups FINAL
  WHERE project_id = {p1:String}
AND id = {p2:String}
AND deleted = 0
```

**Flush** (`packages/core/src/buffers/group-buffer.ts:162`): `LRANGE group-buffer 0 199`, chunks of 1000 raw lines → `INSERT INTO groups
FORMAT JSONEachRow` (async layer if enabled), `LTRIM` n.

### ai-event

**Row** `IClickhouseAiEvent` (`packages/core/src/modules/ai/src/row.ts:43`, columns in `test/clickhouse-schema.sql:168`). Writer: the consumer's
`storeAiEvent` after the event row was stored
(`packages/core/src/modules/ingest/src/incoming-event-handler.ts:180`) → `RPUSH ai-event-buffer <json>`.
**Flush** (`packages/core/src/buffers/ai-event-buffer.ts:49`): `LRANGE ai-event-buffer 0 999`, chunks of 500 raw lines → `INSERT INTO
ai_events FORMAT JSONEachRow` (async layer if enabled), `LTRIM` n. **This buffer is
missing from the metrics registration** (P2.7).

## P2.6 Who writes the shared Redis state (formats must stay shared)

During the migration the TS API and the Go worker read and write the same keys,
so the TS writers stay and Go must read exactly what they write:

| state | written by (TS) | read by flush | note |
|---|---|---|---|
| `event_buffer:queue` | `createEvent` (consumer, session-end job) | event flush | strings = JSONEachRow rows |
| `session-buffer`, `session:*` | `SessionBuffer.persist` (consumer) and `cleanup` (session-end job, vacuum) | session flush; P3 | Lua cleanup is in P3.2 |
| `profile-buffer` | `upsertProfile`: consumer (`session_start`/`session_end` events), Bun API identify/increment/decrement/assign_group, session-end job | profile flush | `profile-cache:*` written by flush, read by API |
| `profile-backfill-buffer` | session-end job | backfill flush | gated by env |
| `bot-events-buffer` | Bun API (bot user agents) | bot flush | Go receiver forwards bots to the API |
| `replay-buffer` | Bun API `replay` route | replay flush | Go receiver forwards replay to the API |
| `group-buffer`, `group-cache:*` | Bun API `group` route | group flush | merge happens in `add`, not the flush |
| `ai-event-buffer` | consumer `storeAiEvent` | ai-event flush | |
| `lock:<buffer>` | every flushing process | | P2.3 |
| `events:batch` pub/sub channel | event flush | dashboard live view | superjson |

The Go receiver (`apps/ingest`) writes none of the buffers (it produces Kafka
records); it only reads `session:<project>:<device>` blobs
(`apps/ingest/internal/identity/redis.go:15`, or the line that builds the key).


## P2.7 Metrics

Registered once per process **only where the role consumes queues**
(`apps/api/src/main.ts:316`, comment: each gauge costs Redis round trips per scrape) on the one
prom-client registry (`packages/core/src/metrics.ts:5`) by `packages/core/src/buffers/buffer.metrics.ts:44`.
The buffers instrumented are, in this registration order: event, profile, bot,
session, replay, group, profile-backfill (`packages/core/src/buffers/buffer.metrics.ts:27`). **`ai-event` is not in
that list**: it has no `buffer_ai_event_count` gauge, no flush metrics and no add
metrics, and the Go port must decide whether to add them (the label value would be
`ai-event`). Bucket arrays, verbatim:

```ts
const FLUSH_DURATION_BUCKETS_MS = [
  5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000,
];
const REDIS_OP_BUCKETS_MS = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000];
const CH_DURATION_BUCKETS_MS = [
  10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000,
];
const LLEN_BUCKETS = [
  0, 10, 100, 500, 1000, 5000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
  5_000_000,
];
const ADD_DURATION_BUCKETS_MS = [
  0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 5000,
];
```

| series | type | labels | observed from |
|---|---|---|---|
| `buffer_<name>_count` (`-` → `_`: `buffer_profile_backfill_count`) | gauge | none | `LLEN` of the buffer's list at scrape time; errors swallowed (series keeps no value) |
| `buffer_event_pending_local_count` | gauge | none | events in the event buffer's in-process micro-batch (not yet in Redis) |
| `buffer_flush_duration_ms` | histogram, `FLUSH_DURATION_BUCKETS_MS` | `buffer, result, trigger` | `totalMs` of every flush including `locked` and `paused` |
| `buffer_flush_total` | counter | `buffer, result, trigger` | one per flush call |
| `buffer_flush_rows_total` | counter | `buffer` | `rowsProcessed` when > 0 |
| `buffer_redis_op_duration_ms` | histogram, `REDIS_OP_BUCKETS_MS` | `buffer, op` (`lrange`, `trim`) | phase `lrangeMs` / `trimMs` when present |
| `buffer_ch_insert_duration_ms` | histogram, `CH_DURATION_BUCKETS_MS` | `buffer` | phase `chInsertMs` (for profile-backfill this is the UPDATE time) |
| `buffer_ch_fetch_duration_ms` | histogram, `CH_DURATION_BUCKETS_MS` | `buffer` | phase `chFetchMs` (profile only) |
| `buffer_flush_llen_at_start` | histogram, `LLEN_BUCKETS` | `buffer` | `llenAtStart` when known |
| `buffer_add_duration_ms` | histogram, `ADD_DURATION_BUCKETS_MS` | `buffer` | add latency, non-skipped adds |
| `buffer_add_total` | counter | `buffer` | non-skipped adds |
| `buffer_add_skipped_total` | counter | `buffer, reason` | skipped adds (`reason="cached"` from profile) |

`trigger` is always `cron`. `transformMs` and `onFlushMs` are logged but not exported.
The session buffer's `ingest` is not timed, so `buffer_add_*{buffer="session"}`
never appears. `SCALING.md` and the e2e load scripts read these names
(`apps/api/e2e/ingest-load.ts` polls the list lengths directly), so names and
labels must not change. Queue-level series (`<queue>_<active|delayed|failed|completed|waiting>_count`,
`job_duration_ms{name,status}` failure-only) belong to BullMQ and are listed in P3.8.

## P2.8 Environment variables read by the flush path

Parsed in `apps/api/src/config/env.ts:571` onwards (`blankToUndefined` first: an unset or blank value is "not set").
"Positive int" = `optionalPositiveInt`: a malformed or non-positive value silently
means "not set" (default kept), it does not fail boot.

| variable | default | parse rule |
|---|---|---|
| `BUFFER_ASYNC_INSERTS` | off | **any non-blank value, including `false` and `0`, turns it on** (`definedIsTrueSchema`, `apps/api/src/config/env.ts:185`) |
| `BUFFER_CH_INSERT_CONCURRENCY` | 5 | positive int |
| `EVENT_BUFFER_BATCH_SIZE` / `_CHUNK_SIZE` | 4000 / 1000 | positive int |
| `EVENT_BUFFER_MICRO_BATCH_MS` / `_MICRO_BATCH_SIZE` | 10 / 100 | positive int (writer side) |
| `SESSION_BUFFER_BATCH_SIZE` / `_CHUNK_SIZE` | 10000 / = batch size | positive int |
| `SESSION_BUFFER_SQUASH` | on | off only for `false` or `0` (`apps/api/src/config/env.ts:202`) |
| `PROFILE_BUFFER_BATCH_SIZE` / `_CHUNK_SIZE` / `_FETCH_CHUNK_SIZE` / `_TTL_IN_SECONDS` | 200 / 1000 / 50 / 3600 | positive int |
| `PROFILE_BACKFILL_BUFFER_BATCH_SIZE` | 1000 | positive int (the 500 UPDATE chunk is a constant) |
| `BOT_BUFFER_BATCH_SIZE` | 1000 | positive int |
| `REPLAY_BUFFER_BATCH_SIZE` / `_CHUNK_SIZE` | 500 / 500 | positive int (8 MiB and 10-item slices are constants) |
| `GROUP_BUFFER_BATCH_SIZE` / `_CHUNK_SIZE` / `_TTL_IN_SECONDS` | 200 / 1000 / 3600 | positive int |
| `AI_EVENT_BUFFER_BATCH_SIZE` / `_CHUNK_SIZE` | 1000 / 500 | positive int |
| `CLICKHOUSE_URL` | none | comma-separated URLs, database in the path |
| `CLICKHOUSE_CLUSTER` | off | `true` or `1` (`apps/api/src/config/env.ts:128`) |
| `CLICKHOUSE_SETTINGS` | `{}` | JSON object merged into the client-level settings (invalid JSON → `{}`) |
| `CLICKHOUSE_SETTINGS_REMOVE_CONVERT_ANY_JOIN` | unset | any non-empty value drops the `query_plan_convert_any_join...` setting |
| `CLICKHOUSE_REQUEST_COMPRESSION` | on | off for `false` or `0` |
| `CLICKHOUSE_MAX_OPEN_CONNECTIONS` | 50 | int, min 1 |
| `CLICKHOUSE_REQUEST_TIMEOUT_MS` | 30000 | int, min 1000 |
| `CLICKHOUSE_UNHEALTHY_MARK_MS` | 5000 | int, min 0 |
| `REDIS_URL`, `REDIS_CACHE_COMMAND_TIMEOUT_MS` | `redis://localhost:23379`, 5000 | |
| `CRON_CONCURRENCY` | 4 | positive int |
| `ENABLED_QUEUES` | all | list; `cron` must be included for flushes to run in a TS process |
| `DISABLED_CRON_JOBS` | none | list of cron job ids (the eight `flush*` ids are valid) |
| `QUEUE_NAMESPACE`, `QUEUE_CLUSTER` | unset, off | decide the BullMQ queue key, hence the pause-flag key (P2.2) |
| `SHUTDOWN_FORCE_EXIT_MS` | 20000 | positive int |

## P2.9 Nondeterminism (mask these in goldens)

**ND-random**: flush lock id; ClickHouse `query_id`; event `id` when the producer
did not mint one (messages from before the field existed); bot event `id`; the
random part of any generated ids in rows.
**ND-clock**: `events.inserted_at`; profile `created_at`/`last_seen_at` (stamped by
`upsertProfile` at write time, second resolution); group `version` (epoch ms) and a
new group's `created_at`; the 6-hour window of the backfill UPDATE (server
clock); all timing phases; the cron alignment. Session rows carry event times
from the payload, not the wall clock; only the wallclock ZSET score (P3) is
wall clock.
Everything else in a flush is a pure function of the Redis list contents, the
cache keys, the ClickHouse state and the env, in the orders stated above.

## P2.10 Hazards and open questions (P2)

1. **Stream replay on retry loses rows** (P2.4, reproduced). Go: rebuild the body per attempt.
2. **Poison line blocks a raw-passthrough buffer forever** (event, bot, replay, group, ai-event). No quarantine exists in TS; a Go flush could move an unparseable batch to a side list, but that changes behaviour operators may rely on.
3. **At-least-once with several chunks** duplicates `events` rows when chunk k fails after chunks 1..k-1 landed. Same in TS.
4. **Profile lookup failure is swallowed**: the incoming (partial) profile is inserted without merging; the newer `last_seen_at` makes it win the ReplacingMergeTree, so the previous properties are lost from the read side until they are re-sent. Not retried.
5. **Event publish** is fire-and-forget on the pub connection, after the insert and before the trim: a crash between insert and trim re-inserts the rows and publishes the counts again (double-counted live view for that window).
6. **Replay drain ignores `lockLost`**; the other drains check it only between batches. A flush that lost its lock can still insert and trim a batch another flush also read.
7. **`BUFFER_ASYNC_INSERTS` is truthy for any value**, including `false`.
8. **ai-event has no metrics**; decide whether the Go port adds them (extends the contract, harmless).
9. **`profile-backfill` does not drain**; one batch of 1000 every 30 s. Go may drain more, the UPDATE is idempotent.
10. **Pause semantics**: pausing the BullMQ `cron` queue stops flushing; a Go worker that does not read the flag silently ignores an operator's pause.
11. Open: should the Go flush await the same `lock:<name>` in the same Redis database as the TS one (required for the per-buffer cut-over), and what happens to a TS flush that is running while the Go one is switched on? Both use `SET NX`, so one wins per tick; no coordination beyond that exists or is needed.
12. Open: the Redis Lua scripts here have one key and are Cluster-safe; session's cleanup script (P3) has three keys and is not (P3.2).

## P2.11 ClickHouse 26.5+ check (production is capped at 26.3)

Nothing in the flush paths needs 26.5 or later. What I ran on 26.3.67.30 `[ran]`:
the profile lookup (real ids, real result), the unclustered backfill `UPDATE`
against a throwaway table with the block-number/offset columns (updated rows
newer than 6 h, left older ones), `SELECT name, value FROM system.settings` for
every setting this section names (all exist: `allow_experimental_lightweight_update` (default 1),
`enable_lightweight_update` (also present), `async_insert`, `wait_for_async_insert`,
`parallel_view_processing`, `query_plan_convert_any_join_to_semi_or_anti_join`,
`mutations_sync`, `input_format_parallel_parsing`, `send_progress_in_http_headers`).
Version-sensitive points to keep in mind rather than errors:
- `allow_experimental_lightweight_update` is the name the TS sends; on 26.3 a second
  name `enable_lightweight_update` exists and the old one defaults to 1. Send the TS
  name on ≤ 26.3; revisit when the cluster moves past 26.3 (the "experimental" name
  may be retired).
- The server default `async_insert=1` (26.3) is why `async_insert=0` must be sent
  explicitly on every request; an older server defaults to 0, so sending it is safe
  in both directions.
- Not run: `UPDATE ... ON CLUSTER '{cluster}'` (needs a cluster).


---

# P3: session lifecycle

Everything that turns a session's last event into a `session_end` event and
removes its Redis state, plus the counters refreshed afterwards. Idle timeout
**`SESSION_TIMEOUT_MS` = 1 800 000 ms (30 min)** (`packages/core/src/modules/session/src/keys.ts:7`;
`packages/core/src/buffers/session-buffer.ts:24` reads the override).

## P3.1 Components

| component | what it does | trigger | where |
|---|---|---|---|
| `session` job on queue `sessions` | emits one `session_end`, notifies funnel rules, removes the session's Redis state, then refreshes event counters | enqueued at a session **boundary** (ingest) or by the **reaper** | `packages/core/src/modules/session/session.jobs.ts:86` |
| `sessionReaper` (cron queue) | finds sessions idle for ≥ the deadman and enqueues their close | every 300 000 ms (`packages/core/src/modules/session/session.jobs.ts:24`) | `packages/core/src/modules/session/session.jobs.ts:126` |
| `sessionVacuum` (cron queue) | deletes Redis state that `cleanup` missed | pattern `0 4 * * *` (`packages/core/src/modules/session/session.jobs.ts:26`) | `packages/core/src/modules/session/session.jobs.ts:138` |
| `updateEventsCount` | project and organization event counters, usage alerts | at the end of every `session` job | `packages/core/src/modules/session/src/usage.ts:43` |
| `flushSessions`, `flushReplay` | buffer flushes | cron | P2 |

The cron timezone of `0 4 * * *`: BullMQ parses the pattern with `cron-parser` and
passes no `tz` (`packages/core/node_modules/bullmq/dist/esm/classes/job-scheduler.js:296`), so it is the **process timezone**
of whichever worker owns the scheduler; the source comment "Daily 04:00 UTC"
holds only when the container runs in UTC (not checked for production). The
reaper's `every` is aligned to epoch multiples of 5 min like the flushes (P2.2).

## P3.2 Redis session state (shared with ingest)

Written by ingest in one `MULTI` per event (`packages/core/src/buffers/session-buffer.ts:392`): `SET` blob, `ZADD` wallclock, `SADD` projects,
optional `DEL` of a stale profile pointer, optional `SET ... PX` of the profile pointer,
`RPUSH session-buffer` of the ClickHouse rows). Read, deleted and expired by the P3
components.

| key | type | content | lifetime | written by |
|---|---|---|---|---|
| `session:<project>:<device>` (`packages/core/src/buffers/session-buffer.ts:31`) | string | `JSON.stringify(IClickhouseSession)` of the live session | **no TTL**; removed only by `cleanup` (Lua) | ingest `SET` |
| `session:wallclock:<project>` (`packages/core/src/modules/session/src/keys.ts:13`) | zset | member = deviceId, score = `Date.now()` in ms at the moment the **worker received** the event (**ND-clock**), not event time | member removed by `cleanup`, `ZREM` in reaper/vacuum orphan paths | ingest `ZADD` |
| `session:projects` (`packages/core/src/modules/session/src/keys.ts:10`) | set | project ids with at least one live session | reaper `SREM` when the project's zset is empty | ingest `SADD` |
| `session:profile:<project>:<profile>` (`packages/core/src/buffers/session-buffer.ts:33`) | string | deviceId, so server-side events can ride a client session | `PX` = 2 × idle timeout = 3 600 000 ms (`packages/core/src/buffers/session-buffer.ts:16`), refreshed on every persist; also deleted by `cleanup` | ingest |
| `session:end:emitted:<project>:<device>:<sessionId>` | string `1` | idempotency claim | `EX 7200` | session job |
| `session:reaper:lock:<project>` | string `1` | reaper advisory lock | `EX 60` | reaper |
| `session-buffer` | list | ClickHouse rows (P2.5) | until flushed | ingest |
| `session:hwm:<project>` | – | **read** by the `sessions_hwm_lag_ms` gauge (`packages/core/src/modules/session/src/session.metrics.ts:157`); **no writer exists** in the repository (searched all of `packages`, `apps`), so the gauge is always 0 | – | nobody |

**The cleanup Lua script**, verbatim (`packages/core/src/buffers/session-buffer.ts:42`), called as `EVALSHA`/`EVAL` with three keys and two args:
`KEYS[1]` = `session:<project>:<device>`, `KEYS[2]` = `session:wallclock:<project>`,
`KEYS[3]` = `session:profile:<project>:<profile>` **or the empty string**
(when `profileId` is empty or equals `deviceId`, `packages/core/src/buffers/session-buffer.ts:232`), `ARGV[1]` = sessionId,
`ARGV[2]` = deviceId. It returns 1 (deleted) or 0 (a different session now owns the slot).

```lua
local cur = redis.call('GET', KEYS[1])
if cur then
  local ok, parsed = pcall(cjson.decode, cur)
  if ok and parsed and parsed.id ~= ARGV[1] then
    return 0
  end
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[2])
if KEYS[3] ~= '' then
  redis.call('DEL', KEYS[3])
end
return 1
```

Behaviour to copy exactly: if the blob exists, parses, and its `id` differs from
`ARGV[1]`, **nothing** is deleted (the boundary already put a new session in the
slot). A missing or unparsable blob does **not** stop the script: the `ZREM` and the
pointer `DEL` still run. The pointer `DEL` is not checked against the device it
points to. Three keys in three hash slots: **not Redis-Cluster safe** (a
single-instance Redis is assumed; the plan lists this as an open question).
Ingest never runs this script (the test `packages/core/src/buffers/session-buffer.test.ts:424` asserts the receive path sends no
`EVAL`/`EVALSHA`); P3 owns it.

## P3.3 The `sessions` queue job

**Producers.** (1) **Boundary**, in the consumer when an event arrives more than
the idle timeout after the stored session's `ended_at`
(`packages/core/src/modules/ingest/src/incoming-event-handler.ts:466`): `enqueueSessionEnd({payload: baseEvent, closedSession})`
with `payload` = the **new** event's base payload (`packages/core/src/modules/ingest/src/incoming-event-handler.ts:472`), counted in
`session_ends_enqueued_total{source="boundary"}`; an enqueue error is logged and
ignored (the reaper will still catch the session). (2) **Reaper**
(`packages/core/src/modules/session/src/session-reaper.ts:145`), `source="reaper"`), with a payload rebuilt from the blob
(P3.6). Both call `deps.queues.sessions.session.add(sessionEndJobPayload(input), sessionEndEnqueueOptions(id))`
(`packages/core/src/modules/session/session.service.ts:480`).

**Job identity and options** (`packages/core/src/modules/session/src/session-end.ts:47`, queue defaults at
`packages/core/src/jobs.registry.ts:55`, mapping at `packages/core/src/jobs/producers.ts:168`):

| item | value |
|---|---|
| queue | `sessions` (Redis key `sessions`, `sessions-<QUEUE_NAMESPACE>`, `{sessions}` under `QUEUE_CLUSTER`; BullMQ prefix `bull`) |
| job name | `session` (`packages/core/src/modules/session/session.jobs.ts:86`) |
| jobId | `sessionEnd:v2:<sessionId>` (`packages/core/src/modules/session/src/session-end.ts:25`); the id has exactly three `:`-parts on purpose (BullMQ rule) |
| attempts | 3 (`packages/core/src/modules/session/src/session-end.ts:26`) |
| backoff | exponential, base 200 ms (`packages/core/src/modules/session/src/session-end.ts:27`): BullMQ delay = `round(2^(attemptsMade-1) × 200)` → 200 ms before attempt 2, 400 ms before attempt 3, no jitter (bullmq 6.7.8 `backoffs.js`) |
| removeOnComplete | `true` (record deleted at completion) |
| removeOnFail | `{age: 86400 s, count: 1000}` |
| worker concurrency | 1 (`packages/core/src/jobs.registry.ts:43`), env `SESSIONS_CONCURRENCY` |
| worker library defaults | lock 30 s renewed every 15 s, stalled check every 30 s, a stalled job is re-queued once (`maxStalledCount=1`) then failed (`packages/core/node_modules/bullmq/dist/esm/classes/worker.js:40`; library defaults, not repo choices) |

**Deduplication.** BullMQ ignores an `add` whose jobId still has a record in any
state. With `removeOnComplete: true` the id is free again as soon as the job
completes; the retained failed record (24 h / 1000) blocks re-enqueue of that id
until trimmed. The real idempotency guard is the Redis claim in `createSessionEnd`
(P3.4), not the jobId.

**Wire data** (what is stored as the job's `data` JSON; envelope
`packages/core/src/jobs/envelope.ts:28`): `{"payload": {"event": <IServiceCreateEventPayload>, "snapshot": <IClickhouseSession>}, "meta": {"requestId": "<id>"}}` (`meta` may be `{}`).
- `event` = the producer's payload with `projectId, deviceId, sessionId` overwritten from the
  **closed** session and `profileId = closedSession.profile_id || payload.profileId`
  (`packages/core/src/modules/session/src/session-end.ts:78`). `IServiceCreateEventPayload` fields (camelCase): `name, profileId, projectId,
  deviceId, sessionId, properties, groups, createdAt, duration, sdkName, sdkVersion, city,
  country, region, longitude, latitude, path, origin, referrer, referrerName, referrerType,
  os, osVersion, browser, browserVersion, device, brand, model, revenue` (`packages/core/src/modules/event/event.service.ts:197`). After BullMQ's JSON round trip
  `createdAt` is an ISO string, and `undefined` fields are absent.
- `snapshot` = the `IClickhouseSession` blob at close time (fields as in P2.5).
- Validation on the worker is a **shape check only**: `event.projectId, event.deviceId` and
  `snapshot.{id, project_id, device_id, ended_at}` must be strings (`packages/core/src/modules/session/session.jobs.ts:35`).
- A legacy shape `{type:'createSessionEnd', payload, snapshot}` is still accepted
  (`packages/core/src/jobs/compat.ts:64`); remove only when no such job can be replayed).

## P3.4 `createSessionEnd`, step by step

`packages/core/src/modules/session/src/session-end.ts:142`, called from the job handler, which then calls
`updateEventsCount` (P3.5) **whether or not an event was emitted**
(`packages/core/src/modules/session/session.jobs.ts:99`, errors there are logged and swallowed).

1. **Live lookup**: `GET session:<project>:<device>` (`getExistingSession`; unparsable
   or missing → none) (`packages/core/src/modules/session/src/session-end.ts:151`).
2. **Extended since enqueue?** If the live blob has the **same id** as the snapshot and
   `live.ended_at > snapshot.ended_at` (plain string comparison of the fixed-width
   `yyyy-MM-dd HH:mm:ss.SSS` values; the source comment says no fractional part, it does
   have milliseconds, still fixed width) → `session_ends_skipped_total{reason="extended_after_enqueue"}`,
   return nothing. The job completes (no retry), the blob stays, the reaper tries
   again after the session has been idle another deadman.
   At a **boundary** the slot already holds the *new* session (different id), so this
   never triggers and the snapshot is used.
3. `session = sameSession ? live : snapshot` (the `not_found` skip is unreachable because
   `snapshot` is mandatory).
4. **Claim**: `SET session:end:emitted:<project>:<device>:<sessionId> 1 EX 7200 NX`
   (`packages/core/src/modules/session/src/session-end.ts:32` = 2 h). Not acquired → `skipped{reason="already_emitted"}`,
   return nothing, **no cleanup is performed** (see hazards).
5. Metrics: `session_ends_emitted_total` +1; `session_duration_ms_on_close` observes
   `max(0, session.duration ?? 0)`; `session_events_on_close` observes
   `event_count + screen_view_count`.
6. `profileId = session.profile_id || payload.profileId`.
7. **Profile backfill entry**: if `profileId ≠ session.device_id` and
   `EXPERIMENTAL_PROFILE_BACKFILL` is on (and, when `EXPERIMENTAL_PROFILE_BACKFILL_PROJECTS` is
   non-empty, the project is listed): `RPUSH profile-backfill-buffer {"projectId","sessionId","profileId"}`
   (`packages/core/src/modules/session/src/session-end.ts:124`; the buffer swallows its own errors).
8. **Create the `session_end` event** via `createEvent` (below). The result's `document` is
   the row that was queued.
9. **Funnel notifications** (P3.4b), wrapped in try/catch: an error is logged
   (`"Creating notificatios for session end failed"`) and does not stop the job.
10. **Cleanup**: the Lua script of P3.2 with `(projectId, deviceId, sessionId, profileId)` of the
    session; an error is logged and swallowed.
11. Return the event row.

**Failure behaviour.** Only Redis errors in steps 1 and 4 (before the claim) can
realistically fail the job; after the claim every step is either caught (steps 9 and 10, and
the buffers' own `add` try/catch for step 7) or does no I/O that can reject (`createEvent`: a
synchronous array push plus an `add` that catches), so a retry after a post-claim failure
would see `already_emitted` and do nothing: the result is **at-most-once** for the `session_end` row and the cleanup. BullMQ
attempts 2 and 3 therefore matter only for pre-claim failures.

**The `createEvent` input** (`packages/core/src/modules/session/src/session-end.ts:223`): the job's
`event` spread first, then overrides:

| field | value |
|---|---|
| `name` | `session_end` |
| `sessionId` | `session.id` |
| `projectId, deviceId` | from the job's `event` (the closed session's) |
| `profileId` | step 6 |
| `createdAt` | `ended_at` parsed as UTC (`new Date(ended_at.replace(' ','T')+'Z')`) **+ 1000 ms** (`packages/core/src/modules/session/src/session-end.ts:36`) |
| `duration` | `session.duration ?? 0` (milliseconds) |
| `path` | `session.exit_path ?? ''` |
| `properties` | `{...event.properties, "__bounce": session.is_bounce}` (stored flattened: `"__bounce":"true"`/`"false"`) |
| **everything else** (`origin, referrer*, os*, browser*, device, brand, model, country, region, city, longitude, latitude, groups, sdkName, sdkVersion, revenue`) | the job's `event` **as is**, not the session's |

So the row depends on who enqueued it. **Boundary**: `event` is the first event of the
*next* session, so `origin`, geo, user agent, referrer, `groups`, `sdkName/sdkVersion`,
the other `properties` (`__query`, `__hash`, ...) and, when that event was a `revenue`
event, `revenue` all come from that *new* event. **Reaper**: `event` is rebuilt from the
blob (`origin = exit_origin`, `groups = session.groups`, `properties = {}`, `sdkName = ''`,
`sdkVersion = ''`, no revenue). A faithful port must keep both behaviours, or
consciously normalise them (a data change visible in dashboards that group
session_end rows by country/browser).

`createEvent` then (P2.5 event) builds the row: `id` = **ND-random** UUID (the payload
has none), `created_at` formatted from `createdAt`, `inserted_at` **ND-clock**, `profile_id`
as given, `properties` flattened with `toDots`; calls `deps.buffers.event.add(event)`
(synchronous, joins the 10 ms / 100-event micro-batch, **plain `add`**: no durability
gate, so a process that dies within ~10 ms of this call loses the event while the
claim is already set); and, because the name is `session_end` and `profileId` is
non-empty, `upsertProfile(..., isFromEvent=true)` with `id = profileId`,
`isExternal = profileId ≠ deviceId` and properties `path, country, city, region,
longitude, latitude, os, os_version, browser, browser_version, device, brand, model,
referrer, referrer_name, referrer_type` from the payload, undefined/empty removed
(skipped if `profile-cache:<project>:<id>` exists; **ND-clock** timestamps).
The session buffer is **not** touched: session_start/end events never extend or
create sessions (`packages/core/src/buffers/session-buffer.ts:172`).

### P3.4b Funnel notification checks

`packages/core/src/modules/session/src/session-end.ts:271`:
1. **Rules**: `getNotificationRulesByProjectId(projectId)`, a Postgres read cached in
   Redis under `cachable:getNotificationRulesByProjectId:[<projectId>]` (key rule
   `packages/redis/cachable.ts:218`: `cachable:<name>:<stringify(args)>` with whitespace removed), TTL **60×24 = 1440 s**
   (not 24 h; `packages/core/src/modules/notification/notification.service.ts:74`), empty arrays cached. Value = JSON array of
   `{id, name, sendToApp, sendToEmail, config, template, integrations: [{id}]}`. A
   rule is a funnel rule iff `config.type === 'funnel'` (`packages/core/src/modules/notification/notification.service.ts:176`);
   `config.events` is the ordered list of steps `{name, filters: [{name, operator, value[]}]}`.
2. **Gate**: no funnel rule, or `event_count + screen_view_count ≥ 500`
   (`packages/core/src/modules/session/src/session-end.ts:30`) → stop.
3. **Events**: one ClickHouse query on `events` (rendered `[ran]`-style by the repository
   builder; dates are `YYYY-MM-DD HH:mm:ss` strings **without milliseconds**, the
   window being `created_at − 1 s` to `ended_at + 1 s` **truncated to whole seconds**
   (`packages/core/src/modules/session/src/session-end.ts:292`, `packages/core/src/modules/session/src/session-end.ts:34`); client-default settings; format JSON):

```sql
SELECT *
FROM events
WHERE session_id = {p1:String}
  AND project_id = {p2:String}
  AND created_at BETWEEN {p3:String} AND {p4:String}
ORDER BY created_at DESC
LIMIT {p5:UInt64}
```

   The query sees only rows **already in ClickHouse**: at a boundary the closed
   session's last events can still be in the Redis event list (flush every 10 s), so
   a funnel step on a very recent event can be missed. Rows → `transformEvent`
   (`packages/core/src/modules/event/event.service.ts:294`; `createdAt` parsed as UTC, `country` padding stripped).
4. Prepend a synthetic `screen_view` built from the session
   (`transformSessionToEvent`, `packages/core/src/modules/event/event.service.ts:245`: `path = exit_path`, `createdAt = ended_at`,
   `properties = {is_bounce, __query: {utm_*}}`), sort **newest first** by `createdAt`
   (stable sort). The list always contains the synthetic event, so the `events.length > 0` guard in
   `packages/core/src/modules/session/src/session-end.ts:286` is always true: the check runs even when the query returned nothing.
5. Append `transformEvent(sessionEndEvent)` and call `checkNotificationRulesForSessionEnd`
   (`packages/core/src/modules/notification/src/notification-dispatch.ts:187`): sort **oldest first** (stable, in place),
   for every funnel rule walk the events once, greedily matching `config.events[i]` in
   order (`matchEvent`: `name` equal or `'*'`, then `matchEventFilters` with operators `is, isNot,
   contains, doesNotContain, startsWith, endsWith, regex, isNull, isNotNull, gt, lt, gte,
   lte` and the special `has_profile`; a filter with an empty value list passes;
   `packages/core/src/modules/notification/notification.service.ts:93`). Regex uses JavaScript `RegExp` semantics. Property
   filters read `pathOr('', name.split('.'), event).trim()`, which **throws** for a non-string
   (the synthetic event's boolean `is_bounce` and object `__query`) and aborts the whole
   check for the session (caught and logged by step 9 above). If the funnel completes,
   one notification is created per attached integration, plus one for `sendToApp` and one for
   `sendToEmail`.
6. **A notification** (`packages/core/src/modules/notification/src/notification-dispatch.ts:74`): data `{title, message, projectId,
   payload: {type: 'funnel', funnel: <matched events>}, integrationId, notificationRuleId, sendToApp, sendToEmail}`
   (title from `notificationTemplateFunnel`, message `"Project: <name>"` or empty; NUL
   characters stripped from the payload). If `sendToApp`: **insert the `Notification` row in
   Postgres** first. Then, always, `queues.notification.sendNotification.add({notification: data})`
   on the `notification` queue (envelope as in P3.3; no jobId, no deduplication, no
   attempts beyond the queue's default 1; queue defaults `removeOnComplete: 10`,
   `removeOnFail: {age 1 d, count 100}`; handler `packages/core/src/modules/notification/notification.jobs.ts:27`). An email or
   integration delivery has no row, so a redelivery would resend (comment in the file).
   Per the plan the Go worker hands this enqueue to the TS relay.

## P3.5 `updateEventsCount` (usage)

`packages/core/src/modules/session/src/usage.ts:43`. **At most once per hour per project, best effort**: the result
`true` is cached in Redis at `cachable:updateEventsCount:[<projectId>]` with TTL 3600 s
(`packages/core/src/modules/session/src/usage.ts:15`) by a plain `SETEX` **after** the work (not `NX`: two jobs for the same
project that start before the first finishes both run). An `undefined` result (organization
not found) is not cached. The in-process LRU in front of it is per request scope, so
effectively absent.

Steps (all Postgres through Prisma, all ClickHouse through `chQuery`):
1. `organization.findFirst({where: {projects: {some: {id}}}, include: {projects: true}})`; none → return.
2. **Organization count** (`packages/core/src/modules/organization/organization.service.ts:368`): period = `subscriptionCurrentPeriodStart/End`,
   or for status `trialing` the organization's `createdAt` → `subscriptionEndsAt`; no period or
   no projects → 0 without a query; else (rendered, `p1` = project ids, dates
   `YYYY-MM-DD HH:mm:ss`):

```sql
SELECT COUNT(*) AS count
FROM events e
WHERE project_id IN {p1:Array(String)}
  AND created_at BETWEEN {p2:String} AND {p3:String}
  AND name NOT IN {p4:Array(String)}
```

3. **Project count** (`packages/core/src/modules/project/project.service.ts:154`; transcribed from the source, the repository
   builder could not be imported by the one-off renderer): `SELECT sum(event_count) as count FROM
   distinct_event_names_mv WHERE project_id = {p1:String} AND name NOT IN {p2:Array(String)}` with `p2 =
   ['session_start','session_end']` (`packages/core/src/modules/project/project.service.ts:26`; the billing query uses the same two names,
   `packages/core/src/modules/organization/organization.service.ts:60`).
4. `if (projectEventsCount)` (truthy only) → `project.update({eventsCount: min(count, 2147483647)})`.
5. `if (organizationEventsCount)` (truthy only) → `organization.update` with
   `subscriptionPeriodEventsCount = min(count, INT4_MAX)` and
   `subscriptionPeriodEventsCountExceededAt` = (`nextExceededAt`, `packages/core/src/modules/session/src/usage.ts:23`): self-hosted → null; `count > limit` and
   no marker → now (**ND-clock**); `count ≤ limit` → null; otherwise the existing marker.
6. If not self-hosted: **usage alerts** (`packages/core/src/modules/session/src/usage.ts:103`): limit > 0; "exceeded" when
   `count > limit` and `usageExceededSentAt` is null; "near limit" when `count ≥ 0.8 × limit`,
   `count ≤ limit` and `usageWarningSentAt` is null. Claim first with a conditional
   `updateMany` (sets `usageExceededSentAt` and `usageWarningSentAt` for exceeded, only
   `usageWarningSentAt` for near-limit), then email every member whose stored role is in
   `STORED_ADMIN_ROLES` and user not deleted, through the React Email templates `usage-limit-exceeded`
   and `usage-near-limit`; on a failure of the whole send the claim is released. **The
   emails are rendered by TypeScript templates**: the Go port cannot send them without a
   decision (keep this step in TS, or have Go enqueue it).

## P3.6 The reaper

`packages/core/src/modules/session/src/session-reaper.ts:35`, every 5 min, on any worker (no global lock; one per-project lock).
Disabled when `SESSION_REAPER=0`.
1. `SMEMBERS session:projects`; empty → return.
2. For each project, sequentially; a thrown error is counted and logged, the next project
   continues (`packages/core/src/modules/session/src/session-reaper.ts:67`):
   - `SET session:reaper:lock:<project> 1 EX 60 NX`; not acquired → skip the project.
   - `cutoff = Date.now() − deadman` (**ND-clock**), deadman = `SESSION_REAPER_WALLCLOCK_DEADMAN_MS` ??
     `SESSION_TIMEOUT_MS` ?? 1 800 000.
   - `ZRANGEBYSCORE session:wallclock:<project> 0 <cutoff> LIMIT 0 <batch>`, batch =
     `SESSION_REAPER_BATCH_SIZE` (default 5000). Candidates are **not** removed here.
   - For each candidate deviceId, **sequentially** (`packages/core/src/modules/session/src/session-reaper.ts:122`): `GET session:<project>:<device>`;
     missing blob → `sessions_reaper_orphans_total{reason="deadman"}` +1, `ZREM` the member, not
     counted as reaped; present → enqueue (P3.3, payload below), `sessions_reaped_total{reason="deadman"}` +1
     and `session_ends_enqueued_total{source="reaper"}` +1; an enqueue error is logged and the
     member stays.
   - `ZCARD session:wallclock:<project>`; 0 → `SREM session:projects <project>`.
   - **finally** a plain `DEL session:reaper:lock:<project>` (not value-checked; if the 60 s lock
     expired and another worker holds it, this deletes theirs).
3. Summary log if anything was reaped or failed.

Because candidates stay in the zset until the session job's cleanup removes them, a session
whose job has not finished by the next tick (5 min) is enqueued again; the jobId dedup makes
that a no-op while the previous job record exists, and the claim makes it a no-op after.

**Reaper payload** (`packages/core/src/modules/session/src/session-reaper.ts:167`): `projectId, deviceId, sessionId, profileId = profile_id ?? '',
name 'session_end', properties {}, groups, createdAt = new Date(session.ended_at), duration, sdkName '',
sdkVersion '', city, country, region, longitude, latitude, path = exit_path, origin = exit_origin, referrer,
referrerName, referrerType, os, osVersion, browser, browserVersion, device, brand, model` (nulls become
`''` or absent). `createdAt` is built by `new Date("yyyy-MM-dd HH:mm:ss.SSS")`, i.e. parsed as
**local time** (invalid or shifted on a non-UTC host), but it is overwritten in step 8 of P3.4, so it
never reaches a row. A Go port should simply omit it.

Latency: the deadman uses the wall-clock time the *worker received* the last event, so a session
ends 30–35 min after its last event is **processed**, regardless of event timestamps
(comment `packages/core/src/modules/session/src/session-reaper.ts:28`).

## P3.7 The vacuum

`packages/core/src/modules/session/src/session-vacuum.ts:21`, daily, **no lock** (two workers racing both delete; harmless). Disabled when `SESSION_VACUUM=0`.
1. `SMEMBERS session:projects`; empty → return.
2. `cutoff = Date.now() − SESSION_VACUUM_STALE_THRESHOLD_MS` (**ND-clock**, default 7 days, `packages/core/src/modules/session/src/session-vacuum.ts:8`);
   per project `ZRANGEBYSCORE session:wallclock:<project> 0 <cutoff> LIMIT 0 <SESSION_VACUUM_BATCH_SIZE, default 1000>`
   (`packages/core/src/modules/session/src/session-vacuum.ts:6`). One batch per project per run; an error in one project is logged and the next goes on.
3. Each candidate: blob present → run the **cleanup Lua** with the blob's own id and profile id,
   `sessions_vacuumed_total{reason="stale_blob"}` +1; blob missing → `ZREM` the member,
   `{reason="missing_blob"}` +1.
It emits **no** `session_end` event and never removes a project from `session:projects` (the reaper does).

## P3.8 Metrics (names, labels, buckets)

Registered at import time on the core registry (`packages/core/src/modules/session/src/session.metrics.ts:29`):

| series | type | labels | buckets / note |
|---|---|---|---|
| `session_ends_enqueued_total` | counter | `source`: `boundary` \| `reaper` | |
| `session_ends_emitted_total` | counter | – | after the claim |
| `session_ends_skipped_total` | counter | `reason`: `not_found` \| `already_emitted` \| `extended_after_enqueue` | |
| `sessions_reaped_total` | counter | `reason`: `deadman` | |
| `sessions_reaper_orphans_total` | counter | `reason`: `deadman` | |
| `session_duration_ms_on_close` | histogram | – | 1 s, 10 s, 1 min, 5 min, 15 min, 30 min, 1 h, 1 d (ms values) |
| `session_events_on_close` | histogram | – | 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000 |
| `sessions_vacuumed_total` | counter | `reason`: `stale_blob` \| `missing_blob` | |
| `sessions_active_total` (gauge) | gauge | – | at scrape: sum of `ZCARD session:wallclock:<p>` over `session:projects` (one pipeline) |
| `sessions_projects_active` (gauge) | gauge | – | `SCARD session:projects` |
| `sessions_hwm_lag_ms` (gauge) | gauge | – | reads `session:hwm:<p>`, never written (always 0) |

(`packages/core/src/modules/session/src/session.metrics.ts:100` registers the three gauges only on consuming roles.)
`sessions_started_total{kind}` belongs to ingest (P1). BullMQ queue gauges (consuming roles only,
`packages/core/src/jobs/jobs.metrics.ts:89`): for every queue, `<queue>_<active|delayed|failed|completed|waiting>_count`
(name from the Redis key, non-alphanumerics → `_`, so `sessions_waiting_count`, `cron_waiting_count`);
`job_duration_ms{name,status}`, histogram buckets 10, 25, 50, 100, 250, 500, 750, 1000, 2000, 5000,
10000, 30000, observed **only for terminal failures** (`packages/core/src/jobs/workers.ts:90`; the label `name` is the Redis queue key; do not
add success timings to this series).

## P3.9 Environment variables

| variable | default | rule |
|---|---|---|
| `SESSION_TIMEOUT_MS` | 1 800 000 | positive int; also shared with ingest, and the e2e harness and the stack must agree |
| `SESSION_REAPER` | on | off only for `0` (`apps/api/src/config/env.ts:595`) |
| `SESSION_REAPER_BATCH_SIZE` | 5000 | positive int |
| `SESSION_REAPER_WALLCLOCK_DEADMAN_MS` | = timeout | positive int; boot fails if below `SESSION_TIMEOUT_MS` when both set (`apps/api/src/config/env.ts:742`) |
| `SESSION_VACUUM` | on | off only for `0` |
| `SESSION_VACUUM_BATCH_SIZE` | 1000 | positive int |
| `SESSION_VACUUM_STALE_THRESHOLD_MS` | 604 800 000 (7 d) | positive int; boot fails if ≤ the deadman (the timeout when no deadman) (`apps/api/src/config/env.ts:758`) |
| `EXPERIMENTAL_PROFILE_BACKFILL` | off | on only for exactly `1` |
| `EXPERIMENTAL_PROFILE_BACKFILL_PROJECTS` | empty (all) | comma list; boot fails if set while the flag is off |
| `SESSIONS_CONCURRENCY` | 1 | positive int |
| `QUEUE_NAMESPACE`, `QUEUE_CLUSTER` | unset, off | queue key forms (P3.3) |
| `DISABLED_CRON_JOBS` | none | `sessionReaper`, `sessionVacuum`, `flushSessions`, `flushReplay` are valid ids |
| `SELF_HOSTED` (via config) | – | selects the no-limits path in `nextExceededAt` and skips usage alerts |
| `DASHBOARD_URL` | `https://dashboard.openpanel.dev` for the alert link | |

## P3.10 Nondeterminism (mask these)

- **ND-random**: the `session_end` event `id` (`crypto.randomUUID()`); the request id minted for a
  scheduled job (21-char nanoid, `packages/core/src/logger.ts:26`); the flush/lock ids; ClickHouse query ids.
- **ND-clock**: `inserted_at` of the `session_end` row; profile timestamps from the profile upsert;
  the wallclock ZSET scores; the reaper and vacuum cutoffs; `nextExceededAt` and `claimedAt` in usage;
  BullMQ timestamps; the 2 h and 1 h TTL starts.
- **Deterministic given inputs**: `createdAt` of the `session_end` row = `ended_at` + 1 s;
  duration; path; `__bounce`; the claim key; the job id; backoff delays (no jitter).
- Event query tie order: `ORDER BY created_at DESC` with equal timestamps has no defined
  order in ClickHouse; the funnel match depends on it only when two events share a millisecond.

## P3.11 Hazards and open questions (P3)

1. **Claim before write; no cleanup on "already emitted"** (P3.4 step 4): if the process dies after the
   claim and before the micro-batch is pushed, the `session_end` is lost and the blob stays; later
   reaper ticks re-enqueue the session, each job finds the claim and returns without cleanup, until the
   claim expires after 2 h and a **second** job emits the event (the blob is still there). Vacuum
   would finally delete it after 7 days. The Go port should at least await the `RPUSH` before
   treating the event as written, and decide whether `already_emitted` may still run the cleanup.
2. **Plain `add` in the job**: no durability gate (see P3.4); TS relies on the shutdown flush only.
3. **`session_end` rows copy the triggering event** at a boundary (P3.4): revenue and geo of the next
   session can land on the closed one. Mirror or fix on purpose.
4. **The funnel query can miss unflushed events** and truncates its window to whole seconds.
5. **`pathOr(...).trim()` throws for non-strings** in funnel filters (P3.4b step 5).
6. **Usage alert emails need the TS email templates** (P3.5); **`updateEventsCount` also runs for
   skipped closes** and its once-an-hour guard is not atomic.
7. **Reaper lock release is an unconditional `DEL`**; reaper and vacuum use wall-clock cutoffs, so a
   host clock jump closes or keeps sessions wrongly.
8. **`session:hwm:*` gauge has no writer** (always 0): either drop it or implement the writer; the
   plan says metric names must not change.
9. **Vacuum cron timezone** depends on the container's TZ.
10. **Cleanup script is not Redis-Cluster safe** (3 keys, no hash tags).
11. Open: Go consumes `sessions` how? The plan puts a Redis Stream relay between Go and BullMQ; the
    job's idempotency (claim) is independent of the transport, but the **retry/backoff** (3 attempts,
    200/400 ms) and the jobId dedup are BullMQ features the relay must replace.
12. Open: who sends the usage-alert emails and the funnel notification rows after the move
    (Postgres insert + BullMQ enqueue are both in the TS notification path today).

## P3.12 ClickHouse 26.5+ check (P3)

The session lifecycle's ClickHouse reads are: the funnel `SELECT * FROM events ...` query, the project
event count on `distinct_event_names_mv` and the billing `COUNT(*)` on `events`. None uses a
version-gated feature; the funnel query was rendered by the repository builder; the billing count was
rendered the same way (`[ran]` rendering only, I did not execute the two count queries). With `CLICKHOUSE_CLUSTER`
the reads hit the Distributed tables (`events`), which the funnel query's `session_id`/`project_id`
filters handle without `GLOBAL` (no subqueries). Nothing here depends on 26.5.


---

## How this was checked

- **Read**: every file cited above, at the snapshot named at the top; the plan (`docs/GO_WORKER.md`), `packages/core/AGENTS.md`
  and `apps/ingest/README.md` first.
- **Citations**: generated by anchor search and re-verified by a second, independent pass that re-reads each file and
  checks that the cited line still contains its anchor text (224 citations). Re-run it after any change to
  the cited files; a drifted line fails the check. (The generator and checker are scratch scripts, not committed.)
- **Ran against the shared local stack, read-only except where stated**: `SELECT version()` (26.3.67.30);
  `system.settings` lookups for the settings named in P2.4 and P2.11; the profile lookup (rendered query, real
  profile ids, JSON result shape); `redis-cli` `--scan` and `HGETALL bull:cron:meta` and `LLEN event_buffer:queue`
  (pause flag key shape); superjson output of the event-batch payload; the repository's `sql` builders and buffer
  classes with a stub client to render the SQL text in this document (a one-off script; the project-count query
  could not be imported and is transcribed). **Created and dropped** the throwaway database
  `openpanel_wparity_flush` (one small table) for the lightweight `UPDATE` check and the stream-retry repro;
  confirmed afterwards that no `openpanel_wparity_*` database remains. Nothing else was created, restarted,
  migrated or reseeded; `.env` and `docker/data` were not touched.
- **Not run (needs staging)**: anything with `CLICKHOUSE_CLUSTER=true` (the `ON CLUSTER` UPDATE, Distributed
  forwarding); Redis Cluster behaviour of the cleanup script; the namespaced/braced BullMQ key forms; the
  production container timezone; a real flush of each buffer end to end (that is the P2 shadow comparison); the
  stream-retry hazard against production traffic patterns.
