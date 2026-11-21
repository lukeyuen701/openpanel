# @openpanel/ingest — the Go event receiver

A Go service that receives what the SDKs send and writes it to the same Kafka
topic the Bun API writes to. The worker (`apps/api` with `ROLE=worker`)
consumes those records as it consumes the API's. Every request the receiver
does not handle itself goes to the Bun API unchanged, so it can sit in front
of the whole `/api` path or only in front of the ingest routes.

It exists because `/track` is the CPU hot path: one Bun process tops out at
one core. On the same machine, against the same Redis and Redpanda, the
receiver answered 3.6× the requests at the same CPU cost per request (see
[Benchmark](#benchmark)).

## What it handles, and what it hands to the Bun API

| Request | Here | Why |
|---|---|---|
| `POST /track`, `type: "track"` | **yes** | the hot path: Kafka only |
| `GET /track/device-id` | **yes** | reads Redis only |
| `POST /event` (legacy) | **yes** | Kafka only |
| `identify`, `increment`, `decrement`, `alias`, `replay`, `group`, `assign_group`, `ai` | Bun API | profile, replay and group buffers, ClickHouse reads, AI normalisation: state the worker's TypeScript owns |
| a `track` whose properties carry `__identify` | Bun API | it writes the profile |
| a user agent on the bot list (`POST`) | Bun API | the bot event goes to the bot buffer |
| a body that fails the schema, is not JSON, or is not `application/json` | Bun API | zod's own 400 message |
| `__timestamp` other than a number, an ISO date, or an ISO date-time with `Z`/`±HH:MM` | Bun API | JavaScriptCore's `new Date()` reads local-time and RFC 2822 forms this port does not reproduce |
| non-ASCII header values, an `Origin` that is not `http(s)://host`, a body over `INGEST_MAX_BODY_BYTES` or without `Content-Length` | Bun API | Bun decodes headers as Latin-1 and parses URLs the WHATWG way |
| `OPTIONS` and every other path | Bun API | |

Every decision to forward is taken before the first side effect (the dedupe
lock, the auth caches, Kafka), so a forwarded request reaches the API exactly
as sent and is never counted twice. `ingest_forwarded_total{reason}` on the
admin port says how often each reason fires.

## What "the same" means

The receiver is a port, not a rewrite: for the requests it handles, the HTTP
answer and the Kafka record are the ones the Bun API produces.

- **The record**: the same JSON, byte for byte (key order and number
  formatting included), except the random event `id` and, for server events
  with no profile, the random partition key. Same key, same partition:
  kafkajs's murmur2 mapping, held to `partitioner.fixture.json`.
- **The answer**: status, body, content type and CORS headers. The only
  difference is the order of the request header names echoed in
  `Access-Control-Allow-Headers`/`-Expose-Headers` on non-preflight
  responses, which browsers do not read.
- **Shared state**: it reads the API's Redis entries (`cachable::[<client>]`,
  `cachable:op:salt:[]`, `cachable:getOrganizationByProjectId:[<project>]`,
  `session:<project>:<device>`) and falls back to Postgres; it writes only
  what the API writes with the same meaning (the dedupe lock and the
  `client:auth:*` verification cache), so a request may land on either
  process.
- **User agents**: ua-parser-js 2.x and OpenPanel's corrections, ported;
  ua-parser-js's tables and the bot list are generated from the installed
  packages (`bun run gen`), never copied by hand. JavaScript regexes run on
  an ECMAScript engine, with Go's linear-time engine and literal prefilters
  used only where they provably give the same answer.

How that is checked:

| Check | Command | Needs |
|---|---|---|
| Unit tests against TypeScript goldens (UA, bots, ids, timestamps, origins, IPs, schema, JSON, hashes, geo) | `bun run --filter @openpanel/ingest test` | Go |
| Generated tables match the installed packages | `bun run --filter @openpanel/ingest gen:check` | Bun |
| Regenerate the goldens after an upstream change | `bun run --filter @openpanel/ingest golden` | Bun |
| Differential run: ~80 requests, byte for byte, against a Bun API and the receiver, comparing answers and Kafka records | `bun run --filter @openpanel/ingest parity` | the local stack, GeoLite files (`bun run --filter @openpanel/core codegen`); `DOCKER_CONTEXT` and `PARITY_RP_CONTAINER` (default `openpanel-op-rp-1`) say where `rpk` runs |
| Full UA corpus (40k user agents) | `INGEST_GOLDEN_UA=<ua-full.json> go test ./internal/useragent ./internal/bots` | a corpus, see `parity/golden-ua.ts` |

The session lifecycle e2e (`apps/api/e2e/session-e2e.ts`) also passes with
`E2E_API_URL` pointed at the receiver.

## Configuration

It reads the API's own variables, so one `.env` serves both: `DATABASE_URL`,
`REDIS_URL`, `KAFKA_*` (brokers, topic, client id, SASL/TLS, producer
retries, linger, max message bytes), `SESSION_TIMEOUT_MS`, `IP_HEADER_ORDER`,
`SELF_HOSTED`, `NODE_ENV`, `LOG_LEVEL`. Its own:

| Variable | Default | |
|---|---|---|
| `INGEST_UPSTREAM_URL` | required | the Bun API, e.g. `http://op-api:3000` |
| `INGEST_PORT` / `INGEST_HOST` | `3000` / all interfaces | the public port |
| `INGEST_ADMIN_PORT` | `3001` | `/healthz/live`, `/healthz/ready`, `/metrics`, `/debug/pprof`; keep it off the edge |
| `INGEST_GEO_DIR` | `.` (`/app/geo` in the image) | where `GeoLite2-City.mmdb` and `GeoLite2-ASN.mmdb` are |
| `INGEST_MAX_BODY_BYTES` | 4 MiB | larger bodies go to the API unread |
| `INGEST_REDIS_TIMEOUT_MS` | 1000 | per Redis command |

## Running it

```bash
# Local, next to bun run dev (api on :3333):
INGEST_PORT=3340 INGEST_UPSTREAM_URL=http://127.0.0.1:3333 INGEST_GEO_DIR=../../packages/core \
  bun run --filter @openpanel/ingest dev

# Image; the GeoLite files come from the public redistribution, or from
# MaxMind with a license key passed as a build secret:
docker build -t openpanel-ingest apps/ingest
docker build --secret id=maxmind_license_key,env=MAXMIND_LICENSE_KEY -t openpanel-ingest apps/ingest
```

In front of a self-hosted install, run the image next to `op-api` with the
same `.env` plus `INGEST_UPSTREAM_URL=http://op-api:3000`, and send the
ingest routes to it in the Caddyfile, next to the existing `handle_path /api*`
block (Caddy tries the longer paths first; this routing was checked against
`caddy:2-alpine`):

```
handle /api/track* {
    uri strip_prefix /api
    reverse_proxy op-ingest:3000
}
handle /api/event {
    uri strip_prefix /api
    reverse_proxy op-ingest:3000
}
```

The self-hosting templates are unchanged: no published image exists yet, and
the receiver is opt-in.

## Benchmark

`go run ./cmd/loadgen` against each receiver in turn, same machine (Apple
M-series, Redis and Redpanda in a local VM, load generator on the same box),
each producing to its own topic with no consumer, 20 s per run:

| Auth | Concurrency | Bun API | Go receiver |
|---|---|---|---|
| Origin (web SDK) | 50 | 4,046 req/s, p99 27 ms, 1.0 core | 7,656 req/s, p99 16 ms, 1.7 cores |
| Origin (web SDK) | 200 | 3,812 req/s, p99 86 ms, 0.9 core | 13,566 req/s, p99 31 ms, 2.5 cores |
| Secret (server SDK) | 100 | 6,949 req/s, p99 30 ms, 1.0 core | 7,880 req/s, p99 71 ms, 1.2 cores |

CPU per request is the same order on both (0.15–0.25 ms); the gain is that
the receiver uses more than one core. With secret auth neither side used
much more than a core, so something other than CPU bounds that row; which
it is was not isolated. These are one machine's numbers, with the load
generator on the same box and nothing consuming the topic.

## The Go worker

`/app/worker` (`cmd/worker`, built into the same image) is the Bun worker's
hot half in Go: the events consumer, the session lifecycle and the buffer
flushes. It is a separate process from the receiver, so `/track` keeps
accepting events while ClickHouse is down or the worker is restarted. The
migration plan, rollout and rollback are in `docs/GO_WORKER.md`; the behaviour
it must match is specified in `docs/GO_WORKER_CONTRACTS.md` (events path) and
`docs/GO_WORKER_CONTRACTS_FLUSH.md` (flushes and sessions).

### What it runs

Any subset of three parts runs in one process; a process with none refuses to
start.

| Part | Switched on by | What it does |
|---|---|---|
| Events consumer | `ROLE=worker` (or `all`) and `events` in an explicitly set `ENABLED_QUEUES` (unlike the Bun worker, an unset `ENABLED_QUEUES` does not start it, so a Go container sharing the Bun env file never joins the group by accident) | joins the consumer group `openpanel-events` (the same group protocol name as the Bun consumer, so Bun and Go members share one group and split its partitions), turns each record into the event row, session, profile and AI buffer writes, project filters, first-event marker, notification rules, and enqueues the `sessions` and `notification` BullMQ jobs |
| BullMQ queues | `GO_WORKER_QUEUES` | consumes the `sessions` queue (session end, the funnel notification check, the usage count) with BullMQ's own Lua scripts; a Bun worker can consume the same queue at the same time |
| Cron jobs | `GO_WORKER_CRON_JOBS` | `flushEvents`, `flushSessions`, `flushProfiles`, `flushProfileBackfill`, `flushBots`, `flushReplay`, `flushGroups`, `flushAiEvents`, `sessionReaper`, `sessionVacuum`; each takes the same `lock:<buffer>` Redis lock as the Bun flush, so the two never flush one buffer at once |

`ROLE=api` or `DISABLE_WORKERS` runs nothing. The ids in `GO_WORKER_CRON_JOBS`
are the Bun registry's ids (the list `DISABLED_CRON_JOBS` accepts); an id the
Go worker has no implementation of fails the boot.

### What stays in the Bun worker

The Go worker does not consume or run: `notification` (sending notifications
and e-mails: the React Email templates are TypeScript), `import`,
`insights`, `gsc`, `cohortCompute`, and the cron jobs that are not flushes,
the reaper or the vacuum (`salt`, `insightsDaily`, `weeklyDigest`,
`cohortRefresh`, `delete`, `windDown`, `onboarding`, `flushExports`, ...).
Those are low-volume business logic shared with tRPC, and a second copy would
have to be kept in step with every upstream change. The Go worker still
**writes** to two of these queues: it enqueues `sendNotification` jobs, and
the usage count enqueues a `sendUsageAlert` job on `notification` instead of
sending the e-mail itself, so a Bun worker consuming `notification` must run
a version that has that job before Go takes the `sessions` queue. At boot it
also writes the queue meta hash of the queues it never adds to, as the Bun
worker does.

### Configuration

It reads the Bun API's variables with the same names, defaults and parsing
(`apps/api/src/config/env.ts`; a malformed boot-critical value fails the
start and every problem is listed at once), so one `.env` serves both:
`ROLE`, `ENABLED_QUEUES`, `DISABLE_WORKERS`, `QUEUE_NAMESPACE`,
`QUEUE_CLUSTER`, `API_PORT`, `LOG_LEVEL`, `SHUTDOWN_FORCE_EXIT_MS`,
`DATABASE_URL`, `REDIS_URL`, `REDIS_CACHE_COMMAND_TIMEOUT_MS`,
`CLICKHOUSE_URL` (and `CLICKHOUSE_CLUSTER`, `CLICKHOUSE_SETTINGS`,
`CLICKHOUSE_SETTINGS_REMOVE_CONVERT_ANY_JOIN`, `CLICKHOUSE_REQUEST_COMPRESSION`,
`CLICKHOUSE_MAX_OPEN_CONNECTIONS`, `CLICKHOUSE_REQUEST_TIMEOUT_MS`,
`CLICKHOUSE_UNHEALTHY_MARK_MS`), every `KAFKA_*` variable of the consumer
(`KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP`, `KAFKA_EVENTS_TOPIC`,
`KAFKA_PARTITIONS_CONCURRENT`, the handler retry settings, SASL and TLS),
`INGEST_DEAD_LETTER_MAX_ENTRIES`, `SESSION_TIMEOUT_MS`, the buffer sizing
(`EVENT_BUFFER_*`, `SESSION_BUFFER_*`, `PROFILE_BUFFER_*`,
`PROFILE_BACKFILL_BUFFER_BATCH_SIZE`, `BOT_BUFFER_BATCH_SIZE`,
`REPLAY_BUFFER_*`, `GROUP_BUFFER_*`, `AI_EVENT_BUFFER_*`,
`BUFFER_ASYNC_INSERTS`, `BUFFER_CH_INSERT_CONCURRENCY`), the session
lifecycle (`SESSION_REAPER*`, `SESSION_VACUUM*`,
`EXPERIMENTAL_PROFILE_BACKFILL` and `_PROJECTS`, `SESSIONS_CONCURRENCY`) and
`SELF_HOSTED`. Its own:

| Variable | Default | |
|---|---|---|
| `GO_WORKER_QUEUES` | empty | BullMQ queues this process consumes. Only `sessions` is accepted; leave `sessions` in the Bun worker's `ENABLED_QUEUES` as well to share the queue |
| `GO_WORKER_CRON_JOBS` | empty (none) | cron job ids this process runs, comma separated; list the same ids in the Bun worker's `DISABLED_CRON_JOBS` |

The ops port is `API_PORT` (default 3000), the same as the Bun worker's:
`GET /healthz/live`, `GET /healthz/ready` (503 while booting, shutting down,
or when the consumer loop has not ticked for 60 s), `GET /metrics` and
`GET /debug/pprof/` (`profile`, `trace`, `symbol`). Keep it off the edge. The
image's own healthcheck and entrypoint are the receiver's, so a worker service
overrides both (`entrypoint: ["/app/worker"]`, healthcheck
`["CMD", "/app/worker", "healthcheck"]`, which asks `/healthz/ready` on
`API_PORT`). SIGTERM stops consuming, finishes the running jobs, flushes the
event buffer and leaves the consumer group; if the buffer cannot be flushed
it exits without leaving, so the broker redelivers what was not committed.

The metrics keep the Bun worker's names and labels (`kafka_events_*`,
`buffer_*`, `sessions_*`, `session_ends_*`, `<queue>_waiting_count` and
the other queue gauges), because `docs/SCALING.md` and the load scripts read
them. The flush series (`buffer_flush_*`, `buffer_ch_*`) exist only once a
flush job runs in the process.

### How it is checked

| Check | Command | Needs |
|---|---|---|
| Unit tests against TypeScript goldens (JavaScript semantics, notification matching, consumer, BullMQ wire format, event handler) | `bun run --filter @openpanel/ingest test` | Go |
| Regenerate those goldens after an upstream change | `golden:jsport`, `golden:notify`, `golden:consumer`, `golden:bullmq`, `golden:events` (same `--filter @openpanel/ingest`) | Bun, the local stack for `golden:events` |
| Events path, Bun worker against Go worker: the same Kafka records, then the Redis, BullMQ and Postgres state each leaves, diffed (19 hand-written and 1,970 generated records on 4 partitions) | `bun run --filter @openpanel/ingest worker-parity:go` (`worker-parity` compares two Bun workers, to show which fields are nondeterministic) | the local stack |
| Flushes, Bun buffers against Go: the same Redis lists flushed by each into its own ClickHouse database, tables and leftover Redis state compared; the lock shared by both | `WPARITY_CH=<clickhouse url> go test ./internal/worker/flush/...` (`WPARITY_BUN` names the bun binary; Redis is parity database 13) | ClickHouse, Redis, Bun |
| Session lifecycle: session end variants, funnel notification, reaper, vacuum, usage count and alert, Bun handlers against Go | `bun parity/p3-diff.ts` from `apps/ingest` with the repository `.env` loaded | the local stack, Go |
| Consumer against a real broker, Redis dead-letter list | `WPARITY_KAFKA=<broker> go test ./internal/worker/consumer`, `WPARITY_REDIS=redis://<host>/11` | Redpanda, Redis |

The rigs create only what is prefixed `wparity` (topics, groups, Redis
databases 11 to 15, BullMQ namespaces, `openpanel_wparity_*` databases,
`wparity-*` Postgres rows) and remove it on exit. The `-race` build of the
worker ran the events rig without a data race.

### Where it differs from the Bun worker, on purpose

- **Profile backfill**: 200 sessions per `UPDATE`. ClickHouse refuses more
  than 1,000 form fields, which the Bun flush hits above about 245 sessions.
- **Insert bodies are rebuilt on every attempt.** The Bun client reuses a
  stream, so a retry after a failed node can send an empty body that
  ClickHouse accepts and the flush then trims the rows from Redis. The Go
  client builds the body again, and fails over between nodes.
- **One lock-loss rule for every flush.** A flush counts its lock as lost
  only when the extend script says another owner holds it, or when ownership
  cannot be confirmed before the lock would expire; a transport error on the
  keepalive is retried. A flush that lost its lock inserts nothing, and rows
  that already landed are always trimmed, so a flaky keepalive cannot insert a
  session batch twice.
- **`cachable:*` is read only.** Project lookups read the shared entries, fall
  back to Postgres and keep the result in a 60 second in-process cache; they
  never write the shared key (the receiver does the same). The exception is
  the usage count's own `updateEventsCount` marker, and the `DEL` of a
  project's key after its first event.
- **Timestamps**: a record whose `timestamp` is a string outside the ISO 8601
  forms is not interpreted (JavaScriptCore's legacy parser reads month
  names, slashes and RFC 2822). It fails the handler like any other error and,
  after the retries, is dead-lettered with its payload, where it can be
  replayed after a fix.
- **Regexes** in project filters and notification rules run on Go's `regexp`
  instead of RE2J; both are RE2, and every golden case agreed.
- **Usage alerts**: queued for the Bun `sendUsageAlert` job rather than sent.

### TypeScript defects it reproduces on purpose

So that both implementations leave identical state and a rollback finds what
it expects (`docs/GO_WORKER_CONTRACTS.md` P1.17 and
`docs/GO_WORKER_CONTRACTS_FLUSH.md` P2.10, P3.11 have the full lists):

- a handler retry or a redelivered batch writes the event row again with the
  same id; the session is already advanced, so `session_start` is not emitted
  again and `event_count` advances twice;
- a non-string value at a filter or rule path (for example `value: 5`) makes
  the project filter dead-letter the event and silently skips the notification
  rules of that event;
- a notification is created again on every retry;
- a record with no `headers` object, or a non-string `request-id`, fails
  three times and is dead-lettered;
- a server, past or AI event with no live session loses its referrer;
- a notification template replaces `$&`-style patterns inside property
  values, as `String.replaceAll` does;
- a poison line in a raw-passthrough buffer (event, bot, replay, group, AI)
  blocks that buffer, and a chunk failing after earlier chunks landed
  duplicates rows.

The two that lose data (the redelivery state in the first item, and the empty
retry body fixed above) are to be fixed in the TypeScript worker as well, in
separate commits; the Go worker adds no new loss path.

## Layout

```
cmd/ingest      the service
cmd/loadgen     the load generator above
cmd/worker      the Go worker (see The Go worker)
internal/
  server        routes, forwarding, CORS, metrics
  track         the pipeline: schema, timestamps, bot marking, the Kafka record
  clientauth    the ingest auth tier: client lookup, secret verification, origin rule
  identity      device id, session id, live session lookup
  useragent     parseUserAgent (ua-parser-js port + OpenPanel's corrections)
  bots          the bot list
  geo           MaxMind lookups and the datacenter ASN set
  jsjson        JavaScript-ordered JSON, JSON.stringify, fast-json-stable-hash
  jsregex       JavaScript regexes on regexp2, with RE2 and literal fast paths
  kafka, kv, lookups, dedupe, reqheaders, config
  worker/       the Go worker: config, consumer, wire, events, projects, notify, bullmq,
                chclient, flush, sessions, jsport
scripts/gen-tables.ts   exports ua-parser-js's regex map, bots.ts and the ASNs
parity/                 golden generators and the differential runner
```

ua-parser-js is AGPL-3.0-or-later, as OpenPanel is; the generated
`internal/useragent/data/regexes.json` is derived from it.
