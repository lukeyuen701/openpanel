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

## Layout

```
cmd/ingest      the service
cmd/loadgen     the load generator above
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
scripts/gen-tables.ts   exports ua-parser-js's regex map, bots.ts and the ASNs
parity/                 golden generators and the differential runner
```

ua-parser-js is AGPL-3.0-or-later, as OpenPanel is; the generated
`internal/useragent/data/regexes.json` is derived from it.
