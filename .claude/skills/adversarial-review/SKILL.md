---
name: adversarial-review
description: Adversarially review an OpenPanel change set — uncommitted changes, a commit range, a branch or a PR — for logic breaks, cross-layer seams (TS ↔ Go, producer ↔ consumer, SDK ↔ ingest, docs ↔ code) and departures from industry best practice, WITHOUT modifying code, and file the conclusions as a dated record under docs/reviews/. Use when asked for an adversarial review / audit, "对抗性检查/审查", "逻辑断裂点", "是否达到业界最佳实践", "审查未提交的修改", or a pre-merge / pre-release review.
---

# Adversarial review

The job is to find what is WRONG, prove it from the code, and leave a record —
not to fix it. A review that edits code is no longer a review: the person asked
for a list they can judge.

## Hard rules

- **Read-only.** No edits, no `npx ultracite fix` / `bun run fix`, no
  regenerating (`golden*`, `gen:bots`, `gen:referrers`, `codegen`), no
  `migrate:deploy`, no `seed --reset`, no restarting supervised processes, no
  `git stash` / `checkout` / `reset`. Allowed: `git diff` / `log` / `show`,
  reading, grep, `bun run typecheck`, `bun run check:deps`, targeted
  `bun test --isolate <file>` in `packages/core`, `go build ./...` / `go vet` /
  `go test -run X` in `apps/ingest`, `bunx vitest run` in `apps/start`, the ingest `gen:check`, and read-only
  queries against your own worktree's databases. The ONE write is the record in
  `docs/reviews/` (below).
- **No production, no shared data.** Only the databases, Redis db and Kafka
  topics your `WORKTREE.md` / env name. Never a live third-party provider
  (billing, email, LLM, GSC): judge those paths by reading and by the mocked tests.
- **Every Medium-or-worse claim is re-read by you before it is reported.**
  Reviewer line numbers drift, and reviewers reason about the other side of a
  boundary they did not read — the TS side of a Go port, the consumer of a
  queue payload, the shard layout behind a query. Mark each finding CONFIRMED
  (read the code, or reproduced) or INFERRED (reasoned, not reproduced).

## 1. Fix the scope

```bash
git status --short            # ?? = untracked: git diff does NOT show them
git diff --stat | tail -3     # or: git diff --stat <base>...HEAD for a branch / range
git log --oneline -1          # the baseline commit for the record
```

For a PR, `gh pr diff <n>` (pass `--repo` when the default remote is not the
PR's). Untracked files are usually the NEW heart of the change (a new module,
a migration, a Go package) — read them whole. Generated and vendored files
(the excluded paths in `biome.json`, Go goldens, the vendored BullMQ Lua,
referrer / bot tables, Prisma output) are checked against their source and
generator, not read line by line.

Then read, before splitting anything — these are the CLAIMS and invariants to
test:

- root `AGENTS.md` and `packages/core/AGENTS.md` (R1–R22, the `sql` tag, the
  layer order, the exports map, `mock.module` idiom);
- `apps/ingest/README.md` and `docs/GO_WORKER*.md` when the diff touches ingest,
  buffers, sessions or anything the Go receiver / worker ports;
- the commit bodies of the range (`git log --format=%B <range>`) and any doc the
  diff adds or edits.

## 2. Split by layer and run reviewers in parallel

Up to five reviewers for a large diff; fewer (or none — do it yourself) for a
small one. Launch them in ONE message (Agent tool, `general-purpose`), each
with disjoint files. A split that fits this repo:

| Reviewer | Owns |
| --- | --- |
| Write path | `modules/ingest`, `modules/event` (`createEvent`), `buffers/*`, `modules/session`, `jobs/`, `jobs.registry.ts`, Kafka / BullMQ wiring in `apps/api` |
| Go receiver & worker | `apps/ingest` (`cmd/*`, `internal/*`, parity rigs, goldens vs generators, Dockerfile) |
| Queries & data | `packages/db` (ClickHouse client, `sql` tag, Prisma schema), `code-migrations/`, chart / report / overview / cohort / insight services |
| API surface | `*.rpc.ts`, `*.routes.ts`, `rpc/base.ts`, `modules/auth/src/access.ts`, MCP tools, the public / manage REST API |
| Clients & ops | `apps/start`, `packages/sdks/*`, `self-hosting/*` (compose, CLI doctor), env schema and docs |

Reviewer prompt skeleton (fill the brackets):

```
ADVERSARIAL CODE REVIEW of [scope] in [repo path]. STRICTLY READ-ONLY: no edits,
no formatters, no generators, no migrations, no git stash/checkout/reset. You may
git diff, read, grep, run `bun run typecheck`, `bun test --isolate <file>` in
packages/core, `go build ./... / go vet / go test -run X` in apps/ingest.
First read [AGENTS.md files / docs that govern these files].
YOUR FILES: [list; modified → git diff -- <file>; new → read whole].
HUNT FOR: [the checklist rows below that apply to this layer].
OUTPUT: numbered findings — severity (High/Medium/Low/Nit), file:line, the
defect in one sentence, a CONCRETE failure scenario (state/input → wrong
result), confidence (Confirmed by reading code / Plausible). Verify every
claim. Then a short "checked and found correct" list. Answer in English.
```

## 3. The join — your job, not a reviewer's

Reviewers split by layer cannot see a break that lives BETWEEN layers. Do
these yourself:

- **TS ↔ Go parity.** A change to `modules/ingest`, `parseUserAgent`, `bots.ts`,
  `event.service`, the buffers or the session lifecycle must be mirrored in
  `apps/ingest` (receiver or worker) — or the record says why not. Same Kafka
  record bytes, same Redis keys and Lua flush locks, same BullMQ job names,
  payloads and options, same ClickHouse rows. Name the parity gate that would
  catch the drift (`parity`, `worker-parity:go`, `--full`, `--mixed`) and say
  whether it was run.
- **Producer ↔ consumer.** For every queue payload, Kafka record or Redis
  structure the diff touches: who writes it, who reads it, and what happens
  when an OLD producer meets a NEW consumer and the reverse (rolling deploy,
  jobs already queued, buffers already holding rows).
- **Status ↔ client handling.** For each new write path, list every outcome the
  server can produce (validation error, 401/403/404, 429, 5xx, timeout,
  partial insert) and what the caller does with it — the SDK retries / drops,
  the dashboard toasts / spins, the job retries / dead-letters. A permanent
  refusal retried forever and a retriable failure dropped are both bugs, and
  neither half looks wrong alone.
- **Retries replay the whole thing.** Any retry (ClickHouse insert, job
  attempt, Kafka redelivery) — does it resend the full payload, and is the
  write idempotent or deduplicated if the first attempt actually landed?
- **Claims vs code.** Every sentence in the commit bodies, the docs the diff
  edits and the env docs that states a behaviour ("never loses events", "safe
  to rerun", "byte-identical", "opt-in") — find the code that makes it true.

## 4. Checklist (OpenPanel-specific)

**Data loss & duplicates on the hot path** — a buffer that trims rows it did
not insert, or inserts after losing its lock; a parse failure that drops a row
silently instead of dead-lettering it; a Kafka offset committed before the
write is durable; session start/end emitted zero or two times; a retry that
resends a consumed stream. Per-request work on `/track`, the consumer or the
buffers that could have been per-boot.

**ClickHouse** — production is 2 shards × 2 replicas: a plain `IN (subquery)`
on a `Distributed` table is per-shard (use `GLOBAL IN` or a join); every query
filters by `project_id`; dynamic SQL only through the `sql` tag / `{name:Type}`
(a `${}` of a raw value is a defect); unbounded scans (no time range, no
LIMIT); request size limits (`http_max_fields`, max query size) on generated
batches; new SQL that was never executed against a real server; a replaced
query whose old/new results were not diffed.

**Migrations** — the api serves while ClickHouse code migrations run, all
pending, in file order: each one safe to rerun from any point; a data move
correct while ingestion reads and writes the table (copy before swap when the
write path reads it back); a long move must not hold a schema change the new
code needs; Prisma migrations backward-compatible with the previous image.

**Access** — `protectedProcedure`'s `enforceAccess` only checks top-level
`projectId` / `organizationId`: every secondary id (report, dashboard, client,
share, export, notification…) needs an in-handler ownership check; existence
of another tenant's object must not leak; MCP tools and the public / manage
REST API go through the same checks; client secrets never logged or returned.

**Core structure** — layer imports point downward (R22); services are
`(deps, services)` factories with explicit return types; nothing opens a
connection at import time (R15); clients hold no logger (R9);
`ProviderError.retryable` classified once (R19); `*.constants.ts` imports
only zod / other constants (isomorphism rule); the exports map not widened.

**Queues & jobs** — new jobs declared in the module's `.jobs.ts` and the
registry; payload schema changes tolerate jobs already queued; cron jobs
idempotent and safe when two workers (or a TS and a Go worker) run; a job that
only logs a permanent failure vs one that throws a retriable one.

**Self-hosting** — a new or renamed env var is in `apps/api/src/config/env.ts`,
`.env.example` and the env docs, with a safe default; a compose / migration /
removed-feature change is stated in a commit body so the upgrade notes get it,
with the matching `self-hosting/cli` doctor check; a
released image still boots against the previous schema while migrations run.

**Clients** — dashboard: loading / empty / error states, query invalidation
after mutations, keyboard and labels. SDK: payload shape matches `ingest.constants.ts`, no breaking change
to a published API without a version decision, bundle size.

**Tests** — for each risky path above, is there a test that would fail if it
regressed? Every `mock.module` test asserts its mock was called; live tests
are gated so they cannot touch real data; concurrency, retry exhaustion,
cross-tenant access, rerun-from-midway migrations and sharded-only behaviour
are the usual holes.

**Docs** — AGENTS.md files, READMEs and `docs/*` describe what the code now
does; commit types follow AGENTS.md (no breaking markers); unrelated changes
bundled into the diff.

## 5. Report, then file the record

Severity: **High** = exploitable, cross-tenant data exposure, data loss or
duplication at scale, wrong billing; **Medium** = a reachable path that loses or
corrupts some data, breaks a self-hosted upgrade, or costs unbounded resources;
**Low** = robustness, inconsistency, edge cases; **Nit** = docs, naming, copy.

Answer the person in THEIR language, conclusion first:

1. One paragraph: anything exploitable or losing data? where do the real
   breaks cluster?
2. Medium-or-worse, each with file:line, defect, scenario, confirmed/inferred.
3. Low as a table; test gaps; docs / nits.
4. Recommended order (group findings that must be fixed together).

Then write the same content to `docs/reviews/YYYY-MM-DD-NN-<slug>.md`
(conventions in `docs/reviews/README.md`: next `NN` for the day, stable IDs
`F` / `T` / `N`, a status per finding starting `open`, a header with the
baseline commit, scope, method and the gates NOT run). Tell the person the
path. Do not commit unless asked.

## Follow-ups

When asked to fix findings from a record, update each finding's status in that
record in the same change (`fixed (<commit>)`, `wontfix (<why>)`,
`invalid (<why>)`) — never delete one. A re-review is a new record that links
the old one; when it finds an old fix incomplete, the old line keeps `fixed`
and gains a pointer to the new record's IDs (format in `docs/reviews/README.md`).
Fixes are verified the way AGENTS.md's "How to verify your work" table says.
