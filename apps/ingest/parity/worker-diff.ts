// Differential test for the worker: the same Kafka records, byte for byte,
// through two worker implementations, comparing what each leaves in Redis
// (event/session/profile buffers, BullMQ jobs, caches) and in Postgres
// (first-event marker, notification rows).
//
//   bun run worker-parity -- --a ts --b ts
//   bun x dotenv -e ../../.env -- bun parity/worker-diff.ts --a ts --b go --go-bin bin/worker
//
// Each side runs on its own: fresh topic, consumer group, Redis db index and
// BullMQ namespace. Nothing it makes outlives the run: topics, groups, Redis
// dbs (only indexes 11-15, only when empty at the start), ClickHouse databases
// and fixture rows are removed in a finally block and on SIGINT/SIGTERM.
//
// --full runs each side as the complete worker for its language and compares the
// end state in ClickHouse as well (see the "--full" section below):
//
//   bun x dotenv -e ../../.env -- bun parity/worker-diff.ts --a ts --b go --full --go-bin <worker>
//
// Needs the local stack (Postgres, Redis, Redpanda). Fixture rows are named
// wparity-*; the rule, client and notification ids are uuid columns, so those
// use fixed uuids and go away with the organization.
import { SQL, spawn } from 'bun';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { Producer } from '../../../packages/core/node_modules/@platformatic/kafka/dist/index.js';
import { Queue } from '../../../packages/core/node_modules/bullmq';
import { eventsPartitioner } from '../../../packages/core/src/modules/ingest/src/partitioner';
import Redis from '../../../packages/redis/node_modules/ioredis';
import {
  alignSessionsToTimeout,
  buildCorpus,
  buildGeneratedCorpus,
  EXCLUDED_EVENT,
  fixtureIds,
  GENERATED_PARTITIONS,
  NOTIFY_EVENT,
  SESSION_TIMEOUT_MS as P1_SESSION_TIMEOUT_MS,
} from './worker-corpus';

const ROOT = new URL('../../..', import.meta.url).pathname;
const args = process.argv.slice(2);
const flag = (name: string) =>
  args.includes(name) ? args[args.indexOf(name) + 1] : undefined;
const SIDES = [flag('--a') ?? 'ts', flag('--b') ?? 'ts'] as const;
// The binary, relative to the working directory or to the repository root.
const GO_BIN_FLAG = flag('--go-bin') ?? 'apps/ingest/bin/worker';
const GO_BIN =
  [resolve(GO_BIN_FLAG), resolve(ROOT, GO_BIN_FLAG)].find((path) =>
    existsSync(path)
  ) ?? resolve(ROOT, GO_BIN_FLAG);
const GO_ARGS = (flag('--go-args') ?? '').split(' ').filter(Boolean);
const OUT_DIR = process.env.WPARITY_OUT ?? tmpdir();
const RUN = Date.now().toString(36);
const FULL = args.includes('--full');
// --full needs sessions to end within the run: SESSION_TIMEOUT_MS is also the
// wall-clock deadman of the session reaper, so a short value is what lets the
// reaper close the last session of every device. It also moves the worker's
// event-time session boundary (see the --full section).
const FULL_SESSION_TIMEOUT_DEFAULT_MS = 20_000;
const SESSION_TIMEOUT_MS = FULL
  ? Number(flag('--session-timeout-ms') ?? FULL_SESSION_TIMEOUT_DEFAULT_MS)
  : P1_SESSION_TIMEOUT_MS;

const DOCKER = process.env.DOCKER_CONTEXT
  ? ['docker', '--context', process.env.DOCKER_CONTEXT]
  : ['docker'];
const RP_CONTAINER = process.env.PARITY_RP_CONTAINER ?? 'openpanel-op-rp-1';
// rpk runs inside the container, where the broker listens on its own port.
const RP_BROKERS = 'brokers=127.0.0.1:9092';
const KAFKA_BROKERS = process.env.KAFKA_BROKERS ?? '127.0.0.1:23092';
const REDIS_BASE = (process.env.REDIS_URL ?? 'redis://127.0.0.1:23379').replace(
  /\/\d+$/,
  ''
);
const REDIS_DB_INDEXES = [11, 12, 13, 14, 15];
const WORKER_HTTP_PORT_BASE = 3351;
const READY_TIMEOUT_MS = 120_000;
const DRAIN_TIMEOUT_MS = 60_000;
const SETTLE_MS = 4000;
const SETTLE_TIMEOUT_MS = 60_000;
const POLL_MS = 400;
const RPK_TIMEOUT_MS = 30_000;
const STOP_TIMEOUT_MS = 30_000;
const TTL_BUCKET_SECONDS = 60;
const MAX_REPORTED_DIFFS = Number(flag('--max-diffs') ?? 60);
const MAX_VALUE_CHARS = 300;

// The two corpora: a small hand-written one (one partition, every branch once)
// and a generated one (four partitions, ~2,000 records) for lanes, same-key
// serialization, micro-batching and the long tail of JavaScript semantics.
type CorpusKind = 'basic' | 'generated';
interface Corpus {
  kind: CorpusKind;
  partitions: number;
  records: ReturnType<typeof buildCorpus>;
}
function loadCorpus(kind: CorpusKind): Corpus {
  const corpus: Corpus =
    kind === 'basic'
      ? { kind, partitions: 1, records: buildCorpus() }
      : {
          kind,
          partitions: GENERATED_PARTITIONS,
          records: buildGeneratedCorpus(SESSION_TIMEOUT_MS, FULL),
        };
  return FULL
    ? {
        ...corpus,
        records: alignSessionsToTimeout(corpus.records, SESSION_TIMEOUT_MS),
      }
    : corpus;
}
const CORPUS_FLAG = flag('--corpus') ?? 'all';
const CORPUS_KINDS: CorpusKind[] =
  CORPUS_FLAG === 'all' ? ['basic', 'generated'] : [CORPUS_FLAG as CorpusKind];
let corpus: Corpus = loadCorpus(CORPUS_KINDS[0]!);

const dbUrl = (process.env.DATABASE_URL ?? '').split('?')[0]!;
const sql = new SQL(dbUrl);

// ---------- cleanup ----------
const cleanups: (() => Promise<void>)[] = [];
let cleaningUp: Promise<void> | undefined;
function cleanup() {
  cleaningUp ??= (async () => {
    for (const fn of cleanups.splice(0).reverse()) {
      try {
        await fn();
      } catch (error) {
        console.error('cleanup step failed:', error);
      }
    }
  })();
  return cleaningUp;
}
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    cleanup().finally(() => process.exit(130));
  });
}

// ---------- rpk ----------
async function rpk(...rest: string[]) {
  const proc = spawn({
    cmd: [...DOCKER, 'exec', RP_CONTAINER, 'rpk', ...rest, '-X', RP_BROKERS],
    stdout: 'pipe',
    stderr: 'pipe',
    // A docker exec that hangs (seen against the Kiri VM) must not stall a
    // poll loop: the caller sees a failed call and asks again.
    timeout: RPK_TIMEOUT_MS,
  });
  const [out, err] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  const code = await proc.exited;
  return { out, err, code };
}

interface GroupState {
  state: string;
  current: number;
  end: number;
  lag: number;
}
async function describeGroup(
  group: string,
  topic: string
): Promise<GroupState> {
  const { out } = await rpk('group', 'describe', group);
  const state = /^STATE\s+(\S+)/m.exec(out)?.[1] ?? 'Unknown';
  const rows = out
    .split('\n')
    .map((line) => line.trim().split(/\s+/))
    .filter((cols) => cols[0] === topic && /^\d+$/.test(cols[1] ?? ''));
  const num = (v: string | undefined) => (v && /^\d+$/.test(v) ? Number(v) : 0);
  const sum = (column: number) =>
    rows.reduce((total, cols) => total + num(cols[column]), 0);
  // A partition with no committed offset prints '-': it is all lag.
  return {
    state,
    current: sum(2),
    end: sum(4),
    lag: rows.length >= corpus.partitions ? sum(5) : Number.POSITIVE_INFINITY,
  };
}

// ---------- fixtures ----------
const PROJECT_IDS = [
  fixtureIds.projectA,
  fixtureIds.projectB,
  fixtureIds.projectC,
];
async function teardownFixtures() {
  await sql`DELETE FROM notifications WHERE "projectId" IN ${sql(PROJECT_IDS)}`;
  await sql`DELETE FROM notification_rules WHERE "projectId" IN ${sql(PROJECT_IDS)}`;
  await sql`DELETE FROM clients WHERE id = ${fixtureIds.client}::uuid`;
  await sql`DELETE FROM projects WHERE id IN ${sql(PROJECT_IDS)}`;
  await sql`DELETE FROM organizations WHERE id = ${fixtureIds.org}`;
}

async function setupFixtures() {
  await teardownFixtures();
  await sql`INSERT INTO organizations (id, name) VALUES (${fixtureIds.org}, 'Worker parity')`;
  const filters = [
    { type: 'event', name: EXCLUDED_EVENT, segment: 'event', filters: [] },
  ];
  await sql`INSERT INTO projects (id, name, "organizationId", cors, filters) VALUES (${fixtureIds.projectA}, 'Worker parity A', ${fixtureIds.org}, ${'{*}'}::text[], ${JSON.stringify(filters)}::text::jsonb)`;
  await sql`INSERT INTO projects (id, name, "organizationId", cors) VALUES (${fixtureIds.projectB}, 'Worker parity B', ${fixtureIds.org}, ${'{*}'}::text[])`;
  await sql`INSERT INTO projects (id, name, "organizationId", cors) VALUES (${fixtureIds.projectC}, 'Worker parity C', ${fixtureIds.org}, ${'{*}'}::text[])`;
  await sql`INSERT INTO clients (id, name, secret, "projectId", "organizationId", "ignoreCorsAndSecret") VALUES (${fixtureIds.client}::uuid, 'wparity', NULL, ${fixtureIds.projectA}, ${fixtureIds.org}, true)`;
  const config = {
    type: 'events',
    events: [
      {
        id: 'wparity-rule-event',
        name: NOTIFY_EVENT,
        segment: 'event',
        filters: [],
      },
    ],
  };
  await sql`INSERT INTO notification_rules (id, name, "projectId", "sendToApp", "sendToEmail", config, template) VALUES (${fixtureIds.rule}::uuid, 'wparity rule', ${fixtureIds.projectA}, true, false, ${JSON.stringify(config)}::text::jsonb, 'Event {{name}} on {{path}}')`;
  // Project B: an email delivery behind a property filter, with a template that
  // reads nested payload paths and the rule name.
  const configB = {
    type: 'events',
    events: [
      {
        id: 'wparity-rule-b-event',
        name: NOTIFY_EVENT,
        segment: 'event',
        filters: [
          { id: '1', name: 'properties.plan', operator: 'is', value: ['team'] },
        ],
      },
    ],
  };
  await sql`INSERT INTO notification_rules (id, name, "projectId", "sendToApp", "sendToEmail", config, template) VALUES (${fixtureIds.ruleB}::uuid, 'wparity rule b', ${fixtureIds.projectB}, false, true, ${JSON.stringify(configB)}::text::jsonb, '{{properties.plan}} plan on $EVENT_NAME ({{rule_name}}) {{properties.seats}}')`;
  // Project C: every event name, filtered on the path (also matches the derived
  // session_start rows), and a regex filter on revenue.
  const configC = {
    type: 'events',
    events: [
      {
        id: 'wparity-rule-c-any',
        name: '*',
        segment: 'event',
        filters: [
          { id: '1', name: 'path', operator: 'contains', value: ['pricing'] },
        ],
      },
      {
        id: 'wparity-rule-c-revenue',
        name: 'revenue',
        segment: 'event',
        filters: [
          {
            id: '2',
            name: 'properties.currency',
            operator: 'regex',
            value: ['/^(EUR|usd)$/'],
          },
        ],
      },
    ],
  };
  await sql`INSERT INTO notification_rules (id, name, "projectId", "sendToApp", "sendToEmail", config, template) VALUES (${fixtureIds.ruleC}::uuid, 'wparity rule c', ${fixtureIds.projectC}, true, false, ${JSON.stringify(configC)}::text::jsonb, NULL)`;
}

async function resetFixtureState() {
  await sql`DELETE FROM notifications WHERE "projectId" IN ${sql(PROJECT_IDS)}`;
  await sql`UPDATE projects SET "firstEventAt" = NULL WHERE id IN ${sql(PROJECT_IDS)}`;
  if (FULL) {
    // The session-end job counts events per project and per organization.
    await sql`UPDATE projects SET "eventsCount" = 0, "noDataNotifiedAt" = NULL, "dataStoppedNotifiedAt" = NULL WHERE id IN ${sql(PROJECT_IDS)}`;
    await sql`UPDATE organizations SET "subscriptionPeriodEventsCount" = 0, "subscriptionPeriodEventsCountExceededAt" = NULL, "usageWarningSentAt" = NULL, "usageExceededSentAt" = NULL WHERE id = ${fixtureIds.org}`;
  }
}

interface PostgresDump {
  projects: unknown[];
  notifications: unknown[];
  organizations?: unknown[];
}
async function dumpPostgres(): Promise<PostgresDump> {
  const projects =
    await sql`SELECT id, ("firstEventAt" IS NOT NULL) AS "firstEventSet", "eventsCount", "noDataNotifiedAt", "dataStoppedNotifiedAt" FROM projects WHERE id IN ${sql(PROJECT_IDS)} ORDER BY id`;
  const notifications =
    await sql`SELECT title, message, "projectId", "sendToApp", "sendToEmail", "integrationId", "notificationRuleId", "isReadAt", payload FROM notifications WHERE "projectId" IN ${sql(PROJECT_IDS)} ORDER BY title, payload::text`;
  if (!FULL) {
    return { projects: [...projects], notifications: [...notifications] };
  }
  const organizations =
    await sql`SELECT id, "subscriptionPeriodEventsCount" AS "eventsCount", ("subscriptionPeriodEventsCountExceededAt" IS NOT NULL) AS exceeded, ("usageWarningSentAt" IS NOT NULL) AS warned, ("usageExceededSentAt" IS NOT NULL) AS alerted FROM organizations WHERE id = ${fixtureIds.org}`;
  return {
    projects: [...projects],
    notifications: [...notifications],
    organizations: [...organizations],
  };
}

// ---------- redis ----------
const redisUrl = (index: number) => `${REDIS_BASE}/${index}`;
function redisClient(index: number) {
  return new Redis(redisUrl(index), { maxRetriesPerRequest: 3 });
}

async function pickRedisIndexes(count: number): Promise<number[]> {
  const free: number[] = [];
  for (const index of REDIS_DB_INDEXES) {
    const client = redisClient(index);
    try {
      if ((await client.dbsize()) === 0) {
        free.push(index);
      }
    } finally {
      client.disconnect();
    }
  }
  if (free.length < count) {
    throw new Error(
      `need ${count} empty Redis dbs among ${REDIS_DB_INDEXES.join(',')}, found ${free.join(',') || 'none'}`
    );
  }
  return free.slice(0, count);
}

async function flushRedisIndex(index: number) {
  if (!REDIS_DB_INDEXES.includes(index)) {
    throw new Error(`refusing to flush Redis db ${index}`);
  }
  const client = redisClient(index);
  try {
    await client.flushdb();
  } finally {
    client.disconnect();
  }
}

export interface RedisEntry {
  type: string;
  ttl: number;
  value: unknown;
}
export type RedisDump = Record<string, RedisEntry>;

async function readRedisKey(
  client: Redis,
  key: string
): Promise<RedisEntry | null> {
  const type = await client.type(key);
  const ttlMs = await client.pttl(key);
  const ttl =
    ttlMs < 0
      ? ttlMs
      : Math.round(ttlMs / 1000 / TTL_BUCKET_SECONDS) * TTL_BUCKET_SECONDS;
  switch (type) {
    case 'string':
      return { type, ttl, value: await client.get(key) };
    case 'list':
      return { type, ttl, value: await client.lrange(key, 0, -1) };
    case 'set':
      return { type, ttl, value: (await client.smembers(key)).sort() };
    case 'hash': {
      const hash = await client.hgetall(key);
      return {
        type,
        ttl,
        value: Object.fromEntries(
          Object.entries(hash).sort(([x], [y]) => (x < y ? -1 : 1))
        ),
      };
    }
    case 'zset': {
      const flat = await client.zrange(key, 0, -1, 'WITHSCORES');
      const pairs: [string, number][] = [];
      for (let i = 0; i < flat.length; i += 2) {
        pairs.push([flat[i]!, Number(flat[i + 1])]);
      }
      return { type, ttl, value: pairs };
    }
    case 'stream': {
      const entries = await client.xrange(key, '-', '+');
      return { type, ttl, value: entries };
    }
    default:
      return null;
  }
}

async function dumpRedis(index: number): Promise<RedisDump> {
  const client = redisClient(index);
  try {
    const keys: string[] = [];
    let cursor = '0';
    do {
      const [next, batch] = await client.scan(cursor, 'COUNT', 500);
      cursor = next;
      keys.push(...batch);
    } while (cursor !== '0');
    const dump: RedisDump = {};
    for (const key of keys.sort()) {
      const entry = await readRedisKey(client, key);
      if (entry) {
        dump[key] = entry;
      }
    }
    return dump;
  } finally {
    client.disconnect();
  }
}

// Keys BullMQ rewrites on its own clock even when idle.
const IDLE_NOISE = /:(stalled-check|marker|events)$|:limiter$/;
async function redisFingerprint(index: number): Promise<string> {
  const dump = await dumpRedis(index);
  const stable = Object.entries(dump)
    .filter(([key]) => !IDLE_NOISE.test(key))
    .map(([key, entry]) => [key, entry.type, entry.value]);
  return Bun.hash(JSON.stringify(stable)).toString();
}

// The one error the corpus provokes on purpose and the TS worker logs: a
// notification whose payload holds an unpaired surrogate cannot be stored in a
// jsonb column (the generated corpus carries a few such property values), and
// the delivery is dropped without failing the event. The Go worker logs a failed
// notification check at Warn, not Error, so parity of what such a failure leaves
// behind rests on the Postgres and BullMQ dumps, not on this log filter.
const EXPECTED_ERRORS = [
  /notification\.create.*invalid input syntax for type json/,
];

// ---------- worker processes ----------
interface SideContext {
  side: string;
  name: string;
  topic: string;
  group: string;
  redisIndex: number;
  namespace: string;
  clickhouseDb: string;
  httpPort: number;
}

function workerEnv(ctx: SideContext): Record<string, string> {
  return {
    ...(process.env as Record<string, string>),
    LOG_LEVEL: 'info',
    KAFKA_BROKERS,
    KAFKA_EVENTS_TOPIC: ctx.topic,
    KAFKA_CONSUMER_GROUP: ctx.group,
    KAFKA_EVENTS_DLQ_TOPIC: `${ctx.topic}-dlq`,
    REDIS_URL: redisUrl(ctx.redisIndex),
    QUEUE_NAMESPACE: ctx.namespace,
    // The events path never reads ClickHouse, so this database stays absent
    // unless a worker turns out to need it; the name keeps it off 'openpanel'.
    CLICKHOUSE_URL: `${(process.env.CLICKHOUSE_URL ?? 'http://localhost:23123').replace(/\/[^/]*$/, '')}/${ctx.clickhouseDb}`,
    SESSION_TIMEOUT_MS: String(SESSION_TIMEOUT_MS),
    AUTOMATIC_MIGRATIONS: 'false',
    API_PORT: String(ctx.httpPort),
    AUTH_SECRET:
      process.env.AUTH_SECRET ?? 'wparity-auth-secret-0123456789abcdef',
    SELF_HOSTED: 'false',
    ROLE: 'worker',
    ENABLED_QUEUES: 'events',
  };
}

// undici (a transitive dependency) needs worker_threads.markAsUncloneable, which
// Bun older than the repo's .bun-version does not have. A no-op is enough for the
// worker; it is only written when the running Bun lacks the function.
const SHIM_PATH = `${OUT_DIR}/wparity-${RUN}-shim.ts`;
async function workerBunArgs(): Promise<string[]> {
  const workerThreads = await import('node:worker_threads');
  if ('markAsUncloneable' in workerThreads) {
    return [];
  }
  await Bun.write(
    SHIM_PATH,
    "import wt from 'node:worker_threads';\n(wt as Record<string, unknown>).markAsUncloneable ??= () => undefined;\n"
  );
  return ['--preload', SHIM_PATH];
}

// part is only set by --full: the Go side runs two processes, the Go worker and
// a Bun worker ('companion') for what Go does not take over.
async function startWorker(
  kind: string,
  ctx: SideContext,
  part?: FullPart,
  phase?: FullPhase
) {
  const env = part && phase ? fullEnv(ctx, part, phase) : workerEnv(ctx);
  const label = [ctx.name, part === 'companion' ? 'bun' : '', phase ?? '']
    .filter(Boolean)
    .join('-');
  const stdout = Bun.file(`${OUT_DIR}/wparity-${RUN}-${label}.log`);
  const stderr = Bun.file(`${OUT_DIR}/wparity-${RUN}-${label}.err`);
  if (kind === 'ts' || part === 'companion') {
    return {
      proc: spawn({
        cmd: [
          process.execPath,
          'run',
          ...(await workerBunArgs()),
          'src/main.ts',
        ],
        cwd: `${ROOT}apps/api`,
        env,
        stdout,
        stderr,
      }),
      log: stdout.name!,
    };
  }
  return {
    proc: spawn({
      cmd: [GO_BIN, ...GO_ARGS],
      cwd: `${ROOT}apps/ingest`,
      env,
      stdout,
      stderr,
    }),
    log: stdout.name!,
  };
}

async function stopWorker(proc: ReturnType<typeof spawn>) {
  proc.kill('SIGTERM');
  const timer = setTimeout(() => proc.kill('SIGKILL'), STOP_TIMEOUT_MS);
  const code = await proc.exited;
  clearTimeout(timer);
  return code;
}

async function until<T>(
  what: string,
  timeoutMs: number,
  probe: () => Promise<T | undefined | false>
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await probe();
    if (value) {
      return value;
    }
    await Bun.sleep(POLL_MS);
  }
  throw new Error(`timed out waiting for ${what}`);
}

// ---------- producing ----------
async function produceWave(
  producer: Producer,
  topic: string,
  records: ReturnType<typeof buildCorpus>
) {
  await producer.send({
    messages: records.map((record) => ({
      topic,
      key: Buffer.from(record.key),
      value: Buffer.from(record.value),
    })),
  });
}

// ---------- one side ----------
export interface SideResult {
  side: string;
  redis: RedisDump;
  postgres: PostgresDump;
  clickhouse?: ClickhouseDump;
  logErrors: string[];
  durationMs: number;
}

async function runSide(kind: string, ctx: SideContext): Promise<SideResult> {
  const started = Date.now();
  await resetFixtureState();
  await flushRedisIndex(ctx.redisIndex);
  cleanups.push(async () => {
    await flushRedisIndex(ctx.redisIndex);
  });
  const created = await rpk(
    'topic',
    'create',
    ctx.topic,
    '-p',
    String(corpus.partitions),
    '-r',
    '1'
  );
  if (created.code !== 0) {
    throw new Error(`topic create failed: ${created.out}${created.err}`);
  }
  cleanups.push(async () => {
    await rpk('topic', 'delete', ctx.topic, `${ctx.topic}-dlq`);
    await rpk('group', 'delete', ctx.group);
  });

  const { proc, log } = await startWorker(kind, ctx);
  let workerStopped = false;
  cleanups.push(async () => {
    if (!workerStopped) {
      await stopWorker(proc);
    }
  });

  const producer = new Producer({
    clientId: `wparity-${RUN}`,
    bootstrapBrokers: [KAFKA_BROKERS],
    partitioner: eventsPartitioner,
  });
  cleanups.push(() => producer.close());

  // A fresh group starts from the earliest offset, but producing before the
  // worker has joined would hide a worker that never starts: wait for the
  // group first.
  await until('the consumer group to be stable', READY_TIMEOUT_MS, async () => {
    if (proc.exitCode !== null) {
      throw new Error(
        `worker exited with ${proc.exitCode} before joining; see ${log}`
      );
    }
    return (await describeGroup(ctx.group, ctx.topic)).state === 'Stable';
  });

  const waves = [
    ...new Set(corpus.records.map((record) => record.wave)),
  ].sort();
  let produced = 0;
  for (const wave of waves) {
    const records = corpus.records.filter((record) => record.wave === wave);
    await produceWave(producer, ctx.topic, records);
    produced += records.length;
    await until(`wave ${wave} to be consumed`, DRAIN_TIMEOUT_MS, async () => {
      if (proc.exitCode !== null) {
        throw new Error(`worker died (${proc.exitCode}); see ${log}`);
      }
      const state = await describeGroup(ctx.group, ctx.topic);
      return state.lag === 0 && state.current >= produced;
    });
  }

  let previous = '';
  let stableSince = Date.now();
  await until('Redis to settle', SETTLE_TIMEOUT_MS, async () => {
    const fingerprint = await redisFingerprint(ctx.redisIndex);
    if (fingerprint !== previous) {
      previous = fingerprint;
      stableSince = Date.now();
      return false;
    }
    return Date.now() - stableSince >= SETTLE_MS;
  });

  const exitCode = await stopWorker(proc);
  workerStopped = true;
  if (exitCode !== 0) {
    console.error(`${ctx.name}: worker exited with ${exitCode}`);
  }
  const redis = await dumpRedis(ctx.redisIndex);
  const postgres = await dumpPostgres();
  const logText = await Bun.file(log).text();
  const logErrors = logText
    .split('\n')
    .filter((line) => /"level":(50|60|"ERROR")/.test(line))
    .filter((line) => !EXPECTED_ERRORS.some((pattern) => pattern.test(line)));
  await Bun.write(
    `${OUT_DIR}/wparity-${RUN}-${ctx.name}.dump.json`,
    JSON.stringify({ redis, postgres }, null, 1)
  );
  return {
    side: ctx.name,
    redis,
    postgres,
    logErrors,
    durationMs: Date.now() - started,
  };
}

// ---------- normalizers ----------
// Each rule exists because that field differs between two identical TS runs
// (checked with --a ts --b ts); nothing is masked on a hunch. Everything else is
// compared as the worker wrote it, JSON key order included.
//
//  - inserted_at, profile created_at/last_seen_at, project firstEventAt/updatedAt,
//    BullMQ job timestamp, session:wallclock scores, stream entry ids: the wall
//    clock of the run.
//  - uuids the worker mints (the session_start row, a record without an id,
//    derived ai span ids): random per run. Masked only when the uuid is not one
//    the corpus carries, and sanity() checks they are distinct and never reuse
//    a producer id, so "session_start shares the event's id" still fails.
//  - meta.requestId on BullMQ envelopes: random per message unless the record
//    carried a request-id header; corpus values are kept.
//  - list order across Kafka keys: keys of one partition run concurrently, so
//    the order of rows of different devices or profiles varies from run to run.
//    Rows are regrouped by project and device (or profile) and each group keeps
//    its own order, so an ordering bug inside one device's rows still shows.
const WALLCLOCK = '<wallclock>';
const MINTED_ID = '<minted-id>';
const MINTED_REQUEST_ID = '<minted-request-id>';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

interface KnownValues {
  ids: Set<string>;
  requestIds: Set<string>;
}
function knownValues(): KnownValues {
  const ids = new Set<string>(Object.values(fixtureIds));
  const requestIds = new Set<string>();
  for (const record of corpus.records) {
    const payload = JSON.parse(record.value);
    if (payload.id) {
      ids.add(payload.id);
    }
    if (payload.headers?.['request-id']) {
      requestIds.add(payload.headers['request-id']);
    }
  }
  return { ids, requestIds };
}

const isRecord = (v: unknown): v is Record<string, unknown> =>
  typeof v === 'object' && v !== null && !Array.isArray(v);

function maskDeep(
  value: unknown,
  key: string,
  redisKey: string,
  known: KnownValues
): unknown {
  if (typeof value === 'string') {
    const parsed = tryParse(value);
    if (parsed !== value) {
      return maskDeep(parsed, key, redisKey, known);
    }
    if (UUID.test(value) && !known.ids.has(value)) {
      return MINTED_ID;
    }
    if (key === 'requestId' && !known.requestIds.has(value)) {
      return MINTED_REQUEST_ID;
    }
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => maskDeep(item, key, redisKey, known));
  }
  if (isRecord(value)) {
    const out: Record<string, unknown> = {};
    for (const [childKey, child] of Object.entries(value)) {
      out[childKey] = isWallclockField(childKey, redisKey)
        ? WALLCLOCK
        : maskDeep(child, childKey, redisKey, known);
    }
    return out;
  }
  return value;
}

function isWallclockField(field: string, redisKey: string): boolean {
  if (field === 'inserted_at') {
    return true;
  }
  if (
    redisKey.startsWith('profile-buffer') ||
    redisKey.startsWith('profile-cache')
  ) {
    return field === 'created_at' || field === 'last_seen_at';
  }
  if (redisKey.startsWith('cachable:getProjectById')) {
    return field === 'firstEventAt' || field === 'updatedAt';
  }
  if (redisKey.startsWith('bull:')) {
    return (
      field === 'timestamp' || field === 'processedOn' || field === 'finishedOn'
    );
  }
  return false;
}

function groupRows(rows: Record<string, unknown>[]): Record<string, unknown[]> {
  const groups: Record<string, unknown[]> = {};
  for (const row of rows) {
    const who = row.device_id || row.profile_id || row.id || row.name || '';
    const groupKey = `${String(row.project_id)}|${String(who)}`;
    const group = groups[groupKey] ?? [];
    group.push(row);
    groups[groupKey] = group;
  }
  return Object.fromEntries(
    Object.entries(groups).sort(([x], [y]) => (x < y ? -1 : 1))
  );
}

// --full: a BullMQ queue's event stream records every transition of every job,
// and which transitions happen depends on timing: 'drained' fires whenever the
// queue empties, 'duplicated' when a job id is added while the job still exists
// (the boundary and the reaper both close a session; whether the first job is
// gone by then is a race), and 'prev' names the state a job came from.
const BULLMQ_TIMING_EVENTS = new Set(['drained', 'duplicated']);
function withoutPrev(fields: string[]): string[] {
  const out: string[] = [];
  for (let i = 0; i < fields.length; i += 2) {
    if (fields[i] !== 'prev') {
      out.push(fields[i]!, fields[i + 1]!);
    }
  }
  return out;
}

function normalizeEntry(
  redisKey: string,
  entry: RedisEntry,
  known: KnownValues
): RedisEntry {
  if (entry.type === 'list') {
    const rows = (entry.value as string[]).map((item) => {
      const parsed = tryParse(item);
      return maskDeep(parsed, '', redisKey, known);
    });
    const isRowList =
      rows.length > 0 &&
      rows.every((row) => isRecord(row) && 'project_id' in row);
    return {
      ...entry,
      value: isRowList ? groupRows(rows as Record<string, unknown>[]) : rows,
    };
  }
  if (entry.type === 'zset' && redisKey.startsWith('session:wallclock:')) {
    return {
      ...entry,
      // Scores are the wall clock, so the order they give is arrival order.
      value: (entry.value as [string, number][])
        .map(([member]) => [member, WALLCLOCK])
        .sort((x, y) => (x[0]! < y[0]! ? -1 : 1)),
    };
  }
  if (entry.type === 'string' && redisKey.endsWith(':stalled-check')) {
    return { ...entry, value: WALLCLOCK };
  }
  if (entry.type === 'stream') {
    const entries = (entry.value as [string, string[]][]).filter(
      ([, fields]) => !(FULL && BULLMQ_TIMING_EVENTS.has(fields[1] ?? ''))
    );
    // Entries arrive in the order lanes reached their messages: compare them
    // as a multiset.
    return {
      ...entry,
      value: entries
        .map(([, fields]) => [
          WALLCLOCK,
          maskDeep(FULL ? withoutPrev(fields) : fields, '', redisKey, known),
        ])
        .sort((x, y) => (JSON.stringify(x) < JSON.stringify(y) ? -1 : 1)),
    };
  }
  return { ...entry, value: maskDeep(entry.value, '', redisKey, known) };
}

// Decision 3 (docs/GO_WORKER.md section 8): the Go worker only reads the shared
// cachable:* entries and never writes them, while the TS worker caches what it
// loads (SETEX on a miss). Whether such a key exists is an implementation
// detail of the cache, not state either side hands on, so both dumps drop them.
// The DEL after the first event is the one write the worker makes to them; it
// observable only through later reads, so it is covered by the stale-read
// behaviour of the next messages, not by a key comparison.
const CACHABLE_PREFIX = 'cachable:';

// BullMQ numbers jobs with an INCR counter, so which job gets which number
// depends on which message a lane reached first. A numbered job is therefore
// named by what it holds (queue, job name, masked data) instead, and the lists
// that point at job numbers are sorted by that name. Custom job ids
// (sessionEnd:v2:...) are stable and stay as they are.
const NUMBERED_JOB_KEY = /^(bull:[^:]+:)(\d+)$/;
const BULL_ID_LISTS =
  /^bull:[^:]+:(wait|paused|active|completed|failed|prioritized|delayed)$/;

function rekeyNumberedJobs(dump: RedisDump, known: KnownValues): RedisDump {
  const names = new Map<string, string>();
  const seen = new Map<string, number>();
  for (const [key, entry] of Object.entries(dump)) {
    const match = NUMBERED_JOB_KEY.exec(key);
    if (!match || entry.type !== 'hash') {
      continue;
    }
    const fields = entry.value as Record<string, unknown>;
    const content = JSON.stringify([
      fields.name,
      maskDeep(tryParse(fields.data), '', key, known),
    ]);
    const digest = Bun.hash(content).toString(36);
    const count = (seen.get(`${match[1]}${digest}`) ?? 0) + 1;
    seen.set(`${match[1]}${digest}`, count);
    names.set(key, `${match[1]}job-${digest}-${count}`);
  }
  const out: RedisDump = {};
  for (const [key, entry] of Object.entries(dump)) {
    const renamed = names.get(key);
    if (renamed) {
      out[renamed] = entry;
      continue;
    }
    const prefix = BULL_ID_LISTS.exec(key)
      ? key.slice(0, key.lastIndexOf(':') + 1)
      : '';
    if (prefix && entry.type === 'list') {
      const mapped = (entry.value as string[]).map(
        (id) => names.get(`${prefix}${id}`) ?? id
      );
      out[key] = { ...entry, value: mapped.sort() };
      continue;
    }
    if (prefix && entry.type === 'zset') {
      // The scores of completed, failed and delayed are the wall clock.
      const mapped = (entry.value as [string, number][])
        .map(([id, score]): [string, number | string] => [
          names.get(`${prefix}${id}`) ?? id,
          key.endsWith(':prioritized') ? score : WALLCLOCK,
        ])
        .sort((x, y) => (x[0] < y[0] ? -1 : 1));
      out[key] = { ...entry, value: mapped };
      continue;
    }
    if (entry.type === 'stream' && key.endsWith(':events')) {
      const queuePrefix = key.slice(0, key.lastIndexOf(':') + 1);
      const entries = entry.value as [string, string[]][];
      out[key] = {
        ...entry,
        value: entries.map(([id, fields]) => [
          id,
          fields.map((field, index) =>
            fields[index - 1] === 'jobId'
              ? (names
                  .get(`${queuePrefix}${field}`)
                  ?.slice(queuePrefix.length) ??
                (FULL && /^\d+$/.test(field) ? '<numbered-job>' : field))
              : field
          ),
        ]),
      };
      continue;
    }
    out[key] = entry;
  }
  return out;
}

// --full: the cron queue is the Bun scheduler's own bookkeeping (a delayed job
// per schedule, the last completed runs). The Go worker has no BullMQ cron jobs
// on purpose: it runs the flushes and the reaper from in-process timers on the
// same wall-clock ticks, so only the queue's meta is compared.
//
// The stalled-check key of a queue is a string with a 30 s expiry that
// whichever worker last ran the stalled check sets; whether it still exists when
// the dump is taken depends on how long ago that was.
const CRON_QUEUE_KEY = /^bull:cron-/;
const STALLED_CHECK_KEY = /:stalled-check$/;
const isSchedulerBookkeeping = (key: string) =>
  FULL &&
  ((CRON_QUEUE_KEY.test(key) && !key.endsWith(':meta')) ||
    STALLED_CHECK_KEY.test(key));

// --full: the notification queue keeps the last ten completed jobs
// (removeOnComplete: 10), and which ten is the order the jobs finished in.
// Their payloads are what the Postgres notification rows already show, so only
// how many are kept is compared.
function reduceRetainedCompletedJobs(dump: RedisDump): RedisDump {
  if (!FULL) {
    return dump;
  }
  const out = { ...dump };
  for (const [key, entry] of Object.entries(dump)) {
    const queue = /^(bull:[^:]+:)completed$/.exec(key);
    if (!queue || entry.type !== 'zset') {
      continue;
    }
    const members = entry.value as [string, number][];
    for (const [id] of members) {
      delete out[`${queue[1]}${id}`];
    }
    out[key] = {
      ...entry,
      value: [[`<${members.length} retained completed jobs>`, 0]],
    };
  }
  return out;
}

export function normalize(rawDump: RedisDump): RedisDump {
  const known = knownValues();
  const dump = rekeyNumberedJobs(reduceRetainedCompletedJobs(rawDump), known);
  return Object.fromEntries(
    Object.entries(dump)
      .filter(
        ([key]) =>
          !(key.startsWith(CACHABLE_PREFIX) || isSchedulerBookkeeping(key))
      )
      .map(([key, entry]) => [key, normalizeEntry(key, entry, known)])
  );
}

// ---------- diff ----------
function tryParse(value: unknown): unknown {
  if (typeof value !== 'string') {
    return value;
  }
  const first = value[0];
  if (first !== '{' && first !== '[') {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function show(value: unknown) {
  const text = typeof value === 'string' ? value : JSON.stringify(value);
  return text && text.length > MAX_VALUE_CHARS
    ? `${text.slice(0, MAX_VALUE_CHARS)}…(${text.length})`
    : String(text);
}

function diffValues(a: unknown, b: unknown, path: string, out: string[]) {
  const pa = tryParse(a);
  const pb = tryParse(b);
  if (pa === pb) {
    return;
  }
  const isObj = (v: unknown): v is Record<string, unknown> =>
    typeof v === 'object' && v !== null;
  if (isObj(pa) && isObj(pb) && Array.isArray(pa) === Array.isArray(pb)) {
    const keysA = Object.keys(pa);
    const keysB = Object.keys(pb);
    const keys = new Set([...keysA, ...keysB]);
    for (const key of keys) {
      if (!(key in pa)) {
        out.push(`${path}.${key}: only in b: ${show(pb[key])}`);
      } else if (key in pb) {
        diffValues(pa[key], pb[key], `${path}.${key}`, out);
      } else {
        out.push(`${path}.${key}: only in a: ${show(pa[key])}`);
      }
    }
    // Key order is part of the contract (JSON.stringify order on the wire).
    if (
      !Array.isArray(pa) &&
      keysA.join() !== keysB.join() &&
      out.length === 0
    ) {
      out.push(
        `${path}: key order differs: a=[${keysA.join(',')}] b=[${keysB.join(',')}]`
      );
    }
    return;
  }
  out.push(`${path}: a=${show(pa)}  b=${show(pb)}`);
}

// --full: a TTL counts down for as long as the run waits, and the Go side
// finishes in about a minute where the Bun side waits for BullMQ's first reaper
// tick (five minutes). Rounding the time left up to the hour gives back the
// nominal TTL (the profile cache is 1 h, the session-end marker 2 h).
const TTL_HOUR_SECONDS = 3600;
const comparableTtl = (ttl: number) =>
  FULL && ttl > 0 ? Math.ceil(ttl / TTL_HOUR_SECONDS) * TTL_HOUR_SECONDS : ttl;

export function diffDumps(a: SideResult, b: SideResult): string[] {
  const out: string[] = [];
  const ra = normalize(a.redis);
  const rb = normalize(b.redis);
  for (const key of new Set([...Object.keys(ra), ...Object.keys(rb)])) {
    const ea = ra[key];
    const eb = rb[key];
    if (!ea) {
      out.push(`redis ${key}: only in b (${eb!.type}) ${show(eb!.value)}`);
      continue;
    }
    if (!eb) {
      out.push(`redis ${key}: only in a (${ea.type}) ${show(ea.value)}`);
      continue;
    }
    if (ea.type !== eb.type) {
      out.push(`redis ${key}: type a=${ea.type} b=${eb.type}`);
      continue;
    }
    if (comparableTtl(ea.ttl) !== comparableTtl(eb.ttl)) {
      out.push(`redis ${key}: ttl a=${ea.ttl} b=${eb.ttl}`);
    }
    diffValues(ea.value, eb.value, `redis ${key}`, out);
  }
  diffValues(a.postgres, b.postgres, 'postgres', out);
  if (a.clickhouse && b.clickhouse) {
    out.push(...diffClickhouse(a.clickhouse, b.clickhouse));
  }
  return out;
}

// ---------- sanity ----------
// A comparison of two empty runs would pass; these keep the rig honest.
export function sanity(result: SideResult): string[] {
  const problems: string[] = [];
  const keys = Object.keys(result.redis);
  const listEvents = Object.values(result.redis)
    .filter((entry) => entry.type === 'list')
    .flatMap((entry) => entry.value as string[]);
  const eventRows = listEvents.filter(
    (row) => row.includes('"project_id"') && row.includes('"device_id"')
  );
  const excluded = corpus.records.filter(
    (record) =>
      record.value.includes(`"name":"${EXCLUDED_EVENT}"`) &&
      record.value.includes(`"projectId":"${fixtureIds.projectA}"`)
  ).length;
  const expectMin = corpus.records.length - excluded;
  if (eventRows.length < expectMin) {
    problems.push(
      `only ${eventRows.length} event rows in Redis lists, expected at least ${expectMin}`
    );
  }
  if (
    eventRows.some(
      (row) =>
        row.includes(`"name":"${EXCLUDED_EVENT}"`) &&
        row.includes(`"project_id":"${fixtureIds.projectA}"`)
    )
  ) {
    problems.push('the project-filtered event reached the event buffer');
  }
  const known = knownValues();
  const sessionStartIds = eventRows
    .map((row) => JSON.parse(row) as { name: string; id: string })
    .filter((row) => row.name === 'session_start')
    .map((row) => row.id);
  if (
    sessionStartIds.length === 0 ||
    new Set(sessionStartIds).size !== sessionStartIds.length ||
    sessionStartIds.some((id) => known.ids.has(id))
  ) {
    problems.push(
      'session_start rows are missing or share ids with each other or with a producer id'
    );
  }
  if (!keys.some((key) => key.startsWith('session:'))) {
    problems.push('no session:* keys');
  }
  if (!keys.some((key) => /bull:.*sessions/.test(key))) {
    problems.push(
      'no BullMQ sessions job (the boundary did not enqueue session_end)'
    );
  }
  if (!keys.some((key) => /bull:.*notification/.test(key))) {
    problems.push('no BullMQ notification job');
  }
  if (keys.some((key) => /dead|dlq/i.test(key))) {
    problems.push('a dead-letter key exists');
  }
  const projects = result.postgres.projects as {
    id: string;
    firstEventSet: boolean;
  }[];
  const projectsWithEvents = new Set(
    corpus.records.map((record) => JSON.parse(record.value).projectId as string)
  );
  const unmarked = projects
    .filter((project) => projectsWithEvents.has(project.id))
    .filter((project) => !project.firstEventSet);
  if (unmarked.length > 0) {
    problems.push(
      `firstEventAt not set on ${unmarked.map((project) => project.id).join(', ')}`
    );
  }
  if (result.postgres.notifications.length === 0) {
    problems.push('no notification row');
  }
  if (result.logErrors.length > 0) {
    problems.push(
      `${result.logErrors.length} error lines in the worker log, first: ${result.logErrors[0]!.slice(0, 300)}`
    );
  }
  return problems;
}

// ---------- --full: the complete worker, ClickHouse included ----------
// Each side is the whole worker of its language, so the comparison reaches
// ClickHouse, where the flushes, the session lifecycle and the profile merge
// leave their rows.
//
//  ts: one Bun worker, ENABLED_QUEUES=events,sessions,cron,notification. The task
//      text lists events,sessions,cron; notification is added because the
//      sessions and the notification rules enqueue sendNotification jobs, which
//      would otherwise wait for ever and the "no job waiting" condition could
//      not hold.
//  go: the Go worker (the events consumer, GO_WORKER_QUEUES=sessions,
//      GO_WORKER_CRON_JOBS = every buffer flush plus the session reaper and
//      vacuum) and next to it a Bun worker on ENABLED_QUEUES=notification,cron
//      for what Go does not take over (the notification queue, which renders
//      the React Email templates, and the remaining cron jobs).
//
// Every side has its own ClickHouse database built from test/clickhouse-schema.sql,
// its own Redis db, topic, consumer group and BullMQ namespace.
type FullPart = 'ts' | 'go' | 'companion';
// A --full side runs its workers twice over the same Redis, topic and group:
// 'collect' takes the corpus in and ends every session with no buffer flushing,
// 'flush' then empties the buffers. See the note above PAUSED_QUEUES.
type FullPhase = 'collect' | 'flush';

const PIPELINE_CRON_JOBS = [
  'flushEvents',
  'flushSessions',
  'flushProfiles',
  'flushProfileBackfill',
  'flushBots',
  'flushReplay',
  'flushGroups',
  'flushAiEvents',
  'sessionReaper',
  'sessionVacuum',
];

// Cron jobs unrelated to the pipeline, off on every Bun worker of a --full run.
// Postgres is the shared dev database, so a job that sweeps every organization
// or project would act on rows that are not this run's fixtures.
const UNRELATED_CRON_JOBS: Record<string, string> = {
  salt: 'rotates the salts table of the shared Postgres',
  insightsDaily:
    'enqueues insight jobs for every project of the shared Postgres (AI calls)',
  insightCleanup: 'deletes insights in the shared Postgres',
  weeklyDigest: 'sends the weekly digest e-mail to every member',
  gscSync: 'calls the Google Search Console API',
  cohortRefresh: 'recomputes the dev database cohorts into this ClickHouse',
  delete: 'deletes organizations scheduled for removal in the shared Postgres',
  windDown: 'e-mails and winds down organizations of the shared Postgres',
  onboarding: 'sends onboarding e-mails to users of the shared Postgres',
  dataHealth: 'e-mails data-health reports for every project',
  flushExports: 'uploads to S3/GCS integrations',
};
const UNRELATED_CRON_NAMES = Object.keys(UNRELATED_CRON_JOBS);

const REAPER_INTERVAL_MS = 5 * 60 * 1000;
const PAUSED_QUEUES = ['sessions'];
// Why a --full side runs its workers twice. Two races exist in both languages
// and would make any comparison flaky: a profile keeps the properties of the
// last event row that reached its buffer before a flush caches it (an event
// whose profile is already cached adds no row), and a session_end event, which
// always adds a profile row, arrives before or after the next event of its
// device, or before or after the first flush. So phase 1 takes the corpus in
// with the buffer flushes off (DISABLED_CRON_JOBS on the Bun worker, a shorter
// GO_WORKER_CRON_JOBS on the Go one) and the sessions queue paused while the
// corpus is produced (a paused queue is BullMQ's own: both workers consume
// sessions through its scripts), then resumes it once every session is older
// than the idle timeout and lets the reaper end the last ones. Phase 2 starts
// the same workers with the flushes on over the full buffers.
const RESUME_MARGIN_MS = 5000;
const IDLE_HOLD_MS = 15_000;
const IDLE_TIMEOUT_MS = REAPER_INTERVAL_MS + 4 * 60 * 1000;
const FULL_READY_TIMEOUT_MS = 180_000;
const BUFFER_LISTS = [
  'event_buffer:queue',
  'session-buffer',
  'profile-buffer',
  'profile-backfill-buffer',
  'bot-events-buffer',
  'replay-buffer',
  'group-buffer',
  'ai-event-buffer',
];
const BULLMQ_PIPELINE_QUEUES = ['sessions', 'notification'];

const REAPER_CRON_JOBS = ['sessionReaper', 'sessionVacuum'];
const FLUSH_CRON_JOBS = PIPELINE_CRON_JOBS.filter(
  (job) => !REAPER_CRON_JOBS.includes(job)
);

function fullEnv(
  ctx: SideContext,
  part: FullPart,
  phase: FullPhase
): Record<string, string> {
  const database = new URL(CH_RAW.href);
  database.pathname = `/${ctx.clickhouseDb}`;
  const base = {
    ...workerEnv(ctx),
    CLICKHOUSE_URL: database.href,
    // No e-mail leaves the run: an unset key makes the e-mail client a no-op.
    RESEND_API_KEY: '',
    SMTP_HOST: '',
    SESSION_TIMEOUT_MS: String(SESSION_TIMEOUT_MS),
    DISABLED_CRON_JOBS: '',
    GO_WORKER_QUEUES: '',
    GO_WORKER_CRON_JOBS: '',
    API_PORT: String(ctx.httpPort + (part === 'companion' ? 1 : 0)),
  };
  if (part === 'ts') {
    return {
      ...base,
      ENABLED_QUEUES: 'events,sessions,cron,notification',
      DISABLED_CRON_JOBS: [
        ...UNRELATED_CRON_NAMES,
        ...(phase === 'collect' ? FLUSH_CRON_JOBS : []),
      ].join(','),
    };
  }
  if (part === 'go') {
    return {
      ...base,
      ENABLED_QUEUES: 'events',
      GO_WORKER_QUEUES: 'sessions',
      GO_WORKER_CRON_JOBS: (phase === 'collect'
        ? REAPER_CRON_JOBS
        : PIPELINE_CRON_JOBS
      ).join(','),
    };
  }
  return {
    ...base,
    ENABLED_QUEUES: 'notification,cron',
    DISABLED_CRON_JOBS: [...PIPELINE_CRON_JOBS, ...UNRELATED_CRON_NAMES].join(
      ','
    ),
  };
}

// ----- ClickHouse -----
const CH_RAW = new URL(process.env.CLICKHOUSE_URL ?? 'http://localhost:23123');
const DEV_CLICKHOUSE_DB = CH_RAW.pathname.replace(/^\//, '') || 'default';

async function chRequest(database: string | undefined, statement: string) {
  const url = new URL(CH_RAW.origin);
  if (database) {
    url.searchParams.set('database', database);
  }
  const headers: Record<string, string> = {};
  if (CH_RAW.username) {
    headers.authorization = `Basic ${btoa(`${decodeURIComponent(CH_RAW.username)}:${decodeURIComponent(CH_RAW.password)}`)}`;
  }
  const response = await fetch(url, {
    method: 'POST',
    body: statement,
    headers,
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`clickhouse ${response.status}: ${text.slice(0, 300)}`);
  }
  return text;
}

async function chRows<T>(database: string, statement: string): Promise<T[]> {
  const text = await chRequest(database, `${statement} FORMAT JSONEachRow`);
  return text
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as T);
}

async function createClickhouse(database: string) {
  if (!database.startsWith('openpanel_wparity_')) {
    throw new Error(`refusing to create ClickHouse database ${database}`);
  }
  await chRequest(undefined, `DROP DATABASE IF EXISTS ${database}`);
  await chRequest(undefined, `CREATE DATABASE ${database}`);
  cleanups.push(async () => {
    await chRequest(undefined, `DROP DATABASE IF EXISTS ${database}`);
  });
  const schema = await Bun.file(`${ROOT}test/clickhouse-schema.sql`).text();
  const statements = schema
    .split(/^;$/m)
    .map((statement) =>
      statement
        .split('\n')
        .filter((line) => !line.startsWith('--'))
        .join('\n')
        .trim()
    )
    .filter(Boolean);
  for (const statement of statements) {
    await chRequest(database, statement);
  }
  // Without merges the rows of a table (sign pairs of sessions, versions of a
  // profile) are exactly what was inserted, not what a background merge made of
  // them when it happened to run.
  const tables = await chRows<{ name: string }>(
    database,
    `SELECT name FROM system.tables WHERE database = '${database}' AND engine LIKE '%MergeTree'`
  );
  for (const { name } of tables) {
    await chRequest(undefined, `SYSTEM STOP MERGES ${database}.\`${name}\``);
  }
}

async function devClickhouseWparityRows(): Promise<number> {
  const [row] = await chRows<{ n: string }>(
    DEV_CLICKHOUSE_DB,
    "SELECT count() AS n FROM events WHERE project_id LIKE 'wparity-%'"
  );
  return Number(row?.n ?? 0);
}

export type ClickhouseDump = Record<string, string[]>;

const FINAL_ENGINES = /Replacing|Collapsing|Aggregating|Summing/;
async function dumpClickhouse(database: string): Promise<ClickhouseDump> {
  const tables = await chRows<{ name: string; engine: string }>(
    database,
    `SELECT name, engine FROM system.tables WHERE database = '${database}' AND name NOT LIKE '.inner%' ORDER BY name`
  );
  const dump: ClickhouseDump = {};
  for (const { name, engine } of tables) {
    const columns = await chRows<{ name: string; type: string }>(
      database,
      `SELECT name, type FROM system.columns WHERE database = '${database}' AND table = '${name}' ORDER BY position`
    );
    // An aggregate state is read through its final value: the bytes of a state
    // depend on the order it was merged in.
    const select = columns
      .map((column) =>
        column.type.startsWith('AggregateFunction(')
          ? `finalizeAggregation(\`${column.name}\`) AS \`${column.name}\``
          : `\`${column.name}\``
      )
      .join(', ');
    const read = async (final: boolean) => {
      const text = await chRequest(
        database,
        `SELECT ${select} FROM \`${name}\`${final ? ' FINAL' : ''} SETTINGS output_format_json_quote_64bit_integers = 1 FORMAT JSONEachRow`
      );
      return text.split('\n').filter(Boolean);
    };
    const hasStates = columns.some((column) =>
      column.type.startsWith('AggregateFunction(')
    );
    dump[`${name} engine`] = [hasStates ? `${engine}+states` : engine];
    dump[name] = await read(false);
    if (FINAL_ENGINES.test(engine) || engine === 'MaterializedView') {
      dump[`${name} FINAL`] = await read(true).catch((error) => [
        `<query failed: ${String(error).slice(0, 200)}>`,
      ]);
    }
  }
  return dump;
}

// What differs between two identical runs of one implementation, per column
// (checked with --a ts --b ts --full). Everything else is compared as written.
//  - inserted_at: the wall clock of the insert.
//  - profiles created_at / last_seen_at: the wall clock of the profile upsert.
//  - minted uuids (session_start ids, ids of records that carried none): random.
const CLICKHOUSE_WALLCLOCK_COLUMNS: Record<string, string[]> = {
  '*': ['inserted_at'],
  profiles: ['created_at', 'last_seen_at'],
};

function normalizeClickhouseRow(
  table: string,
  line: string,
  known: KnownValues
): string {
  if (line.startsWith('<query failed')) {
    return line;
  }
  const row = JSON.parse(line) as Record<string, unknown>;
  const wallclock = new Set([
    ...CLICKHOUSE_WALLCLOCK_COLUMNS['*']!,
    ...(CLICKHOUSE_WALLCLOCK_COLUMNS[table.replace(/ FINAL$/, '')] ?? []),
  ]);
  const out: Record<string, unknown> = {};
  for (const [column, value] of Object.entries(row)) {
    out[column] = wallclock.has(column)
      ? WALLCLOCK
      : maskDeep(value, column, '', known);
  }
  return JSON.stringify(out);
}

// What an insert's batching decides, not the worker, and so is not compared:
//  - a ReplacingMergeTree table (profiles, groups, ...) keeps one row per
//    insert that touched a key until a merge; how many inserts a key went
//    through depends on which flush tick its rows met. The merged row (FINAL)
//    is compared.
//  - a materialized view's rows are one aggregate per inserted block, and its
//    plain columns (event_count, max(created_at)) are not merged by the engine:
//    FINAL keeps one block's value per key. A view of aggregate states is read
//    merged; the others are summed per natural key (everything but the time and
//    count columns) over the rows of all blocks, which is what a query on the
//    view means.
const MV_SUM_COLUMNS = ['event_count'];
const MV_MIN_COLUMNS = ['first_event_time'];
const MV_MAX_COLUMNS = ['created_at', 'last_event_time'];

function reduceMaterializedView(lines: string[]): string[] {
  const grouped = new Map<string, Record<string, unknown>>();
  for (const line of lines) {
    const row = JSON.parse(line) as Record<string, unknown>;
    const natural = Object.fromEntries(
      Object.entries(row).filter(
        ([column]) =>
          !(
            MV_SUM_COLUMNS.includes(column) ||
            MV_MIN_COLUMNS.includes(column) ||
            MV_MAX_COLUMNS.includes(column)
          )
      )
    );
    const key = JSON.stringify(natural);
    const total = grouped.get(key) ?? { ...row };
    if (grouped.has(key)) {
      for (const column of MV_SUM_COLUMNS) {
        if (column in row) {
          total[column] = String(Number(total[column]) + Number(row[column]));
        }
      }
      for (const column of MV_MIN_COLUMNS) {
        if (column in row && String(row[column]) < String(total[column])) {
          total[column] = row[column];
        }
      }
      for (const column of MV_MAX_COLUMNS) {
        if (column in row && String(row[column]) > String(total[column])) {
          total[column] = row[column];
        }
      }
    }
    grouped.set(key, total);
  }
  return [...grouped.values()].map((row) => JSON.stringify(row));
}

function normalizeClickhouse(dump: ClickhouseDump): Record<string, string[]> {
  const known = knownValues();
  const out: Record<string, string[]> = {};
  for (const [table, lines] of Object.entries(dump)) {
    if (table.endsWith(' engine')) {
      out[table] = lines;
      continue;
    }
    const engine = dump[`${table.replace(/ FINAL$/, '')} engine`]?.[0] ?? '';
    const isFinal = table.endsWith(' FINAL');
    if (engine.includes('Replacing') && !isFinal) {
      continue;
    }
    if (engine.startsWith('MaterializedView')) {
      // A view holding aggregate states is read merged (FINAL); the other
      // views are summed from the rows of every block, because FINAL keeps the
      // count of one block per key, whichever merges first.
      const merged = engine.endsWith('+states');
      if (merged === isFinal) {
        const rows = merged ? lines : reduceMaterializedView(lines);
        out[`${table} (per key)`] = rows
          .map((line) => normalizeClickhouseRow(table, line, known))
          .sort();
      }
      continue;
    }
    out[table] = lines
      .map((line) => normalizeClickhouseRow(table, line, known))
      .sort();
  }
  return out;
}

function multiset(lines: string[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const line of lines) {
    counts.set(line, (counts.get(line) ?? 0) + 1);
  }
  return counts;
}

function columnDiff(a: string, b: string): string {
  try {
    const rowA = JSON.parse(a) as Record<string, unknown>;
    const rowB = JSON.parse(b) as Record<string, unknown>;
    return Object.keys({ ...rowA, ...rowB })
      .filter((key) => JSON.stringify(rowA[key]) !== JSON.stringify(rowB[key]))
      .map((key) => `${key}: a=${show(rowA[key])} b=${show(rowB[key])}`)
      .join('; ');
  } catch {
    return '';
  }
}

const ROWS_SHOWN_PER_TABLE = 4;
function diffClickhouse(a: ClickhouseDump, b: ClickhouseDump): string[] {
  const out: string[] = [];
  const na = normalizeClickhouse(a);
  const nb = normalizeClickhouse(b);
  for (const table of new Set([...Object.keys(na), ...Object.keys(nb)])) {
    const rowsA = na[table];
    const rowsB = nb[table];
    if (!(rowsA && rowsB)) {
      out.push(`clickhouse ${table}: only in ${rowsA ? 'a' : 'b'}`);
      continue;
    }
    const countsA = multiset(rowsA);
    const countsB = multiset(rowsB);
    const onlyA: string[] = [];
    const onlyB: string[] = [];
    for (const [row, count] of countsA) {
      for (let i = count - (countsB.get(row) ?? 0); i > 0; i--) {
        onlyA.push(row);
      }
    }
    for (const [row, count] of countsB) {
      for (let i = count - (countsA.get(row) ?? 0); i > 0; i--) {
        onlyB.push(row);
      }
    }
    if (onlyA.length === 0 && onlyB.length === 0) {
      continue;
    }
    out.push(
      `clickhouse ${table}: ${rowsA.length} rows in a, ${rowsB.length} in b; ${onlyA.length} only in a, ${onlyB.length} only in b`
    );
    for (
      let i = 0;
      i < Math.min(ROWS_SHOWN_PER_TABLE, Math.max(onlyA.length, onlyB.length));
      i++
    ) {
      const rowA = onlyA[i];
      const rowB = onlyB[i];
      if (rowA && rowB) {
        out.push(
          `  ${table} row pair: ${columnDiff(rowA, rowB) || `a=${show(rowA)} b=${show(rowB)}`}`
        );
      } else {
        out.push(
          `  ${table} only in ${rowA ? 'a' : 'b'}: ${show(rowA ?? rowB)}`
        );
      }
    }
  }
  return out;
}

// ----- the run -----
async function readyPort(port: number): Promise<boolean> {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/healthz/ready`);
    return response.ok;
  } catch {
    return false;
  }
}

// Pausing is BullMQ's own: the meta field both workers look at.
async function setQueuesPaused(ctx: SideContext, paused: boolean) {
  const connection = new Redis(redisUrl(ctx.redisIndex), {
    maxRetriesPerRequest: null,
  });
  try {
    for (const name of PAUSED_QUEUES) {
      const queue = new Queue(`${name}-${ctx.namespace}`, { connection });
      await (paused ? queue.pause() : queue.resume());
      await queue.close();
    }
  } finally {
    connection.disconnect();
  }
}

interface Idle {
  idle: boolean;
  why: string;
}
async function fullIdle(
  ctx: SideContext,
  client: Redis,
  produced: number,
  phase: FullPhase
): Promise<Idle> {
  const group = await describeGroup(ctx.group, ctx.topic);
  if (group.lag !== 0 || group.current < produced) {
    return { idle: false, why: `kafka lag ${group.lag}` };
  }
  for (const key of phase === 'flush' ? BUFFER_LISTS : []) {
    const length = await client.llen(key);
    if (length > 0) {
      return { idle: false, why: `${key} holds ${length}` };
    }
  }
  const [, live] = await client.scan(
    '0',
    'MATCH',
    'session:wallclock:*',
    'COUNT',
    1000
  );
  if (live.length > 0) {
    return { idle: false, why: 'sessions still open' };
  }
  for (const queue of BULLMQ_PIPELINE_QUEUES) {
    const prefix = `bull:${queue}-${ctx.namespace}:`;
    const counts = [
      ['wait', await client.llen(`${prefix}wait`)],
      ['active', await client.llen(`${prefix}active`)],
      ['paused', await client.llen(`${prefix}paused`)],
      ['delayed', await client.zcard(`${prefix}delayed`)],
      ['prioritized', await client.zcard(`${prefix}prioritized`)],
      ['waiting-children', await client.zcard(`${prefix}waiting-children`)],
    ] as const;
    const busy = counts.find(([, count]) => count > 0);
    if (busy) {
      return { idle: false, why: `${queue} has ${busy[1]} ${busy[0]}` };
    }
  }
  return { idle: true, why: '' };
}

const ERROR_LEVEL = /"level":(50|60|"ERROR")/;
async function unexpectedErrors(logs: string[]): Promise<string[]> {
  const lines: string[] = [];
  for (const log of logs) {
    const text = await Bun.file(log).text();
    lines.push(
      ...text
        .split('\n')
        .filter((line) => ERROR_LEVEL.test(line))
        .filter(
          (line) => !EXPECTED_ERRORS.some((pattern) => pattern.test(line))
        )
    );
  }
  return lines;
}

async function runFullSide(
  kind: string,
  ctx: SideContext
): Promise<SideResult> {
  const started = Date.now();
  await resetFixtureState();
  await flushRedisIndex(ctx.redisIndex);
  cleanups.push(async () => {
    await flushRedisIndex(ctx.redisIndex);
  });
  await createClickhouse(ctx.clickhouseDb);
  const devRowsBefore = await devClickhouseWparityRows();
  const created = await rpk(
    'topic',
    'create',
    ctx.topic,
    '-p',
    String(corpus.partitions),
    '-r',
    '1'
  );
  if (created.code !== 0) {
    throw new Error(`topic create failed: ${created.out}${created.err}`);
  }
  cleanups.push(async () => {
    await rpk('topic', 'delete', ctx.topic, `${ctx.topic}-dlq`);
    await rpk('group', 'delete', ctx.group);
  });

  const parts: FullPart[] = kind === 'ts' ? ['ts'] : ['go', 'companion'];
  const logs: string[] = [];
  const startWorkers = async (phase: FullPhase) => {
    const workers = [] as {
      proc: ReturnType<typeof spawn>;
      log: string;
      port: number;
      stopped: boolean;
    }[];
    for (const part of parts) {
      const spawned = await startWorker(kind, ctx, part, phase);
      const worker = {
        ...spawned,
        port: ctx.httpPort + (part === 'companion' ? 1 : 0),
        stopped: false,
      };
      workers.push(worker);
      logs.push(worker.log);
      cleanups.push(async () => {
        if (!worker.stopped) {
          await stopWorker(worker.proc);
        }
      });
    }
    await until('the workers to be ready', FULL_READY_TIMEOUT_MS, async () => {
      for (const worker of workers) {
        if (worker.proc.exitCode !== null) {
          throw new Error(
            `worker exited with ${worker.proc.exitCode} before it was ready; see ${worker.log}`
          );
        }
      }
      const group = await describeGroup(ctx.group, ctx.topic);
      const ready = await Promise.all(
        workers.map((worker) => readyPort(worker.port))
      );
      return group.state === 'Stable' && ready.every(Boolean);
    });
    return workers;
  };
  const stopWorkers = async (
    workers: Awaited<ReturnType<typeof startWorkers>>
  ) => {
    for (const worker of workers) {
      const exitCode = await stopWorker(worker.proc);
      worker.stopped = true;
      if (exitCode !== 0) {
        console.error(`${ctx.name}: ${worker.log} exited with ${exitCode}`);
      }
    }
  };
  const waitForIdle = async (
    workers: Awaited<ReturnType<typeof startWorkers>>,
    phase: FullPhase,
    produced: number
  ) => {
    const client = redisClient(ctx.redisIndex);
    let idleSince = 0;
    let lastWhy = '';
    try {
      await until(`the ${phase} phase to finish`, IDLE_TIMEOUT_MS, async () => {
        for (const worker of workers) {
          if (worker.proc.exitCode !== null) {
            throw new Error(
              `worker died (${worker.proc.exitCode}); see ${worker.log}`
            );
          }
        }
        // A flush ClickHouse rejects is retried for ever with the same rows,
        // so the run would only wait out its timeout.
        const errors = await unexpectedErrors(logs);
        if (errors.length > 0) {
          throw new Error(`worker error: ${errors[0]!.slice(0, 600)}`);
        }
        const state = await fullIdle(ctx, client, produced, phase);
        if (!state.idle) {
          idleSince = 0;
          lastWhy = state.why;
          return false;
        }
        idleSince ||= Date.now();
        return Date.now() - idleSince >= IDLE_HOLD_MS;
      }).catch((error) => {
        throw new Error(`${error.message} (last: ${lastWhy})`);
      });
    } finally {
      client.disconnect();
    }
  };

  const producer = new Producer({
    clientId: `wparity-${RUN}`,
    bootstrapBrokers: [KAFKA_BROKERS],
    partitioner: eventsPartitioner,
  });
  cleanups.push(() => producer.close());

  // Phase 1: the corpus goes in and every session ends; nothing is flushed.
  const collecting = await startWorkers('collect');
  await setQueuesPaused(ctx, true);
  const producingFrom = Date.now();
  const waves = [
    ...new Set(corpus.records.map((record) => record.wave)),
  ].sort();
  let produced = 0;
  for (const wave of waves) {
    const records = corpus.records.filter((record) => record.wave === wave);
    await produceWave(producer, ctx.topic, records);
    produced += records.length;
    await until(`wave ${wave} to be consumed`, DRAIN_TIMEOUT_MS, async () => {
      for (const worker of collecting) {
        if (worker.proc.exitCode !== null) {
          throw new Error(
            `worker died (${worker.proc.exitCode}); see ${worker.log}`
          );
        }
      }
      const state = await describeGroup(ctx.group, ctx.topic);
      return state.lag === 0 && state.current >= produced;
    });
  }
  const producingTo = Date.now();
  console.log(
    `  produced ${produced} records in ${((producingTo - producingFrom) / 1000).toFixed(1)}s`
  );
  await Bun.sleep(SESSION_TIMEOUT_MS + RESUME_MARGIN_MS);
  await setQueuesPaused(ctx, false);
  const resumedAt = Date.now();
  await waitForIdle(collecting, 'collect', produced);
  console.log(
    `  every session ended ${((Date.now() - resumedAt) / 1000).toFixed(0)}s after the sessions queue was resumed`
  );
  await stopWorkers(collecting);

  // Phase 2: the same workers with the flushes on, over a full set of buffers.
  const flushStarted = Date.now();
  const flushing = await startWorkers('flush');
  await waitForIdle(flushing, 'flush', produced);
  console.log(
    `  flushed ${((Date.now() - flushStarted) / 1000).toFixed(0)}s after the flushing workers started`
  );
  await stopWorkers(flushing);
  const redis = await dumpRedis(ctx.redisIndex);
  const postgres = await dumpPostgres();
  const clickhouse = await dumpClickhouse(ctx.clickhouseDb);
  const devRowsAfter = await devClickhouseWparityRows();
  const logErrors: string[] = [];
  if (devRowsAfter !== devRowsBefore) {
    logErrors.push(
      `the dev ClickHouse database '${DEV_CLICKHOUSE_DB}' gained ${devRowsAfter - devRowsBefore} wparity rows`
    );
  }
  logErrors.push(...(await unexpectedErrors(logs)));
  await Bun.write(
    `${OUT_DIR}/wparity-${RUN}-${ctx.name}.dump.json`,
    JSON.stringify({ redis, postgres, clickhouse }, null, 1)
  );
  await chRequest(undefined, `DROP DATABASE IF EXISTS ${ctx.clickhouseDb}`);
  return {
    side: ctx.name,
    redis,
    postgres,
    clickhouse,
    logErrors,
    durationMs: Date.now() - started,
  };
}

// What a --full end state must hold for a comparison of it to mean anything.
export function sanityFull(result: SideResult): string[] {
  const problems: string[] = [];
  const clickhouse = result.clickhouse ?? {};
  const rows = (table: string) => clickhouse[table]?.length ?? 0;
  const eventNames = (clickhouse.events ?? []).map(
    (line) => (JSON.parse(line) as { name: string }).name
  );
  const excluded = corpus.records.filter(
    (record) =>
      record.value.includes(`"name":"${EXCLUDED_EVENT}"`) &&
      record.value.includes(`"projectId":"${fixtureIds.projectA}"`)
  ).length;
  if (rows('events') < corpus.records.length - excluded) {
    problems.push(
      `only ${rows('events')} rows in events, expected at least ${corpus.records.length - excluded}`
    );
  }
  for (const name of ['session_start', 'session_end']) {
    if (!eventNames.includes(name)) {
      problems.push(`no ${name} row in events`);
    }
  }
  for (const table of [
    'sessions',
    'sessions FINAL',
    'profiles',
    'profiles FINAL',
  ]) {
    if (rows(table) === 0) {
      problems.push(`${table} is empty`);
    }
  }
  const keys = Object.keys(result.redis);
  if (
    keys.some((key) => /dead|dlq/i.test(key) && !key.startsWith('session:'))
  ) {
    problems.push('a dead-letter key exists');
  }
  for (const key of keys) {
    if (/^bull:[^:]+:failed$/.test(key)) {
      problems.push(`failed BullMQ jobs: ${key}`);
    }
    if (
      /^bull:[^:]+:(wait|active|delayed|paused)$/.test(key) &&
      !key.startsWith('bull:cron-')
    ) {
      problems.push(`BullMQ list left behind: ${key}`);
    }
  }
  if (keys.some((key) => key.startsWith('session:wallclock:'))) {
    problems.push('a session is still open');
  }
  if (result.logErrors.length > 0) {
    problems.push(
      `${result.logErrors.length} error lines in the worker logs, first: ${result.logErrors[0]!.slice(0, 300)}`
    );
  }
  return problems;
}

// ---------- main ----------
async function compareSaved(fileA: string, fileB: string) {
  const load = async (path: string, side: string): Promise<SideResult> => ({
    ...(await Bun.file(path).json()),
    side,
    logErrors: [],
    durationMs: 0,
  });
  return [await load(fileA, 'a'), await load(fileB, 'b')];
}

async function main() {
  const saved = flag('--compare');
  if (saved) {
    const results = await compareSaved(saved, flag('--with')!);
    report(results, false);
    return;
  }
  await setupFixtures();
  cleanups.push(teardownFixtures);

  for (const kind of CORPUS_KINDS) {
    corpus = loadCorpus(kind);
    console.log(
      `corpus ${kind}: ${corpus.records.length} records, ${corpus.partitions} partition(s)`
    );
    const indexes = await pickRedisIndexes(2);
    const sides: SideContext[] = SIDES.map((side, i) => ({
      side,
      name: `${kind}-${i === 0 ? 'a' : 'b'}-${side}`,
      topic: `wparity-${RUN}-${kind}-${i === 0 ? 'a' : 'b'}`,
      group: `wparity-${RUN}-${kind}-${i === 0 ? 'a' : 'b'}`,
      redisIndex: indexes[i]!,
      namespace: `wparity-${RUN}`,
      clickhouseDb: FULL
        ? `openpanel_wparity_full_${i === 0 ? 'a' : 'b'}`
        : `openpanel_wparity_${i === 0 ? 'a' : 'b'}`,
      // A --full side may run two processes (the Go worker and a Bun worker).
      httpPort: WORKER_HTTP_PORT_BASE + i * 2,
    }));
    const results: SideResult[] = [];
    for (const ctx of sides) {
      console.log(
        `running ${ctx.name} (topic ${ctx.topic}, redis db ${ctx.redisIndex})`
      );
      results.push(await (FULL ? runFullSide : runSide)(ctx.side, ctx));
      console.log(
        `  done in ${(results.at(-1)!.durationMs / 1000).toFixed(1)}s, ${Object.keys(results.at(-1)!.redis).length} redis keys`
      );
    }
    report(results, true, kind);
  }
}

function report(
  results: SideResult[],
  checkSanity: boolean,
  kind = 'saved dumps'
) {
  let failed = false;
  for (const result of checkSanity ? results : []) {
    const problems = FULL ? sanityFull(result) : sanity(result);
    for (const problem of problems) {
      console.error(`SANITY ${result.side}: ${problem}`);
      failed = true;
    }
  }
  const diffs = diffDumps(results[0]!, results[1]!);
  if (diffs.length > 0) {
    failed = true;
    console.error(
      `${diffs.length} difference(s) between ${results[0]!.side} and ${results[1]!.side}:`
    );
    for (const line of diffs.slice(0, MAX_REPORTED_DIFFS)) {
      console.error(`  ${line}`);
    }
    if (diffs.length > MAX_REPORTED_DIFFS) {
      console.error(`  ... ${diffs.length - MAX_REPORTED_DIFFS} more`);
    }
  } else {
    console.log(`[${kind}] no differences`);
  }
  process.exitCode = failed ? 1 : (process.exitCode ?? 0);
}

try {
  await main();
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  await cleanup();
  await sql.close();
  process.exit(process.exitCode ?? 0);
}
