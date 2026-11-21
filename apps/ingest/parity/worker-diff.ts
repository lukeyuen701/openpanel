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
// Needs the local stack (Postgres, Redis, Redpanda). Fixture rows are named
// wparity-*; the rule, client and notification ids are uuid columns, so those
// use fixed uuids and go away with the organization.
import { SQL, spawn } from 'bun';
import { tmpdir } from 'node:os';
import { Producer } from '../../../packages/core/node_modules/@platformatic/kafka/dist/index.js';
import { eventsPartitioner } from '../../../packages/core/src/modules/ingest/src/partitioner';
import Redis from '../../../packages/redis/node_modules/ioredis';
import {
  buildCorpus,
  EXCLUDED_EVENT,
  fixtureIds,
  NOTIFY_EVENT,
  SESSION_TIMEOUT_MS,
} from './worker-corpus';

const ROOT = new URL('../../..', import.meta.url).pathname;
const args = process.argv.slice(2);
const flag = (name: string) =>
  args.includes(name) ? args[args.indexOf(name) + 1] : undefined;
const SIDES = [flag('--a') ?? 'ts', flag('--b') ?? 'ts'] as const;
const GO_BIN = flag('--go-bin') ?? `${ROOT}apps/ingest/bin/worker`;
const GO_ARGS = (flag('--go-args') ?? 'worker').split(' ').filter(Boolean);
const OUT_DIR = process.env.WPARITY_OUT ?? tmpdir();
const RUN = Date.now().toString(36);

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
const STOP_TIMEOUT_MS = 30_000;
const TTL_BUCKET_SECONDS = 60;
const MAX_REPORTED_DIFFS = Number(flag('--max-diffs') ?? 60);
const MAX_VALUE_CHARS = 300;

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
  const row = out
    .split('\n')
    .map((line) => line.trim().split(/\s+/))
    .find((cols) => cols[0] === topic && /^\d+$/.test(cols[1] ?? ''));
  const num = (v: string | undefined) => (v && /^\d+$/.test(v) ? Number(v) : 0);
  return {
    state,
    current: num(row?.[2]),
    end: num(row?.[4]),
    lag: row ? num(row[5]) : Number.POSITIVE_INFINITY,
  };
}

// ---------- fixtures ----------
const PROJECT_IDS = [fixtureIds.projectA, fixtureIds.projectB];

async function teardownFixtures() {
  await sql`DELETE FROM notifications WHERE "projectId" IN ${sql(PROJECT_IDS)}`;
  await sql`DELETE FROM notification_rules WHERE id = ${fixtureIds.rule}::uuid`;
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
}

async function resetFixtureState() {
  await sql`DELETE FROM notifications WHERE "projectId" IN ${sql(PROJECT_IDS)}`;
  await sql`UPDATE projects SET "firstEventAt" = NULL WHERE id IN ${sql(PROJECT_IDS)}`;
}

interface PostgresDump {
  projects: unknown[];
  notifications: unknown[];
}
async function dumpPostgres(): Promise<PostgresDump> {
  const projects =
    await sql`SELECT id, ("firstEventAt" IS NOT NULL) AS "firstEventSet", "eventsCount", "noDataNotifiedAt", "dataStoppedNotifiedAt" FROM projects WHERE id IN ${sql(PROJECT_IDS)} ORDER BY id`;
  const notifications =
    await sql`SELECT title, message, "projectId", "sendToApp", "sendToEmail", "integrationId", "notificationRuleId", "isReadAt", payload FROM notifications WHERE "projectId" IN ${sql(PROJECT_IDS)} ORDER BY title, payload::text`;
  return {
    projects: [...projects],
    notifications: [...notifications],
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

async function startWorker(kind: string, ctx: SideContext) {
  const env = workerEnv(ctx);
  const stdout = Bun.file(`${OUT_DIR}/wparity-${RUN}-${ctx.name}.log`);
  const stderr = Bun.file(`${OUT_DIR}/wparity-${RUN}-${ctx.name}.err`);
  if (kind === 'ts') {
    return {
      proc: spawn({
        cmd: [
          process.execPath,
          'run',
          ...(await workerBunArgs()),
          'src/main.ts',
        ],
        cwd: `${ROOT}apps/api`,
        env: { ...env, ROLE: 'worker', ENABLED_QUEUES: 'events' },
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
  const created = await rpk('topic', 'create', ctx.topic, '-p', '1', '-r', '1');
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

  const corpus = buildCorpus();
  const waves = [...new Set(corpus.map((record) => record.wave))].sort();
  let produced = 0;
  for (const wave of waves) {
    const records = corpus.filter((record) => record.wave === wave);
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
    .filter((line) => /"level":(50|60)\b/.test(line));
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
  for (const record of buildCorpus()) {
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
  if (redisKey.startsWith('profile-buffer')) {
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
      value: (entry.value as [string, number][]).map(([member]) => [
        member,
        WALLCLOCK,
      ]),
    };
  }
  if (entry.type === 'stream') {
    const entries = entry.value as [string, string[]][];
    return {
      ...entry,
      value: entries.map(([, fields]) => [
        WALLCLOCK,
        maskDeep(fields, '', redisKey, known),
      ]),
    };
  }
  return { ...entry, value: maskDeep(entry.value, '', redisKey, known) };
}

export function normalize(dump: RedisDump): RedisDump {
  const known = knownValues();
  return Object.fromEntries(
    Object.entries(dump).map(([key, entry]) => [
      key,
      normalizeEntry(key, entry, known),
    ])
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
    if (ea.ttl !== eb.ttl) {
      out.push(`redis ${key}: ttl a=${ea.ttl} b=${eb.ttl}`);
    }
    diffValues(ea.value, eb.value, `redis ${key}`, out);
  }
  diffValues(a.postgres, b.postgres, 'postgres', out);
  return out;
}

// ---------- sanity ----------
// A comparison of two empty runs would pass; these keep the rig honest.
export function sanity(result: SideResult): string[] {
  const problems: string[] = [];
  const keys = Object.keys(result.redis);
  const corpus = buildCorpus();
  const listEvents = Object.values(result.redis)
    .filter((entry) => entry.type === 'list')
    .flatMap((entry) => entry.value as string[]);
  const eventRows = listEvents.filter(
    (row) => row.includes('"project_id"') && row.includes('"device_id"')
  );
  const expectMin = corpus.length - 1;
  if (eventRows.length < expectMin) {
    problems.push(
      `only ${eventRows.length} event rows in Redis lists, expected at least ${expectMin}`
    );
  }
  if (eventRows.some((row) => row.includes(`"name":"${EXCLUDED_EVENT}"`))) {
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
  if (!projects.every((project) => project.firstEventSet)) {
    problems.push('firstEventAt not set on both projects');
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
  const indexes = await pickRedisIndexes(2);
  const sides: SideContext[] = SIDES.map((kind, i) => ({
    side: kind,
    name: `${i === 0 ? 'a' : 'b'}-${kind}`,
    topic: `wparity-${RUN}-${i === 0 ? 'a' : 'b'}`,
    group: `wparity-${RUN}-${i === 0 ? 'a' : 'b'}`,
    redisIndex: indexes[i]!,
    namespace: `wparity-${RUN}`,
    clickhouseDb: `openpanel_wparity_${i === 0 ? 'a' : 'b'}`,
    httpPort: WORKER_HTTP_PORT_BASE + i,
  }));
  await setupFixtures();
  cleanups.push(teardownFixtures);

  const results: SideResult[] = [];
  for (const ctx of sides) {
    console.log(
      `running ${ctx.name} (topic ${ctx.topic}, redis db ${ctx.redisIndex})`
    );
    results.push(await runSide(ctx.side, ctx));
    console.log(
      `  done in ${(results.at(-1)!.durationMs / 1000).toFixed(1)}s, ${Object.keys(results.at(-1)!.redis).length} redis keys`
    );
  }

  report(results, true);
}

function report(results: SideResult[], checkSanity: boolean) {
  let failed = false;
  for (const result of checkSanity ? results : []) {
    const problems = sanity(result);
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
    console.log('no differences');
  }
  process.exitCode = failed ? 1 : 0;
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
