// Differential test for the session lifecycle: the same Redis, ClickHouse and
// Postgres state through the TypeScript handlers (p3-side.ts, runJob) and through
// the Go worker (go test -run TestP3ParitySide), comparing what each leaves in
// Redis (keys, BullMQ jobs, the event/profile/profile-backfill buffers) and
// Postgres (notifications, counters, usage-alert markers).
//
//   bun parity/p3-diff.ts            (from apps/ingest, with the repo .env loaded)
//
// Side A and B run one after the other on their own Redis db (11, 12), BullMQ
// namespace and ClickHouse database (openpanel_wparity_p3_a/_b, built from
// test/clickhouse-schema.sql). Postgres is shared: the fixture rows (wparity-p3-*)
// are reset between the sides. Everything it makes is removed in a finally block
// and on SIGINT/SIGTERM. Needs the local stack and Go.
import { SQL, spawn } from 'bun';
import { tmpdir } from 'node:os';
import Redis from '../../../packages/redis/node_modules/ioredis';

const ROOT = new URL('../../..', import.meta.url).pathname;
const OUT_DIR = process.env.WPARITY_OUT ?? tmpdir();
const RUN = Date.now().toString(36);
const REDIS_BASE = (process.env.REDIS_URL ?? 'redis://127.0.0.1:23379').replace(
  /\/\d+$/,
  ''
);
const CH_RAW = new URL(process.env.CLICKHOUSE_URL ?? 'http://localhost:23123');
const dbUrl = (process.env.DATABASE_URL ?? '').split('?')[0]!;
const sql = new SQL(dbUrl);

const ORG = 'wparity-p3-org';
const P1 = 'wparity-p3-a';
const P2 = 'wparity-p3-b';
const RULE = '0a1a0000-0000-4000-8000-0000000000c1';
const PROJECTS = [P1, P2];
const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;
const NOW = Date.now();

const SIDES = [
  {
    name: 'ts',
    redisIndex: 11,
    namespace: 'wparity-p3ns-a',
    chDb: 'openpanel_wparity_p3_a',
  },
  {
    name: 'go',
    redisIndex: 12,
    namespace: 'wparity-p3ns-b',
    chDb: 'openpanel_wparity_p3_b',
  },
] as const;

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

// ---------- ClickHouse ----------
function chRequest(database: string | undefined, statement: string) {
  const url = new URL(CH_RAW.origin);
  if (database) {
    url.searchParams.set('database', database);
  }
  const headers: Record<string, string> = {};
  if (CH_RAW.username) {
    headers.authorization = `Basic ${btoa(`${decodeURIComponent(CH_RAW.username)}:${decodeURIComponent(CH_RAW.password)}`)}`;
  }
  return fetch(url, { method: 'POST', body: statement, headers }).then(
    async (response) => {
      const text = await response.text();
      if (!response.ok) {
        throw new Error(`clickhouse ${response.status}: ${text.slice(0, 300)}`);
      }
      return text;
    }
  );
}

async function createClickhouse(database: string) {
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
}

// ---------- scenario ----------
const clickhouseDate = (ms: number) =>
  new Date(ms).toISOString().replace('T', ' ').replace('Z', '');
const SESSION_START = Date.UTC(2026, 5, 8, 10, 30, 0);
const SESSION_END = Date.UTC(2026, 5, 8, 11, 0, 0);

function blob(overrides: Record<string, unknown>) {
  return JSON.stringify({
    id: 's',
    project_id: P1,
    device_id: 'd',
    profile_id: 'd',
    event_count: 3,
    screen_view_count: 2,
    entry_path: '/',
    entry_origin: 'https://x.test',
    exit_path: '/end',
    exit_origin: 'https://x.test',
    created_at: clickhouseDate(SESSION_START),
    ended_at: clickhouseDate(SESSION_END),
    referrer: '',
    referrer_name: '',
    referrer_type: '',
    os: 'Mac',
    os_version: '14',
    browser: 'Chrome',
    browser_version: '128',
    device: 'desktop',
    brand: '',
    model: '',
    country: 'SE',
    region: '',
    city: 'Stockholm',
    longitude: 18.07,
    latitude: 59.33,
    is_bounce: false,
    duration: 1_800_000,
    utm_medium: '',
    utm_source: '',
    utm_campaign: '',
    utm_content: '',
    utm_term: '',
    revenue: 0,
    sign: 1,
    version: 2,
    groups: [],
    ...overrides,
  });
}

interface SessionSeed {
  project: string;
  device: string;
  blob?: string;
  wallclock: number;
  pointer?: { profile: string };
}
const sessionSeeds: SessionSeed[] = [
  {
    project: P1,
    device: 'dev-1',
    blob: blob({ id: 's1', device_id: 'dev-1', profile_id: 'u1' }),
    wallclock: NOW - 40 * MINUTE,
    pointer: { profile: 'u1' },
  },
  {
    project: P1,
    device: 'dev-2',
    blob: blob({ id: 's2', device_id: 'dev-2', profile_id: 'dev-2' }),
    wallclock: NOW - 41 * MINUTE,
  },
  {
    project: P1,
    device: 'dev-3',
    blob: blob({ id: 's3', device_id: 'dev-3', profile_id: 'dev-3' }),
    wallclock: NOW - MINUTE,
  },
  { project: P1, device: 'dev-4', wallclock: NOW - 50 * MINUTE },
  {
    project: P1,
    device: 'dev-5',
    blob: blob({ id: 's5', device_id: 'dev-5', profile_id: 'dev-5' }),
    wallclock: NOW - 8 * DAY,
  },
  { project: P1, device: 'dev-6', wallclock: NOW - 9 * DAY },
  {
    project: P1,
    device: 'dev-7',
    blob: blob({
      id: 's7',
      device_id: 'dev-7',
      profile_id: 'dev-7',
      ended_at: clickhouseDate(SESSION_END + 10 * MINUTE),
    }),
    wallclock: NOW - 2 * MINUTE,
  },
  {
    project: P1,
    device: 'dev-8',
    blob: blob({ id: 's8b', device_id: 'dev-8', profile_id: 'dev-8' }),
    wallclock: NOW - 3 * MINUTE,
  },
  {
    project: P1,
    device: 'dev-9',
    blob: blob({ id: 's9', device_id: 'dev-9', profile_id: 'dev-9' }),
    wallclock: NOW - 4 * MINUTE,
  },
  {
    project: P2,
    device: 'dev-20',
    blob: blob({
      id: 's20',
      project_id: P2,
      device_id: 'dev-20',
      profile_id: 'dev-20',
    }),
    wallclock: NOW - 60 * MINUTE,
  },
];

async function seedRedis(index: number) {
  const client = new Redis(`${REDIS_BASE}/${index}`);
  try {
    await client.flushdb();
    for (const seed of sessionSeeds) {
      if (seed.blob) {
        await client.set(`session:${seed.project}:${seed.device}`, seed.blob);
      }
      await client.zadd(
        `session:wallclock:${seed.project}`,
        seed.wallclock,
        seed.device
      );
      await client.sadd('session:projects', seed.project);
      if (seed.pointer) {
        await client.set(
          `session:profile:${seed.project}:${seed.pointer.profile}`,
          seed.device,
          'PX',
          3_600_000
        );
      }
    }
    await client.set(`session:reaper:lock:${P2}`, '1', 'EX', 600);
  } finally {
    client.disconnect();
  }
}

function sessionEvent(
  deviceId: string,
  sessionId: string,
  extra: Record<string, unknown> = {}
) {
  return {
    projectId: P1,
    deviceId,
    sessionId,
    profileId: 'u1',
    name: 'screen_view',
    properties: { __query: { utm_source: 'next' }, plan: 'pro' },
    groups: ['g1'],
    createdAt: '2026-06-08T12:00:00.000Z',
    duration: 0,
    sdkName: 'web',
    sdkVersion: '1.2.3',
    city: 'Oslo',
    country: 'NO',
    region: '',
    longitude: 10.75,
    latitude: 59.91,
    path: '/next',
    origin: 'https://x.test',
    referrer: '',
    referrerName: '',
    referrerType: '',
    os: 'Mac',
    osVersion: '14',
    browser: 'Chrome',
    browserVersion: '128',
    device: 'desktop',
    brand: '',
    model: '',
    revenue: 5,
    ...extra,
  };
}
const envelope = (
  event: unknown,
  snapshot: unknown,
  requestId = 'wparity-req'
) => ({
  payload: { event, snapshot: JSON.parse(snapshot as string) },
  meta: { requestId },
});

const steps = [
  { type: 'vacuum' },
  { type: 'reap' },
  {
    type: 'job',
    data: envelope(sessionEvent('dev-1', 's1'), sessionSeeds[0]!.blob!),
  },
  // The same close again: the claim is taken.
  {
    type: 'job',
    data: envelope(sessionEvent('dev-1', 's1'), sessionSeeds[0]!.blob!),
  },
  // Extended after enqueue: nothing is emitted.
  {
    type: 'job',
    data: envelope(
      sessionEvent('dev-7', 's7'),
      blob({ id: 's7', device_id: 'dev-7', profile_id: 'dev-7' })
    ),
  },
  // A boundary: the slot already holds the next session.
  {
    type: 'job',
    data: envelope(
      sessionEvent('dev-8', 's8', { profileId: '' }),
      blob({ id: 's8', device_id: 'dev-8', profile_id: 'dev-8' })
    ),
  },
  // The legacy wire shape.
  {
    type: 'job',
    data: {
      type: 'createSessionEnd',
      payload: sessionEvent('dev-9', 's9', { profileId: 'dev-9' }),
      snapshot: JSON.parse(sessionSeeds[8]!.blob!),
    },
  },
] as const;

const eventId = (n: number) =>
  `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;

async function seedClickhouse(database: string) {
  const rows: Record<string, unknown>[] = [
    {
      id: eventId(1),
      name: 'screen_view',
      session_id: 's1',
      created_at: clickhouseDate(SESSION_START + MINUTE),
      device_id: 'dev-1',
      profile_id: 'u1',
      path: '/a',
    },
    {
      id: eventId(2),
      name: 'purchase',
      session_id: 's1',
      created_at: clickhouseDate(SESSION_START + 15 * MINUTE),
      device_id: 'dev-1',
      profile_id: 'u1',
      path: '/buy',
      properties: { plan: 'pro' },
    },
  ];
  for (let i = 3; i <= 9; i += 1) {
    rows.push({
      id: eventId(i),
      name: 'click',
      session_id: `other-${i}`,
      created_at: clickhouseDate(Date.UTC(2026, 5, i, 9, 0, 0)),
      device_id: 'dev-x',
      profile_id: 'dev-x',
    });
  }
  const body = rows
    .map((row) => JSON.stringify({ project_id: P1, ...row }))
    .join('\n');
  await chRequest(database, `INSERT INTO events FORMAT JSONEachRow\n${body}`);
}

// ---------- Postgres ----------
async function teardownFixtures() {
  await sql`DELETE FROM notifications WHERE "projectId" IN ${sql(PROJECTS)}`;
  await sql`DELETE FROM notification_rules WHERE "projectId" IN ${sql(PROJECTS)}`;
  await sql`DELETE FROM projects WHERE id IN ${sql(PROJECTS)}`;
  await sql`DELETE FROM organizations WHERE id = ${ORG}`;
}

async function setupFixtures() {
  await teardownFixtures();
  await sql`INSERT INTO organizations (id, name, "subscriptionStatus", "subscriptionStartsAt", "subscriptionEndsAt", "subscriptionInterval", "subscriptionPeriodEventsLimit") VALUES (${ORG}, 'P3 parity', 'active', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z', 'month', 10)`;
  await sql`INSERT INTO projects (id, name, "organizationId", cors) VALUES (${P1}, 'P3 parity A', ${ORG}, ${'{*}'}::text[])`;
  await sql`INSERT INTO projects (id, name, "organizationId", cors) VALUES (${P2}, 'P3 parity B', ${ORG}, ${'{*}'}::text[])`;
  const config = {
    type: 'funnel',
    events: [
      { name: 'screen_view', segment: 'event', filters: [] },
      {
        name: 'purchase',
        segment: 'event',
        filters: [{ name: 'properties.plan', operator: 'is', value: ['pro'] }],
      },
    ],
  };
  await sql`INSERT INTO notification_rules (id, name, "projectId", "sendToApp", "sendToEmail", config, template) VALUES (${RULE}::uuid, 'p3 funnel', ${P1}, true, false, ${JSON.stringify(config)}::text::jsonb, 'Funnel $RULE_NAME: $EVENT_NAME')`;
}

async function resetFixtureState() {
  await sql`DELETE FROM notifications WHERE "projectId" IN ${sql(PROJECTS)}`;
  await sql`UPDATE projects SET "eventsCount" = 0 WHERE id IN ${sql(PROJECTS)}`;
  await sql`UPDATE organizations SET "subscriptionPeriodEventsCount" = 0, "subscriptionPeriodEventsCountExceededAt" = NULL, "usageWarningSentAt" = NULL, "usageExceededSentAt" = NULL WHERE id = ${ORG}`;
}

async function dumpPostgres() {
  const notifications =
    await sql`SELECT "projectId", title, message, "sendToApp", "sendToEmail", "integrationId", "notificationRuleId", payload FROM notifications WHERE "projectId" IN ${sql(PROJECTS)} ORDER BY "projectId", title, payload::text`;
  const projects =
    await sql`SELECT id, "eventsCount" FROM projects WHERE id IN ${sql(PROJECTS)} ORDER BY id`;
  const [org] =
    await sql`SELECT "subscriptionPeriodEventsCount" AS count, "subscriptionPeriodEventsCountExceededAt" IS NOT NULL AS exceeded, "usageWarningSentAt" IS NOT NULL AS warned, "usageExceededSentAt" IS NOT NULL AS alerted FROM organizations WHERE id = ${ORG}`;
  return JSON.parse(JSON.stringify({ notifications, projects, org }));
}

// ---------- Redis dump ----------
const NOISE = /:(stalled-check|marker|events)$|:limiter$/;
// The rule and project caches are written by the TypeScript reads only
// (decision 8.3); updateEventsCount's is shared on purpose.
const IGNORED_CACHES = /^cachable:(?!updateEventsCount)/;
const MASKS: [RegExp, string][] = [
  [/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, '<uuid>'],
  [/"inserted_at":"[^"]*"/g, '"inserted_at":"<t>"'],
  [/"requestId":"[^"]*"/g, '"requestId":"<id>"'],
];

function mask(text: string, key: string, namespace: string) {
  let out = text.replaceAll(namespace, 'NS');
  for (const [pattern, replacement] of MASKS) {
    out = out.replace(pattern, replacement);
  }
  if (key === 'profile-buffer') {
    out = out.replace(/"(created_at|last_seen_at)":"[^"]*"/g, '"$1":"<t>"');
  }
  return out;
}

async function dumpRedis(index: number, namespace: string) {
  const client = new Redis(`${REDIS_BASE}/${index}`);
  try {
    const keys: string[] = [];
    let cursor = '0';
    do {
      const [next, batch] = await client.scan(cursor, 'COUNT', 500);
      cursor = next;
      keys.push(...batch);
    } while (cursor !== '0');
    const dump: Record<string, unknown> = {};
    for (const key of keys.sort()) {
      if (NOISE.test(key) || IGNORED_CACHES.test(key)) {
        continue;
      }
      const type = await client.type(key);
      const ttl = await client.pttl(key);
      const label = key.replaceAll(namespace, 'NS');
      let value: unknown;
      if (type === 'string') {
        value = mask((await client.get(key)) ?? '', key, namespace);
      } else if (type === 'list') {
        value = (await client.lrange(key, 0, -1)).map((line) =>
          mask(line, key, namespace)
        );
      } else if (type === 'set') {
        value = (await client.smembers(key)).sort();
      } else if (type === 'zset') {
        value = await client.zrange(key, 0, -1, 'WITHSCORES');
      } else if (type === 'hash') {
        const hash = await client.hgetall(key);
        for (const field of [
          'timestamp',
          'processedOn',
          'finishedOn',
          'processedBy',
          'atm',
        ]) {
          if (field in hash) {
            hash[field] = '<t>';
          }
        }
        value = Object.fromEntries(
          Object.entries(hash)
            .sort()
            .map(([field, text]) => [field, mask(text, key, namespace)])
        );
      } else {
        value = null;
      }
      dump[label] = { type, ttl: ttl > 0 ? 'ttl' : ttl, value };
    }
    return dump;
  } finally {
    client.disconnect();
  }
}

async function flushRedis(index: number) {
  const client = new Redis(`${REDIS_BASE}/${index}`);
  try {
    await client.flushdb();
  } finally {
    client.disconnect();
  }
}

// ---------- running a side ----------
function sideEnv(side: (typeof SIDES)[number]) {
  const chUrl = new URL(CH_RAW.href);
  chUrl.pathname = `/${side.chDb}`;
  const env: Record<string, string> = {
    ...(process.env as Record<string, string>),
    TZ: 'UTC',
    LOG_LEVEL: 'warn',
    REDIS_URL: `${REDIS_BASE}/${side.redisIndex}`,
    QUEUE_NAMESPACE: side.namespace,
    CLICKHOUSE_URL: chUrl.href,
    AUTOMATIC_MIGRATIONS: 'false',
    AUTH_SECRET:
      process.env.AUTH_SECRET ?? 'wparity-auth-secret-0123456789abcdef',
    SELF_HOSTED: 'false',
    ROLE: 'worker',
    ENABLED_QUEUES: 'sessions',
    RESEND_API_KEY: '',
  };
  return env;
}

async function exec(
  cmd: string[],
  cwd: string,
  env: Record<string, string>,
  label: string
) {
  const proc = spawn({ cmd, cwd, env, stdout: 'pipe', stderr: 'pipe' });
  const [out, err] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  const code = await proc.exited;
  if (code !== 0) {
    throw new Error(
      `${label} failed (${code}):\n${out.slice(-2000)}\n${err.slice(-2000)}`
    );
  }
  return out;
}

async function runSide(side: (typeof SIDES)[number]) {
  await resetFixtureState();
  await createClickhouse(side.chDb);
  await seedClickhouse(side.chDb);
  await seedRedis(side.redisIndex);
  cleanups.push(() => flushRedis(side.redisIndex));
  const file = `${OUT_DIR}/wparity-p3-${RUN}-${side.name}`;
  await Bun.write(
    `${file}.json`,
    JSON.stringify({ out: `${file}.out.json`, side: side.name, steps })
  );
  const env = sideEnv(side);
  if (side.name === 'ts') {
    await exec(
      [process.execPath, 'run', 'parity/p3-side.ts', `${file}.json`],
      `${ROOT}apps/ingest`,
      env,
      'ts side'
    );
  } else {
    await exec(
      [
        'go',
        'test',
        './internal/worker/sessions',
        '-run',
        'TestP3ParitySide',
        '-count=1',
      ],
      `${ROOT}apps/ingest`,
      { ...env, WPARITY_P3_SCENARIO: `${file}.json` },
      'go side'
    );
    await runEnqueuedUsageAlerts(side, file);
  }
  return {
    outcomes: await Bun.file(`${file}.out.json`).json(),
    redis: await dumpRedis(side.redisIndex, side.namespace),
    postgres: await dumpPostgres(),
  };
}

// The Go side hands usage alerts to the TypeScript worker as sendUsageAlert jobs;
// run what it enqueued the way the Bun worker would, and take them off the queue.
async function runEnqueuedUsageAlerts(
  side: (typeof SIDES)[number],
  file: string
) {
  const client = new Redis(`${REDIS_BASE}/${side.redisIndex}`);
  const alerts: unknown[] = [];
  try {
    const base = `bull:notification-${side.namespace}`;
    for (const id of await client.lrange(`${base}:wait`, 0, -1)) {
      const job = await client.hgetall(`${base}:${id}`);
      if (job.name === 'sendUsageAlert') {
        alerts.push(JSON.parse(job.data!));
        await client.lrem(`${base}:wait`, 0, id);
        await client.del(`${base}:${id}`);
      }
    }
    if (alerts.length > 0) {
      await client.decrby(`${base}:id`, alerts.length);
    }
  } finally {
    client.disconnect();
  }
  for (const [index, data] of alerts.entries()) {
    alertJobs.push({ side: side.name, index, data });
  }
  if (alerts.length === 0) {
    return;
  }
  const scenario = `${file}.alerts.json`;
  await Bun.write(
    scenario,
    JSON.stringify({
      out: `${file}.alerts.out.json`,
      steps: alerts.map((data) => ({ type: 'usageAlert', data })),
    })
  );
  await exec(
    [process.execPath, 'run', 'parity/p3-side.ts', scenario],
    `${ROOT}apps/ingest`,
    sideEnv(side),
    'usage alert jobs'
  );
}
const alertJobs: { side: string; index: number; data: unknown }[] = [];

// ---------- diff ----------
function diff(path: string, a: unknown, b: unknown, out: string[]) {
  if (JSON.stringify(a) === JSON.stringify(b)) {
    return;
  }
  if (
    a &&
    b &&
    typeof a === 'object' &&
    typeof b === 'object' &&
    !Array.isArray(a) &&
    !Array.isArray(b)
  ) {
    for (const key of new Set([...Object.keys(a), ...Object.keys(b)])) {
      diff(
        `${path}.${key}`,
        (a as Record<string, unknown>)[key],
        (b as Record<string, unknown>)[key],
        out
      );
    }
    return;
  }
  const show = (value: unknown) =>
    value === undefined ? '<absent>' : JSON.stringify(value).slice(0, 400);
  out.push(`${path}\n   ts: ${show(a)}\n   go: ${show(b)}`);
}

async function main() {
  cleanups.push(async () => {
    await teardownFixtures();
  });
  await setupFixtures();
  const [ts, go] = [await runSide(SIDES[0]), await runSide(SIDES[1])];
  const differences: string[] = [];
  diff('outcomes', ts.outcomes, go.outcomes, differences);
  diff('redis', ts.redis, go.redis, differences);
  diff('postgres', ts.postgres, go.postgres, differences);
  console.log(
    `redis keys: ts ${Object.keys(ts.redis).length}, go ${Object.keys(go.redis).length}; usage alerts handed over: ${alertJobs.length}`
  );
  console.log(
    `org (ts): ${JSON.stringify(ts.postgres.org)}; org (go): ${JSON.stringify(go.postgres.org)}`
  );
  console.log(
    `notifications: ts ${ts.postgres.notifications.length}, go ${go.postgres.notifications.length}`
  );
  if (differences.length > 0) {
    console.log(
      `${differences.length} difference(s):\n${differences.join('\n')}`
    );
    process.exitCode = 1;
  } else {
    console.log('no differences');
  }
}

try {
  await main();
} finally {
  await cleanup();
  await sql.close();
}
