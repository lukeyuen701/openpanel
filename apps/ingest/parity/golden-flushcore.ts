// Goldens for the Go worker's transforming flushes (internal/worker/flush/core), recorded from the real
// TypeScript buffers (event, session, profile, profile-backfill) run against a stub ClickHouse client and a
// parity Redis database:
//   REDIS_URL=redis://127.0.0.1:23379/13 bun parity/golden-flushcore.ts
// writes internal/worker/flush/core/testdata/flush.json (or the directory given as the first argument).
//
// Each scenario fills the buffer's Redis list (and profile-cache:* keys), runs tryFlush, and records what
// the buffer sent to ClickHouse (insert bodies as the client serialises them, queries and commands with
// their parameters and settings), the events:batch pub/sub messages, the Redis state left behind, and the
// flush observation. The Go tests replay the same inputs against fakes and compare.
// Run from a checkout whose Redis database 13 is empty: the buffers' real key names are used.

import { createHash } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

const REPO = join(import.meta.dir, '../../..');
const OUT = join(
  process.argv[2] ??
    join(import.meta.dir, '../internal/worker/flush/core/testdata'),
  'flush.json'
);
process.env.REDIS_URL ??= 'redis://127.0.0.1:23379/13';
if (!/\/13$/.test(process.env.REDIS_URL)) {
  throw new Error(
    `REDIS_URL must name database 13, got ${process.env.REDIS_URL}`
  );
}

const redisModule = await import(join(REPO, 'packages/redis/index'));
const { testCoreConfig } = await import(
  join(REPO, 'packages/core/test/config-fixture')
);
const { EventBuffer } = await import(
  join(REPO, 'packages/core/src/buffers/event-buffer')
);
const { SessionBuffer } = await import(
  join(REPO, 'packages/core/src/buffers/session-buffer')
);
const { ProfileBuffer } = await import(
  join(REPO, 'packages/core/src/buffers/profile-buffer')
);
const { ProfileBackfillBuffer } = await import(
  join(REPO, 'packages/core/src/buffers/profile-backfill-buffer')
);

const redis = redisModule.getRedisCache();
const subscriber = redisModule.getRedisSub();

const silent: any = {
  fatal: () => undefined,
  error: () => undefined,
  warn: () => undefined,
  info: () => undefined,
  debug: () => undefined,
  trace: () => undefined,
  child: () => silent,
};

const LIST_KEYS = [
  'event_buffer:queue',
  'session-buffer',
  'profile-buffer',
  'profile-backfill-buffer',
];
const LOCK_KEYS = [
  'lock:event',
  'lock:session',
  'lock:profile',
  'lock:profile-backfill',
];

type Kind = 'event' | 'session' | 'profile' | 'backfill';
interface Scenario {
  name: string;
  kind: Kind;
  // buffer sizing, by the keys of CoreConfig.buffers.<kind>
  sizing?: Record<string, number | boolean>;
  asyncInserts?: boolean;
  clustered?: boolean;
  list: string[];
  cache?: Record<string, string>;
  chRows?: unknown[];
  chQueryFails?: boolean;
  failInsertAt?: number; // 1-based
  failCommandAt?: number;
}

const BUFFERS = {
  event: { Class: EventBuffer, list: 'event_buffer:queue', config: 'event' },
  session: { Class: SessionBuffer, list: 'session-buffer', config: 'session' },
  profile: { Class: ProfileBuffer, list: 'profile-buffer', config: 'profile' },
  backfill: {
    Class: ProfileBackfillBuffer,
    list: 'profile-backfill-buffer',
    config: 'profileBackfill',
  },
} as const;

const TTL = 3600;
const BS = String.fromCharCode(92);
const LINE_SEPARATOR = String.fromCharCode(0x20_28);
const LONE_SURROGATE = String.fromCharCode(0xd8_00);
const ev = (o: Record<string, unknown>) => JSON.stringify(o);

function event(
  i: number,
  project: string,
  extra: Record<string, unknown> = {}
) {
  return ev({
    id: `00000000-0000-4000-8000-${String(i).padStart(12, '0')}`,
    name: i % 2 ? 'screen_view' : 'click',
    device_id: `dev${i % 3}`,
    profile_id: `dev${i % 3}`,
    project_id: project,
    session_id: `ses${i % 4}`,
    properties: { 'a.b': `x${i}` },
    path: `/p/${i}`,
    origin: 'https://example.com',
    created_at: `2026-10-08 09:00:${String(i % 60).padStart(2, '0')}.000`,
    country: 'SE',
    city: `Ö${i}`,
    region: '',
    longitude: null,
    latitude: null,
    os: 'iOS',
    os_version: '17',
    browser: 'Safari',
    browser_version: '17',
    device: 'mobile',
    brand: '',
    model: '',
    duration: 0,
    referrer: '',
    referrer_name: '',
    referrer_type: '',
    imported_at: null,
    inserted_at: '2026-10-08 09:00:00.000',
    sdk_name: 'web',
    sdk_version: '1.0',
    groups: [],
    ...extra,
  });
}

function session(
  id: string,
  sign: 1 | -1,
  version: number,
  extra: Record<string, unknown> = {}
) {
  return ev({
    id,
    project_id: 'wparity-p1',
    profile_id: `u-${id}`,
    device_id: `d-${id}`,
    created_at: '2026-10-08 09:00:00.000',
    ended_at: `2026-10-08 09:0${version % 10}:00.000`,
    is_bounce: version < 2,
    entry_path: '/',
    exit_path: `/p/${version}`,
    screen_view_count: version,
    event_count: version,
    duration: version * 1000,
    revenue: 0,
    sign,
    version,
    ...extra,
  });
}

function profile(
  id: string,
  extra: Record<string, unknown> = {},
  project = 'wparity-p1'
) {
  return ev({
    id,
    first_name: '',
    last_name: '',
    email: '',
    avatar: '',
    properties: {},
    project_id: project,
    is_external: true,
    created_at: '2026-10-08 09:00:00',
    last_seen_at: '2026-10-08 09:00:00',
    groups: [],
    ...extra,
  });
}

// What ClickHouse answers for the lookup: SELECT order, JSON format.
function stored(
  id: string,
  extra: Record<string, unknown> = {},
  project = 'wparity-p1'
) {
  return {
    id,
    project_id: project,
    first_name: '',
    last_name: '',
    email: '',
    avatar: '',
    properties: {},
    is_external: true,
    groups: [],
    created_at: '2025-01-01 00:00:00.000',
    last_seen_at: '2025-02-01 10:00:00.000',
    ...extra,
  };
}

const cacheKey = (id: string, project = 'wparity-p1') =>
  `profile-cache:${project}:${id}`;

const SCENARIOS: Scenario[] = [];

// --- event ---
SCENARIOS.push(
  {
    name: 'event: batches, chunks and one message per project in first-seen order',
    kind: 'event',
    sizing: { batchSize: 4, chunkSize: 2 },
    list: [
      event(1, 'wparity-b'),
      event(2, 'wparity-a'),
      event(3, 'wparity-b'),
      event(4, 'wparity-c'),
      event(5, 'wparity-a'),
      event(6, 'wparity-a'),
      event(7, 'wparity-b'),
    ],
  },
  {
    name: 'event: project id extraction variants',
    kind: 'event',
    list: [
      event(10, 'wparity-a', { properties: { project_id: 'user-supplied' } }),
      ev({
        properties: { project_id: 'user-supplied' },
        project_id: 'wparity-b',
      }),
      event(11, 'wparity-c', {
        properties: { note: 'has "project_id":"fake" inside' },
      }),
      ev({ id: 'x', name: 'no project' }),
      ev({ project_id: '' }),
      'not json at all',
      `{"project_id":"wparity-d${BS}u00e9${BS}${BS}x"}`,
      '{"project_id":"wparity-e","x":{"project_id":"deep"}}',
      '{"project_id":1}',
      '{"project_id":"wparity-f", "properties":{"project_id":"wparity-g"}}',
    ],
  },
  {
    name: 'event: async inserts settings',
    kind: 'event',
    asyncInserts: true,
    list: [event(20, 'wparity-a'), event(21, 'wparity-a')],
  },
  {
    name: 'event: a failed chunk publishes nothing and keeps every row',
    kind: 'event',
    sizing: { batchSize: 6, chunkSize: 2 },
    failInsertAt: 2,
    list: [1, 2, 3, 4, 5, 6, 7].map((i) => event(30 + i, 'wparity-a')),
  },
  {
    name: 'event: empty list',
    kind: 'event',
    list: [],
  }
);

// --- session ---
const create = [session('s1', 1, 1)];
const updates = [2, 3, 4, 5, 6, 7, 8].flatMap((v) => [
  session('s1', -1, v - 1),
  session('s1', 1, v),
]);
SCENARIOS.push(
  {
    name: 'session: a new session netted to its last row',
    kind: 'session',
    list: [...create, ...updates],
  },
  {
    name: 'session: mid-life update keeps the resident -1 and the new +1',
    kind: 'session',
    list: [
      session('m1', -1, 3),
      session('m1', 1, 4),
      session('m2', 1, 1),
      session('m1', -1, 4),
      session('m1', 1, 5),
    ],
  },
  {
    name: 'session: first-appearance order, lone and repeated rows',
    kind: 'session',
    list: [
      session('b', 1, 1),
      session('a', -1, 2),
      session('b', -1, 1),
      session('c', -1, 4),
      session('c', -1, 4),
      session('b', 1, 2),
      session('d', -1, 9),
      session('c', 1, 5),
    ],
  },
  {
    name: 'session: duration clamp and key order',
    kind: 'session',
    list: [
      session('d1', 1, 1, { duration: -5 }),
      session('d2', 1, 1, { duration: null }),
      session('d3', 1, 1, { duration: '7' }),
      ev({ id: 'd4', sign: 1, version: 1, project_id: 'wparity-p1' }),
      session('d5', 1, 1, { duration: 'abc' }),
      session('d6', 1, 1, { duration: 12.5 }),
      session('d7', 1, 1, { duration: [3] }),
      session('d8', 1, 1, { duration: true }),
      session('d9', 1, 1, { duration: 1e21, revenue: 0.1 + 0.2 }),
      session('d10', 1, 1, {
        duration: 0,
        entry_path: `é${LINE_SEPARATOR}😀`,
        utm_source: null,
      }),
    ],
  },
  {
    name: 'session: unparsable rows are skipped and trimmed',
    kind: 'session',
    list: [
      'not json',
      'null',
      session('g1', 1, 1),
      '{"id":"g2"',
      session('g2', 1, 1),
    ],
  },
  {
    name: 'session: squash off inserts every row',
    kind: 'session',
    sizing: { squash: false },
    list: [...create, ...updates.slice(0, 4)],
  },
  {
    name: 'session: chunks of two',
    kind: 'session',
    sizing: { chunkSize: 2 },
    list: ['a', 'b', 'c', 'd', 'e'].map((id) => session(id, 1, 1)),
  },
  {
    name: 'session: backlog larger than a batch drains in batches',
    kind: 'session',
    sizing: { batchSize: 3 },
    list: ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'].map((id) =>
      session(id, 1, 1)
    ),
  },
  {
    name: 'session: a failed insert keeps the batch',
    kind: 'session',
    failInsertAt: 1,
    list: ['a', 'b'].map((id) => session(id, 1, 1)),
  }
);

// --- profile ---
const SERVER_STORED = stored('u4', {
  properties: {
    device: 'desktop',
    country: 'SE',
    browser: 'Chrome',
    plan: 'free',
    os: 'macOS',
  },
});
SCENARIOS.push(
  {
    name: 'profile: a new profile is inserted as it came and cached',
    kind: 'profile',
    list: [profile('n1', { first_name: 'Ada', properties: { plan: 'pro' } })],
  },
  {
    name: 'profile: rows of one profile merge in the batch',
    kind: 'profile',
    list: [
      profile('u1', {
        first_name: 'John',
        email: 'john@example.com',
        properties: { a: '1', b: '2' },
      }),
      profile('u2', { properties: { x: 'y' } }),
      profile('u1', {
        first_name: '',
        email: '',
        groups: ['group-abc'],
        properties: { b: '3', c: '4' },
      }),
      profile('u1', {
        last_name: 'Doe',
        groups: ['group-abc', 'group-xyz'],
        last_seen_at: '2026-10-08 09:05:00',
      }),
    ],
  },
  {
    name: 'profile: merge with the stored profile, nested properties, groups union, created_at kept',
    kind: 'profile',
    list: [
      profile('u3', {
        first_name: '',
        email: '',
        properties: {
          os: 'iOS',
          plan: 'pro',
          nested: { b: { d: 3 }, e: [1, 2] },
          arr: ['x'],
        },
        groups: ['g1', 'g0'],
        created_at: '2026-10-08 09:00:00',
      }),
    ],
    chRows: [
      stored('u3', {
        first_name: 'Jane',
        email: 'jane@x.io',
        properties: {
          os: 'iOS',
          first_referrer: 'google',
          nested: { a: 1, b: { c: 2 } },
          arr: ['y', 'z'],
        },
        groups: ['g0'],
      }),
    ],
  },
  {
    name: 'profile: a server update does not bring browser properties into a browser profile',
    kind: 'profile',
    list: [
      profile('u4', {
        properties: {
          device: 'server',
          country: 'US',
          plan: 'pro',
          isServer: true,
          osVersion: '1',
          custom: 'k',
        },
      }),
    ],
    chRows: [SERVER_STORED],
  },
  {
    name: 'profile: the cache answers before ClickHouse does',
    kind: 'profile',
    list: [
      profile('c1', { first_name: '', properties: { new: '1' } }),
      profile('c2', { first_name: 'Two', properties: { k: 'v' } }),
    ],
    cache: {
      [cacheKey('c1')]: JSON.stringify({
        last_seen_at: '2026-10-07 10:00:00',
        created_at: '2026-10-01 00:00:00',
        id: 'c1',
        project_id: 'wparity-p1',
        first_name: 'Cached',
        properties: { old: 'x' },
        groups: ['gc'],
        zeta: 1,
      }),
    },
    chRows: [stored('c2', { first_name: '', properties: { fromCh: '1' } })],
  },
  {
    name: 'profile: a failed lookup merges against nothing and still inserts',
    kind: 'profile',
    chQueryFails: true,
    list: [profile('f1', { first_name: 'John' })],
  },
  {
    name: 'profile: fetch chunks of two, insert chunks of two',
    kind: 'profile',
    sizing: { fetchChunkSize: 2, chunkSize: 2 },
    list: ['q1', 'q2', 'q3', 'q4', 'q5'].map((id) =>
      profile(id, { properties: { n: id } })
    ),
    chRows: [
      stored('q2', { first_name: 'Q2' }),
      stored('q5', { first_name: 'Q5', groups: ['g'] }),
    ],
  },
  {
    name: 'profile: unparsable rows, odd keys and strings',
    kind: 'profile',
    list: [
      'not json',
      'null',
      profile('w1', {
        first_name: 'Ünï 😀',
        properties: {
          '10': 'a',
          '2': 'b',
          z: 'c',
          lone: `${LONE_SURROGATE}x`,
          sep: `a${LINE_SEPARATOR}b`,
        },
      }),
      profile('w1', { properties: { '10': 'A' } }),
    ],
  },
  {
    name: 'profile: only unreadable rows',
    kind: 'profile',
    list: ['not json', 'null'],
  },
  {
    name: 'profile: async inserts settings',
    kind: 'profile',
    asyncInserts: true,
    list: [profile('as1'), profile('as2')],
  },
  {
    name: 'profile: null and empty fields against stored values',
    kind: 'profile',
    list: [
      ev({
        id: 'z1',
        first_name: null,
        last_name: '',
        email: 'new@x',
        avatar: '',
        properties: { keep: '', gone: null, fresh: 'f' },
        project_id: 'wparity-p1',
        is_external: false,
        created_at: '2026-10-08 09:00:00',
        last_seen_at: '2026-10-08 09:00:00',
      }),
    ],
    chRows: [
      stored('z1', {
        first_name: 'Zed',
        last_name: 'Z',
        email: 'old@x',
        avatar: 'a.png',
        properties: { keep: 'kept', gone: 'was', other: 'o' },
        is_external: true,
      }),
    ],
  },
  {
    name: 'profile: drains batches of two',
    kind: 'profile',
    sizing: { batchSize: 2 },
    list: ['b1', 'b2', 'b3', 'b4', 'b5'].map((id) => profile(id)),
  },
  {
    name: 'profile: a later batch merges with what an earlier batch cached',
    kind: 'profile',
    sizing: { batchSize: 2 },
    list: [
      profile('r1', {
        first_name: 'First',
        email: 'r1@x',
        properties: { a: '1' },
        groups: ['g1'],
      }),
      profile('r2', { properties: { z: '1' } }),
      profile('r1', {
        first_name: '',
        email: '',
        properties: { b: '2' },
        groups: ['g1', 'g2'],
        last_seen_at: '2026-10-08 09:01:00',
      }),
      profile('r1', {
        last_name: 'Last',
        properties: { a: '3' },
        last_seen_at: '2026-10-08 09:02:00',
      }),
      profile('r3'),
    ],
    chRows: [stored('r1', { first_name: 'Stored', properties: { s: '1' } })],
  },
  {
    name: 'profile: a failed insert keeps the batch and caches nothing',
    kind: 'profile',
    failInsertAt: 1,
    list: [profile('k1'), profile('k2')],
  }
);

// --- profile backfill ---
const entry = (i: number, projectId = 'wparity-p1') =>
  JSON.stringify({ projectId, sessionId: `sess-${i}`, profileId: `prof-${i}` });
const MANY = Array.from({ length: 1203 }, (_, i) =>
  entry(i % 1100, `wparity-p${i % 3}`)
);
SCENARIOS.push(
  {
    name: 'backfill: duplicates, unreadable rows and quoting',
    kind: 'backfill',
    clustered: false,
    list: [
      entry(1),
      entry(2),
      'not json',
      JSON.stringify({
        projectId: 'wparity-p2',
        sessionId: 'sess-1',
        profileId: `it's ${BS} a\nb\t`,
      }),
      entry(3, 'wparity-p3'),
    ],
  },
  {
    name: 'backfill: clustered target',
    kind: 'backfill',
    clustered: true,
    list: [entry(1), entry(2)],
  },
  {
    name: 'backfill: more than one chunk, one batch per tick',
    kind: 'backfill',
    clustered: false,
    list: MANY,
  },
  {
    name: 'backfill: a failed chunk keeps the whole batch',
    kind: 'backfill',
    clustered: false,
    sizing: { batchSize: 1100 },
    failCommandAt: 2,
    list: MANY.slice(0, 1100),
  },
  {
    name: 'backfill: nothing readable',
    kind: 'backfill',
    clustered: false,
    list: ['not json', 'null'],
  }
);

async function linesOf(values: unknown): Promise<string[]> {
  if (Array.isArray(values)) {
    return values.map((v) => (typeof v === 'string' ? v : JSON.stringify(v)));
  }
  const out: string[] = [];
  for await (const chunk of values as AsyncIterable<unknown>) {
    out.push(typeof chunk === 'string' ? chunk : JSON.stringify(chunk));
  }
  return out;
}

const settingsList = (s: Record<string, unknown> | undefined) =>
  s === undefined ? null : Object.entries(s).map(([k, v]) => [k, String(v)]);

function digest(query: string, params: Record<string, unknown>) {
  const text = Object.entries(params)
    .map(([k, v]) => `${k}=${v}`)
    .join('\x01');
  return createHash('sha256').update(`${query}\x00${text}`).digest('hex');
}

const published: string[] = [];
await subscriber.subscribe('events:batch');
subscriber.on('message', (_channel: string, message: string) => {
  if (message.includes('"wparity-')) {
    published.push(message);
  }
});

async function wipe() {
  const keys = [
    ...LIST_KEYS,
    ...LOCK_KEYS,
    ...(await redis.keys('profile-cache:wparity-*')),
  ];
  await redis.del(...keys);
}

for (const key of [...LIST_KEYS, ...LOCK_KEYS]) {
  if ((await redis.exists(key)) !== 0) {
    throw new Error(
      `${key} is not empty in Redis database 13: not touching it`
    );
  }
}

const results: unknown[] = [];
for (const scenario of SCENARIOS) {
  await wipe();
  const spec = BUFFERS[scenario.kind];
  const config = testCoreConfig();
  config.clickhouseClustered = scenario.clustered ?? false;
  config.buffers.asyncInserts = scenario.asyncInserts ?? false;
  Object.assign((config.buffers as any)[spec.config], scenario.sizing ?? {});

  const inserts: unknown[] = [];
  const queries: unknown[] = [];
  const commands: unknown[] = [];
  let insertCalls = 0;
  let commandCalls = 0;
  const ch: any = {
    insert: async (o: any) => {
      insertCalls++;
      if (scenario.failInsertAt === insertCalls) {
        throw new Error('ClickHouse unavailable');
      }
      inserts.push({
        table: o.table,
        lines: await linesOf(o.values),
        settings: settingsList(o.clickhouse_settings),
      });
    },
    query: async (o: any) => {
      queries.push({
        query: o.query,
        params: Object.entries(o.query_params ?? {}),
        settings: settingsList(o.clickhouse_settings),
      });
      if (scenario.chQueryFails) {
        throw new Error('ClickHouse unavailable');
      }
      const data = scenario.chRows ?? [];
      return {
        json: async () => ({
          data,
          meta: [],
          rows: data.length,
          statistics: {},
        }),
      };
    },
    command: async (o: any) => {
      commandCalls++;
      if (scenario.failCommandAt === commandCalls) {
        throw new Error('ClickHouse unavailable');
      }
      const params = o.query_params ?? {};
      const full = {
        query: o.query,
        params: Object.entries(params),
        settings: settingsList(o.clickhouse_settings),
      };
      commands.push(
        JSON.stringify(full).length > 20_000
          ? {
              query: null,
              hash: digest(o.query, params),
              paramCount: Object.keys(params).length,
              settings: full.settings,
            }
          : full
      );
    },
  };

  if (scenario.list.length > 0) {
    await redis.rpush(spec.list, ...scenario.list);
  }
  for (const [key, value] of Object.entries(scenario.cache ?? {})) {
    await redis.set(key, value, 'EX', 1800);
  }
  published.length = 0;

  const buffer = new spec.Class({
    createLogger: () => silent,
    ch,
    isCronPaused: async () => false,
    config,
  } as any);
  let observation: any;
  buffer.flushObserver = (o: any) => {
    observation = {
      result: o.result,
      rows: o.rowsProcessed,
      err: String(o.err ?? ''),
    };
  };
  await buffer.tryFlush({ trigger: 'cron' });
  await new Promise((resolve) => setTimeout(resolve, 150));

  const cacheAfter: Record<string, { value: string; ttlOk: boolean }> = {};
  for (const key of (await redis.keys('profile-cache:wparity-*')).sort()) {
    cacheAfter[key] = {
      value: (await redis.get(key)) as string,
      ttlOk: (await redis.ttl(key)) >= TTL - 5,
    };
  }
  results.push({
    name: scenario.name,
    kind: scenario.kind,
    sizing: scenario.sizing ?? {},
    asyncInserts: scenario.asyncInserts ?? false,
    clustered: scenario.clustered ?? false,
    list: scenario.list,
    cache: scenario.cache ?? {},
    chRows: (scenario.chRows ?? []).map((row) => JSON.stringify(row)),
    chQueryFails: scenario.chQueryFails ?? false,
    failInsertAt: scenario.failInsertAt ?? 0,
    failCommandAt: scenario.failCommandAt ?? 0,
    expect: {
      result: observation.result,
      rows: observation.rows,
      err: observation.err,
      inserts,
      queries,
      commands,
      published: [...published],
      remaining: await redis.lrange(spec.list, 0, -1),
      cache: cacheAfter,
      ttlSeconds: TTL,
    },
  });
}
await wipe();

mkdirSync(dirname(OUT), { recursive: true });
writeFileSync(OUT, `${JSON.stringify({ scenarios: results }, null, 1)}\n`);
console.log(`wrote ${results.length} scenarios to ${OUT}`);
process.exit(0);
