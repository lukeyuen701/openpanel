// Golden outputs for internal/worker/bullmq: the real BullMQ 6.7.8 (through
// the API's own createProducers) adds a fixed set of jobs into Redis, and the
// commands it sent plus every resulting key are written to testdata/. The Go
// tests replay the same jobs into another database and compare.
//   bun parity/golden-bullmq.ts            writes internal/worker/bullmq/testdata/
// REDIS_URL's host and port are used; the database is always 13.
import { mkdirSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { defineJob, defineQueue } from '../../../packages/core/src/jobs/define';
import { createProducers } from '../../../packages/core/src/jobs/producers';
import {
  sessionEndEnqueueOptions,
  sessionEndJobPayload,
} from '../../../packages/core/src/modules/session/src/session-end';

const REDIS_DB = 13;
const FIXED_NOW_MS = Date.parse('2026-01-01T00:00:00.000Z');
const OUT = join(import.meta.dir, '../internal/worker/bullmq/testdata');

const coreRequire = createRequire(
  join(import.meta.dir, '../../../packages/core/package.json')
);
const redisRequire = createRequire(
  join(import.meta.dir, '../../../packages/redis/package.json')
);
const Redis = redisRequire('ioredis') as typeof import('ioredis').default;
const { z } = coreRequire('zod') as typeof import('zod');

// The one wall-clock read BullMQ makes for a job (Job constructor).
Date.now = () => FIXED_NOW_MS;

const url = new URL(process.env.REDIS_URL ?? 'redis://127.0.0.1:23379');
const redis = new Redis({
  host: url.hostname,
  port: Number(url.port || 6379),
  db: REDIS_DB,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
});

const sent: { command: string; args: string[] }[] = [];
const sendCommand = redis.sendCommand.bind(redis);
redis.sendCommand = ((command: { name: string; args: unknown[] }) => {
  if (['evalsha', 'eval', 'hset', 'info'].includes(command.name)) {
    sent.push({
      command: command.name,
      args: command.args.map((arg) =>
        Buffer.from(arg as string | Buffer).toString('base64')
      ),
    });
  }
  return sendCommand(command as never);
}) as typeof redis.sendCommand;

const noop = async () => undefined;
const sessions = defineQueue('sessions', {
  defaults: {
    removeOnComplete: true,
    removeOnFail: { age: 86_400, count: 1000 },
  },
  jobs: { session: defineJob({ payload: z.any(), handler: noop }) },
});
const notification = defineQueue('notification', {
  defaults: { removeOnComplete: 10, removeOnFail: { age: 86_400, count: 100 } },
  jobs: {
    sendNotification: defineJob({ payload: z.any(), handler: noop }),
  },
});

const closed = {
  id: 'sess1',
  project_id: 'p1',
  device_id: 'd1',
  profile_id: 'd1',
  ended_at: '2026-01-01 00:00:00.000',
};
const event = {
  name: 'screen_view',
  profileId: 'd1',
  projectId: 'p1',
  deviceId: 'd1',
  sessionId: 's2',
  properties: { a: 'b', unicode: 'héllo ☃ 😀 "q" \\ /', skip: undefined },
  createdAt: new Date('2026-01-01T00:00:00.123Z'),
  groups: [],
  duration: 0,
  path: '/x',
  origin: '',
  referrer: undefined,
};
const notificationPayload = (title: string) => ({
  notification: {
    title,
    message: 'm',
    projectId: 'p1',
    payload: { type: 'event', event },
    integrationId: null,
    sendToApp: true,
    sendToEmail: false,
    notificationRuleId: 'r1',
  },
});

interface Scenario {
  namespace?: string;
  cluster?: boolean;
  requestId: string;
  adds: {
    queue: 'sessions' | 'notification';
    job: string;
    payload: unknown;
    options?: unknown;
  }[];
}
const scenarios: Scenario[] = [
  {
    namespace: 'wparity',
    requestId: 'wparity-req-1',
    adds: [
      {
        queue: 'sessions',
        job: 'session',
        payload: sessionEndJobPayload({
          payload: event as never,
          closedSession: closed as never,
        }),
        options: sessionEndEnqueueOptions('sess1'),
      },
      // Same id again: the duplicated path.
      {
        queue: 'sessions',
        job: 'session',
        payload: sessionEndJobPayload({
          payload: event as never,
          closedSession: closed as never,
        }),
        options: sessionEndEnqueueOptions('sess1'),
      },
      {
        queue: 'notification',
        job: 'sendNotification',
        payload: notificationPayload('first'),
      },
      {
        queue: 'notification',
        job: 'sendNotification',
        payload: notificationPayload('second'),
      },
    ],
  },
  {
    requestId: '',
    adds: [
      {
        queue: 'sessions',
        job: 'session',
        payload: sessionEndJobPayload({
          payload: event as never,
          closedSession: closed as never,
        }),
        options: sessionEndEnqueueOptions('sess2'),
      },
      {
        queue: 'notification',
        job: 'sendNotification',
        payload: notificationPayload('plain'),
      },
    ],
  },
  {
    namespace: 'wparity',
    cluster: true,
    requestId: 'wparity-req-3',
    adds: [
      {
        queue: 'sessions',
        job: 'session',
        payload: sessionEndJobPayload({
          payload: event as never,
          closedSession: closed as never,
        }),
        options: sessionEndEnqueueOptions('sess3'),
      },
    ],
  },
];

async function snapshot() {
  const out: Record<string, unknown> = {};
  const keys = (await redis.keys('*')).sort();
  for (const key of keys) {
    const type = await redis.type(key);
    if (type === 'string') {
      out[key] = { string: await redis.get(key) };
    } else if (type === 'hash') {
      out[key] = { hash: await redis.hgetall(key) };
    } else if (type === 'list') {
      out[key] = { list: await redis.lrange(key, 0, -1) };
    } else if (type === 'zset') {
      out[key] = { zset: await redis.zrange(key, 0, -1, 'WITHSCORES') };
    } else if (type === 'set') {
      out[key] = { set: (await redis.smembers(key)).sort() };
    } else if (type === 'stream') {
      const entries = await redis.xrange(key, '-', '+');
      out[key] = { stream: entries.map(([, fields]) => fields) };
    } else {
      throw new Error(`unexpected type ${type} for ${key}`);
    }
  }
  return out;
}

await redis.flushdb();
const cases: unknown[] = [];
const results: unknown[] = [];
for (const scenario of scenarios) {
  const handle = createProducers({ sessions, notification } as never, {
    connection: redis,
    namespace: scenario.namespace,
    cluster: scenario.cluster,
  });
  const scoped: any = handle.scope(
    scenario.requestId ? { requestId: scenario.requestId } : {}
  );
  for (const add of scenario.adds) {
    const id = await scoped[add.queue][add.job].add(add.payload, add.options);
    cases.push({
      namespace: scenario.namespace ?? '',
      cluster: scenario.cluster ?? false,
      requestId: scenario.requestId,
      queue: add.queue,
      job: add.job,
      payloadJson: JSON.stringify(add.payload),
      options: add.options ?? null,
    });
    results.push(id);
  }
  await handle.close();
}

const save = (name: string, data: unknown) => {
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, name), `${JSON.stringify(data, null, 1)}\n`);
};
// msgpackr is what BullMQ packs ARGV with; its choices (str8, map16, float64
// above 2^32) are what the Go encoder must reproduce.
const bullmqRequire = createRequire(coreRequire.resolve('bullmq'));
const { pack } = bullmqRequire('msgpackr') as {
  pack: (value: unknown) => Uint8Array;
};
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');
const vectorValues: Record<string, unknown> = {
  nil: null,
  true: true,
  false: false,
  zero: 0,
  fixintMax: 127,
  uint8: 128,
  uint8Max: 255,
  uint16: 256,
  uint16Max: 65_535,
  uint32: 65_536,
  uint32Max: 4_294_967_295,
  aboveUint32: 4_294_967_296,
  timestamp: FIXED_NOW_MS,
  fraction: 1.5,
  emptyString: '',
  fixstrMax: 'a'.repeat(31),
  str8: 'a'.repeat(32),
  str8Max: 'a'.repeat(255),
  str16: 'a'.repeat(256),
  unicode: 'héllo ☃ 😀',
  emptyArray: [],
  array: [1, 'a', null],
  array16: Array.from({ length: 16 }, (_, i) => i),
  emptyMap: {},
  map: { a: 1, b: [true, null] },
  map16: Object.fromEntries(Array.from({ length: 16 }, (_, i) => [`k${i}`, i])),
};
save(
  'msgpack-vectors.json',
  Object.fromEntries(
    Object.entries(vectorValues).map(([name, value]) => [
      name,
      hex(pack(value)),
    ])
  )
);
save('cases.json', { nowMs: FIXED_NOW_MS, cases, ids: results });
save('commands.json', sent);
save('snapshot.json', await snapshot());
await redis.flushdb();
redis.disconnect();
console.log('wrote', OUT, 'ids', JSON.stringify(results));
