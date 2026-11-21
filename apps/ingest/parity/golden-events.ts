// Golden outputs from the real TypeScript incomingEvent for the Go worker's
// handler test (internal/worker/events/testdata/handler.json.gz).
//
//   bun run golden:events
//
// Records go through the real handler one at a time, in corpus order, with the
// real SessionBuffer on a scratch Redis database (an empty one among 11-15,
// flushed at the end) and the real createEvent and AI-row builders. What is
// stubbed is only what leaves the process: the event and profile buffers
// record what they are given, the notification check records the payload it
// receives, and the session-end enqueue records the job through the real
// sessionEndJobPayload / sessionEndEnqueueOptions.
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import Redis from '../../../packages/redis/node_modules/ioredis';
import {
  buildCorpus,
  buildGeneratedCorpus,
  EXCLUDED_EVENT,
  fixtureIds,
} from './worker-corpus';

const OUT = join(
  import.meta.dir,
  '../internal/worker/events/testdata/handler.json.gz'
);
const REDIS_BASE = (process.env.REDIS_URL ?? 'redis://127.0.0.1:23379').replace(
  /\/\d+$/,
  ''
);
const SCRATCH_INDEXES = [11, 12, 13, 14, 15];
const GENERATED_DEVICE_LIMIT = 60;

async function pickScratchIndex(): Promise<number> {
  for (const index of SCRATCH_INDEXES) {
    const client = new Redis(`${REDIS_BASE}/${index}`);
    try {
      if ((await client.dbsize()) === 0) {
        return index;
      }
    } finally {
      client.disconnect();
    }
  }
  throw new Error('no empty scratch Redis db among 11-15');
}

const index = await pickScratchIndex();
process.env.REDIS_URL = `${REDIS_BASE}/${index}`;
const redis = new Redis(process.env.REDIS_URL);

// Deterministic minted ids: the Go test feeds the same sequence.
let uuidCounter = 0;
const mintedUuid = () => {
  uuidCounter += 1;
  return `00000000-0000-4000-8000-${String(uuidCounter).padStart(12, '0')}`;
};
Object.defineProperty(globalThis.crypto, 'randomUUID', {
  value: mintedUuid,
  configurable: true,
  writable: true,
});

const { incomingEvent } = await import(
  '../../../packages/core/src/modules/ingest/src/incoming-event-handler'
);
const { createEvent } = await import(
  '../../../packages/core/src/modules/event/event.service'
);
const { SessionBuffer } = await import(
  '../../../packages/core/src/buffers/session-buffer'
);
const { sessionEndEnqueueOptions, sessionEndJobPayload } = await import(
  '../../../packages/core/src/modules/session/src/session-end'
);
const { bufferDepsWithCh } = await import(
  '../../../packages/core/test/buffer-fixtures'
);

const noop = () => undefined;
const logger: any = {
  fatal: noop,
  error: noop,
  warn: noop,
  info: noop,
  debug: noop,
  trace: noop,
  child: () => logger,
};

const NOW_FIELDS =
  /"(inserted_at|created_at|last_seen_at)":"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3})?"/g;
// created_at of an event row is data; the profile's two and inserted_at are the clock.
const maskClock = (line: string, isProfile: boolean) =>
  line.replace(NOW_FIELDS, (match, field) =>
    field === 'inserted_at' || isProfile ? `"${field}":"<now>"` : match
  );

interface Captured {
  events: string[];
  profiles: { row: string; isFromEvent: boolean }[];
  notifications: string[];
  sessionEnds: { data: string; options: unknown }[];
  ai: string[];
  sessionStarted: string[];
  sessionEndEnqueued: string[];
  firstEvent: string[];
}
let captured: Captured;
const reset = () => {
  captured = {
    events: [],
    profiles: [],
    notifications: [],
    sessionEnds: [],
    ai: [],
    sessionStarted: [],
    sessionEndEnqueued: [],
    firstEvent: [],
  };
};
reset();

const serviceDeps: any = {
  buffers: {
    event: {
      add: (event: unknown) =>
        captured.events.push(maskClock(JSON.stringify(event), false)),
    },
    profile: {
      add: async (profile: unknown, isFromEvent: boolean) => {
        captured.profiles.push({
          row: maskClock(JSON.stringify(profile), true),
          isFromEvent,
        });
      },
    },
  },
};
const sessions = new SessionBuffer(bufferDepsWithCh({}));
const AI_PRIVATE_PROJECT = fixtureIds.projectC;
const projectFilters: Record<string, unknown[]> = {
  [fixtureIds.projectA]: [
    { type: 'event', name: EXCLUDED_EVENT, segment: 'event', filters: [] },
  ],
};

const deps: any = {
  logger,
  sessions,
  createEvent: (payload: any) => createEvent(serviceDeps, payload),
  checkNotificationRulesForEvent: async (payload: unknown) => {
    captured.notifications.push(JSON.stringify(payload));
    return null;
  },
  projects: {
    getCached: async (projectId: string) => ({
      firstEventAt: new Date(),
      filters: projectFilters[projectId] ?? [],
    }),
    markFirstEvent: async (projectId: string) => {
      captured.firstEvent.push(projectId);
    },
    getAiStoreContent: async (projectId: string) =>
      projectId !== AI_PRIVATE_PROJECT,
  },
  enqueueSessionEnd: async (input: any) => {
    captured.sessionEnds.push({
      data: JSON.stringify(sessionEndJobPayload(input)),
      options: sessionEndEnqueueOptions(input.closedSession.id),
    });
  },
  metrics: {
    sessionStarted: (kind: string) => captured.sessionStarted.push(kind),
    sessionEndEnqueued: (source: string) =>
      captured.sessionEndEnqueued.push(source),
  },
  aiEvents: {
    add: async (row: unknown) => captured.ai.push(JSON.stringify(row)),
  },
};

const basic = buildCorpus();
const generated = buildGeneratedCorpus().filter((record) => {
  const match = /^g(\d+)-/.exec(record.label);
  return match
    ? Number(match[1]) < GENERATED_DEVICE_LIMIT
    : record.label.startsWith('gsrv-') || record.label.startsWith('gai-');
});
const records = [...basic, ...generated];

const results: unknown[] = [];
let sessionListSeen = 0;
try {
  for (const record of records) {
    reset();
    const payload = JSON.parse(record.value);
    let error: string | undefined;
    try {
      await incomingEvent(payload, deps, { partition: 0, offset: '0' });
    } catch (caught) {
      error = caught instanceof Error ? caught.message : String(caught);
    }
    const sessionRows = await redis.lrange(
      'session-buffer',
      sessionListSeen,
      -1
    );
    sessionListSeen += sessionRows.length;
    const projectId = payload.projectId as string;
    const deviceId = payload.deviceId as string;
    const profileId = payload.event?.profileId
      ? String(payload.event.profileId)
      : '';
    results.push({
      label: record.label,
      value: record.value,
      error: error !== undefined,
      events: captured.events,
      profiles: captured.profiles,
      notifications: captured.notifications,
      sessionEnds: captured.sessionEnds,
      ai: captured.ai.map((line) => line),
      sessionStarted: captured.sessionStarted,
      sessionEndEnqueued: captured.sessionEndEnqueued,
      firstEvent: captured.firstEvent,
      sessionRows,
      sessionBlob: deviceId
        ? await redis.get(`session:${projectId}:${deviceId}`)
        : null,
      pointer: profileId
        ? await redis.get(`session:profile:${projectId}:${profileId}`)
        : null,
    });
  }
} finally {
  // The scratch database was empty when this started.
  await redis.flushdb();
  redis.disconnect();
}
mkdirSync(dirname(OUT), { recursive: true });
writeFileSync(
  OUT,
  Bun.gzipSync(Buffer.from(JSON.stringify({ records: results })))
);
console.log(`${results.length} records -> ${OUT}`);
process.exit(0);
