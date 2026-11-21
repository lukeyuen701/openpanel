// Flushes the Redis lists of the transforming buffers with the real TypeScript buffers, for the Go worker's
// live parity test (internal/worker/flush/core). Run from packages/core with CLICKHOUSE_URL (a database of
// its own) and REDIS_URL (a parity database) set:
//   bun parity/flush-ts-core.ts event session profile backfill
// Prints one RESULT line of JSON with each buffer's observation. TS_ASYNC_INSERTS=1 turns BUFFER_ASYNC_INSERTS on.
import { join } from 'node:path';

const REPO = join(import.meta.dir, '../../..');
const { ch } = await import(join(REPO, 'packages/db/src/clickhouse/client'));
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

const silent: any = {
  fatal: () => undefined,
  error: () => undefined,
  warn: () => undefined,
  info: () => undefined,
  debug: () => undefined,
  trace: () => undefined,
  child: () => silent,
};
const config = testCoreConfig();
config.clickhouseClustered = false;
config.buffers.asyncInserts = Boolean(process.env.TS_ASYNC_INSERTS);
const deps = {
  createLogger: () => silent,
  ch,
  isCronPaused: async () => false,
  config,
} as any;

const BUFFERS: Record<string, any> = {
  event: EventBuffer,
  session: SessionBuffer,
  profile: ProfileBuffer,
  backfill: ProfileBackfillBuffer,
};
const results: Record<string, unknown> = {};
for (const name of process.argv.slice(2)) {
  const buffer = new BUFFERS[name](deps);
  let observation: any;
  buffer.flushObserver = (obs: any) => {
    observation = {
      result: obs.result,
      rows: obs.rowsProcessed,
      err: String(obs.err ?? ''),
    };
  };
  await buffer.tryFlush({ trigger: 'cron' });
  results[name] = observation;
}
// The events:batch messages are not awaited by the flush: let them leave before exiting.
await new Promise((resolve) => setTimeout(resolve, 500));
console.log(`RESULT ${JSON.stringify(results)}`);
process.exit(0);
