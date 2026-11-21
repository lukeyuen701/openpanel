// Flushes the Redis lists of the simple buffers with the real TypeScript buffers, for the Go
// worker's live parity test (internal/worker/flush/simple). Run from packages/core with
// CLICKHOUSE_URL (a database of its own), REDIS_URL (a parity database) set:
//   bun parity/flush-ts-flush.ts bot replay group ai-event
// Prints one RESULT line of JSON with each buffer's observation.
import { join } from 'node:path';

const REPO = join(import.meta.dir, '../../..');
const { ch } = await import(join(REPO, 'packages/db/src/clickhouse/client'));
const { testCoreConfig } = await import(
  join(REPO, 'packages/core/test/config-fixture')
);
const { BotBuffer } = await import(
  join(REPO, 'packages/core/src/buffers/bot-buffer')
);
const { ReplayBuffer } = await import(
  join(REPO, 'packages/core/src/buffers/replay-buffer')
);
const { GroupBuffer } = await import(
  join(REPO, 'packages/core/src/buffers/group-buffer')
);
const { AiEventBuffer } = await import(
  join(REPO, 'packages/core/src/buffers/ai-event-buffer')
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
config.buffers.asyncInserts = Boolean(process.env.TS_ASYNC_INSERTS);
const deps = {
  createLogger: () => silent,
  ch,
  isCronPaused: async () => false,
  config,
} as any;

const BUFFERS: Record<string, any> = {
  bot: BotBuffer,
  replay: ReplayBuffer,
  group: GroupBuffer,
  'ai-event': AiEventBuffer,
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
console.log(`RESULT ${JSON.stringify(results)}`);
process.exit(0);
