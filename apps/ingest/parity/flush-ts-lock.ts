// A TypeScript BaseBuffer taking or probing the flush lock, for the Go worker's lock
// compatibility test (internal/worker/flush). Run from packages/core with REDIS_URL set:
//   bun parity/flush-ts-lock.ts hold <name>   takes the lock, prints HOLDING, releases when stdin
//                                             gets a line, prints RESULT <observation>
//   bun parity/flush-ts-lock.ts try <name>    one tryFlush, prints RESULT <observation>
import { join } from 'node:path';

const REPO = join(import.meta.dir, '../../..');
const { BaseBuffer } = await import(
  join(REPO, 'packages/core/src/buffers/base-buffer')
);
const { stubBufferDeps } = await import(
  join(REPO, 'packages/core/test/buffer-fixtures')
);

const [mode, name] = process.argv.slice(2) as [string, string];

function nextLine(): Promise<void> {
  return new Promise((resolve) => {
    process.stdin.once('data', () => resolve());
  });
}

class LockProbe extends (BaseBuffer as any) {
  constructor() {
    super(stubBufferDeps, {
      name,
      onFlush: async () => {
        if (mode === 'hold') {
          console.log('HOLDING');
          await nextLine();
        }
      },
    });
  }
  protected getRedisListKey() {
    return `wparity:${name}:queue`;
  }
}

const buffer: any = new LockProbe();
buffer.flushObserver = (obs: any) => console.log(`RESULT ${obs.result}`);
await buffer.tryFlush({ trigger: 'cron' });
process.exit(0);
