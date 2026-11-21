// The TypeScript side of the Go BullMQ worker's live interoperability tests
// (internal/worker/bullmq/worker_live_test.go). It runs the real BullMQ 6.7.8
// the API uses, so the Go test can compare what each leaves in Redis.
//   bun parity/bullmq-consumer.ts produce '<json>'   add jobs with Queue.add
//   bun parity/bullmq-consumer.ts work '<json>'      run a Worker until SIGTERM
// Handlers follow job.data.mode: ok, sleep, failTimes, unrecoverable, count,
// hangFirst. The Redis database is part of the JSON; the tests own 13 and 14.
import { createRequire } from 'node:module';
import { join } from 'node:path';

const coreRequire = createRequire(
  join(import.meta.dir, '../../../packages/core/package.json')
);
const { Queue, Worker, UnrecoverableError } = coreRequire(
  'bullmq'
) as typeof import('bullmq');
// BullMQ's own ioredis, for the handlers' side effects.
const Redis = createRequire(coreRequire.resolve('bullmq'))(
  'ioredis'
) as typeof import('ioredis').default;

interface JobSpec {
  name: string;
  data: Record<string, unknown>;
  opts?: object;
}
interface Config {
  host: string;
  port: number;
  db: number;
  queue: string;
  tag: string;
  jobs?: JobSpec[];
  concurrency?: number;
  lockDuration?: number;
  stalledInterval?: number;
}

const [command, rawConfig] = process.argv.slice(2);
const config = JSON.parse(rawConfig ?? '{}') as Config;
const connection = {
  host: config.host,
  port: config.port,
  db: config.db,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const never = () => new Promise<never>(() => undefined);

async function produce() {
  const queue = new Queue(config.queue, { connection });
  for (const job of config.jobs ?? []) {
    await queue.add(job.name, job.data, job.opts);
  }
  await queue.close();
}

function work() {
  const sideEffects = new Redis({
    host: config.host,
    port: config.port,
    db: config.db,
  });
  const worker = new Worker(
    config.queue,
    async (job) => {
      const data = job.data as {
        mode: string;
        ms?: number;
        times?: number;
        maxMs?: number;
      };
      switch (data.mode) {
        case 'sleep':
          await sleep(data.ms ?? 0);
          return;
        case 'failTimes':
          if (job.attemptsMade < (data.times ?? 0)) {
            throw new Error('boom');
          }
          return;
        case 'unrecoverable':
          throw new UnrecoverableError('nope');
        case 'count': {
          await sleep(Math.floor(Math.random() * (data.maxMs ?? 10)));
          await sideEffects.incr(`wparity:proc:${config.tag}:${job.id}`);
          await sideEffects.hset(
            `wparity:by:${config.tag}`,
            job.id as string,
            'ts'
          );
          return;
        }
        case 'hangFirst':
          if (job.attemptsStarted <= 1) {
            await never();
          }
          return;
        default:
          return;
      }
    },
    {
      connection,
      concurrency: config.concurrency ?? 1,
      // An explicit undefined would override BullMQ's defaults.
      ...(config.lockDuration && { lockDuration: config.lockDuration }),
      ...(config.stalledInterval && {
        stalledInterval: config.stalledInterval,
      }),
    }
  );
  worker.on('error', (error) => console.error('worker error', error.message));
  worker.waitUntilReady().then(() => console.log('ready'));
  const stop = async () => {
    await worker.close();
    sideEffects.disconnect();
    process.exit(0);
  };
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
}

if (command === 'produce') {
  await produce();
} else if (command === 'work') {
  work();
} else {
  console.error('usage: bullmq-consumer.ts produce|work <json>');
  process.exit(2);
}
