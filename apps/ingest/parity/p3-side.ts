// One side of the session-lifecycle differential (p3-diff.ts): the real TypeScript
// handlers run in this process against the side's Redis db, BullMQ namespace and
// ClickHouse database.
//
//   bun parity/p3-side.ts <scenario.json>
//
// 'run' executes the scenario's steps through runJob, as the worker would; 'alert'
// runs the sendUsageAlert jobs a Go side enqueued.
process.env.TZ = 'UTC';

import {
  type AppDeps,
  createBuffers,
  createClients,
  createLogger,
  createProducers,
  queueKey,
  queues,
  runJob,
} from '../../../packages/core/src/index';
import { ch } from '../../../packages/db/src/clickhouse/client';
import { db, Prisma } from '../../../packages/db/src/prisma-client';
import { getRedisCache, getRedisQueue } from '../../../packages/redis';
import { loadConfig } from '../../api/src/config/env';

interface Step {
  type: 'job' | 'reap' | 'vacuum' | 'usageAlert';
  data?: unknown;
  name?: string;
}
interface Scenario {
  out: string;
  steps: Step[];
}

const scenario: Scenario = await Bun.file(process.argv[2]!).json();
const config = loadConfig({
  ...process.env,
  ROLE: 'worker',
  ENABLED_QUEUES: 'sessions,notification,cron',
});
const logger = createLogger({ name: 'p3-side', config: config.core });
const producers = createProducers(queues, {
  connection: getRedisQueue(),
  cluster: config.QUEUE_CLUSTER,
  namespace: config.QUEUE_NAMESPACE,
  logger,
});
const cronKey = queueKey('cron', {
  cluster: config.QUEUE_CLUSTER,
  namespace: config.QUEUE_NAMESPACE,
});
const cron = producers.bullQueues.find((queue) => queue.name === cronKey);
const deps: AppDeps = {
  db,
  prisma: { DbNull: Prisma.DbNull, JsonNull: Prisma.JsonNull },
  ch,
  redis: getRedisCache(),
  clients: createClients(),
  buffers: createBuffers({
    createLogger: (name) => logger.child({ name }),
    isCronPaused: async () => (cron ? await cron.isPaused() : false),
    config: config.core,
    ch,
  }),
  producers,
  logger,
  config: config.core,
  auth: null,
} as unknown as AppDeps;

const outcomes: { type: string; failed: boolean; error?: string }[] = [];
let sequence = 0;
for (const step of scenario.steps) {
  sequence += 1;
  const job =
    step.type === 'job'
      ? {
          id: `wparity-${sequence}`,
          name: 'session',
          data: step.data,
          attemptsMade: 0,
        }
      : step.type === 'usageAlert'
        ? {
            id: `wparity-${sequence}`,
            name: 'sendUsageAlert',
            data: step.data,
            attemptsMade: 0,
          }
        : {
            id: `wparity-${sequence}`,
            name: step.type === 'reap' ? 'sessionReaper' : 'sessionVacuum',
            data: { payload: null, meta: {} },
            attemptsMade: 0,
          };
  const definition =
    step.type === 'job'
      ? queues.sessions
      : step.type === 'usageAlert'
        ? queues.notification
        : queues.cron;
  try {
    await runJob(definition, job, deps);
    outcomes.push({ type: step.type, failed: false });
  } catch (error) {
    outcomes.push({
      type: step.type,
      failed: true,
      error: String(error).slice(0, 200),
    });
  }
}
// The event buffer's micro-batch (10 ms) and the fire-and-forget cache writes.
await Bun.sleep(600);
await Bun.write(scenario.out, JSON.stringify(outcomes));
process.exit(0);
