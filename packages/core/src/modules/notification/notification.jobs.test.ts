// Job wiring — no ClickHouse/Postgres touched. The rule matching and
// dispatch logic themselves are exercised in notification.service.test.ts.

import { expect, test } from 'bun:test';
import { testCoreConfig } from '../../../test/config-fixture';
import type { AppDeps, Buffers, JobCtx } from '../../context';
import { createRecordingProducers } from '../../jobs/testing';
import { queues } from '../../jobs.registry';
import type { Logger } from '../../logger';
import type { Services } from '../../services';
import { notificationQueueJobs } from './notification.jobs';
import type { createNotificationService } from './notification.service';

function stubLogger(): Logger {
  const noop = () => undefined;
  const logger: Logger = {
    fatal: noop,
    error: noop,
    warn: noop,
    info: noop,
    debug: noop,
    trace: noop,
    child: () => logger,
  };
  return logger;
}

function stubJobCtx(
  notification: Partial<ReturnType<typeof createNotificationService>>
): JobCtx {
  const services: Services = {
    auth: {} as Services['auth'],
    notification: notification as ReturnType<typeof createNotificationService>,
    insight: {} as Services['insight'],
    integration: {} as Services['integration'],
    cohort: {} as Services['cohort'],
    gsc: {} as Services['gsc'],
    import: {} as Services['import'],
    ingest: {} as Services['ingest'],
    organization: {} as Services['organization'],
    onboarding: {} as Services['onboarding'],
    session: {} as Services['session'],
    customEvent: {} as Services['customEvent'],
    event: {} as Services['event'],
    profile: {} as Services['profile'],
    group: {} as Services['group'],
    chart: {} as Services['chart'],
    funnel: {} as Services['funnel'],
    conversion: {} as Services['conversion'],
    sankey: {} as Services['sankey'],
    retention: {} as Services['retention'],
    overview: {} as Services['overview'],
    pages: {} as Services['pages'],
    realtime: {} as Services['realtime'],
    misc: {} as Services['misc'],
    report: {} as Services['report'],
    dashboard: {} as Services['dashboard'],
    export: {} as Services['export'],
    share: {} as Services['share'],
    reference: {} as Services['reference'],
    client: {} as Services['client'],
    project: {} as Services['project'],
    user: {} as Services['user'],
    subscription: {} as Services['subscription'],
    salt: {} as Services['salt'],
    conversation: {} as Services['conversation'],
    assistant: {} as Services['assistant'],
    mcp: {} as Services['mcp'],
    ai: {} as Services['ai'],
  };
  return {
    auth: null,
    db: {} as AppDeps['db'],
    prisma: {} as AppDeps['prisma'],
    ch: {} as AppDeps['ch'],
    redis: {} as AppDeps['redis'],
    clients: {} as AppDeps['clients'],
    buffers: {} as Buffers,
    logger: stubLogger(),
    queues: createRecordingProducers(queues).queues,
    config: testCoreConfig(),
    services,
    requestId: 'req_1',
    job: {
      id: 'job_1',
      attempt: 0,
      queue: 'notification',
      name: 'sendNotification',
    },
  };
}

test('the notification queue declares the sendNotification and sendUsageAlert jobs', () => {
  expect(Object.keys(notificationQueueJobs)).toEqual([
    'sendUsageAlert',
    'sendNotification',
  ]);
  expect(queues.notification.jobs.sendNotification).toMatchObject({
    queue: 'notification',
    name: 'sendNotification',
  });
});

test('sendNotification validates its payload', () => {
  expect(() =>
    notificationQueueJobs.sendNotification.payload.parse({})
  ).toThrow();
  expect(
    notificationQueueJobs.sendNotification.payload.parse({
      notification: {
        projectId: 'proj_1',
        title: 'Hello',
        message: 'World',
      },
    })
  ).toEqual({
    notification: {
      projectId: 'proj_1',
      title: 'Hello',
      message: 'World',
    },
  });
});

test('sendNotification delegates to ctx.services.notification.dispatch', async () => {
  const calls: unknown[] = [];
  const ctx = stubJobCtx({
    dispatch: async (notification) => {
      calls.push(notification);
    },
  });

  await notificationQueueJobs.sendNotification.handler({
    payload: {
      notification: {
        projectId: 'proj_1',
        title: 'Hello',
        message: 'World',
        sendToApp: true,
      },
    },
    ctx,
  });

  expect(calls).toEqual([
    { projectId: 'proj_1', title: 'Hello', message: 'World', sendToApp: true },
  ]);
});

function ctxWithOrganization(
  organization: Record<string, unknown> | null,
  updateMany: (args: unknown) => Promise<{ count: number }>
): { ctx: JobCtx; reads: unknown[] } {
  const reads: unknown[] = [];
  const base = stubJobCtx({});
  const db = {
    organization: {
      findUnique: async (args: unknown) => {
        reads.push(args);
        return organization;
      },
      updateMany,
    },
    member: {
      findMany: () => {
        throw new Error(
          'no e-mail may be prepared for a claimed or idle alert'
        );
      },
    },
  } as unknown as AppDeps['db'];
  return { ctx: { ...base, db }, reads };
}

test('sendUsageAlert validates its payload', () => {
  expect(() =>
    notificationQueueJobs.sendUsageAlert.payload.parse({})
  ).toThrow();
  expect(
    notificationQueueJobs.sendUsageAlert.payload.parse({
      organizationId: 'org_1',
      count: 900,
    })
  ).toEqual({ organizationId: 'org_1', count: 900 });
});

test('sendUsageAlert re-reads the organization and ignores an unknown one', async () => {
  const { ctx, reads } = ctxWithOrganization(null, async () => {
    throw new Error('nothing to claim');
  });
  await notificationQueueJobs.sendUsageAlert.handler({
    payload: { organizationId: 'org_1', count: 900 },
    ctx,
  });
  expect(reads).toEqual([{ where: { id: 'org_1' } }]);
});

test('sendUsageAlert sends nothing when the organization has no limit', async () => {
  const claims: unknown[] = [];
  const { ctx } = ctxWithOrganization(
    { id: 'org_1', subscriptionPeriodEventsLimit: 0 },
    async (args) => {
      claims.push(args);
      return { count: 1 };
    }
  );
  await notificationQueueJobs.sendUsageAlert.handler({
    payload: { organizationId: 'org_1', count: 900 },
    ctx,
  });
  expect(claims).toEqual([]);
});

test('sendUsageAlert claims once: a repeated job whose claim was lost sends nothing', async () => {
  const claims: unknown[] = [];
  const { ctx } = ctxWithOrganization(
    {
      id: 'org_1',
      name: 'Acme',
      subscriptionPeriodEventsLimit: 1000,
      usageWarningSentAt: null,
      usageExceededSentAt: null,
    },
    async (args) => {
      claims.push(args);
      return { count: 0 };
    }
  );
  await notificationQueueJobs.sendUsageAlert.handler({
    payload: { organizationId: 'org_1', count: 900 },
    ctx,
  });
  expect(claims).toHaveLength(1);
  expect(claims[0]).toMatchObject({
    where: { id: 'org_1', usageWarningSentAt: null },
  });
});
