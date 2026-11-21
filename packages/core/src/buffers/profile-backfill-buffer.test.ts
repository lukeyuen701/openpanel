import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'bun:test';
import { createClient } from '@openpanel/db/src/clickhouse/client';
import { getRedisCache } from '@openpanel/redis';
import { TEST_CLICKHOUSE_URL } from '../../../../test/databases';
import { bufferDepsWithCh } from '../../test/buffer-fixtures';
import { testCoreConfig } from '../../test/config-fixture';
import {
  BACKFILL_SESSIONS_PER_UPDATE,
  ProfileBackfillBuffer,
} from './profile-backfill-buffer';

const PROJECT_ID = 'test-profile-backfill';
const REDIS_KEY = 'profile-backfill-buffer';
const SESSION_COUNT = 300;
const OLD_PROFILE = 'anonymous-device';
const NEW_PROFILE = 'identified-user';
const APPLY_TIMEOUT_MS = 10_000;
const POLL_INTERVAL_MS = 100;

const singleNodeConfig = { ...testCoreConfig(), clickhouseClustered: false };

const redis = getRedisCache();

const buffersWith = (ch: unknown) =>
  new ProfileBackfillBuffer(bufferDepsWithCh(ch, singleNodeConfig));
const client = createClient({ url: TEST_CLICKHOUSE_URL });

const sessionIds = Array.from(
  { length: SESSION_COUNT },
  (_, index) => `backfill-session-${index}`
);

async function deleteFixtureRows() {
  await client.command({
    query: `DELETE FROM events WHERE project_id = '${PROJECT_ID}'`,
    clickhouse_settings: { mutations_sync: '2' },
  });
}

async function countWithProfile(profileId: string): Promise<number> {
  const result = await client.query({
    query: `SELECT count() AS total FROM events WHERE project_id = '${PROJECT_ID}' AND profile_id = {profileId:String}`,
    query_params: { profileId },
    format: 'JSONEachRow',
  });
  const [row] = await result.json<{ total: string }>();
  return Number(row?.total ?? 0);
}

async function waitForCount(profileId: string, expected: number) {
  const deadline = Date.now() + APPLY_TIMEOUT_MS;
  let count = await countWithProfile(profileId);
  while (count !== expected && Date.now() < deadline) {
    await Bun.sleep(POLL_INTERVAL_MS);
    count = await countWithProfile(profileId);
  }
  return count;
}

async function queueEntries() {
  await redis.del(REDIS_KEY);
  await redis.rpush(
    REDIS_KEY,
    ...sessionIds.map((sessionId) =>
      JSON.stringify({
        projectId: PROJECT_ID,
        sessionId,
        profileId: NEW_PROFILE,
      })
    )
  );
}

beforeAll(async () => {
  const { bootstrapTestDatabases } = await import(
    '../../../../test/bootstrap-databases'
  );
  await bootstrapTestDatabases();
});

beforeEach(async () => {
  const fixtures = await import('../../../../test/fixtures');
  await deleteFixtureRows();
  await client.insert({
    table: 'events',
    format: 'JSONEachRow',
    values: sessionIds.map((sessionId) =>
      fixtures.buildEvent(
        new Date(),
        PROJECT_ID,
        crypto.randomUUID(),
        'screen_view',
        OLD_PROFILE,
        sessionId,
        0
      )
    ),
  });
  await queueEntries();
});

afterAll(async () => {
  await deleteFixtureRows();
  await redis.del(REDIS_KEY);
  await client.close();
});

describe('ProfileBackfillBuffer', () => {
  it('rewrites the profile of 300 sessions and trims the list', async () => {
    const buffer = buffersWith(client);

    await buffer.processBuffer();

    expect(await waitForCount(NEW_PROFILE, SESSION_COUNT)).toBe(SESSION_COUNT);
    expect(await redis.llen(REDIS_KEY)).toBe(0);
  });

  it('sends one UPDATE per chunk of sessions', async () => {
    const statements: string[] = [];
    const recordingCh = {
      command: (options: { query: string }) => {
        statements.push(options.query);
        return Promise.resolve();
      },
    };
    const buffer = buffersWith(recordingCh);

    await buffer.processBuffer();

    expect(statements).toHaveLength(
      Math.ceil(SESSION_COUNT / BACKFILL_SESSIONS_PER_UPDATE)
    );
  });

  it('trims nothing when a later chunk fails', async () => {
    let calls = 0;
    const failingCh = {
      command: () => {
        calls++;
        return calls === 1
          ? Promise.resolve()
          : Promise.reject(new Error('chunk failed'));
      },
    };
    const buffer = buffersWith(failingCh);

    await expect(buffer.processBuffer()).rejects.toThrow('chunk failed');

    expect(await redis.llen(REDIS_KEY)).toBe(SESSION_COUNT);
  });
});
