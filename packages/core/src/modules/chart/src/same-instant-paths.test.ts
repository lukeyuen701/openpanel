/**
 * Events in one millisecond are common (a page view and the click it
 * carries), and ClickHouse versions break such ties differently, so the
 * user-flow and journey paths rank them explicitly instead of trusting the
 * order rows are read in. Each tie below is inserted in the order the rule
 * must undo.
 */

import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { fetchRows } from '../../../../test/clickhouse-rows';
import { transitionsQuery } from '../../overview/src/overview.sql';
import { sankeySessionPathsQuery } from './sankey.sql';

const PROJECT_ID = 'same-instant-paths-test';
const SESSION_ID = 'same-instant-session';
const ORIGIN = 'https://example.com';
const FIRST_INSTANT = '2026-01-05 09:00:00.250';
const SECOND_INSTANT = '2026-01-05 09:00:05.500';
const WINDOW = {
  startDate: '2026-01-05 00:00:00',
  endDate: '2026-01-06 00:00:00',
};
const STEPS = 10;

let ch: typeof import('@openpanel/db/src/clickhouse/client').ch;

function event(name: string, createdAt: string, path = '/') {
  return {
    project_id: PROJECT_ID,
    session_id: SESSION_ID,
    profile_id: 'same-instant-profile',
    device_id: 'same-instant-device',
    name,
    path,
    origin: ORIGIN,
    created_at: createdAt,
  };
}

async function insertEachAlone(rows: ReturnType<typeof event>[]) {
  for (const row of rows) {
    await ch.insert({ table: 'events', values: [row], format: 'JSONEachRow' });
  }
}

async function removeEvents() {
  await ch.command({
    query: `DELETE FROM events WHERE project_id = '${PROJECT_ID}'`,
    clickhouse_settings: { lightweight_deletes_sync: '2' },
  });
}

beforeAll(async () => {
  ({ ch } = await import('@openpanel/db/src/clickhouse/client'));
  const { bootstrapTestDatabases } = await import(
    '../../../../../../test/bootstrap-databases'
  );
  await bootstrapTestDatabases();
  await removeEvents();
  await insertEachAlone([
    event('session_end', SECOND_INSTANT),
    event('signup_completed', SECOND_INSTANT),
    event('cta_clicked', FIRST_INSTANT),
    event('screen_view', FIRST_INSTANT, '/pricing'),
    event('screen_view', FIRST_INSTANT, '/docs'),
    event('session_start', FIRST_INSTANT),
  ]);
}, 30_000);

afterAll(removeEvents);

describe('paths over events of one millisecond', () => {
  it('user flow: bookends outside, the page view before the click', async () => {
    const rows = await fetchRows(
      ch,
      sankeySessionPathsQuery({
        projectId: PROJECT_ID,
        ...WINDOW,
        steps: STEPS,
        mode: 'before',
        startEvent: { name: 'session_end', whereClause: null },
        exclude: [],
      })
    );

    expect(rows.map((row) => JSON.parse(row).events)).toEqual([
      [
        'session_start',
        'screen_view',
        'cta_clicked',
        'signup_completed',
        'session_end',
      ],
    ]);
  });

  it('journey: page views of one millisecond follow their path', async () => {
    const rows = await fetchRows(
      ch,
      transitionsQuery({
        projectId: PROJECT_ID,
        ...WINDOW,
        rawFilterWhere: null,
        steps: STEPS,
        topEntryPages: [`${ORIGIN}/docs`],
      })
    );

    expect(rows.map((row) => JSON.parse(row))).toEqual([
      {
        source: `${ORIGIN}/docs`,
        target: `${ORIGIN}/pricing`,
        step: 1,
        value: 1,
      },
    ]);
  });
});
