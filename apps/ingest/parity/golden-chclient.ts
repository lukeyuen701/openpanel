// Goldens for the Go worker's ClickHouse client and flush env (internal/worker/{chclient,config}),
// generated from the real TypeScript code:
//   bun parity/golden-chclient.ts          writes each package's testdata/
//   bun parity/golden-chclient.ts <dir>    writes everything to <dir> instead
//
// wire.json: what the Node client puts on the wire (URL pairs in order, headers, decompressed body)
// for a set of calls, one subprocess per environment because the client reads it at import.
// flushenv.json: how loadConfig reads the buffer knobs; cronjobs.json: the cron job ids.

import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { gunzipSync } from 'node:zlib';

const WORKER = join(import.meta.dir, '../internal/worker');
const REPO = join(import.meta.dir, '../../..');

type Call =
  | {
      op: 'insert';
      table: string;
      lines?: string[];
      objects?: unknown[];
      settings?: [string, string | number][];
    }
  | {
      op: 'command';
      query: string;
      params?: [string, unknown][];
      settings?: [string, string | number][];
    }
  | { op: 'query'; query: string; params: [string, unknown][] };

interface Scenario {
  name: string;
  env: Record<string, string>;
  url: string; // path/credentials part; the host is the capture server's
  calls: Call[];
}

const ASYNC: [string, number][] = [
  ['async_insert', 1],
  ['wait_for_async_insert', 0],
  ['parallel_view_processing', 1],
];
const SCENARIOS: Scenario[] = [
  {
    name: 'defaults',
    env: {},
    url: '/mydb',
    calls: [
      {
        op: 'insert',
        table: 'events_bots',
        lines: ['{"a":1}', '{"b":"é"}'],
        settings: ASYNC,
      },
      { op: 'insert', table: 'sessions', objects: [{ x: 1 }] },
      { op: 'insert', table: 'groups', lines: ['{"id":"g"}'], settings: [] },
      {
        op: 'query',
        query: 'SELECT {p1:String} AS a, {p2:Array(String)} AS b',
        params: [
          ['p1', "x y\t'\\\n"],
          ['p2', ['a', "b'c\\"]],
        ],
      },
      {
        op: 'command',
        query: 'ALTER TABLE events UPDATE a = 1 WHERE b = 2',
        settings: [
          ['allow_experimental_lightweight_update', 1],
          ['foo', 'a b'],
        ],
      },
      {
        op: 'command',
        query:
          'UPDATE events SET profile_id = {p1:String} WHERE session_id = {p2:String}',
        params: [
          ['p1', "u'1\\"],
          ['p2', 's\t2'],
        ],
        settings: [['mutations_sync', '0']],
      },
    ],
  },
  {
    name: 'no-credentials-no-database',
    env: {},
    url: '',
    calls: [{ op: 'insert', table: 'events', lines: ['{}'] }],
  },
  {
    name: 'credentials-and-default-database',
    env: {},
    url: '/default',
    calls: [{ op: 'insert', table: 'events', lines: ['{}'] }],
  },
  {
    name: 'compression-off',
    env: { CLICKHOUSE_REQUEST_COMPRESSION: '0' },
    url: '/mydb',
    calls: [
      { op: 'insert', table: 'ai_events', lines: ['{"id":1}', '{"id":2}'] },
    ],
  },
  {
    name: 'settings-override',
    env: {
      CLICKHOUSE_SETTINGS:
        '{"async_insert":1,"max_threads":4,"date_time_input_format":"basic","flag":true}',
    },
    url: '/mydb',
    calls: [
      { op: 'insert', table: 'events', lines: ['{}'] },
      { op: 'query', query: 'SELECT 1', params: [] },
    ],
  },
  {
    name: 'remove-convert-any-join',
    env: { CLICKHOUSE_SETTINGS_REMOVE_CONVERT_ANY_JOIN: '1' },
    url: '/mydb',
    calls: [{ op: 'insert', table: 'events', lines: ['{}'] }],
  },
  {
    name: 'settings-invalid-json',
    env: { CLICKHOUSE_SETTINGS: 'not json' },
    url: '/mydb',
    calls: [{ op: 'insert', table: 'events', lines: ['{}'] }],
  },
];

async function runChild(scenario: Scenario) {
  const seen: unknown[] = [];
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const buf = new Uint8Array(await req.arrayBuffer());
      const encoding = req.headers.get('content-encoding');
      const body =
        encoding === 'gzip'
          ? gunzipSync(buf).toString()
          : new TextDecoder().decode(buf);
      const url = new URL(req.url);
      const pairs = [...url.searchParams.entries()].map(([k, v]) =>
        k === 'query_id' ? [k, '<id>'] : [k, v]
      );
      seen.push({
        method: req.method,
        path: url.pathname,
        pairs,
        authorization: req.headers.get('authorization'),
        contentEncoding: encoding,
        body,
      });
      if (
        url.searchParams.get('query')?.startsWith('INSERT') ||
        !url.searchParams.has('default_format')
      ) {
        return new Response('', { status: 200 });
      }
      return new Response(JSON.stringify({ meta: [], data: [], rows: 0 }), {
        status: 200,
      });
    },
  });
  const userinfo = scenario.name === 'no-credentials-no-database' ? '' : 'u:p@';
  process.env.CLICKHOUSE_URL = `http://${userinfo}127.0.0.1:${server.port}${scenario.url}`;
  Object.assign(process.env, scenario.env);
  const { ch } = await import(join(REPO, 'packages/db/src/clickhouse/client'));
  for (const call of scenario.calls) {
    if (call.op === 'insert') {
      await ch.insert({
        table: call.table,
        values: call.lines ?? call.objects,
        format: 'JSONEachRow',
        ...(call.settings
          ? { clickhouse_settings: Object.fromEntries(call.settings) }
          : {}),
      });
    } else if (call.op === 'command') {
      await ch.command({
        query: call.query,
        ...(call.params
          ? { query_params: Object.fromEntries(call.params) }
          : {}),
        clickhouse_settings: Object.fromEntries(call.settings ?? []),
      });
    } else {
      await (
        await ch.query({
          query: call.query,
          query_params: Object.fromEntries(call.params),
        })
      ).json();
    }
  }
  console.log(`CAPTURE ${JSON.stringify(seen)}`);
  process.exit(0);
}

async function runScenario(scenario: Scenario) {
  const child = Bun.spawn(['bun', import.meta.path, '--child', scenario.name], {
    env: {
      ...process.env,
      CLICKHOUSE_URL: '',
      CLICKHOUSE_SETTINGS: '',
      CLICKHOUSE_REQUEST_COMPRESSION: '',
      CLICKHOUSE_SETTINGS_REMOVE_CONVERT_ANY_JOIN: '',
    },
    stdout: 'pipe',
    stderr: 'ignore',
  });
  const text = await new Response(child.stdout).text();
  const line = text.split('\n').find((l) => l.startsWith('CAPTURE '));
  if (!line) {
    throw new Error(`scenario ${scenario.name} captured nothing:\n${text}`);
  }
  return { ...scenario, requests: JSON.parse(line.slice('CAPTURE '.length)) };
}

async function main() {
  if (process.argv[2] === '--child') {
    await runChild(SCENARIOS.find((s) => s.name === process.argv[3])!);
    return;
  }
  const outDir = process.argv[2];
  const save = (pkg: string, name: string, data: unknown) => {
    const path = outDir
      ? join(outDir, name)
      : join(WORKER, pkg, 'testdata', name);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, `${JSON.stringify(data, null, 1)}\n`);
  };

  save('chclient', 'wire.json', await Promise.all(SCENARIOS.map(runScenario)));

  const { loadConfig, CRON_JOB_VALUES } = await import(
    '../../api/src/config/env'
  );
  const ENV_SETS: Record<string, string>[] = [
    {},
    { BUFFER_ASYNC_INSERTS: 'false' },
    { BUFFER_ASYNC_INSERTS: '0' },
    { BUFFER_ASYNC_INSERTS: '  ' },
    { BUFFER_CH_INSERT_CONCURRENCY: '3' },
    { BUFFER_CH_INSERT_CONCURRENCY: 'abc' },
    { BUFFER_CH_INSERT_CONCURRENCY: '0' },
    { BUFFER_CH_INSERT_CONCURRENCY: '-4' },
    { EVENT_BUFFER_BATCH_SIZE: '123', EVENT_BUFFER_CHUNK_SIZE: '45' },
    { SESSION_BUFFER_BATCH_SIZE: '77', SESSION_BUFFER_SQUASH: 'false' },
    { SESSION_BUFFER_SQUASH: '0' },
    { SESSION_BUFFER_SQUASH: 'no' },
    {
      PROFILE_BUFFER_BATCH_SIZE: '11',
      PROFILE_BUFFER_CHUNK_SIZE: '12',
      PROFILE_BUFFER_FETCH_CHUNK_SIZE: '13',
      PROFILE_BUFFER_TTL_IN_SECONDS: '14',
    },
    { PROFILE_BACKFILL_BUFFER_BATCH_SIZE: '15' },
    { BOT_BUFFER_BATCH_SIZE: '16' },
    { REPLAY_BUFFER_BATCH_SIZE: '17', REPLAY_BUFFER_CHUNK_SIZE: '18' },
    {
      GROUP_BUFFER_BATCH_SIZE: '19',
      GROUP_BUFFER_CHUNK_SIZE: '20',
      GROUP_BUFFER_TTL_IN_SECONDS: '21',
    },
    { AI_EVENT_BUFFER_BATCH_SIZE: '22', AI_EVENT_BUFFER_CHUNK_SIZE: 'x' },
    { REPLAY_BUFFER_BATCH_SIZE: '1e2', BOT_BUFFER_BATCH_SIZE: '0x10' },
  ];
  save(
    'config',
    'flushenv.json',
    ENV_SETS.map((env) => ({
      env,
      buffers: loadConfig({ ROLE: 'worker', ...env }).core.buffers,
    }))
  );
  save('config', 'cronjobs.json', CRON_JOB_VALUES);

  const { queues } = await import('../../../packages/core/src/jobs.registry');
  const jobs = queues.cron.jobs as Record<
    string,
    { cron?: { every?: number } }
  >;
  const intervals = Object.fromEntries(
    Object.entries(jobs)
      .filter(([, job]) => job.cron?.every !== undefined)
      .map(([name, job]) => [name, job.cron!.every])
  );

  // The lock scripts as base-buffer.ts has them: the SHA covers the template
  // literal exactly, its leading and trailing newline included.
  const source = readFileSync(
    join(REPO, 'packages/core/src/buffers/base-buffer.ts'),
    'utf8'
  );
  const script = (name: string) => {
    const match = source.match(
      new RegExp(`const ${name} = defineRedisScript\\(\\\`([\\s\\S]*?)\\\`\\);`)
    );
    if (!match) {
      throw new Error(`${name} not found in base-buffer.ts`);
    }
    return {
      lua: match[1],
      sha: createHash('sha1').update(match[1]!).digest('hex'),
    };
  };
  save('flush', 'locks.json', {
    release: script('releaseLockScript'),
    extend: script('extendLockScript'),
    keepaliveIntervalMs: (60 * 1000) / 3,
    lockTtlSeconds: 60,
    intervals,
  });
}

await main();
