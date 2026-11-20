// Differential test: the same requests, byte for byte, against the Bun API and
// the Go receiver, comparing the HTTP answers and the Kafka records each one
// produced. Each side produces to its own fresh topic.
//
//   bun x dotenv -e ../../.env -- bun parity/diff.ts [--go-bin path] [--keep]
//
// Needs the local stack (Postgres, Redis, Redpanda) and the GeoLite files in
// packages/core. It writes fixture rows named ingest-parity-* and removes them.
import { SQL, spawn } from 'bun';
import { hash as argon2Hash } from '../../../packages/core/node_modules/@node-rs/argon2';
import { hashClientSecret } from '../../../packages/core/src/shared/client-secret';

const ROOT = new URL('../../..', import.meta.url).pathname;
const args = process.argv.slice(2);
const goBin = args.includes('--go-bin')
  ? args[args.indexOf('--go-bin') + 1]!
  : `${ROOT}apps/ingest/bin/ingest`;
const RUN = Date.now().toString(36);
const TS_PORT = 3334;
const GO_PORT = 3341;
const TS_TOPIC = `parity-ts-${RUN}`;
const GO_TOPIC = `parity-go-${RUN}`;
const DOCKER = process.env.DOCKER_CONTEXT
  ? ['docker', '--context', process.env.DOCKER_CONTEXT]
  : ['docker'];
const RP_CONTAINER = process.env.PARITY_RP_CONTAINER ?? 'openpanel-op-rp-1';

// ---------- fixtures ----------
const ids = {
  org: 'ingest-parity-org',
  orgWound: 'ingest-parity-org-wd',
  project: 'ingest-parity',
  projectRevenue: 'ingest-parity-rev',
  projectWound: 'ingest-parity-wd',
  scrypt: '0f0f0000-0000-4000-8000-000000000001',
  ignore: '0f0f0000-0000-4000-8000-000000000002',
  argon: '0f0f0000-0000-4000-8000-000000000003',
  revenue: '0f0f0000-0000-4000-8000-000000000004',
  root: '0f0f0000-0000-4000-8000-000000000005',
  wound: '0f0f0000-0000-4000-8000-000000000006',
};
const secrets = {
  scrypt: 'sec_parity_scrypt',
  argon: 'sec_parity_argon',
  revenue: 'sec_parity_revenue',
};
const dbUrl = (process.env.DATABASE_URL ?? '').split('?')[0]!;
const sql = new SQL(dbUrl);

async function setupFixtures() {
  await teardownFixtures();
  await sql`INSERT INTO organizations (id, name) VALUES (${ids.org}, 'Ingest parity')`;
  await sql`INSERT INTO organizations (id, name, "windDownStep") VALUES (${ids.orgWound}, 'Ingest parity wound down', 'blocked')`;
  await sql`INSERT INTO projects (id, name, "organizationId", cors, filters) VALUES (${ids.project}, 'Parity', ${ids.org}, ${'{https://parity.example,*.wild.example,http://localhost:3000}'}::text[], ${JSON.stringify(
    [
      { type: 'ip', ip: '203.0.113.9' },
      { type: 'profile_id', profileId: 'blocked-user' },
      { type: 'profile_id', profileId: 777 },
    ]
  )}::text::jsonb)`;
  await sql`INSERT INTO projects (id, name, "organizationId", cors, "allowUnsafeRevenueTracking") VALUES (${ids.projectRevenue}, 'Parity revenue', ${ids.org}, ${'{*}'}::text[], true)`;
  await sql`INSERT INTO projects (id, name, "organizationId", cors) VALUES (${ids.projectWound}, 'Parity wound', ${ids.orgWound}, ${'{*}'}::text[])`;
  const client = (
    id: string,
    project: string | null,
    secret: string | null,
    ignore = false
  ) =>
    sql`INSERT INTO clients (id, name, secret, "projectId", "organizationId", "ignoreCorsAndSecret") VALUES (${id}::uuid, 'parity', ${secret}, ${project}, ${ids.org}, ${ignore})`;
  await client(ids.scrypt, ids.project, await hashClientSecret(secrets.scrypt));
  await client(ids.ignore, ids.project, null, true);
  await client(ids.argon, ids.project, await argon2Hash(secrets.argon));
  await client(
    ids.revenue,
    ids.projectRevenue,
    await hashClientSecret(secrets.revenue)
  );
  await client(ids.root, null, await hashClientSecret('sec_root'));
  await client(ids.wound, ids.projectWound, null);
}

async function teardownFixtures() {
  await sql`DELETE FROM clients WHERE "organizationId" IN (${ids.org}, ${ids.orgWound})`;
  await sql`DELETE FROM projects WHERE "organizationId" IN (${ids.org}, ${ids.orgWound})`;
  await sql`DELETE FROM organizations WHERE id IN (${ids.org}, ${ids.orgWound})`;
}

// ---------- processes ----------
const children: ReturnType<typeof spawn>[] = [];
async function waitFor(url: string) {
  for (let i = 0; i < 120; i++) {
    try {
      const res = await fetch(url);
      if (res.status < 500) {
        return;
      }
    } catch {
      // not listening yet
    }
    await Bun.sleep(500);
  }
  throw new Error(`not up: ${url}`);
}
async function portInUse(port: number) {
  try {
    await fetch(`http://127.0.0.1:${port}/`);
    return true;
  } catch {
    return false;
  }
}
async function startServers() {
  for (const port of [TS_PORT, GO_PORT, GO_PORT + 1]) {
    if (await portInUse(port)) {
      throw new Error(
        `port ${port} is taken: stop whatever listens there first`
      );
    }
  }
  const env = {
    ...process.env,
    LOG_LEVEL: 'warn',
    KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? '127.0.0.1:23092',
  };
  children.push(
    spawn({
      cmd: [process.execPath, 'run', 'src/main.ts'],
      cwd: `${ROOT}apps/api`,
      env: {
        ...env,
        ROLE: 'api',
        API_PORT: String(TS_PORT),
        KAFKA_EVENTS_TOPIC: TS_TOPIC,
        AUTH_SECRET:
          process.env.AUTH_SECRET ?? 'parity-auth-secret-0123456789abcdef',
      },
      stdout: Bun.file(`/tmp/parity-ts-${RUN}.log`),
      stderr: Bun.file(`/tmp/parity-ts-${RUN}.err`),
    })
  );
  children.push(
    spawn({
      cmd: [goBin],
      cwd: `${ROOT}apps/ingest`,
      env: {
        ...env,
        INGEST_PORT: String(GO_PORT),
        INGEST_ADMIN_PORT: String(GO_PORT + 1),
        INGEST_UPSTREAM_URL: `http://127.0.0.1:${TS_PORT}`,
        KAFKA_EVENTS_TOPIC: GO_TOPIC,
        INGEST_GEO_DIR: `${ROOT}packages/core`,
      },
      stdout: Bun.file(`/tmp/parity-go-${RUN}.log`),
      stderr: Bun.file(`/tmp/parity-go-${RUN}.err`),
    })
  );
  await waitFor(`http://127.0.0.1:${TS_PORT}/healthcheck`);
  await waitFor(`http://127.0.0.1:${GO_PORT + 1}/healthz/live`);
}
function stopServers() {
  for (const child of children) {
    child.kill();
  }
}

// ---------- raw HTTP ----------
interface RawRequest {
  method: string;
  path: string;
  headers: [string, string | Uint8Array][];
  body?: string | Uint8Array;
}
interface RawResponse {
  status: number;
  headers: Record<string, string>;
  body: string;
}

function encodeRequest(port: number, req: RawRequest): Uint8Array {
  const body =
    req.body === undefined
      ? undefined
      : typeof req.body === 'string'
        ? new TextEncoder().encode(req.body)
        : req.body;
  const parts: Uint8Array[] = [];
  const enc = (s: string) => parts.push(new TextEncoder().encode(s));
  enc(
    `${req.method} ${req.path} HTTP/1.1\r\nhost: 127.0.0.1:${port}\r\nconnection: close\r\n`
  );
  let hasLength = false;
  for (const [name, value] of req.headers) {
    if (name.toLowerCase() === 'content-length') {
      hasLength = true;
    }
    enc(`${name}: `);
    parts.push(
      typeof value === 'string' ? new TextEncoder().encode(value) : value
    );
    enc('\r\n');
  }
  if (body && !hasLength) {
    enc(`content-length: ${body.length}\r\n`);
  }
  enc('\r\n');
  if (body) {
    parts.push(body);
  }
  return Buffer.concat(parts);
}

const SEND_TIMEOUT_MS = 15_000;
async function send(port: number, req: RawRequest): Promise<RawResponse> {
  const chunks: Uint8Array[] = [];
  let pending: Uint8Array = new Uint8Array();
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`timeout ${req.method} ${req.path} on ${port}`)),
      SEND_TIMEOUT_MS
    );
    Bun.connect({
      hostname: '127.0.0.1',
      port,
      socket: {
        // write() takes only what fits; the rest goes out on drain.
        open(socket) {
          pending = encodeRequest(port, req);
          pending = pending.subarray(socket.write(pending));
        },
        drain(socket) {
          if (pending.length) {
            pending = pending.subarray(socket.write(pending));
          }
        },
        data(socket, data) {
          chunks.push(data);
          // A server that answers before reading the whole body may keep the
          // socket open, so stop once the declared length has arrived.
          const text = Buffer.concat(chunks).toString('latin1');
          const split = text.indexOf('\r\n\r\n');
          if (split < 0) {
            return;
          }
          const length = /\r\ncontent-length: *(\d+)/i.exec(
            text.slice(0, split)
          );
          if (length && text.length - split - 4 >= Number(length[1])) {
            clearTimeout(timer);
            socket.end();
            resolve();
          }
        },
        close() {
          clearTimeout(timer);
          resolve();
        },
        error(_, error) {
          reject(error);
        },
        connectError(_, error) {
          reject(error);
        },
      },
    });
  });
  const raw = Buffer.concat(chunks).toString('latin1');
  const split = raw.indexOf('\r\n\r\n');
  const head = raw.slice(0, split).split('\r\n');
  let body = raw.slice(split + 4);
  const headers: Record<string, string> = {};
  for (const line of head.slice(1)) {
    const colon = line.indexOf(':');
    headers[line.slice(0, colon).toLowerCase()] = line.slice(colon + 1).trim();
  }
  if (headers['transfer-encoding'] === 'chunked') {
    let out = '';
    let rest = body;
    while (rest.length) {
      const nl = rest.indexOf('\r\n');
      const size = Number.parseInt(rest.slice(0, nl), 16);
      if (!size) {
        break;
      }
      out += rest.slice(nl + 2, nl + 2 + size);
      rest = rest.slice(nl + 2 + size + 2);
    }
    body = out;
  }
  return {
    status: Number(head[0]!.split(' ')[1]),
    headers,
    body: Buffer.from(body, 'latin1').toString('utf8'),
  };
}

// ---------- kafka ----------
interface KafkaRecord {
  partition: number;
  key: string;
  value: Record<string, unknown>;
}
async function readTopic(topic: string): Promise<KafkaRecord[]> {
  const proc = spawn({
    cmd: [
      ...DOCKER,
      'exec',
      RP_CONTAINER,
      'rpk',
      'topic',
      'consume',
      topic,
      '-X',
      'brokers=127.0.0.1:9092',
      '-o',
      ':end',
      '-f',
      '%p\\t%k\\t%v\\n',
    ],
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const text = await new Response(proc.stdout).text();
  await proc.exited;
  return text
    .split('\n')
    .filter(Boolean)
    .map((line) => {
      const [partition, key, ...value] = line.split('\t');
      return {
        partition: Number(partition),
        key: key!,
        value: JSON.parse(value.join('\t')),
      };
    });
}
async function deleteTopic(topic: string) {
  await spawn({
    cmd: [
      ...DOCKER,
      'exec',
      RP_CONTAINER,
      'rpk',
      'topic',
      'delete',
      topic,
      '-X',
      'brokers=127.0.0.1:9092',
    ],
    stdout: 'ignore',
    stderr: 'ignore',
  }).exited;
}

// ---------- cases ----------
export interface Case {
  name: string;
  req: RawRequest;
  expect?: 'native' | 'forwarded';
  produces?: number;
  timestampFree?: boolean;
  randomKey?: boolean;
}

const CHROME =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36';
type H = [string, string][];
const browserHeaders = (
  clientId: string,
  origin = 'https://parity.example',
  extra: H = []
): H => [
  ['openpanel-client-id', clientId],
  ['origin', origin],
  ['user-agent', CHROME],
  ['x-client-ip', '81.2.69.142'],
  ['sec-ch-ua', '"Chromium";v="128"'],
  ['sec-fetch-mode', 'cors'],
  ['accept-language', 'en-US'],
  ['content-type', 'application/json'],
  ...extra,
];
const replaceHeader = (headers: H, name: string, value: string): H =>
  headers.map(([n, v]) => (n === name ? [n, value] : [n, v]));
const dropHeader = (headers: H, ...names: string[]): H =>
  headers.filter(([n]) => !names.includes(n));
const fixedTs = () => new Date(Date.now() - 10 * 60 * 1000).toISOString();
let counter = 0;
const nextName = () => `parity_${RUN}_${++counter}`;
const trackBody = (name: string, payload: Record<string, unknown> = {}) =>
  JSON.stringify({ type: 'track', payload: { name, ...payload } });

export function buildCases(): Case[] {
  const cases: Case[] = [];
  const add = (c: Omit<Case, 'name'> & { name?: string }) =>
    cases.push({ ...c, name: c.name ?? nextName() } as Case);
  const track = (
    headers: H,
    payload: Record<string, unknown> = {},
    opts: Partial<Case> = {}
  ) => {
    const name = (payload.name as string) ?? nextName();
    const props = { __timestamp: fixedTs(), ...(payload.properties as object) };
    add({
      name,
      req: {
        method: 'POST',
        path: '/track',
        headers,
        body: trackBody(name, { ...payload, name, properties: props }),
      },
      expect: 'native',
      produces: 1,
      ...opts,
    });
  };
  const c = ids;
  // Native, browser
  track(browserHeaders(c.scrypt));
  track(browserHeaders(c.scrypt), {
    properties: {
      __path: '/a?x=1',
      __referrer: 'https://google.com',
      b: 1,
      a: 2,
      2: 'two',
      1: 'one',
      nested: { z: [1, 1.5, -0, 1e21, 1.234_567_890_123_456_8e20], y: null },
      unicode: 'ünï 😀 中文',
      ctl: String.fromCharCode(1),
    },
  });
  track(browserHeaders(c.scrypt), { profileId: 'user-1' });
  track(browserHeaders(c.scrypt), { profileId: 12_345 });
  track(browserHeaders(c.scrypt), { profileId: 0 });
  track(browserHeaders(c.scrypt), { groups: ['g1', 'g2'] });
  track(browserHeaders(c.scrypt), { groups: null });
  track(browserHeaders(c.scrypt), {
    properties: { __deviceId: '  custom-device  ' },
  });
  track(browserHeaders(c.scrypt), { properties: { __ip: '8.8.8.8' } });
  track(browserHeaders(c.scrypt), {
    properties: { __bot: '1', __bot_reasons: 'forged', keep: true },
  });
  track(browserHeaders(c.scrypt), {
    properties: { __os: 'CustomOS', __device: 'tablet', __model: 'X1' },
  });
  track(
    browserHeaders(c.scrypt, 'https://parity.example', [
      ['openpanel-sdk-name', 'web'],
      ['openpanel-sdk-version', '1.2.3'],
      ['request-id', 'req-abc_123'],
    ])
  );
  track(browserHeaders(c.scrypt, 'https://app.wild.example'));
  track(browserHeaders(c.scrypt, 'http://localhost:3000'));
  track(dropHeader(browserHeaders(c.scrypt), 'sec-ch-ua', 'sec-fetch-mode'));
  track(
    replaceHeader(browserHeaders(c.scrypt), 'x-client-ip', '52.94.236.248')
  );
  track(
    replaceHeader(
      browserHeaders(c.scrypt),
      'x-client-ip',
      '2001:4860:4860::8888'
    )
  );
  track([
    ...dropHeader(browserHeaders(c.scrypt), 'x-client-ip'),
    ['x-forwarded-for', '81.2.69.142, 10.0.0.1'],
  ]);
  track(dropHeader(browserHeaders(c.scrypt), 'x-client-ip'));
  track(
    replaceHeader(
      browserHeaders(c.scrypt),
      'user-agent',
      'Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1'
    )
  );
  track(replaceHeader(browserHeaders(c.scrypt), 'user-agent', ''));
  track(dropHeader(browserHeaders(c.scrypt), 'user-agent'));
  // __timestamp forms
  const now = Date.now();
  const stamps: [unknown, boolean][] = [
    [now - 20 * 60 * 1000, false],
    [new Date(now - 20 * 60 * 1000).toISOString(), false],
    [new Date(now - 3_600_000).toISOString().replace('Z', '+00:00'), false],
    [now + 10 * 60 * 1000, true],
    [12_345, true],
    ['', true],
  ];
  for (const [ts, free] of stamps) {
    const name = nextName();
    add({
      name,
      req: {
        method: 'POST',
        path: '/track',
        headers: browserHeaders(c.scrypt),
        body: trackBody(name, { properties: { __timestamp: ts } }),
      },
      expect: 'native',
      produces: 1,
      timestampFree: free,
    });
  }
  // Server SDK (secret)
  const server = (ua: string): H => [
    ['openpanel-client-id', c.scrypt],
    ['openpanel-client-secret', secrets.scrypt],
    ['user-agent', ua],
    ['content-type', 'application/json'],
  ];
  track(server('node'), {}, { randomKey: true });
  track(server('OpenPanel-Node/1.0'), { profileId: 'srv-user' });
  track(server('curl/8.4.0'), {}, { randomKey: true });
  track(server(CHROME), { properties: { __revenue: 1999 } });
  track([
    ['openpanel-client-id', c.argon],
    ['openpanel-client-secret', secrets.argon],
    ['user-agent', 'node'],
    ['content-type', 'application/json'],
  ]);
  track([
    ['mixan-client-id', c.scrypt],
    ['mixan-client-secret', secrets.scrypt],
    ['user-agent', 'node'],
    ['content-type', 'application/json'],
  ]);
  track([
    ['openpanel-client-id', c.ignore],
    ['user-agent', CHROME],
    ['content-type', 'application/json'],
  ]);
  track(
    [
      ['openpanel-client-id', c.revenue],
      ['origin', 'https://any.example'],
      ['user-agent', CHROME],
      ['content-type', 'application/json'],
    ],
    { properties: { __revenue: 10 } }
  );
  // Auth refusals (native 401) and wind-down
  const refusal = (headers: H, payload: Record<string, unknown> = {}) =>
    track(headers, payload, { produces: 0 });
  refusal(dropHeader(browserHeaders(''), 'openpanel-client-id'));
  refusal(browserHeaders('NOT-A-UUID'));
  refusal(browserHeaders('0f0f0000-0000-4000-8000-0000000000ff'));
  refusal(browserHeaders(c.root));
  refusal(browserHeaders(c.scrypt, 'https://evil.example'));
  refusal([
    ...browserHeaders(c.scrypt, 'https://evil.example'),
    ['openpanel-client-secret', 'wrong'],
  ]);
  refusal(
    replaceHeader(browserHeaders(c.scrypt), 'x-client-ip', '203.0.113.9')
  );
  refusal(browserHeaders(c.scrypt), { profileId: 'blocked-user' });
  refusal(browserHeaders(c.scrypt), { profileId: 777 });
  refusal(browserHeaders(c.scrypt), { properties: { __revenue: 5 } });
  refusal(browserHeaders(c.wound, 'https://x.example'));
  // Forwarded to the Bun API
  const fwd = (
    body: string,
    headers: H = browserHeaders(c.scrypt),
    produces = 0,
    name?: string
  ) =>
    add({
      name,
      req: { method: 'POST', path: '/track', headers, body },
      expect: 'forwarded',
      produces,
    });
  fwd(
    JSON.stringify({
      type: 'identify',
      payload: { profileId: 'p-identify', firstName: 'Ada' },
    })
  );
  let n = nextName();
  fwd(
    trackBody(n, {
      properties: { __identify: { profileId: 'p2', email: 'a@b.co' } },
    }),
    browserHeaders(c.scrypt),
    1,
    n
  );
  n = nextName();
  fwd(
    trackBody(n, { properties: { __ip: 123 } }),
    browserHeaders(c.scrypt),
    1,
    n
  );
  n = nextName();
  fwd(
    trackBody(n, { properties: { __timestamp: '2026-11-20 10:00:00' } }),
    browserHeaders(c.scrypt),
    1,
    n
  );
  fwd(trackBody(''));
  fwd(trackBody('session_start'));
  fwd('{"type":');
  fwd(
    JSON.stringify({ type: 'alias', payload: { profileId: 'a', alias: 'b' } })
  );
  fwd(
    JSON.stringify({
      type: 'increment',
      payload: { profileId: 'nobody', property: 'x' },
    })
  );
  fwd(
    trackBody(nextName()),
    replaceHeader(browserHeaders(c.scrypt), 'content-type', 'text/plain')
  );
  n = nextName();
  fwd(
    trackBody(n, { properties: { __path: '/bot-page' } }),
    replaceHeader(
      browserHeaders(c.scrypt),
      'user-agent',
      'Googlebot/2.1 (+http://www.google.com/bot.html)'
    ),
    0,
    n
  );
  n = nextName();
  fwd(
    trackBody(n),
    browserHeaders(c.scrypt, 'chrome-extension://abcdef'),
    0,
    n
  );
  n = nextName();
  add({
    name: n,
    req: {
      method: 'POST',
      path: '/track',
      headers: [
        ...dropHeader(browserHeaders(c.scrypt), 'user-agent'),
        [
          'user-agent',
          new TextEncoder().encode('Mozilla/5.0 Ünï Chrome/128.0'),
        ] as [string, Uint8Array],
      ],
      body: trackBody(n),
    },
    expect: 'forwarded',
    produces: 1,
  });
  add({
    req: {
      method: 'OPTIONS',
      path: '/track',
      headers: [
        ['origin', 'https://parity.example'],
        ['access-control-request-method', 'POST'],
        ['access-control-request-headers', 'content-type,openpanel-client-id'],
      ],
    },
    expect: 'forwarded',
    produces: 0,
  });
  add({
    req: { method: 'GET', path: '/healthcheck', headers: [] },
    expect: 'forwarded',
    produces: 0,
  });
  // Too large for Kafka but small enough to read natively: 413 after auth.
  track(
    browserHeaders(c.scrypt),
    { properties: { blob: 'x'.repeat(1_100_000) } },
    { produces: 0 }
  );
  // GET /track/device-id
  const deviceId = (headers: H) =>
    add({
      req: {
        method: 'GET',
        path: '/track/device-id',
        headers: dropHeader(headers, 'content-type'),
      },
      expect: 'native',
      produces: 0,
    });
  deviceId(browserHeaders(c.scrypt));
  deviceId(dropHeader(browserHeaders(c.scrypt), 'user-agent'));
  deviceId(dropHeader(browserHeaders(c.scrypt), 'x-client-ip'));
  deviceId(
    replaceHeader(
      browserHeaders(c.scrypt),
      'user-agent',
      'Googlebot/2.1 (+http://www.google.com/bot.html)'
    )
  );
  deviceId(browserHeaders(c.scrypt, 'https://evil.example'));
  deviceId([
    ['openpanel-client-id', c.scrypt],
    ['openpanel-client-secret', secrets.scrypt],
    ['user-agent', 'Googlebot/2.1'],
    ['x-client-ip', '81.2.69.142'],
  ]);
  // POST /event (legacy)
  const legacy = (
    body: Record<string, unknown> | string,
    headers: H = browserHeaders(c.scrypt),
    produces = 1,
    expect: Case['expect'] = 'native',
    opts: Partial<Case> = {}
  ) => {
    const name = typeof body === 'string' ? undefined : (body.name as string);
    add({
      name: name?.trim() ? name : undefined,
      req: {
        method: 'POST',
        path: '/event',
        headers,
        body: typeof body === 'string' ? body : JSON.stringify(body),
      },
      expect,
      produces,
      ...opts,
    });
  };
  legacy({ name: nextName(), properties: { a: 1, __timestamp: fixedTs() } });
  legacy({
    name: nextName(),
    profileId: 42,
    properties: { __timestamp: fixedTs() },
  });
  legacy(
    { name: nextName(), profileId: 'legacy-user' },
    server('node'),
    1,
    'native',
    { timestampFree: true }
  );
  legacy({ name: nextName() }, server('node'), 1, 'native', {
    timestampFree: true,
    randomKey: true,
  });
  legacy({ name: '   ' }, browserHeaders(c.scrypt), 0);
  legacy(
    {
      name: nextName(),
      clientId: c.scrypt,
      clientSecret: secrets.scrypt,
      properties: { __timestamp: fixedTs() },
    },
    [
      ['user-agent', 'node'],
      ['content-type', 'application/json'],
    ],
    1,
    'native',
    { randomKey: true }
  );
  legacy(
    { name: nextName(), properties: 'not-an-object' },
    browserHeaders(c.scrypt),
    1,
    'forwarded'
  );
  legacy({
    name: nextName(),
    type: 'replay',
    properties: { __timestamp: fixedTs() },
  });
  legacy('[1,2]', browserHeaders(c.scrypt), 0, 'forwarded');
  return cases;
}

// ---------- comparison ----------
const IGNORED_RESPONSE_HEADERS = new Set([
  'date',
  'content-length',
  'connection',
  'keep-alive',
  'transfer-encoding',
]);
const SET_HEADERS = new Set([
  'access-control-allow-headers',
  'access-control-expose-headers',
]);
function compareResponses(ts: RawResponse, go: RawResponse): string[] {
  const problems: string[] = [];
  if (ts.status !== go.status) {
    problems.push(`status ${ts.status} vs ${go.status}`);
  }
  if (ts.body !== go.body) {
    problems.push(`body ${ts.body.slice(0, 300)} vs ${go.body.slice(0, 300)}`);
  }
  const names = new Set([
    ...Object.keys(ts.headers),
    ...Object.keys(go.headers),
  ]);
  for (const name of names) {
    if (IGNORED_RESPONSE_HEADERS.has(name)) {
      continue;
    }
    let a = ts.headers[name] ?? '';
    let b = go.headers[name] ?? '';
    if (SET_HEADERS.has(name)) {
      // Connection is hop-by-hop: a proxy does not pass it on.
      const names = (s: string) =>
        s
          .split(/, ?/)
          .filter((h) => h !== 'host' && h !== 'connection')
          .sort()
          .join(',');
      a = names(a);
      b = names(b);
    }
    if (name === 'content-type') {
      a = a.toLowerCase();
      b = b.toLowerCase();
    }
    if (a !== b) {
      problems.push(`header ${name}: ${a} vs ${b}`);
    }
  }
  return problems;
}

function normalizeRecord(
  record: Record<string, unknown>,
  timestampFree: boolean
) {
  const copy = structuredClone(record) as Record<string, any>;
  delete copy.id;
  if (timestampFree) {
    delete copy.event?.timestamp;
    delete copy.event?.isTimestampFromThePast;
    delete copy.sessionId;
  }
  return copy;
}

function deepDiff(a: unknown, b: unknown, path = ''): string[] {
  if (JSON.stringify(a) === JSON.stringify(b)) {
    return [];
  }
  if (
    typeof a !== 'object' ||
    typeof b !== 'object' ||
    a === null ||
    b === null ||
    Array.isArray(a) !== Array.isArray(b)
  ) {
    return [
      `${path || '.'}: ${JSON.stringify(a)?.slice(0, 200)} vs ${JSON.stringify(b)?.slice(0, 200)}`,
    ];
  }
  const keysA = Object.keys(a as object);
  const keysB = Object.keys(b as object);
  const out: string[] = [];
  if (
    keysA.join(',') !== keysB.join(',') &&
    keysA.slice().sort().join(',') === keysB.slice().sort().join(',')
  ) {
    out.push(
      `${path || '.'}: key order ${keysA.join(',')} vs ${keysB.join(',')}`
    );
  }
  for (const key of new Set([...keysA, ...keysB])) {
    out.push(...deepDiff((a as any)[key], (b as any)[key], `${path}.${key}`));
  }
  return out;
}

async function main() {
  await setupFixtures();
  const cases = buildCases();
  const results: { name: string; expect?: string; problems: string[] }[] = [];
  try {
    await startServers();
    for (const [index, c] of cases.entries()) {
      if (index % 10 === 0) {
        console.log(`case ${index}/${cases.length}`);
      }
      const ts = await send(TS_PORT, c.req);
      await Bun.sleep(150);
      const go = await send(GO_PORT, c.req);
      await Bun.sleep(150);
      results.push({
        name: c.name,
        expect: c.expect,
        problems: compareResponses(ts, go),
      });
    }
    // A live session blob in Redis: both sides must extend it rather than mint
    // a deterministic id.
    const liveHeaders = browserHeaders(ids.scrypt, 'https://parity.example', [
      ['x-live-session', '1'],
    ]);
    const probe = JSON.parse(
      (
        await send(TS_PORT, {
          method: 'GET',
          path: '/track/device-id',
          headers: dropHeader(liveHeaders, 'content-type'),
        })
      ).body
    ) as { deviceId: string };
    const redis = new Bun.RedisClient(
      process.env.REDIS_URL ?? 'redis://localhost:23379'
    );
    const endedAt = new Date(Date.now() - 60_000)
      .toISOString()
      .replace('T', ' ')
      .replace('Z', '');
    await redis.set(
      `session:${ids.project}:${probe.deviceId}`,
      JSON.stringify({ id: 'live-session-parity', ended_at: endedAt })
    );
    await Bun.sleep(150);
    const liveName = nextName();
    const liveReq: RawRequest = {
      method: 'POST',
      path: '/track',
      headers: liveHeaders,
      body: trackBody(liveName),
    };
    const liveTs = await send(TS_PORT, liveReq);
    await Bun.sleep(150);
    const liveGo = await send(GO_PORT, liveReq);
    const liveDevice = await send(GO_PORT, {
      method: 'GET',
      path: '/track/device-id',
      headers: dropHeader(
        browserHeaders(ids.scrypt, 'https://parity.example', [
          ['x-live-session', '2'],
        ]),
        'content-type'
      ),
    });
    await redis.del(`session:${ids.project}:${probe.deviceId}`);
    redis.close();
    const liveProblems = compareResponses(liveTs, liveGo);
    if (!liveGo.body.includes('live-session-parity')) {
      liveProblems.push(`go did not reuse the live session: ${liveGo.body}`);
    }
    if (!liveDevice.body.includes('live-session-parity')) {
      liveProblems.push(
        `go device-id did not report the live session: ${liveDevice.body}`
      );
    }
    results.push({ name: liveName, expect: 'native', problems: liveProblems });
    cases.push({
      name: liveName,
      req: liveReq,
      expect: 'native',
      produces: 1,
      timestampFree: true,
    });

    // The dedupe lock is shared: a request repeated at the Go receiver within
    // the window is a duplicate there too.
    const dupName = nextName();
    const dupReq: RawRequest = {
      method: 'POST',
      path: '/track',
      headers: browserHeaders(ids.scrypt),
      body: trackBody(dupName, { properties: { __timestamp: fixedTs() } }),
    };
    const first = await send(TS_PORT, dupReq);
    const second = await send(GO_PORT, dupReq);
    results.push({
      name: 'dedupe-across',
      problems:
        second.body === '{"duplicate":true}' && first.status === 200
          ? []
          : [`expected duplicate, got ${second.status} ${second.body}`],
    });

    await Bun.sleep(3000);
    const tsRecords = await readTopic(TS_TOPIC);
    const goRecords = await readTopic(GO_TOPIC);
    const byName = (records: KafkaRecord[]) => {
      const map = new Map<string, KafkaRecord[]>();
      for (const r of records) {
        const name = (r.value as any).event?.name as string;
        map.set(name, [...(map.get(name) ?? []), r]);
      }
      return map;
    };
    const tsBy = byName(tsRecords);
    const goBy = byName(goRecords);
    for (const c of cases) {
      const result = results.find((r) => r.name === c.name)!;
      const tsList = tsBy.get(c.name) ?? [];
      const goList = goBy.get(c.name) ?? [];
      const expected = c.produces ?? 0;
      if (c.expect === 'forwarded') {
        if (goList.length) {
          result.problems.push(
            `forwarded case produced ${goList.length} on the Go topic`
          );
        }
        if (tsList.length !== expected * 2) {
          result.problems.push(
            `expected ${expected * 2} records on the TS topic, got ${tsList.length}`
          );
        }
        if (tsList.length === 2) {
          result.problems.push(
            ...deepDiff(
              normalizeRecord(tsList[0]!.value, true),
              normalizeRecord(tsList[1]!.value, true)
            ).map((p) => `kafka ${p}`)
          );
        }
        continue;
      }
      if (tsList.length !== expected || goList.length !== expected) {
        result.problems.push(
          `records ts=${tsList.length} go=${goList.length} expected ${expected}`
        );
        continue;
      }
      if (expected === 1) {
        const [a, b] = [tsList[0]!, goList[0]!];
        // A server event with no profile is keyed on a random id (generateId),
        // alone on /track and after the project id on /event.
        const randomPart = (key: string) => key.slice(key.lastIndexOf(':') + 1);
        const serverWithoutProfile =
          (a.value as any).uaInfo?.isServer === true &&
          !(a.value as any).event?.profileId;
        const keyRandom =
          randomPart(a.key).length === 8 &&
          randomPart(b.key).length === 8 &&
          a.key.slice(0, -8) === b.key.slice(0, -8) &&
          (c.randomKey === true || serverWithoutProfile);
        if (a.key !== b.key && !keyRandom) {
          result.problems.push(`key ${a.key} vs ${b.key}`);
        }
        if (!keyRandom && a.partition !== b.partition) {
          result.problems.push(`partition ${a.partition} vs ${b.partition}`);
        }
        const valueA = normalizeRecord(a.value, c.timestampFree ?? false);
        const valueB = normalizeRecord(b.value, c.timestampFree ?? false);
        result.problems.push(
          ...deepDiff(valueA, valueB).map((p) => `kafka ${p}`)
        );
      }
    }
    const failed = results.filter((r) => r.problems.length);
    for (const r of failed) {
      console.log(
        `FAIL ${r.name} (${r.expect ?? ''})\n  ${r.problems.join('\n  ')}`
      );
    }
    console.log(
      `${results.length - failed.length}/${results.length} cases identical; records ts=${tsRecords.length} go=${goRecords.length}`
    );
    process.exitCode = failed.length ? 1 : 0;
  } finally {
    stopServers();
    if (!args.includes('--keep')) {
      await teardownFixtures();
      await deleteTopic(TS_TOPIC);
      await deleteTopic(GO_TOPIC);
    }
    await sql.close();
  }
}

if (import.meta.main) {
  await main();
}
