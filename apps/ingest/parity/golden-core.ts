// Golden outputs from the TypeScript ingest path, the fixtures the Go port's
// unit tests compare against. Re-run after an upstream change to any of the
// functions imported below; a Go test that then fails is real drift.
//   bun parity/golden-core.ts            writes each package's testdata/
//   bun parity/golden-core.ts <dir>      writes everything to <dir> instead
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import fastJsonStableHash from '../../../packages/core/node_modules/fast-json-stable-hash';
import {
  isBlockedEventName,
  zTrackHandlerPayload,
} from '../../../packages/core/src/modules/ingest/ingest.constants';
import {
  getOverrideDeviceId,
  getStringHeaders,
  getTimestamp,
} from '../../../packages/core/src/modules/ingest/ingest.service';
import { applyBotSuspicion } from '../../../packages/core/src/modules/ingest/src/bots/suspicion';
import { getDeviceId } from '../../../packages/core/src/modules/ingest/src/device-id';
import { getClientIpFromHeaders } from '../../../packages/core/src/shared/get-client-ip';
import { isOriginAllowed } from '../../../packages/core/src/shared/origin-allowed';
import { generateDeviceId } from '../../../packages/core/src/shared/profileId';

const outDir = process.argv[2];
const INTERNAL = join(import.meta.dir, '../internal');
const TESTDATA: Record<string, string> = {
  'jsjson.json': 'jsjson',
  'stablehash.json': 'jsjson',
  'deviceid.json': 'identity',
  'session.json': 'identity',
  'timestamps.json': 'track',
  'blocked.json': 'track',
  'schema.json': 'track',
  'suspicion.json': 'track',
  'stringheaders.json': 'track',
  'overridedevice.json': 'track',
  'origins.json': 'clientauth',
  'clientip.json': 'reqheaders',
};
const save = (name: string, data: unknown) => {
  const path = outDir
    ? join(outDir, name)
    : join(INTERNAL, TESTDATA[name]!, 'testdata', name);
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(data, null, 1)}\n`);
};

// Deterministic PRNG so the goldens are stable.
let seed = 42;
const rand = () => {
  seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648;
  return seed / 2_147_483_648;
};
const pick = <T>(xs: T[]) => xs[Math.floor(rand() * xs.length)]!;
const KEYS = [
  'a',
  'b',
  'z',
  '0',
  '1',
  '10',
  '2',
  '01',
  '-1',
  '4294967294',
  '4294967295',
  '__proto_x',
  'ключ',
  '😀',
  'é',
  'k k',
  '$',
  'A',
  String.fromCharCode(0xe0_00),
];
const STRS = [
  '',
  'x',
  'hello world',
  'quote"inside',
  'back\\slash',
  'tab\tnew\nline',
  `${String.fromCharCode(1)}ctl`,
  `${String.fromCharCode(0x20_28)}sep`,
  'émoji 😀',
  '中文',
  '<script>&amp;</script>',
  String.fromCharCode(0x7f),
  String.fromCodePoint(0x1_f6_00),
];
const NUMS = [
  0,
  -0,
  1,
  -1,
  0.1,
  1.5,
  1e21,
  1e20,
  123_456_789_012_345_680_000,
  1e-7,
  1e-6,
  0.000_001_234,
  5e-324,
  1.797_693_134_862_315_7e308,
  2 ** 53,
  2 ** 53 + 2,
  100,
  1.000_000_000_000_000_2,
  33.333_333_333_333_33,
  -1.5e-10,
];
function randomValue(depth: number): unknown {
  const r = rand();
  if (depth > 3 || r < 0.35) {
    return pick(STRS);
  }
  if (r < 0.55) {
    return pick(NUMS);
  }
  if (r < 0.62) {
    return rand() < 0.5;
  }
  if (r < 0.66) {
    return null;
  }
  if (r < 0.8) {
    return Array.from({ length: Math.floor(rand() * 4) }, () =>
      randomValue(depth + 1)
    );
  }
  const o: Record<string, unknown> = {};
  const n = Math.floor(rand() * 5);
  for (let i = 0; i < n; i++) {
    o[pick(KEYS)] = randomValue(depth + 1);
  }
  return o;
}
const BS = String.fromCharCode(92);
const esc = (hex: string) => `${BS}u${hex}`;
// Texts written by hand (so key order and duplicates are as typed) plus random ones.
const texts = [
  '{"b":1,"2":2,"a":3,"1":4,"01":5,"-1":6,"4294967295":7,"4294967294":8}',
  '{"a":1,"a":2,"b":3,"a":4}',
  '{"1":1,"1":2}',
  '[1,1.0,-0,1e21,1E21,1e-7,0.0000001,123456789012345678901,1e400,-1e400,5e-324,0.1,1.5e300,1e+2]',
  '"' +
    ['0000', '001f', '007f', '2028', '2029', 'd83d', 'de00'].map(esc).join('') +
    BS +
    '/' +
    BS +
    'b' +
    BS +
    'f"',
  '{"emoji":"😀","nested":{"z":[true,false,null],"y":{}}}',
  '  {  "ws" :\t[ 1 ,\n2 ]\r}  ',
  '{"":"empty key","__proto__":1}',
  ...Array.from({ length: 400 }, () => JSON.stringify(randomValue(0))),
];
save(
  'jsjson.json',
  texts.map((text) => ({ text, out: JSON.stringify(JSON.parse(text)) }))
);
save(
  'stablehash.json',
  texts.map((text) => ({
    text,
    md5: fastJsonStableHash.hash(JSON.parse(text), 'md5'),
  }))
);

// Device ids: the ua:ip:origin:salt shake256.
const deviceCases = Array.from({ length: 200 }, (_, i) => ({
  salt: pick(['a1b2c3', 'f'.repeat(32), '']),
  ua: pick([
    'Mozilla/5.0 (X11; Linux x86_64) Chrome/120',
    'unknown/1.0',
    'Ünï 😀',
    '',
  ]),
  ip: pick(['81.2.69.142', '2001:db8::1', '']),
  origin: pick(['acme-web', `proj-${i}`]),
}));
save(
  'deviceid.json',
  deviceCases.map((c) => ({ ...c, id: generateDeviceId(c) }))
);

// Session ids and the live-session window, through getDeviceId with a stub buffer.
const sessionCases: unknown[] = [];
const timeouts = [1_800_000, 5000, 60_000, 6, 7, 30_001];
const base = Date.UTC(2026, 10, 20, 12, 0, 0);
for (let i = 0; i < 300; i++) {
  const timeout = pick(timeouts);
  const eventTimeMs =
    base +
    Math.floor(rand() * 4 * 3_600_000) +
    pick([0, 1, 999, 4999, 5000, 5001]);
  const deviceId = pick(['dev-a', 'dev-b', 'd'.repeat(64), 'ü']);
  const projectId = pick(['acme-web', 'p']);
  const live = rand() < 0.4;
  const endedAt = new Date(eventTimeMs - (live ? timeout - 1 : timeout))
    .toISOString()
    .replace('T', ' ')
    .replace('Z', '');
  const sessions =
    rand() < 0.5
      ? []
      : [
          {
            id: `sess-${i}`,
            ended_at: pick([endedAt, 'garbage', endedAt.slice(0, 19)]),
          },
        ];
  const result = await getDeviceId({
    projectId,
    ip: '1.1.1.1',
    ua: 'ua',
    salts: { current: 's1', previous: 's2' },
    overrideDeviceId: deviceId,
    eventTimeMs,
    sessionTimeoutMs: timeout,
    sessionBuffer: { getExistingSessions: async () => sessions as never },
    logger: { error: () => undefined },
  });
  sessionCases.push({
    projectId,
    deviceId,
    eventTimeMs,
    timeout,
    sessions,
    result,
  });
}
save('session.json', sessionCases);

// Timestamps: arrival 2026-11-20T12:00:00Z, many client-supplied forms.
const now = base;
const tsInputs: unknown[] = [
  undefined,
  null,
  0,
  '',
  'x',
  12_345,
  now,
  now - 1,
  now + 60_000,
  now + 60_001,
  now - 900_000,
  now - 900_001,
  Date.UTC(2000, 0, 1),
  Date.UTC(2000, 0, 1) - 1,
  1_700_000_000_000.7,
  -1,
  8.64e15,
  8.64e15 + 1,
  true,
  {},
  [],
  '2026-11-20',
  '2026-11-20T11:59:00Z',
  '2026-11-20T11:59:00.123Z',
  '2026-11-20T11:59:00.1234567Z',
  '2026-11-20T11:59Z',
  '2026-11-20T11:59:00+02:00',
  '2026-11-20T11:59:00-0130',
  '2026-11-20T11:59:00',
  '2026-11-20 11:59:00',
  '2026-11-20T11:59:00.000+00:00',
  '2026-11',
  '2026',
  '+002026-11-20T11:00:00Z',
  '2026-11-20T24:00:00Z',
  '2026-02-30T00:00:00Z',
  '2026-11-20T11:59:60Z',
  'Fri, 20 Nov 2026 11:00:00 GMT',
  'Nov 20 2026',
  '2026/11/20',
  '11/20/2026',
  '1700000000000',
  ' 2026-11-20T11:00:00Z',
  '2026-11-20T11:00:00z',
  '2026-11-20t11:00:00Z',
  '2026-11-20T11:00:00.Z',
  '2026-11-20T11:00:00,123Z',
  '2026-1-2',
  '2026-11-20T11:00:00.5Z',
  '2026-11-20T11:00:00.12Z',
  '1999-12-31T23:59:59.999Z',
  '2026-11-20T12:01:00.001Z',
  '2026-11-20T11:59:00.999999999Z',
  '2026-11-20T11:59:00+14:00',
  '2026-11-20T11:59:00-12:00',
  '2026-11-20T11:59:00+24:00',
  '2026-11-19T23:00:00.000-01:00',
];
save(
  'timestamps.json',
  tsInputs.map((input) => ({
    now,
    input: input === undefined ? '__undefined__' : input,
    out: getTimestamp(now, input),
  }))
);

// Origins against project cors lists.
const corsLists = [
  ['*'],
  ['example.com'],
  ['https://example.com'],
  ['www.example.com'],
  ['*.example.com'],
  ['*.example.com:3000'],
  ['app.*.example.com'],
  ['http://localhost:3000'],
  ['localhost:3000'],
  ['*example*'],
  [],
  ['a.com', 'https://b.com/'],
  ['https://www.example.com'],
  ['example.com:443'],
];
const origins = [
  undefined,
  '',
  'https://example.com',
  'http://example.com',
  'https://www.example.com',
  'https://app.example.com',
  'https://app.example.com:3000',
  'https://a.b.example.com',
  'http://localhost:3000',
  'https://EXAMPLE.com',
  'https://example.com:443',
  'https://example.com/path',
  'null',
  'https://xn--n3h.com',
  'https://☃.com',
  'https://evil.com/?q=example.com',
  'https://app.x.example.com',
  'https://b.com',
  'https://a.com',
  'example.com',
  'https://wwwexample.com',
  'https://www.www.example.com',
  'http://example.com:80',
  'https://app.example.com:8080',
];
const originCases: unknown[] = [];
for (const cors of corsLists) {
  for (const origin of origins) {
    originCases.push({
      cors,
      origin: origin ?? '__undefined__',
      out: isOriginAllowed(cors, origin),
    });
  }
}
save('origins.json', originCases);

// Client ip attribution.
const ipHeaderSets: Record<string, string>[] = [
  {},
  { 'x-forwarded-for': '81.2.69.142, 10.0.0.1' },
  { 'x-forwarded-for': '10.0.0.1, 81.2.69.142' },
  { 'openpanel-client-ip': '81.2.69.142', 'cf-connecting-ip': '1.1.1.1' },
  { 'cf-connecting-ip': '127.0.0.1', 'x-real-ip': '8.8.8.8' },
  { forwarded: 'for=192.0.2.60;proto=http;by=203.0.113.43' },
  { forwarded: 'for="[2001:db8:cafe::17]:4711"' },
  { 'x-client-ip': '::ffff:127.0.0.1', 'x-real-ip': '::ffff:8.8.8.8' },
  { 'true-client-ip': '172.16.5.4', 'fastly-client-ip': '172.32.0.1' },
  { 'x-real-ip': '999.1.1.1' },
  { 'x-real-ip': 'fe80::1', 'do-connecting-ip': '2001:4860:4860::8888' },
  { 'x-forwarded-for': ' 8.8.4.4 ' },
  {
    'x-cluster-client-ip': '192.168.1.1',
    'x-appengine-user-ip': '169.254.1.1',
    'x-forwarded': '9.9.9.9',
  },
  { 'x-forwarded-for': '' },
  { 'x-real-ip': '::1' },
  { 'x-real-ip': 'fd00::1' },
  { 'x-real-ip': '172.15.0.1' },
];
const ipCases: unknown[] = [];
for (const headers of ipHeaderSets) {
  ipCases.push({
    order: null,
    headers,
    out: getClientIpFromHeaders({}, new Headers(headers)),
  });
  ipCases.push({
    order: ['x-real-ip', 'x-forwarded-for'],
    headers,
    out: getClientIpFromHeaders(
      { attributionOrder: ['x-real-ip', 'x-forwarded-for'] },
      new Headers(headers)
    ),
  });
}
save('clientip.json', ipCases);

// Blocked event names.
const names = [
  'page_view',
  'x'.repeat(80),
  'x'.repeat(81),
  'a\nb',
  'a\rb',
  'SELECT pg_sleep(5)',
  '/wp-login.php',
  '/.env',
  '/phpmyadmin',
  '/foo.php',
  '/index.html',
  ['$', '{jndi}'].join(''),
  "x' or '1",
  'Union Select',
  '/admin.json',
  '/a.PHP',
  'session_start',
  'revenue',
  '../etc',
  'Ünïcödé',
  '😀'.repeat(41),
  '😀'.repeat(40),
  '/WP-admin',
  'x/cgi-bin/y',
  '#<Article:0x1>',
];
save(
  'blocked.json',
  names.map((name) => ({ name, out: isBlockedEventName(name) }))
);

// The /track body schema.
const bodies = [
  '{"type":"track","payload":{"name":"a"}}',
  '{"type":"track","payload":{"name":"a","extra":1},"extra":2}',
  '{"payload":{"groups":["g"],"profileId":"p","properties":{"b":1,"a":2,"1":3},"name":"a"},"type":"track"}',
  '{"type":"track","payload":{"name":""}}',
  '{"type":"track","payload":{"name":"session_start"}}',
  '{"type":"track","payload":{"name":"a","profileId":""}}',
  '{"type":"track","payload":{"name":"a","profileId":12}}',
  '{"type":"track","payload":{"name":"a","profileId":1.5}}',
  '{"type":"track","payload":{"name":"a","profileId":null}}',
  '{"type":"track","payload":{"name":"a","groups":null}}',
  '{"type":"track","payload":{"name":"a","groups":[]}}',
  '{"type":"track","payload":{"name":"a","groups":[""]}}',
  '{"type":"track","payload":{"name":"a","groups":["' +
    'g'.repeat(256) +
    '"]}}',
  '{"type":"track","payload":{"name":"a","groups":["' +
    'g'.repeat(257) +
    '"]}}',
  '{"type":"track","payload":{"name":"a","groups":["' +
    '😀'.repeat(128) +
    '"]}}',
  '{"type":"track","payload":{"name":"a","groups":["' +
    '😀'.repeat(129) +
    '"]}}',
  '{"type":"track","payload":{"name":"a","groups":' +
    JSON.stringify(Array.from({ length: 101 }, (_, i) => `g${i}`)) +
    '}}',
  '{"type":"track","payload":{"name":"a","groups":' +
    JSON.stringify(Array.from({ length: 100 }, (_, i) => `g${i}`)) +
    '}}',
  '{"type":"track","payload":{"name":"a","groups":[1]}}',
  '{"type":"track","payload":{"name":"a","properties":null}}',
  '{"type":"track","payload":{"name":"a","properties":[]}}',
  '{"type":"track","payload":{"name":"a","properties":"x"}}',
  '{"type":"track","payload":{"name":"a","properties":{}}}',
  '{"type":"track","payload":{"name":"revenue","properties":{"__revenue":10}}}',
  '{"type":"track","payload":{"name":"revenue","properties":{"__revenue":-1}}}',
  '{"type":"track","payload":{"name":"revenue","properties":{"__revenue":0}}}',
  '{"type":"track","payload":{"name":"revenue","properties":{"__revenue":1.5}}}',
  '{"type":"track","payload":{"name":"revenue","properties":{"__revenue":"10"}}}',
  '{"type":"track","payload":{"name":"revenue","properties":{"__revenue":null}}}',
  '{"type":"track","payload":{"name":"revenue","properties":{"__revenue":1e300}}}',
  '{"type":"track","payload":{"name":"other","properties":{"__revenue":1.5}}}',
  '{"type":"track","payload":{"name":"/wp-admin"}}',
  '{"type":"track","payload":{"name":5}}',
  '{"type":"track"}',
  '{"type":"track","payload":null}',
  '{"type":"track","payload":[]}',
  '{"type":"identify","payload":{"profileId":"x"}}',
  '{"type":"TRACK","payload":{"name":"a"}}',
  '[]',
  'null',
  '"x"',
  '{}',
  '{"type":"track","payload":{"name":"a","properties":{"__proto__":{"x":1}}}}',
];
save(
  'schema.json',
  bodies.map((text) => {
    const parsed = zTrackHandlerPayload.safeParse(JSON.parse(text));
    return {
      text,
      success: parsed.success,
      data: parsed.success ? JSON.stringify(parsed.data) : null,
    };
  })
);

// Bot suspicion marking.
const asnSet = [
  { asn: undefined, org: undefined, isHosting: false },
  { asn: 16_509, org: 'AMAZON', isHosting: true },
  { asn: undefined, org: undefined, isHosting: true },
];
const headerSets = [
  {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0) Chrome/120.0',
    'sec-ch-ua': '"x"',
    'sec-fetch-mode': 'cors',
    'accept-language': 'en',
  },
  { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0) Chrome/120.0' },
  {
    'user-agent': 'Mozilla/5.0 (iPhone) CriOS/120.0 Mobile',
    'sec-fetch-site': 'same-site',
    'accept-language': ' ',
  },
  { 'user-agent': 'curl/8' },
  {},
  {
    'user-agent': 'Mozilla/5.0 Firefox/120',
    'sec-fetch-mode': '',
    'sec-fetch-site': 'x',
  },
];
const propSets = [
  undefined,
  {},
  { __bot: '1', __bot_reasons: 'forged', keep: 1 },
  { a: 1 },
];
const suspicion: unknown[] = [];
for (const asnInfo of asnSet) {
  for (const headers of headerSets) {
    for (const props of propSets) {
      for (const clientSecretAuth of [false, true]) {
        for (const isServer of [false, true]) {
          const copy = props === undefined ? undefined : structuredClone(props);
          const out = applyBotSuspicion(copy, {
            asnInfo,
            headers,
            clientSecretAuth,
            isServer,
          });
          suspicion.push({
            asnInfo: { ...asnInfo, asn: asnInfo.asn ?? null },
            headers,
            props: props ?? null,
            clientSecretAuth,
            isServer,
            out: out === undefined ? '__undefined__' : JSON.stringify(out),
          });
        }
      }
    }
  }
}
save('suspicion.json', suspicion);

// Queue payload headers and the caller-supplied device id.
const headerRecords = [
  {},
  {
    'user-agent': 'ua',
    'openpanel-sdk-name': 'web',
    'openpanel-sdk-version': '1.0',
    'openpanel-client-id': 'c',
    'request-id': 'r',
    'x-other': 'no',
  },
  { 'user-agent': '', 'request-id': 'r' },
];
save(
  'stringheaders.json',
  headerRecords.map((h) => ({
    headers: h,
    out: JSON.stringify(getStringHeaders(h)),
  }))
);
const deviceOverrides = [
  undefined,
  '',
  '  ',
  ' abc ',
  'x'.repeat(64),
  'x'.repeat(65),
  5,
  null,
  true,
  ` ${'y'.repeat(64)} `,
];
save(
  'overridedevice.json',
  deviceOverrides.map((v) => ({
    value: v === undefined ? '__undefined__' : v,
    out:
      getOverrideDeviceId({
        type: 'track',
        payload: {
          name: 'a',
          properties: v === undefined ? {} : { __deviceId: v },
        },
      } as never) ?? null,
  }))
);
console.log('golden written to', outDir ?? 'internal/*/testdata');
