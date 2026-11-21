// Deterministic Kafka records for the worker differential rig, shaped exactly
// as the producers write them: `handleTrack` and `handleAi` in
// packages/core/src/modules/ingest/ingest.service.ts (key order of the payload
// and the partition key rule included), and apps/ingest/internal/track for the
// Go receiver. Everything is a fixed value: the same call always returns the
// same records, so two runs of any worker see the same input.
import { createHash } from 'node:crypto';
import { parseUserAgent } from '../../../packages/shared/src/server/parser-user-agent';

export const FIXTURE_PREFIX = 'wparity-';

export const fixtureIds = {
  org: 'wparity-org',
  projectA: 'wparity-a',
  projectB: 'wparity-b',
  projectC: 'wparity-c',
  client: '0a1a0000-0000-4000-8000-0000000000a1',
  rule: '0a1a0000-0000-4000-8000-0000000000b1',
  ruleB: '0a1a0000-0000-4000-8000-0000000000b2',
  ruleC: '0a1a0000-0000-4000-8000-0000000000b3',
} as const;

/** The event the project filter on project A drops. */
export const EXCLUDED_EVENT = 'wparity_excluded';
/** The event the fixture notification rule on project A matches. */
export const NOTIFY_EVENT = 'wparity_notify';
export const SESSION_TIMEOUT_MS = 30 * 60 * 1000;

const T0 = Date.UTC(2026, 10, 15, 10, 0, 0);
const MINUTE = 60 * 1000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

const CHROME_UA =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36';
const MOBILE_UA =
  'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1';
const SERVER_UA = 'wparity-server/1.0';

const GEO_STOCKHOLM = {
  country: 'SE',
  city: 'Stockholm',
  region: 'AB',
  longitude: 18.0686,
  latitude: 59.3293,
};
const GEO_NONE = {
  country: undefined,
  city: undefined,
  region: undefined,
  longitude: undefined,
  latitude: undefined,
};

type Agent = 'chrome' | 'mobile' | 'server';
const USER_AGENTS: Record<Agent, string> = {
  chrome: CHROME_UA,
  mobile: MOBILE_UA,
  server: SERVER_UA,
};

export interface CorpusRecord {
  /** Records of one wave are produced together; the next wave waits for lag 0. */
  wave: number;
  key: string;
  value: string;
  label: string;
}

interface RecordSpec {
  label: string;
  project: string;
  name: string;
  at: number;
  agent?: Agent;
  /** '' for a record without a device (server side, ai). */
  device?: string;
  /** Which session the producer resolved, as it would have minted it. */
  session?: string;
  profileId?: string | number;
  properties?: Record<string, unknown>;
  groups?: string[];
  past?: boolean;
  requestId?: string;
  /** An older producer: no `id`. */
  noId?: boolean;
  /** An older producer: ISO string instead of epoch milliseconds. */
  isoTimestamp?: boolean;
  ai?: Record<string, unknown>;
  geo?: typeof GEO_STOCKHOLM | typeof GEO_NONE;
}

const sessionId = (project: string, device: string, n: number) =>
  createHash('sha256')
    .update(`wparity:session:${project}:${device}:${n}`)
    .digest('base64url')
    .slice(0, 22);

const eventId = (label: string) => {
  const hex = createHash('sha256').update(`wparity:id:${label}`).digest('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
};

function build(wave: number, spec: RecordSpec): CorpusRecord {
  const agent = spec.agent ?? 'chrome';
  const ua = USER_AGENTS[agent];
  const properties = spec.properties;
  const uaInfo = parseUserAgent(ua, properties);
  const isServer = uaInfo.isServer;
  const device = spec.device ?? '';
  const profileId = spec.profileId;
  const headers: Record<string, string> = {
    'user-agent': ua,
    'openpanel-sdk-name': isServer ? 'node' : 'web',
    'openpanel-sdk-version': '1.0.0',
    'openpanel-client-id': fixtureIds.client,
    'request-id': spec.requestId ?? '',
  };
  const event: Record<string, unknown> = spec.ai
    ? {
        name: spec.name,
        properties,
        profileId: profileId === undefined ? undefined : String(profileId),
        groups: spec.groups ?? [],
        timestamp: spec.at,
        isTimestampFromThePast: false,
      }
    : {
        name: spec.name,
        properties,
        profileId,
        groups: spec.groups ?? [],
        timestamp: spec.isoTimestamp
          ? new Date(spec.at).toISOString()
          : spec.at,
        isTimestampFromThePast: spec.past ?? false,
      };
  const payload: Record<string, unknown> = {
    ...(spec.noId ? {} : { id: eventId(spec.label) }),
    projectId: spec.project,
    headers,
    event,
    uaInfo,
    geo: spec.geo ?? (isServer ? GEO_NONE : GEO_STOCKHOLM),
    deviceId: device,
    sessionId: spec.session ?? '',
    ...(spec.ai ? { ai: spec.ai } : {}),
  };
  // handleTrack: server events group by profile, browsers by device, and a
  // record with neither gets a random key (a fixed one here).
  let key: string;
  if (spec.ai) {
    key = profileId
      ? `${spec.project}:${profileId}`
      : device || `wparity-key-${spec.label}`;
  } else if (isServer) {
    key = profileId
      ? `${spec.project}:${profileId}`
      : `wparity-key-${spec.label}`;
  } else {
    key = device || `wparity-key-${spec.label}`;
  }
  return { wave, key, value: JSON.stringify(payload), label: spec.label };
}

const aiDraft = {
  type: 'generation',
  trace_id: 'wparity-trace-1',
  span_id: '',
  parent_id: '',
  conversation_id: 'wparity-conv-1',
  name: 'chat.completion',
  model: 'claude-sonnet-5',
  provider: 'anthropic',
  input_tokens: 1200,
  output_tokens: 340,
  cache_read_tokens: 800,
  cache_write_tokens: 0,
  reasoning_tokens: 0,
  input_cost: 0.0036,
  output_cost: 0.0051,
  total_cost: 0.0087,
  cost_source: 'computed',
  latency_ms: 2150,
  time_to_first_token_ms: 410,
  is_error: false,
  http_status: 200,
  stop_reason: 'end_turn',
  is_stream: true,
  model_parameters: '{"temperature":0.2}',
  input: 'Summarise the quarter in one line.',
  output: 'Revenue grew 12 percent.',
  tools: '[]',
  error: '',
  properties: { 'feature.name': 'summary', plan: 'pro' },
};

const A = fixtureIds.projectA;
const B = fixtureIds.projectB;
const SESSION_GAP = 2 * HOUR;

/**
 * Five waves. A wave only holds records that do not depend on each other;
 * a later wave may depend on the session a previous one opened (a server event
 * riding a profile's session, a record extending a session), so the runner
 * waits for lag 0 between waves and the result does not depend on how the
 * worker interleaves keys.
 */
export function buildCorpus(): CorpusRecord[] {
  const s = (project: string, device: string, n = 0) =>
    sessionId(project, device, n);
  const waves: RecordSpec[][] = [
    [
      // A new session on a fresh device, with UTM tags and a referrer.
      {
        label: 'a-d1-landing',
        project: A,
        name: 'screen_view',
        at: T0,
        device: 'wparity-dev-1',
        session: s(A, 'wparity-dev-1'),
        requestId: 'wparity-req-1',
        properties: {
          __path:
            'https://wparity.example/home?utm_source=newsletter&utm_medium=email&utm_campaign=autumn',
          __referrer: 'https://www.google.com/search?q=wparity',
          __title: 'Home',
        },
      },
      {
        label: 'a-d2-mobile',
        project: A,
        name: 'screen_view',
        at: T0 + 30_000,
        agent: 'mobile',
        device: 'wparity-dev-2',
        session: s(A, 'wparity-dev-2'),
        properties: { __path: 'https://wparity.example/pricing' },
      },
      // The second project.
      {
        label: 'b-d3-landing',
        project: B,
        name: 'screen_view',
        at: T0 + 45_000,
        device: 'wparity-dev-3',
        session: s(B, 'wparity-dev-3'),
        properties: { __path: 'https://other.example/' },
      },
      // Dropped by project A's event filter.
      {
        label: 'a-d4-excluded',
        project: A,
        name: EXCLUDED_EVENT,
        at: T0 + 50_000,
        device: 'wparity-dev-4',
        session: s(A, 'wparity-dev-4'),
        properties: { __path: 'https://wparity.example/secret' },
      },
      // An identified profile, with unicode and nested custom properties.
      {
        label: 'a-d5-identified',
        project: A,
        name: 'screen_view',
        at: T0 + 60_000,
        device: 'wparity-dev-5',
        session: s(A, 'wparity-dev-5'),
        profileId: 'wparity-user-1',
        properties: {
          __path: 'https://wparity.example/welcome',
          plan: 'pro',
          nested: { team: { name: 'Æblegrød 🍎', size: 12 }, tags: ['a', 'b'] },
          note: '日本語のテスト — ünïcödé',
        },
      },
    ],
    [
      // Extends the session of wave 1.
      {
        label: 'a-d1-docs',
        project: A,
        name: 'screen_view',
        at: T0 + 2 * MINUTE,
        device: 'wparity-dev-1',
        session: s(A, 'wparity-dev-1'),
        properties: {
          __path: 'https://wparity.example/docs/getting-started#install',
        },
      },
      // Matches the notification rule on project A.
      {
        label: 'a-d1-notify',
        project: A,
        name: NOTIFY_EVENT,
        at: T0 + 3 * MINUTE,
        device: 'wparity-dev-1',
        session: s(A, 'wparity-dev-1'),
        properties: { __path: 'https://wparity.example/docs', plan: 'team' },
      },
      {
        label: 'a-d2-revenue',
        project: A,
        name: 'revenue',
        at: T0 + 4 * MINUTE,
        agent: 'mobile',
        device: 'wparity-dev-2',
        session: s(A, 'wparity-dev-2'),
        properties: {
          __path: 'https://wparity.example/checkout',
          __revenue: 4999,
          currency: 'EUR',
        },
      },
      // Event groups, an integer profile id.
      {
        label: 'a-d5-groups',
        project: A,
        name: 'button_click',
        at: T0 + 5 * MINUTE,
        device: 'wparity-dev-5',
        session: s(A, 'wparity-dev-5'),
        profileId: 'wparity-user-1',
        groups: ['wparity-org-1', 'wparity-team-a'],
        properties: {
          __path: 'https://wparity.example/welcome',
          label: 'Start trial',
        },
      },
      {
        label: 'b-d3-extend',
        project: B,
        name: 'button_click',
        at: T0 + 6 * MINUTE,
        device: 'wparity-dev-3',
        session: s(B, 'wparity-dev-3'),
        properties: { __path: 'https://other.example/pricing' },
      },
      // An older producer: no id, ISO timestamp.
      {
        label: 'a-d2-legacy',
        project: A,
        name: 'screen_view',
        at: T0 + 7 * MINUTE,
        agent: 'mobile',
        device: 'wparity-dev-2',
        session: s(A, 'wparity-dev-2'),
        noId: true,
        isoTimestamp: true,
        properties: { __path: 'https://wparity.example/pricing#faq' },
      },
    ],
    [
      // After the idle timeout: the producer minted a new session id.
      {
        label: 'a-d1-after-gap',
        project: A,
        name: 'screen_view',
        at: T0 + SESSION_GAP,
        device: 'wparity-dev-1',
        session: s(A, 'wparity-dev-1', 1),
        properties: { __path: 'https://wparity.example/home' },
      },
      // A server event for the identified profile: rides its session.
      {
        label: 'a-srv-user1',
        project: A,
        name: 'subscription_renewed',
        at: T0 + 10 * MINUTE,
        agent: 'server',
        profileId: 'wparity-user-1',
        properties: { plan: 'pro', seats: 5 },
      },
      // A server event that carries the device the producer derived for it.
      {
        label: 'a-srv-device',
        project: A,
        name: 'webhook_received',
        at: T0 + 10 * MINUTE + 30_000,
        agent: 'server',
        device: 'wparity-dev-srv',
        session: s(A, 'wparity-dev-srv'),
        properties: { source: 'stripe' },
      },
      // A server event with neither profile nor device.
      {
        label: 'a-srv-anon',
        project: A,
        name: 'cron_tick',
        at: T0 + 11 * MINUTE,
        agent: 'server',
        properties: { job: 'nightly' },
      },
      // A numeric profile id, a different device, project B.
      {
        label: 'b-d6-numeric',
        project: B,
        name: 'screen_view',
        at: T0 + 12 * MINUTE,
        device: 'wparity-dev-6',
        session: s(B, 'wparity-dev-6'),
        profileId: 4711,
        properties: { __path: 'https://other.example/account' },
      },
    ],
    [
      // A timestamp from the past: no session of its own. Alone in its wave
      // so it never races the server event of the same profile.
      {
        label: 'a-d5-past',
        project: A,
        name: 'offline_sync',
        at: T0 - 2 * DAY,
        device: 'wparity-dev-5',
        session: s(A, 'wparity-dev-5'),
        profileId: 'wparity-user-1',
        past: true,
        properties: { __path: 'https://wparity.example/sync' },
      },
    ],
    [
      // An ai record, after the profile's session exists.
      {
        label: 'a-ai-user1',
        project: A,
        name: 'ai.generation',
        at: T0 + 13 * MINUTE,
        agent: 'server',
        profileId: 'wparity-user-1',
        properties: { feature: 'summary' },
        ai: aiDraft,
      },
      // An ai record with no profile: deviceless.
      {
        label: 'a-ai-anon',
        project: A,
        name: 'ai.generation',
        at: T0 + 14 * MINUTE,
        agent: 'server',
        properties: { feature: 'summary' },
        ai: {
          ...aiDraft,
          trace_id: 'wparity-trace-2',
          is_error: true,
          http_status: 500,
          error: 'upstream timeout',
        },
      },
    ],
  ];
  return waves.flatMap((specs, index) =>
    specs.map((spec) => build(index, spec))
  );
}

// ---------------------------------------------------------------------------
// The generated corpus: ~2,000 records over four partitions, from a seeded
// generator, so the same call always returns the same records.
//
// Ordering rules that keep two correct workers comparable (see worker-diff.ts):
//  - everything one device does is in one partition and one key, in order;
//  - an identified user lives on exactly one device, so a profile pointer has
//    one writer;
//  - server and AI records that ride a client session come in a later wave
//    than every client record of that session, and waves wait for lag 0.
// Within a wave the devices are interleaved at random, which is what makes the
// four lanes, same-key serialization and micro-batching do real work.

export const GENERATED_PARTITIONS = 4;
const GENERATED_SEED = 20_261_122;
const GENERATED_DEVICES = 480;
const FROM_CHAR = String.fromCharCode;

/** A linear congruential generator: the same on every engine, no bit tricks. */
const LCG_MULTIPLIER = 1_103_515_245;
const LCG_INCREMENT = 12_345;
const LCG_MODULUS = 2_147_483_648;
function seededRandom(seed: number) {
  let state = seed % LCG_MODULUS;
  return () => {
    state = (state * LCG_MULTIPLIER + LCG_INCREMENT) % LCG_MODULUS;
    return state / LCG_MODULUS;
  };
}

const GEN_PATHS = [
  'https://wparity.example/home',
  'https://wparity.example/pricing?plan=team',
  'https://wparity.example/docs/getting-started#install',
  'https://Example.COM:443/Mixed/Case?x=1&x=2&y=%C3%BC#frag',
  'https://wparity.example/日本語/ページ?q=ü&emoji=%F0%9F%8D%8E',
  'https://wparity.example/a/../b/./c?empty=&=novalue&flag',
  'https://wparity.example/caf%C3%A9 /with space',
  'https://wparity.example/pricing?utm_source=newsletter&utm_medium=email&utm_campaign=autumn&utm_content=hero&utm_term=analytics',
  'https://wparity.example/landing?utm_source=Facebook&utm_medium=social',
  'https://wparity.example/landing?ref=producthunt',
  'https://wparity.example/landing?utm_source=&ref=github',
  'https://wparity.example/landing?utm_source=constructor',
  'https://wparity.example/landing?utm_referrer=twitter.com',
  'https://wparity.example/#',
  'https://wparity.example/x?a[]=1&a[]=2&b=%',
  '/relative/path?x=1#h',
  '/pricing',
  '//cdn.wparity.example/asset?v=1',
  'Home Screen',
  'httpfoo',
  'http://[::1]:8080/ipv6',
  'https://user:pw@wparity.example/auth',
  'file:///etc/hosts',
  `https://wparity.example/${'long/'.repeat(30)}`,
];

const GEN_REFERRERS = [
  'https://www.google.com/search?q=wparity',
  'https://www.google.com/',
  'https://t.co/abc123',
  'https://l.facebook.com/l.php?u=https%3A%2F%2Fwparity.example',
  'https://wparity.example/home',
  'https://news.ycombinator.com/item?id=1',
  'android-app://com.google.android.gm',
  'https://www.bing.com/search?q=x',
  'not a url',
  'https://duckduckgo.com',
  'https://yandex.ru/search/?text=x',
  'https://unknown-referrer.example/page/',
];

const GEN_CURRENCIES = ['EUR', 'USD', 'eur', 'GBP', 'JPY'];
const GEN_REVENUES: unknown[] = [
  4999,
  12.5,
  '12.5',
  '0.99',
  'abc',
  0,
  '',
  null,
  100_000,
  1e21,
  '3e2',
];
// The same positions as GEN_REVENUES, with values ClickHouse can store: the
// revenue column is a UInt64, and a value it rejects (0.99, 1e21) fails the
// insert of the whole batch that holds it.
const GEN_REVENUES_CLICKHOUSE: unknown[] = [
  4999,
  1250,
  12,
  99,
  7,
  0,
  3,
  null,
  100_000,
  Number.MAX_SAFE_INTEGER,
  300,
];
const GEN_UNICODE = [
  '日本語のテスト — ünïcödé',
  'Æblegrød 🍎',
  'emoji 👨‍👩‍👧‍👦 flag 🇸🇪',
  `${FROM_CHAR(0xd8_3d)} lone high`,
  `lone low ${FROM_CHAR(0xde_00)}`,
  `  padded with${FROM_CHAR(0xa0)}nbsp${FROM_CHAR(0xfe_ff)}  `,
  'tab\there\nnewline',
  '</script> & "quotes" \\ backslash',
];

type Rand = () => number;
const pick = <T>(rand: Rand, items: readonly T[]): T =>
  items[Math.floor(rand() * items.length)]!;
const between = (rand: Rand, low: number, high: number) =>
  low + Math.floor(rand() * (high - low + 1));

function genProperties(rand: Rand, extra: Record<string, unknown> = {}) {
  const properties: Record<string, unknown> = { ...extra };
  if (rand() < 0.12) {
    properties.nested = {
      team: { name: pick(rand, GEN_UNICODE), size: between(rand, 1, 99) },
      tags: ['a', 'b', pick(rand, GEN_UNICODE)],
      deep: { a: { b: { c: [1, 2, { d: true }] } } },
    };
  }
  if (rand() < 0.1) {
    properties.note = pick(rand, GEN_UNICODE);
  }
  if (rand() < 0.06) {
    properties.numbers = {
      float: 0.1 + 0.2,
      big: 123_456_789_012_345_680_000,
      tiny: 1.5e-7,
      negative: -42,
      zero: 0,
      flag: false,
    };
  }
  if (rand() < 0.04) {
    properties.json_text = '{"inner":{"x":[1,2,3]}}';
    properties.malformed = '{not json';
    properties.blank = '   ';
  }
  if (rand() < 0.05) {
    properties.__query = { first_touch: 'yes', utm_source: 'client-sent' };
  }
  return properties;
}

function genPath(rand: Rand): Record<string, unknown> {
  if (rand() < 0.03) {
    // an older SDK: no double underscore
    return { path: pick(rand, GEN_PATHS), referrer: pick(rand, GEN_REFERRERS) };
  }
  const out: Record<string, unknown> = { __path: pick(rand, GEN_PATHS) };
  if (rand() < 0.4) {
    out.__referrer = pick(rand, GEN_REFERRERS);
  }
  return out;
}

interface GenDevice {
  index: number;
  project: string;
  agent: Agent;
  device: string;
  profileId?: string | number;
  /** A second identity the device switches to in the first client wave. */
  switchTo?: string;
  session: number;
  clock: number;
  groups?: string[];
}

/**
 * The producer's session rule under a different idle timeout: a device's
 * session id changes when the next event comes more than `timeoutMs` after the
 * latest one, which is also the worker's boundary rule (session-buffer.ts). The
 * corpus is written for the 30 minute default; a run with a shorter timeout
 * must have the producers mint ids accordingly, or one id would span several
 * worker sessions and the end of the first would close the live one. Records
 * without a device (server and AI records) and past-timestamp records keep the
 * session of their device.
 */
export function alignSessionsToTimeout(
  records: CorpusRecord[],
  timeoutMs: number
): CorpusRecord[] {
  const devices = new Map<string, { n: number; lastAt: number }>();
  return records.map((record) => {
    const payload = JSON.parse(record.value);
    if (!(payload.deviceId && payload.sessionId)) {
      return record;
    }
    const at = new Date(payload.event.timestamp).getTime();
    const key = `${payload.projectId}|${payload.deviceId}`;
    const state = devices.get(key) ?? {
      n: 0,
      lastAt: Number.NEGATIVE_INFINITY,
    };
    if (!payload.event.isTimestampFromThePast) {
      if (
        at - state.lastAt > timeoutMs &&
        state.lastAt > Number.NEGATIVE_INFINITY
      ) {
        state.n += 1;
      }
      state.lastAt = Math.max(state.lastAt, at);
    }
    devices.set(key, state);
    const aligned = sessionId(payload.projectId, payload.deviceId, state.n);
    return {
      ...record,
      value: record.value.replace(
        `"sessionId":"${payload.sessionId}"`,
        `"sessionId":"${aligned}"`
      ),
    };
  });
}

export function buildGeneratedCorpus(
  sessionTimeoutMs = SESSION_TIMEOUT_MS,
  clickhouseSafe = false
): CorpusRecord[] {
  const revenues = clickhouseSafe ? GEN_REVENUES_CLICKHOUSE : GEN_REVENUES;
  const rand = seededRandom(GENERATED_SEED);
  const waves: RecordSpec[][] = [[], [], [], [], [], [], []];
  const devices: GenDevice[] = [];

  for (let index = 0; index < GENERATED_DEVICES; index++) {
    const roll = rand();
    const project =
      roll < 0.45
        ? fixtureIds.projectA
        : roll < 0.8
          ? fixtureIds.projectB
          : fixtureIds.projectC;
    const identified = rand() < 0.35;
    devices.push({
      index,
      project,
      agent: rand() < 0.3 ? 'mobile' : 'chrome',
      device: `wparity-gdev-${index}`,
      profileId: identified
        ? rand() < 0.1
          ? 100_000 + index
          : `wparity-guser-${index}`
        : undefined,
      switchTo:
        identified && rand() < 0.1 ? `wparity-gswitch-${index}` : undefined,
      session: 0,
      clock: T0 + between(rand, 0, 40 * MINUTE),
      groups:
        rand() < 0.15
          ? [`wparity-gorg-${index % 7}`, `wparity-gteam-${index % 3}`]
          : undefined,
    });
  }

  const sessionOf = (d: GenDevice) => sessionId(d.project, d.device, d.session);
  const clientEvent = (
    d: GenDevice,
    wave: number,
    name: string,
    at: number,
    properties: Record<string, unknown>,
    options: Partial<RecordSpec> = {}
  ) => {
    const roll = rand();
    waves[wave]!.push({
      label: `g${d.index}-w${wave}-${waves[wave]!.length}`,
      project: d.project,
      name,
      at,
      agent: d.agent,
      device: d.device,
      session: sessionOf(d),
      profileId: d.profileId,
      groups: d.groups,
      properties: genProperties(rand, properties),
      requestId:
        roll < 0.3
          ? `wparity-greq-${d.index}-${wave}`
          : roll < 0.4
            ? 'a b/c!'
            : '',
      noId: rand() < 0.04,
      isoTimestamp: rand() < 0.05,
      ...options,
    });
  };

  const extraEvent = (d: GenDevice, wave: number, at: number) => {
    const roll = rand();
    if (roll < 0.5) {
      clientEvent(d, wave, 'screen_view', at, genPath(rand));
    } else if (roll < 0.7) {
      clientEvent(d, wave, 'button_click', at, {
        ...genPath(rand),
        label: pick(rand, GEN_UNICODE),
      });
    } else if (roll < 0.78) {
      clientEvent(d, wave, 'revenue', at, {
        ...genPath(rand),
        __revenue: pick(rand, revenues),
        currency: pick(rand, GEN_CURRENCIES),
      });
    } else if (roll < 0.9) {
      clientEvent(d, wave, NOTIFY_EVENT, at, {
        ...genPath(rand),
        plan: pick(rand, ['team', 'free', 'pro']),
        seats: between(rand, 1, 50),
      });
    } else if (roll < 0.93) {
      clientEvent(d, wave, EXCLUDED_EVENT, at, genPath(rand));
    } else {
      clientEvent(d, wave, 'form_submit', at, {
        form: 'signup',
        ok: rand() < 0.5,
      });
    }
  };

  // Wave 0: every device lands. Wave 1: more events of the same session, in
  // order, plus the odd late one.
  for (const d of devices) {
    clientEvent(d, 0, 'screen_view', d.clock, genPath(rand));
    const extras = between(rand, 0, 3);
    for (let n = 0; n < extras; n++) {
      d.clock += between(rand, 20_000, 9 * MINUTE);
      extraEvent(d, 1, d.clock);
    }
    if (rand() < 0.08) {
      extraEvent(d, 1, d.clock - 3 * MINUTE);
    }
    if (d.switchTo && rand() < 0.5) {
      d.clock += 30_000;
      clientEvent(d, 1, 'screen_view', d.clock, genPath(rand), {
        profileId: d.switchTo,
      });
      d.profileId = d.switchTo;
    }
  }

  // Wave 2: some devices come back after an idle gap around the timeout; the
  // producer mints a new session id when the gap is past it.
  const gaps = [
    sessionTimeoutMs - 1000,
    sessionTimeoutMs,
    sessionTimeoutMs + 1,
    sessionTimeoutMs + 61_000,
    2 * HOUR,
    DAY + 5 * MINUTE,
  ];
  for (const d of devices) {
    if (rand() >= 0.45) {
      continue;
    }
    const gap = pick(rand, gaps);
    d.clock += gap;
    if (gap > sessionTimeoutMs) {
      d.session += 1;
    }
    clientEvent(d, 2, 'screen_view', d.clock, genPath(rand));
    if (rand() < 0.5) {
      d.clock += between(rand, 10_000, 5 * MINUTE);
      extraEvent(d, 2, d.clock);
    }
    if (d.profileId !== undefined && !d.switchTo && rand() < 0.1) {
      // the user signs in as somebody else on the same device
      d.clock += 5000;
      clientEvent(d, 2, 'screen_view', d.clock, genPath(rand), {
        profileId: `wparity-gother-${d.index}`,
      });
    }
  }
  // Wave 3: the new session keeps going.
  for (const d of devices) {
    if (d.session > 0 && rand() < 0.6) {
      d.clock += between(rand, 15_000, 12 * MINUTE);
      extraEvent(d, 3, d.clock);
    } else if (rand() < 0.1) {
      // an anonymous device that becomes identified late
      d.clock += 20_000;
      clientEvent(d, 3, 'screen_view', d.clock, genPath(rand), {
        profileId: `wparity-glate-${d.index}`,
      });
    }
  }

  // Wave 4: server events, riding the sessions above. Wave 5: past events.
  const identified = devices.filter((d) => d.profileId !== undefined);
  for (const d of identified) {
    const base = d.clock + MINUTE;
    if (rand() < 0.5) {
      const count = between(rand, 1, 3);
      for (let n = 0; n < count; n++) {
        waves[4]!.push({
          label: `g${d.index}-srv-${n}`,
          project: d.project,
          name: pick(rand, [
            'subscription_renewed',
            'webhook_received',
            'invoice_paid',
            NOTIFY_EVENT,
          ]),
          at: base + n * 1000,
          agent: 'server',
          profileId: d.profileId,
          groups: rand() < 0.2 ? [`wparity-gorg-${d.index % 7}`] : undefined,
          properties: genProperties(rand, {
            plan: pick(rand, ['team', 'pro']),
            seats: between(rand, 1, 20),
            amount: 19.99,
          }),
          noId: rand() < 0.04,
        });
      }
    }
    if (rand() < 0.07) {
      // Its own wave: a past event is keyed by device, server events by
      // profile, and their rows land in the same device's list, in an order
      // that depends on which lane gets there first.
      waves[5]!.push({
        label: `g${d.index}-past`,
        project: d.project,
        name: 'offline_sync',
        at: T0 - between(rand, 1, 5) * DAY,
        agent: d.agent,
        device: d.device,
        session: sessionOf(d),
        profileId: d.profileId,
        past: true,
        properties: genPath(rand),
      });
    }
  }
  const anyProject = () =>
    pick(rand, [fixtureIds.projectA, fixtureIds.projectB, fixtureIds.projectC]);
  for (let n = 0; n < 40; n++) {
    waves[4]!.push({
      label: `gsrv-anon-${n}`,
      project: anyProject(),
      name: pick(rand, ['cron_tick', 'queue_drained', NOTIFY_EVENT]),
      at: T0 + between(rand, 0, 6 * HOUR),
      agent: 'server',
      properties: { job: `nightly-${n}`, plan: pick(rand, ['team', 'free']) },
    });
  }
  // A server record that names its device but has no profile.
  for (let n = 0; n < 20; n++) {
    const d = pick(rand, devices);
    waves[4]!.push({
      label: `gsrv-dev-${n}`,
      project: d.project,
      name: 'webhook_received',
      at: d.clock + 2 * MINUTE,
      agent: 'server',
      device: `wparity-gsrvdev-${n}`,
      session: sessionId(d.project, `wparity-gsrvdev-${n}`, 0),
      properties: { source: 'stripe', n },
    });
  }

  // Wave 6: AI records, after the sessions they ride exist.
  for (const d of identified) {
    if (rand() < 0.12) {
      waves[6]!.push({
        label: `g${d.index}-ai`,
        project: d.project,
        name: 'ai.generation',
        at: d.clock + 5 * MINUTE,
        agent: 'server',
        profileId: d.profileId,
        properties: { feature: 'summary' },
        ai: {
          ...aiDraft,
          trace_id: `wparity-gtrace-${d.index}`,
          span_id: rand() < 0.5 ? `wparity-gspan-${d.index}` : '',
          is_error: rand() < 0.2,
        },
      });
    }
  }
  for (let n = 0; n < 15; n++) {
    waves[6]!.push({
      label: `gai-anon-${n}`,
      project: anyProject(),
      name: 'ai.generation',
      at: T0 + between(rand, 0, 6 * HOUR),
      agent: 'server',
      properties: { feature: 'chat' },
      ai: { ...aiDraft, trace_id: `wparity-gtrace-anon-${n}` },
    });
  }

  // Interleave the devices inside a wave without reordering one device (or one
  // profile's server records).
  const out: CorpusRecord[] = [];
  waves.forEach((specs, wave) => {
    const groups = new Map<string, RecordSpec[]>();
    for (const spec of specs) {
      const owner =
        spec.agent === 'server' && spec.profileId !== undefined
          ? `${spec.project}:${spec.profileId}`
          : `${spec.project}|${spec.device ?? spec.label}`;
      groups.set(owner, [...(groups.get(owner) ?? []), spec]);
    }
    const queues = [...groups.values()];
    while (queues.length > 0) {
      const index = Math.floor(rand() * queues.length);
      const queue = queues[index]!;
      out.push(build(wave, queue.shift()!));
      if (queue.length === 0) {
        queues.splice(index, 1);
      }
    }
  });
  return out;
}
