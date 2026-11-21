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
  client: '0a1a0000-0000-4000-8000-0000000000a1',
  rule: '0a1a0000-0000-4000-8000-0000000000b1',
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
