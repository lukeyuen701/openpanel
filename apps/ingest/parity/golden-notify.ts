// Golden outputs for the Go worker's notification and project-filter code
// (internal/worker/notify): the real matchEvent, notificationTemplateEvent,
// testFilterPattern, transformProfile and checkNotificationRulesForEvent run
// over generated inputs, and the Go tests must reproduce every answer.
//   bun parity/golden-notify.ts            writes internal/worker/notify/testdata/*.json.gz
//   bun parity/golden-notify.ts <dir>      writes to <dir> instead
// Needs a Redis (REDIS_URL, default the local dev one) for the dispatch
// scenarios: they use db index 15 and delete the keys they set.
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { gzipSync } from 'node:zlib';

const outDir = process.argv[2];
const TESTDATA = join(import.meta.dir, '../internal/worker/notify/testdata');
const save = (name: string, data: unknown, indent = 1) => {
  const path = join(outDir ?? TESTDATA, `${name}.gz`);
  mkdirSync(dirname(path), { recursive: true });
  // gzip writes no mtime or file name, so the same data gives the same bytes.
  writeFileSync(
    path,
    gzipSync(`${JSON.stringify(data, null, indent)}\n`, { level: 9 })
  );
};

const redisBase = process.env.REDIS_URL || 'redis://127.0.0.1:23379';
process.env.REDIS_URL = `${redisBase.replace(/\/\d+$/, '')}/15`;

const service = await import(
  '../../../packages/core/src/modules/notification/notification.service'
);
const { testFilterPattern, toFilterPattern } = await import(
  '../../../packages/core/src/modules/notification/src/filter-regex'
);
const { transformProfile } = await import(
  '../../../packages/core/src/modules/profile/profile.service'
);
const { matchEvent, notificationTemplateEvent } = service;

let seed = 7;
const rand = () => {
  seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648;
  return seed / 2_147_483_648;
};
const pick = <T>(xs: readonly T[]) => xs[Math.floor(rand() * xs.length)]!;

type Outcome = { ok: unknown } | { throws: string };
const run = (fn: () => unknown): Outcome => {
  try {
    return { ok: fn() };
  } catch (error) {
    return { throws: error instanceof TypeError ? 'TypeError' : String(error) };
  }
};
const noop = () => undefined;
const plain = (value: unknown) => JSON.parse(JSON.stringify(value));
// Cases share a few dozen payloads: store each once and refer to it by index.
function dedupePayloads(cases: unknown[]) {
  const payloads: unknown[] = [];
  const index = new Map<string, number>();
  const refs = (cases as { payload: unknown }[]).map((item) => {
    const key = JSON.stringify(item.payload);
    if (!index.has(key)) {
      index.set(key, payloads.length);
      payloads.push(item.payload);
    }
    return { ...item, payload: index.get(key) };
  });
  return { payloads, cases: refs };
}

function basePayload(properties: Record<string, unknown>) {
  return {
    name: 'purchase',
    profileId: 'user-1',
    projectId: 'wparity-proj',
    deviceId: 'device-1',
    sessionId: 'session-1',
    properties,
    groups: [],
    createdAt: new Date('2026-01-01T00:01:00.123Z'),
    duration: 0,
    sdkName: 'web',
    city: 'London',
    country: 'GB',
    region: 'ENG',
    longitude: -0.1,
    latitude: 51.5,
    path: '/order/1',
    origin: 'https://shop.example',
    referrer: 'https://www.google.com',
    referrerName: 'Google',
    referrerType: 'search',
    os: 'Mac OS',
    browser: 'Chrome',
    device: 'desktop',
    revenue: 99,
    id: 'event-1',
  };
}

const PROPERTY_SETS: Record<string, unknown>[] = [
  { value: '99', plan: 'pro' },
  { value: 5, flag: true },
  { value: ' padded ', nbsp: '\u{a0}x\u{a0}', bom: '\u{feff}y' },
  { nested: { a: { b: 'deep' } }, list: ['a', 'b'], empty: '', nothing: null },
  {
    value: '10',
    other: '9',
    exp: '1e3',
    hex: '0x10',
    neg: '-5',
    blank: ' 12 ',
  },
  { text: 'ключ İ UPPER 😀', value: 'abc' },
  { __query: { utm_source: 'google', utm_medium: 'cpc' }, __hash: '#top' },
  {},
];
const FILTER_NAMES = [
  'name',
  'path',
  'country',
  'profileId',
  'deviceId',
  'properties.value',
  'properties.plan',
  'properties.nested.a.b',
  'properties.list.0',
  'properties.list.length',
  'properties.missing',
  'properties.empty',
  'properties.nothing',
  'properties.flag',
  'properties.text',
  'properties.__query.utm_source',
  'properties.nbsp',
  'properties.bom',
  'properties.blank',
  'properties.exp',
  'properties.hex',
  'duration',
  'revenue',
  'createdAt',
  'groups',
  'longitude',
  'has_profile',
  'missingTop',
  'properties.value.length',
  'origin',
  'referrerName',
  'properties.constructor',
  'properties.toString',
  'properties.__proto__',
  'properties.hasOwnProperty',
  'properties.list.map',
  'properties.text.trim',
  'properties.nested.valueOf',
  'properties.value.toFixed',
  'properties.flag.toString',
];
const OPERATORS = [
  'is',
  'isNot',
  'contains',
  'doesNotContain',
  'startsWith',
  'endsWith',
  'regex',
  'isNull',
  'isNotNull',
  'gt',
  'lt',
  'gte',
  'lte',
  'bogus',
];
const VALUE_LISTS: unknown[][] = [
  ['GB'],
  ['gb'],
  [],
  ['a', 'b'],
  [5],
  ['5', 6],
  [null],
  [true],
  ['true'],
  ['false'],
  ['/^Lon/'],
  ['^Lon'],
  ['(?=x)'],
  ['[a-'],
  ['10'],
  [' 10 '],
  ['0x10'],
  [''],
  ['1e1'],
  ['Infinity'],
  ['abc'],
  ['99', '5'],
  ['x', 'deep'],
  ['pro'],
  [['a']],
  [{ a: 1 }],
  ['/product/'],
  ['\\d+'],
  ['purchase'],
];
const EVENT_NAMES = ['purchase', '*', 'other'];

const randomFilter = () => ({
  id: 'f',
  name: pick(FILTER_NAMES),
  operator: pick(OPERATORS),
  value: pick(VALUE_LISTS),
});

const matchCases: unknown[] = [];
for (let i = 0; i < 6000; i++) {
  const payload = basePayload(pick(PROPERTY_SETS));
  if (rand() < 0.1) {
    payload.profileId = payload.deviceId;
  }
  const filterCount = Math.floor(rand() * 4);
  const event: Record<string, unknown> = {
    name: pick(EVENT_NAMES),
    filters: Array.from({ length: filterCount }, randomFilter),
  };
  matchCases.push({
    payload: plain(payload),
    event,
    result: run(() => matchEvent(payload as never, event as never)),
  });
}
const MALFORMED_EVENTS: Record<string, unknown>[] = [
  { name: 'purchase' },
  { name: 'purchase', filters: null },
  { name: 'purchase', filters: [null] },
  { name: 'purchase', filters: [{ operator: 'is', value: ['x'] }] },
  { name: 'purchase', filters: [{ name: 'country', operator: 'is' }] },
  {
    name: 'purchase',
    filters: [{ name: 'country', operator: 'is', value: 'GB' }],
  },
  {
    name: 'purchase',
    filters: [{ name: 'country', operator: 'is', value: '' }],
  },
  { name: 'purchase', filters: [{ name: 'country' }] },
  { name: 'purchase', filters: [{ name: 'country', value: ['GB'] }] },
  { name: 'purchase', filters: [{ name: 'has_profile', value: [] }] },
  {
    name: 'purchase',
    filters: [{ name: 'has_profile', operator: 'is', value: ['false'] }],
  },
  {
    name: 'purchase',
    filters: [{ name: 'has_profile', operator: 'is', value: ['true'] }],
  },
  { filters: [] },
  { name: '*' },
  {},
];
for (const event of MALFORMED_EVENTS) {
  for (const properties of [{}, { value: 5 }]) {
    const payload = basePayload(properties);
    matchCases.push({
      payload: plain(payload),
      event,
      result: run(() => matchEvent(payload as never, event as never)),
    });
  }
}
// The filter throws only when it is reached: every() stops at the first false.
const shortCircuit = {
  name: 'purchase',
  filters: [
    { name: 'country', operator: 'is', value: ['US'] },
    { name: 'duration', operator: 'is', value: ['0'] },
  ],
};
matchCases.push({
  payload: plain(basePayload({})),
  event: shortCircuit,
  result: run(() =>
    matchEvent(basePayload({}) as never, shortCircuit as never)
  ),
});
save('match.json', dedupePayloads(matchCases), 0);

const PATTERNS = [
  'product',
  '^product',
  'product$',
  '^/product/\\d+$',
  '(a+)+$',
  'a|b|c',
  '(?i)LONDON',
  '(?s)a.b',
  '(?m)^x$',
  '\\pL+',
  '\\p{Greek}',
  '\\p{Han}',
  '\\PL',
  '[[:alpha:]]+',
  '[[:^digit:]]',
  '\\Qa.b\\E',
  '\\x41',
  '\\x{1F600}',
  '\\x{41}',
  '\\cA',
  '\\Z',
  '\\z',
  '\\A',
  '\\b\\w+\\b',
  '\\B',
  '\\d{2,}',
  'a{,3}',
  'a{2,1}',
  'a{1001}',
  '(?P<n>x)',
  '(?<n>x)',
  '(?P=n)',
  '(?=x)',
  '(?!x)',
  '(?<=x)',
  '(a)\\1',
  '[a-',
  '(',
  ')',
  '*',
  '+a',
  'a**',
  'a++',
  'a*?',
  'a{2}{3}',
  '\\8',
  '\\0',
  '\\012',
  '\\h',
  '\\R',
  '\\X',
  '\\C',
  '\\_',
  '\\-',
  '\\ ',
  '[\\d-z]',
  '[a-\\d]',
  '[\\w-]',
  '[]a]',
  '[^]a]',
  '(?i)k',
  '(?i)ß',
  '(?i)ſ',
  '(?i)İ',
  '(?i)σ',
  '(?U)a+',
  '(?i:a)b',
  '(?-i)a',
  '(?#c)a',
  '(?x) a b',
  '.',
  '(?s).',
  '^$',
  '',
  '\\s',
  '\\S+',
  '\\w',
  '\\W',
  '[\\s]',
  'x*',
  'é',
  '[é]',
  '\\p{L}',
  '\\p{^L}',
  '\\pN',
  '\\p{Lu}',
  '\\p{IsGreek}',
  '\\p{Script=Greek}',
  '\\p{Foo}',
  '\\N{LATIN}',
  '(?:a|b)+c',
  'a\\/b',
  '/a/',
  '//',
  '/',
  '\\/',
];
const REGEX_INPUTS = [
  '/shop/product/1',
  'product',
  'London',
  'LONDON',
  'london',
  'a\nb',
  'x\ny',
  '/product/42',
  'abc123',
  'Ελλάδα',
  '漢字',
  'A',
  'a.b',
  'ab',
  'K',
  '\u{212a}',
  'ß',
  'ẞ',
  'ſ',
  'İ',
  'i',
  'ς',
  'Σ',
  '',
  ' ',
  'é',
  'e\u{301}',
  '😀',
  '__',
  'aaa',
  'xx',
  'a/b',
  '/',
  'a\r\n',
  '\u{85}',
  '\u{2028}',
];
const regexCases: unknown[] = [];
for (const pattern of PATTERNS) {
  for (const input of REGEX_INPUTS) {
    regexCases.push({
      pattern,
      input,
      result: run(() => testFilterPattern(toFilterPattern(pattern), input)),
    });
  }
}
const LIMIT_PATTERNS = [
  '^.$',
  '^..$',
  '[^a]{2}',
  '^[^a]$',
  '(a{100}){100}',
  '(a{32}){32}',
  '(?:a{2}){501}',
  '((a{100}){100}){100}',
  'a{1000}',
  'a{1001}',
  '(a{500}){3}',
  '(?:a{1000}){1}',
];
const LIMIT_INPUTS = [
  '\u{1f600}',
  '\u{1f600}\u{1f600}',
  'ab',
  'a'.repeat(1000),
  'a'.repeat(1002),
  'a'.repeat(10_000),
  `${'a'.repeat(1000)}b`,
];
for (const pattern of LIMIT_PATTERNS) {
  for (const input of LIMIT_INPUTS) {
    regexCases.push({
      pattern,
      input,
      result: run(() => testFilterPattern(toFilterPattern(pattern), input)),
    });
  }
}
save('regex.json', regexCases, 0);

const TEMPLATES = [
  null,
  '',
  'plain',
  '$EVENT_NAME',
  '$RULE_NAME',
  '{{rule_name}}',
  '$RULE_NAME saw $EVENT_NAME from {{properties.country}}',
  '{{country}} {{city}} {{path}}',
  '{{properties.value}}',
  '{{properties.nested}}',
  '{{properties.nested.a.b}}',
  '{{properties.list}}',
  '{{properties.list.1}}',
  '{{properties.list.length}}',
  '{{properties.flag}}',
  '{{properties.empty}}',
  '{{properties.nothing}}',
  '{{properties.missing}}',
  '{{duration}}',
  '{{revenue}}',
  '{{longitude}}',
  '{{groups}}',
  '{{createdAt}}',
  '{{profile.firstName}}',
  '{{profile.createdAt}}',
  '{{profile.properties.plan}}',
  '{{profile.groups}}',
  '{{profile.isExternal}}',
  '{{profile}}',
  '{{ properties.value }}',
  '{{}}',
  '{{a}',
  '{{properties.value}} and {{properties.value}} and {{properties.plan}}',
  '{{{properties.value}}}',
  '{{properties.value}}}}',
  '{{rule_name}}{{rule_name}}',
  '$&',
  "$'",
  '$`',
  '$$',
  '$1',
  '$<n>',
  '$',
  'cost $$5 for $EVENT_NAME',
  "[$&] [$`] [$'] [$$]",
  '{{properties.dollar}}',
  '{{properties.amp}}',
  '{{properties.tick}}',
  '{{properties.quote}}',
  'x{{properties.dollar}}y{{properties.dollar}}z',
  '{{properties.chain}} {{properties.value}}',
  '{{name}}',
  '{{name.length}}',
  '{{properties.text.length}}',
  '{{properties.text.0}}',
  '{{properties.bignum}}',
  '{{properties.float}}',
  '{{properties.zero}}',
  '{{properties.negzero}}',
  '{{properties.t}}',
  '{{properties.f}}',
  '{{properties.emptyobj}}',
  '{{properties.emptylist}}',
  '{{properties.obj}}',
  '{{properties.uni}}',
];
const TEMPLATE_PROPERTIES = {
  value: '99',
  plan: 'pro',
  country: 'SE',
  flag: true,
  empty: '',
  nothing: null,
  nested: { a: { b: 'deep' } },
  list: ['a', 'b'],
  dollar: "US$&1 $$ $` $'",
  amp: '&',
  tick: '$`',
  quote: "$'",
  chain: '{{properties.value}}',
  text: 'ключ',
  bignum: 1e21,
  float: 0.1,
  zero: 0,
  negzero: -0,
  t: true,
  f: false,
  emptyobj: {},
  emptylist: [],
  obj: { 'k"q': 'v\u{2028}' },
  uni: 'a\x00b\u{1f600}',
};
const profile = transformProfile({
  id: 'user-1',
  first_name: 'Ada',
  last_name: 'Lovelace',
  email: 'ada@example.com',
  avatar: '',
  properties: { plan: 'pro', 'address.city': 'London', 'address.zip': '1' },
  project_id: 'wparity-proj',
  is_external: true,
  created_at: '2026-01-02 03:04:05.678',
  last_seen_at: '2026-01-03 04:05:06.789',
  groups: ['g1'],
} as never);
const templateCases: unknown[] = [];
for (const template of TEMPLATES) {
  for (const withProfile of [false, true]) {
    for (const ruleName of ['My Rule', 'R$&x']) {
      const payload: Record<string, unknown> = basePayload(TEMPLATE_PROPERTIES);
      if (withProfile) {
        payload.profile = profile;
      }
      templateCases.push({
        template,
        ruleName,
        payload: plain(payload),
        result: run(() =>
          notificationTemplateEvent({
            payload: payload as never,
            rule: { template, name: ruleName } as never,
          })
        ),
      });
    }
  }
}
save('template.json', dedupePayloads(templateCases), 0);

const PROFILE_ROWS: Record<string, unknown>[] = [
  {
    id: 'u',
    first_name: 'A',
    last_name: 'B',
    email: 'e',
    avatar: 'a',
    properties: {
      plan: 'pro',
      'a.b.c': '1',
      'a.b': 'x',
      'a.b.d': '2',
      '5': 'five',
      '': 'empty',
      'x.': 't',
    },
    project_id: 'p',
    is_external: false,
    created_at: '2026-01-02 03:04:05.678',
    last_seen_at: '2026-02-03 04:05:06.789',
    groups: ['g'],
  },
  {
    id: 'u',
    first_name: null,
    last_name: '',
    email: '',
    avatar: '',
    properties: {},
    project_id: 'p',
    is_external: true,
    created_at: '2026-01-02 03:04:05',
    last_seen_at: '',
    groups: null,
  },
  {
    id: 'u',
    properties: {},
    project_id: 'p',
    created_at: '2026-01-02 03:04:05',
  },
  {
    id: 'u',
    properties: {},
    project_id: 'p',
    created_at: 'garbage',
    last_seen_at: 'garbage',
  },
  { id: 'u', project_id: 'p', created_at: '2026-01-02 03:04:05' },
  {
    id: 'u',
    properties: null,
    project_id: 'p',
    created_at: '2026-01-02 03:04:05',
  },
  { id: 'u', properties: {}, project_id: 'p' },
  {
    id: 'u',
    properties: { a: 'x', 'a.b': 'y' },
    project_id: 'p',
    created_at: '2026-01-02T03:04:05.000Z',
  },
];
save(
  'profile.json',
  PROFILE_ROWS.map((row) => ({
    row,
    result: run(() => plain(transformProfile(row as never))),
  }))
);

// The real checkNotificationRulesForEvent over stubbed effects, its caches
// seeded in Redis db 15.
const { getRedisCache } = await import('../../../packages/redis/redis');
const { checkNotificationRulesForEvent } = await import(
  '../../../packages/core/src/modules/notification/src/notification-dispatch'
);
const { notificationQueueJobs } = await import(
  '../../../packages/core/src/modules/notification/notification.jobs'
);
const { getProjectByIdCached } = await import(
  '../../../packages/core/src/modules/project/project.service'
);
const redis = getRedisCache();

const rid = (n: number) => `00000000-0000-0000-0000-0000000000a${n}`;
const eventRule = (over: Record<string, unknown> = {}) => ({
  id: rid(1),
  name: 'Purchases',
  sendToApp: false,
  sendToEmail: false,
  config: {
    type: 'events',
    events: [{ id: 'e1', name: 'purchase', filters: [] }],
  },
  template: null,
  integrations: [],
  ...over,
});
const matching = (filters: unknown[] = [], name = 'purchase') => ({
  type: 'events',
  events: [{ name, filters }],
});
const SCENARIOS: Record<string, unknown>[] = [
  { name: 'no rules', rules: [] },
  {
    name: 'app and email, no template',
    rules: [eventRule({ sendToApp: true, sendToEmail: true })],
  },
  {
    name: 'integration, email, app in one rule',
    rules: [
      eventRule({
        sendToApp: true,
        sendToEmail: true,
        integrations: [{ id: rid(5) }, { id: rid(6) }],
        template: '$RULE_NAME: {{properties.value}} by {{profileId}}',
      }),
    ],
  },
  {
    name: 'two rules interleave app and integration deliveries',
    rules: [
      eventRule({
        id: rid(1),
        sendToApp: true,
        integrations: [{ id: rid(5) }],
      }),
      eventRule({ id: rid(2), sendToApp: true, sendToEmail: true }),
      eventRule({ id: rid(3), integrations: [{ id: rid(6) }] }),
    ],
  },
  {
    name: 'rule that does not match',
    rules: [eventRule({ config: matching([], 'other'), sendToApp: true })],
  },
  {
    name: 'funnel rules are skipped',
    rules: [
      eventRule({
        config: { type: 'funnel', events: [{ name: 'purchase', filters: [] }] },
        sendToApp: true,
      }),
    ],
  },
  {
    name: 'first matching definition wins, filters on properties',
    rules: [
      eventRule({
        sendToEmail: true,
        config: {
          type: 'events',
          events: [
            {
              name: 'purchase',
              filters: [
                { name: 'properties.value', operator: 'is', value: ['1'] },
              ],
            },
            {
              name: 'purchase',
              filters: [
                { name: 'properties.value', operator: 'gte', value: ['50'] },
              ],
            },
          ],
        },
      }),
    ],
  },
  {
    name: 'a throwing rule stops the rules after it, earlier deliveries run',
    rules: [
      eventRule({ id: rid(1), sendToApp: true }),
      eventRule({
        id: rid(2),
        sendToApp: true,
        config: matching([{ name: 'duration', operator: 'is', value: ['0'] }]),
      }),
      eventRule({ id: rid(3), sendToApp: true }),
    ],
  },
  {
    name: 'profile template loads the profile from the cache',
    rules: [
      eventRule({
        sendToApp: true,
        template:
          'Hello {{profile.firstName}} {{profile.createdAt}} {{profile.properties.address.city}}',
      }),
    ],
    profile: 'cache',
  },
  {
    name: 'profile template loads the profile from ClickHouse',
    rules: [
      eventRule({
        sendToApp: true,
        template: '{{profile.email}} {{profile.groups}}',
      }),
    ],
    profile: 'clickhouse',
  },
  {
    name: 'profile template, profile unknown',
    rules: [eventRule({ sendToApp: true, template: '{{profile.email}}' })],
    profile: 'none',
  },
  {
    name: 'profile lookup also triggered by a funnel rule template',
    rules: [
      eventRule({ id: rid(1), sendToApp: true }),
      eventRule({
        id: rid(2),
        config: { type: 'funnel', events: [] },
        template: '{{profile.email}}',
      }),
    ],
    profile: 'cache',
  },
  {
    name: 'NUL characters are stripped from the stored payload',
    rules: [eventRule({ sendToApp: true, sendToEmail: true })],
    properties: { note: 'a\x00b', nested: { k: ['x\x00', 1] } },
  },
  {
    name: 'no project name gives an empty message',
    rules: [eventRule({ sendToApp: true })],
    project: null,
  },
  {
    name: 'dollar patterns in the title',
    rules: [
      eventRule({
        sendToApp: true,
        name: 'R$&',
        template: '$RULE_NAME $EVENT_NAME {{properties.dollar}}',
      }),
    ],
    properties: { dollar: '$& $$ $`' },
  },
];

const profileRow = {
  id: 'user-1',
  first_name: 'Ada',
  last_name: 'L',
  email: 'ada@example.com',
  avatar: '',
  properties: { plan: 'pro', 'address.city': 'London' },
  project_id: 'wparity-proj',
  is_external: true,
  created_at: '2026-01-02 03:04:05.678',
  last_seen_at: '2026-01-03 04:05:06.789',
  groups: ['g1'],
};
const keyScope = { db: {} } as never;
const projectKey = getProjectByIdCached.getKey(keyScope, 'wparity-proj');
const rulesKey = service.getNotificationRulesByProjectId.getKey(
  keyScope,
  'wparity-proj'
);
const profileKey = 'profile-cache:wparity-proj:user-1';

const results: unknown[] = [];
for (const scenario of SCENARIOS) {
  const properties = (scenario.properties as Record<string, unknown>) ?? {
    value: '99',
  };
  const payload: Record<string, unknown> = basePayload(properties);
  const project =
    scenario.project === null
      ? null
      : { id: 'wparity-proj', name: 'WParity Project' };
  if (project) {
    await redis.set(
      projectKey,
      JSON.stringify({ ...project, filters: [] }),
      'EX',
      60
    );
  } else {
    await redis.del(projectKey);
  }
  await redis.set(rulesKey, JSON.stringify(scenario.rules), 'EX', 60);
  await redis.del(profileKey);

  const effects: unknown[] = [];
  const db = {
    project: { findUnique: async () => null },
    notification: {
      create: async ({ data }: { data: unknown }) => {
        await new Promise((resolve) => setTimeout(resolve, 5));
        effects.push({ op: 'insert', data: plain(data) });
      },
    },
  };
  const deps = {
    db,
    logger: { info: noop, warn: noop, error: noop, debug: noop },
    buffers: {
      profile: {
        fetchFromCache: async () =>
          scenario.profile === 'cache' ? profileRow : null,
      },
    },
    ch: {
      query: async () => ({
        json: async () => ({
          data: scenario.profile === 'clickhouse' ? [profileRow] : [],
          meta: [],
          rows: 0,
        }),
      }),
    },
    queues: {
      notification: {
        sendNotification: {
          add: async (data: unknown) => {
            effects.push({
              op: 'add',
              data: plain(
                notificationQueueJobs.sendNotification.payload.parse(data)
              ),
            });
          },
        },
      },
    },
  };
  let error: string | null = null;
  try {
    await checkNotificationRulesForEvent(deps as never, payload as never);
  } catch (e) {
    error = e instanceof TypeError ? 'TypeError' : String(e);
  }
  results.push({
    scenario: scenario.name,
    rules: scenario.rules,
    project,
    profile: scenario.profile ?? null,
    profileRow,
    payload: plain(basePayload(properties)),
    effects,
    error,
    finalPayload: plain(payload),
  });
}
await redis.del(projectKey, rulesKey, profileKey);
save('dispatch.json', {
  keys: { projectKey, rulesKey, profileKey },
  scenarios: results,
});
const idsWithSpace = [
  'a',
  'a b',
  'a\tb',
  'a\u{a0}b',
  'a\u{feff}b',
  'a\u{85}b',
  'a\u{2028}b',
  'a\u{180e}b',
  'ü',
  '',
  'a:b',
  '[x]',
];
save(
  'keys.json',
  idsWithSpace.map((id) => ({
    id,
    project: getProjectByIdCached.getKey(keyScope, id),
    rules: service.getNotificationRulesByProjectId.getKey(keyScope, id),
  }))
);
redis.disconnect();
console.log('golden written to', outDir ?? TESTDATA);
process.exit(0);
