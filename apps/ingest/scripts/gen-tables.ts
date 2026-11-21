// Exports the tables the Go receiver matches against, straight from the
// packages the Bun API runs, so the two cannot drift: ua-parser-js's regex
// map, the bot list and the datacenter ASNs. `bun run gen` rewrites them;
// `bun run gen --check` fails when a committed copy is stale.
import { createHash } from 'node:crypto';
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import datacenterAsns from '../../../packages/core/src/clients/datacenter-asns';
import bots from '../../../packages/core/src/modules/ingest/src/bots/bots';

const ROOT = join(import.meta.dir, '..');
const check = process.argv.includes('--check');

// Resolved from @openpanel/shared, the package whose parser the API runs.
const sharedRequire = createRequire(
  join(import.meta.dir, '../../../packages/shared/package.json')
);
const uaParserPath = sharedRequire.resolve('ua-parser-js');
const uaPackage = sharedRequire('ua-parser-js/package.json') as {
  version: string;
};

const EXPORT_ANCHOR = 'UAParser.VERSION = LIBVERSION;';

interface UaInternals {
  defaultRegexes: Record<string, unknown[]>;
  helpers: Record<string, unknown>;
}

function loadUaInternals(): UaInternals {
  const source = readFileSync(uaParserPath, 'utf8');
  if (!source.includes(EXPORT_ANCHOR)) {
    throw new Error(`ua-parser-js changed shape: "${EXPORT_ANCHOR}" not found`);
  }
  const patched = source.replace(
    EXPORT_ANCHOR,
    `${EXPORT_ANCHOR} UAParser.__internals = { defaultRegexes: defaultRegexes, helpers: { strMapper: strMapper, strTest: strTest, lowerize: lowerize, trim: trim } };`
  );
  const module = { exports: {} as Record<string, unknown> };
  new Function('module', 'exports', 'window', patched).call(
    {},
    module,
    module.exports,
    undefined
  );
  const parser = module.exports as { __internals?: UaInternals };
  if (!parser.__internals) {
    throw new Error('ua-parser-js internals were not exported');
  }
  return parser.__internals;
}

function encodeValue(
  value: unknown,
  helpers: Record<string, unknown>
): unknown {
  if (value === undefined) {
    return { undef: true };
  }
  if (value instanceof RegExp) {
    return { re: value.source, flags: value.flags };
  }
  if (typeof value === 'function') {
    const name = Object.entries(helpers).find(([, fn]) => fn === value)?.[0];
    if (!name) {
      throw new Error(
        `unknown ua-parser-js helper: ${value.toString().slice(0, 80)}`
      );
    }
    return { fn: name };
  }
  if (Array.isArray(value)) {
    return { list: value.map((item) => encodeValue(item, helpers)) };
  }
  if (typeof value === 'object' && value !== null) {
    // Object.keys order is the order a for..in walks, which strMapper relies on.
    return {
      map: Object.keys(value).map((key) => [
        key,
        encodeValue((value as Record<string, unknown>)[key], helpers),
      ]),
    };
  }
  if (typeof value === 'string') {
    return { str: value };
  }
  throw new Error(`unexpected ua-parser-js value: ${String(value)}`);
}

const UA_ITEMS = ['browser', 'os', 'device'] as const;

function uaTables() {
  const { defaultRegexes, helpers } = loadUaInternals();
  const items: Record<string, unknown> = {};
  for (const item of UA_ITEMS) {
    const pairs = defaultRegexes[item] as unknown[][];
    const groups: unknown[] = [];
    for (let index = 0; index < pairs.length; index += 2) {
      const regexes = pairs[index] as RegExp[];
      const props = pairs[index + 1] as unknown[];
      groups.push({
        regexes: regexes.map((regex) => ({
          re: regex.source,
          flags: regex.flags,
        })),
        props: props.map((prop) => encodeValue(prop, helpers)),
      });
    }
    items[item] = groups;
  }
  return { version: uaPackage.version, items };
}

function botTables() {
  return bots.map((bot) => {
    const category = 'category' in bot ? (bot.category as string) : 'Unknown';
    if ('regex' in bot) {
      return { regex: bot.regex as string, name: bot.name, category };
    }
    return { includes: bot.includes as string, name: bot.name, category };
  });
}

// The Lua the Go BullMQ producer and worker send, exactly as the bullmq package the
// worker runs ships it (includes already inlined), with the SHA ioredis would
// compute so a test can pin what EVALSHA addresses.
const BULLMQ_SCRIPTS = [
  'addStandardJob',
  'moveToActive',
  'moveToFinished',
  'moveToDelayed',
  'retryJob',
  'extendLocks',
  'moveStalledJobsToWait',
] as const;
const BULLMQ_DIR = 'internal/worker/bullmq/scripts';

function bullmqScripts(): [string, string][] {
  const coreRequire = createRequire(
    join(import.meta.dir, '../../../packages/core/package.json')
  );
  const root = join(dirname(coreRequire.resolve('bullmq')), '../..');
  const { version } = JSON.parse(
    readFileSync(join(root, 'package.json'), 'utf8')
  ) as { version: string };
  const manifest: Record<string, unknown> = {};
  const files: [string, string][] = [];
  for (const name of BULLMQ_SCRIPTS) {
    const dir = join(root, 'dist/cjs/scripts');
    const file = readdirSync(dir).find((entry) =>
      new RegExp(`^${name}-\\d+\\.js$`).test(entry)
    );
    if (!file) {
      throw new Error(`bullmq ${version} ships no script ${name}`);
    }
    const script = coreRequire(join(dir, file))[name] as {
      content: string;
      keys: number;
    };
    manifest[name] = {
      file: `${name}.lua`,
      keys: script.keys,
      sha1: createHash('sha1').update(script.content).digest('hex'),
    };
    files.push([`${BULLMQ_DIR}/${name}.lua`, script.content]);
  }
  return [
    ...files,
    [
      `${BULLMQ_DIR}/manifest.json`,
      `${JSON.stringify({ bullmq: version, scripts: manifest }, null, 1)}\n`,
    ],
  ];
}

const json = (data: unknown): string => `${JSON.stringify(data, null, 1)}\n`;
const outputs: [string, string][] = [
  ['internal/useragent/data/regexes.json', json(uaTables())],
  ['internal/bots/data/bots.json', json(botTables())],
  ['internal/geo/data/datacenter-asns.json', json([...datacenterAsns])],
  ...bullmqScripts(),
];

let stale = 0;
for (const [relative, next] of outputs) {
  const path = join(ROOT, relative);
  let current = '';
  try {
    current = readFileSync(path, 'utf8');
  } catch {
    current = '';
  }
  if (current === next) {
    continue;
  }
  if (check) {
    console.error(`stale: ${relative} (run bun run gen in apps/ingest)`);
    stale += 1;
    continue;
  }
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, next);
  console.log(`wrote ${relative}`);
}
if (stale > 0) {
  process.exit(1);
}
