// parseUserAgent and detectBot over a UA corpus, for the Go port's tests.
//   bun parity/golden-ua.ts <corpus.json> <outDir> [sampleEvery]
// writes ua-full.json (point INGEST_GOLDEN_UA at it) and ua-sample.json, the
// slice committed as internal/useragent/testdata/ua-sample.json. The corpus
// used for the committed sample is the ua-parser-js 2.0.10 test data plus
// matomo device-detector's fixtures, one JSON array of strings.
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { detectBot } from '../../../packages/core/src/modules/ingest/src/bots/detect';
import { parseUserAgent } from '../../../packages/shared/src/server/parser-user-agent';

const corpusPath = process.argv[2];
const outDir = process.argv[3];
const sampleEvery = Number(process.argv[4] ?? 15);
const uas: string[] = JSON.parse(readFileSync(corpusPath, 'utf8'));
const extra = [
  '',
  ' ',
  'Go-http-client/1.1',
  'Go Http Client/1.0',
  'node',
  'curl/8.4.0',
  'python-requests/2.31.0',
  'OpenPanel/1.0 (iOS 17.2; iPhone15,3)',
  'MyApp/2.3 (Android 14; Model=Redmi Note 8 Pro; Manufacturer=Xiaomi)',
  'Mozilla/5.0 (iPad; iPadOS 18_0; like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/18.0',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Mobile/15E148 Safari/604.1',
  '   leading spaces Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
  `${'x'.repeat(600)} Chrome/120.0.0.0`,
  'Ünïcödé Browser/1.0',
  'Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36',
];
const overrideSets: (Record<string, unknown> | undefined)[] = [
  undefined,
  {
    __os: 'CustomOS',
    __osVersion: '9.9',
    __browser: 'B',
    __browserVersion: '1',
    __device: 'tablet',
    __brand: 'Br',
    __model: 'M',
  },
  { __model: '', __os: 5 },
  { __device: 'mobile' },
];
const all = [...extra, ...uas];
const ua = all.map((u, index) => ({
  ua: u,
  overrides: overrideSets[index % overrideSets.length] ?? null,
  out: parseUserAgent(u, overrideSets[index % overrideSets.length]),
  bot: detectBot(u),
}));
mkdirSync(outDir, { recursive: true });
writeFileSync(join(outDir, 'ua-full.json'), JSON.stringify(ua));
writeFileSync(
  join(outDir, 'ua-sample.json'),
  JSON.stringify(
    ua.filter((_, index) => index < extra.length || index % sampleEvery === 0)
  )
);
console.log('ua cases', ua.length, 'bots', ua.filter((u) => u.bot).length);
