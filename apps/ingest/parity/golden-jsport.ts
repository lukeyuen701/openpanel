// Goldens for internal/worker/jsport, produced by the real TypeScript the Bun
// worker runs (packages/shared object.ts / url.ts, core parse-referrer.ts,
// ch-dates.ts, luxon, and the JavaScript built-ins themselves).
//   bun parity/golden-jsport.ts            writes internal/worker/jsport/testdata/*.json.gz
//   bun parity/golden-jsport.ts <dir>      writes to <dir> instead
// Every file is pure ASCII JSON (non-ASCII and lone surrogates as backslash-u
// escapes) so a Go test can read it with the package's own surrogate-preserving
// parser.
process.env.TZ = 'UTC';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { formatClickhouseDate } from '../../../packages/core/src/shared/ch-dates';
import {
  getReferrerWithQuery,
  parseReferrer,
} from '../../../packages/core/src/shared/parse-referrer';
import referrers from '../../../packages/core/src/shared/referrers';
import { DateTime } from '../../../packages/shared/src/date';
import { toDots } from '../../../packages/shared/src/object';
import {
  stripLeadingAndTrailingSlashes,
  stripTrailingSlash,
} from '../../../packages/shared/src/string';
import { isSameDomain, parsePath } from '../../../packages/shared/src/url';

const outDir =
  process.argv[2] ?? join(import.meta.dir, '../internal/worker/jsport/testdata');
mkdirSync(outDir, { recursive: true });

const ascii = (value: unknown): string =>
  (JSON.stringify(value) ?? 'null').replace(
    /[\u007f-\uffff]/g,
    (c) => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0')
  );
const save = (name: string, data: unknown) => {
  // gzip writes no mtime or file name, so the same data gives the same bytes.
  writeFileSync(join(outDir, `${name}.gz`), gzipSync(ascii(data) + '\n', { level: 9 }));
  console.log(name, (data as { cases?: unknown[] }).cases?.length ?? '');
};

// mulberry32: the multiplicative LCG overflows doubles and falls into a short cycle.
let seed = Number(process.env.GOLDEN_SEED ?? 20240607);
const rand = () => {
  seed = (seed + 0x6d2b79f5) | 0;
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};
const int = (n: number) => Math.floor(rand() * n);
const pick = <T>(xs: readonly T[]): T => xs[int(xs.length)]!;
const R = String.raw;
const attempt = <T>(fn: () => T): { ok: true; v: T } | { ok: false } => {
  try {
    return { ok: true, v: fn() };
  } catch {
    return { ok: false };
  }
};

const LONE_HIGH = String.fromCharCode(0xd800);
const LONE_HIGH2 = String.fromCharCode(0xdbff);
const LONE_LOW = String.fromCharCode(0xdc00);
const LONE_LOW2 = String.fromCharCode(0xdfff);
const STRS = [
  '', ' ', 'x', 'hello world', '  padded  ', '\t\n\r\x0b\x0c', '\xa0nbsp\xa0', '\u1680a\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a',
  '\u2028\u2029line', '\u202f\u205f\u3000x', '\ufeffbom\ufeff', '\x85nel\x85', '\u180eMongolian\u180e', '\u200bzwsp\u200b',
  'quote"inside', 'back\\slash', 'tab\there', 'nul\x00byte', 'ctrl\x1f\x01', 'slash/ <tag> & ', 'caf\xe9', 'ключ', '日本語', '😀', 'a😀b',
  '\u0130', 'I\u0307', '\u03a3', 'A\u03a3', 'A\u03a3B', 'a\u03a3.', "a'\u03a3", 'ΑΣ ΑΣΑ', '\u0131', 'ǅ', 'ß', 'ẞ', 'ﬃ', 'ΐ',
  LONE_HIGH, LONE_LOW, 'a' + LONE_HIGH, LONE_LOW + 'a', LONE_HIGH + LONE_HIGH, LONE_LOW + LONE_HIGH, 'x' + LONE_HIGH2 + 'y' + LONE_LOW2 + 'z', '😀' + LONE_LOW, LONE_HIGH + '😀',
  '{"a":1}', '[1,2,3]', '{"a":{"b":[1,{"c":"d"}]}}', '{bad', '[1,2', '{"a":1', '[]', '{}', '{"a":}', '[1,]', '{"x":"\\ud800"}', '{"x":"\ud83d\\ude00"}',
  '{"type":"Buffer","data":[104,105]}', '  {"a":1}', '{"a":1}  ', '{ "a" : 1 }', '[null]', '["", "x"]',
  '0', '1', '-0', '1e21', '0.1', 'true', 'null', 'NaN', 'Infinity',
  'https://example.com/a?b=c', '/path', 'a'.repeat(300),
];
const KEYS = ['a', 'b', 'z', '0', '1', '2', '10', '01', '-1', '4294967294', '4294967295', '__proto__', 'constructor', 'ключ', '😀', 'k k', '$', 'A', 'a.b', '', ' ', 'x.y.z', '1.5', LONE_HIGH, 'toString'];
const NUMS = [0, 1, -1, 5, 42, 0.5, -0.5, 0.1, 0.30000000000000004, 1e21, 1e-7, 123456789012345680000, 2 ** 53, -(2 ** 53), 1.7976931348623157e308, 5e-324, 255, 256, 65, 1e300, 100, 12.5, 1234.5678];

const BUFFER_DATA: unknown[][] = [
  [104, 105], [], [0xe2, 0x82], [0xe2, 0x82, 0x41], [0xf0, 0x9f, 0x98], [0xef, 0xbb, 0xbf, 0x41], [0xff, 0xfe], [0xc0, 0x80], [0xed, 0xa0, 0x80],
  [65, 256, 257, -1, 65.9, '65', null, true, false, [5], [], {}, 'x', '', ' 66 ', '0x41', 1e21, -1e21, 4294967361],
  [123, 34, 97, 34, 58, 49, 125], [91, 49, 44, 50, 93], [0xf4, 0x90, 0x80, 0x80], [0xf0, 0x80, 0x80, 0x80], [0xe0, 0x80, 0x80], [0x80], [0xbf, 0xc2],
  [0xf0, 0x9f, 0x98, 0x80, 0x41], [0xe2, 0x82, 0xac, 0xe2, 0x82], [0x41, 0x00, 0x42],
];

function genValue(depth: number): unknown {
  const r = rand();
  if (depth > 4 || r < 0.45) {
    const k = int(10);
    if (k < 2) return null;
    if (k < 3) return rand() < 0.5;
    if (k < 5) return pick(NUMS);
    return pick(STRS);
  }
  if (r < 0.62) {
    const n = rand() < 0.04 ? 55 + int(10) : int(6);
    return Array.from({ length: n }, () => genValue(depth + 1));
  }
  if (r < 0.9) {
    const o: Record<string, unknown> = {};
    const n = rand() < 0.04 ? 55 + int(10) : int(6);
    for (let i = 0; i < n; i += 1) o[rand() < 0.2 ? 'key' + i : pick(KEYS)] = genValue(depth + 1);
    return o;
  }
  if (r < 0.95) {
    return { type: 'Buffer', data: pick(BUFFER_DATA) };
  }
  if (r < 0.97) {
    return { type: 'Buffer', data: pick([null, 'abc', 5, {}, { length: 2 }]) };
  }
  return JSON.stringify(genValue(depth + 1));
}

// ---------------------------------------------------------------- toDots
{
  const cases: unknown[] = [];
  const add = (text: string) => {
    const result = attempt(() => JSON.stringify(toDots(JSON.parse(text))));
    cases.push({ in: text, out: result.ok ? result.v : null, ok: result.ok });
  };
  const J = (v: unknown) => JSON.stringify(v);
  const handwritten = [
    '{}', '{"a":1}', '{"a":"x","b":" y ","c":"","d":null,"e":true,"f":false,"g":1.5,"h":[],"i":{}}',
    '{"__proto__":"x","a":1}', '{"__proto__":{"b":"c"}}', '{"a":{"__proto__":"x"}}', '{"constructor":"x","toString":"y"}',
    '{"a":1,"a":2,"b":3}', '{"b":1,"1":2,"a":3,"0":4,"4294967295":5,"4294967294":6}', '{"a":{"1":"x","0":"y","b":"z"}}',
    '{"a":[1,[2,[3,[4]]]]}', J({ a: '{"b":{"c":"d"}}' }), J({ a: '[1,2,"x"]' }), '{"a":"{bad}"}', '{"a":"{bad"}', '{"a":"[1"}', J({ a: '{"x":' }), '{"a":"{}"}', '{"a":"[]"}',
    J({ a: JSON.stringify({ b: JSON.stringify({ c: 1 }) }) }), J({ a: '  {"b":1}' }), J({ a: '{"b":1}  ' }), '{"a":"\\ud800"}', '{"a":"x\\ud800y\\udc00z"}', '{"a":"\\ud83d\\ude00"}', '{"\\ud800":"v"}', '{"a":"\\ud800","\\udc00":1}',
    '{"a":"\\ufeff  x \\u0085 \\u00a0\\u2028"}', '{"a":" \\t\\n\\u000b\\u000c\\ufeff"}', '{"a":"\\u0000nul"}', '{"a":"\\u180e"}',
    '{"a":1e21,"b":-0,"c":1e-7,"d":123456789012345680000,"e":0.1,"f":1e400,"g":-1e400,"h":5e-324,"i":1.7976931348623157e308}',
    '{"b":{"type":"Buffer","data":[104,105]}}', '{"b":{"type":"Buffer","data":[]}}', '{"b":{"type":"Buffer","data":[226,130]}}', '{"b":{"type":"Buffer","data":[239,187,191,65]}}',
    '{"b":{"type":"Buffer","data":[65,256,257,-1,65.9,"65",null,true,false,[5],[],{},"x",""," 66 ","0x41",1e21,4294967361]}}',
    '{"b":{"type":"Buffer","data":[123,34,97,34,58,49,125]}}', '{"b":{"type":"Buffer","data":[91,49,44,50,93]}}', '{"b":{"type":"Buffer","data":[123,34,97]}}',
    '{"b":{"type":"Buffer","data":"abc"}}', '{"b":{"type":"Buffer"}}', '{"b":{"data":[1],"type":"buffer"}}', '{"b":{"type":"Buffer","data":[1],"extra":"x"}}', '{"b":{"type":"Buffer","data":{"0":65,"length":1}}}',
    '{"b":[{"type":"Buffer","data":[65]}]}', '{"type":"Buffer","data":[65]}', '{"a":{"type":"Buffer","data":[34,120,34]}}',
    '{"a":{"type":"Buffer","data":[91,34,120,34,93]}}',
    '{"b":{"type":"Buffer","data":[240,159,152,128]}}', '{"b":{"type":"Buffer","data":[237,160,128]}}', '{"b":{"type":"Buffer","data":[244,144,128,128]}}',
    '{"x":{"y":{"z":{"w":[1,2,{"q":"r"}]}}}}',
  ];
  for (const text of handwritten) add(text);
  const bigObject = (n: number) => '{' + Array.from({ length: n }, (_, i) => '"k' + i + '":' + (i % 3 === 0 ? '"v' + i + '"' : i)).join(',') + '}';
  add(bigObject(1000)); add(bigObject(1001)); add(bigObject(1500)); add(bigObject(999));
  add('{"o":' + bigObject(49) + '}'); add('{"o":' + bigObject(50) + '}'); add('{"o":' + bigObject(51) + '}'); add('{"o":' + bigObject(200) + ',"p":' + bigObject(60) + ',"q":1}');
  add('{"a":[' + Array.from({ length: 70 }, (_, i) => i).join(',') + ']}'); add('{"a":[' + Array.from({ length: 50 }, (_, i) => i).join(',') + ']}'); add('{"a":[' + Array.from({ length: 51 }, (_, i) => i).join(',') + ']}');
  add('{"a":[' + Array.from({ length: 1200 }, (_, i) => i).join(',') + ']}');
  add('{"a":' + '['.repeat(40) + '1' + ']'.repeat(40) + '}');
  add('{"o":{"e1":"","e2":null,"e3":"x"' + Array.from({ length: 60 }, (_, i) => ',"f' + i + '":' + (i < 49 ? '""' : '"v"')).join('') + '}}');
  add('{"o":{' + Array.from({ length: 60 }, (_, i) => '"e' + i + '":null').join(',') + ',"late":"x"}}');
  add('{"big":' + JSON.stringify({ type: 'Buffer', data: Array.from({ length: 5000 }, (_, i) => 97 + (i % 26)) }) + '}');
  add('{"nested":' + JSON.stringify(JSON.stringify({ a: [1, { b: 'c' }] })) + '}');
  add('{"r":' + Array.from({ length: 20 }, () => '{"t":').join('') + '1' + '}'.repeat(20) + '}');
  add('{"\\u00e9":1,"é":2,"😀":3}');
  for (let i = 0; i < 450; i += 1) {
    const root: Record<string, unknown> = {};
    const n = int(8);
    for (let j = 0; j < n; j += 1) root[rand() < 0.25 ? 'p' + j : pick(KEYS)] = genValue(0);
    add(JSON.stringify(root));
  }
  save('todots.json', { cases });
}

// ---------------------------------------------------------------- json
{
  const cases: unknown[] = [];
  const add = (text: string) => {
    const result = attempt(() => JSON.stringify(JSON.parse(text)));
    cases.push({ in: text, out: result.ok ? result.v : null, ok: result.ok });
  };
  const texts = [
    'null', 'true', 'false', '0', '-0', '1.5', '1e21', '1E-7', '1e400', '-1e400', '123456789012345678901234567890', '""', '"x"', '[]', '{}', ' [ 1 , 2 ] ', '\t\n\r {"a" : 1 }\n',
    R`"\ud800"`, R`"\udc00"`, R`"\ud800\udc00"`, R`"\ud800x"`, R`"\udc00\ud800"`, R`"a\ud83d"`, R`"\ud83d\ude00"`, R`"\uD83D\uDE00"`, '"\\ud83d\ude00"', '"\ud83d\\ude00"',
    '{"a":1,"a":2}', '{"b":1,"a":2,"b":3}', '{"__proto__":1}', '{"__proto__":{"x":1}}', '{"2":"a","1":"b","x":"c","0":"d","-1":"e","01":"f","4294967295":"g","4294967294":"h"}',
    R`"\u0000\u001f\u007f\u2028\u2029\b\f\n\r\t\"\\\/"`, '"/<>&\'"', '"\u2028\u2029"', '"é😀日本"', R`"\u00e9"`,
    '{"a":[1,2,{"b":null}],"c":{"d":[]}}', '[1,[2,[3,[4,[5]]]]]', '[true,false,null]',
    '', ' ', '{', '[', '{"a"', '{"a":', '{"a":1', '{"a":1,}', '[1,]', '[,1]', '{,}', "{'a':1}", '{a:1}', '01', '+1', '.5', '1.', '1e', '--1', 'NaN', 'Infinity', 'undefined', 'nul', 'tru', 'truee', '"unterminated', R`"bad\x"`, R`"bad\u12"`, R`"\u12g4"`, '"tab\there"', '"nl\nx"', '[1] x', '[1][2]', '\ufeff[1]', R`"\a"`, '1 2',
    '{"a":1}{"b":2}', R`"\u0041"`, '-', '-a', '[-]', '0x10', '1_000', R`"\U0041"`,
    '[' + '1,'.repeat(1000) + '1]', '['.repeat(2000) + ']'.repeat(2000), '['.repeat(2000) + ']'.repeat(1999),
    '{"a":' + '{"a":'.repeat(500) + '1' + '}'.repeat(501),
  ];
  for (const text of texts) add(text);
  for (let i = 0; i < 250; i += 1) {
    const value = genValue(0);
    add(JSON.stringify(value));
    if (i % 5 === 0) add(JSON.stringify(value, null, 2));
  }
  save('json.json', { cases });
}

// ---------------------------------------------------------------- strings
{
  const cases: Record<string, unknown[]> = { trim: [], lower: [], len16: [], wellFormed: [], removeLone: [], replaceAll: [], stripTrailing: [], stripBoth: [], decode: [] };
  const pool: string[] = [...STRS];
  for (let i = 0; i < 800; i += 1) {
    let s = '';
    const n = int(8);
    for (let j = 0; j < n; j += 1) s += pick(STRS).slice(0, 1 + int(6));
    pool.push(s);
  }
  for (let i = 0; i < 300; i += 1) {
    let s = '';
    const n = 1 + int(10);
    for (let j = 0; j < n; j += 1) {
      const k = int(6);
      s += k === 0 ? String.fromCharCode(0xd800 + int(0x800)) : k === 1 ? String.fromCodePoint(0x10000 + int(0xfffff)) : k === 2 ? String.fromCharCode(int(0x3000)) : k === 3 ? ' ' : k === 4 ? '/' : String.fromCharCode(0x3a3, 0x41 + int(26));
    }
    pool.push(s);
  }
  for (const s of pool) {
    cases.trim!.push({ in: s, out: s.trim() });
    cases.lower!.push({ in: s, out: s.toLowerCase() });
    cases.len16!.push({ in: s, out: s.length });
    cases.wellFormed!.push({ in: s, out: s.isWellFormed() });
    cases.removeLone!.push({ in: s, out: s.isWellFormed() ? s : s.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '') });
    cases.stripTrailing!.push({ in: s, out: stripTrailingSlash(s) });
    cases.stripBoth!.push({ in: s, out: stripLeadingAndTrailingSlashes(s) });
  }
  for (const s of ['a/', 'a//', '//a//', '/', '//', 'a/b/', '/a/', 'x/\u2028/', '/😀/', '/' + LONE_HIGH + '/']) {
    cases.stripTrailing!.push({ in: s, out: stripTrailingSlash(s) });
    cases.stripBoth!.push({ in: s, out: stripLeadingAndTrailingSlashes(s) });
  }
  const trimmed: number[] = [];
  const lowered: [number, string][] = [];
  for (let cp = 0; cp <= 0x10ffff; cp += 1) {
    const s = String.fromCodePoint(cp);
    if (s.trim() === '') trimmed.push(cp);
    const l = s.toLowerCase();
    if (l !== s) lowered.push([cp, l]);
  }
  const sigma = '\u03a3';
  const sigmaContexts: Record<string, number[]> = { ignorableBefore: [], followedBy: [], precededBy: [], ignorableAfter: [] };
  for (let cp = 0; cp <= 0x10ffff; cp += 1) {
    if (cp >= 0xd800 && cp <= 0xdfff) continue;
    const c = String.fromCodePoint(cp);
    if (('A' + c + sigma).toLowerCase().endsWith('\u03c2')) sigmaContexts.ignorableBefore!.push(cp);
    if (('A' + sigma + c).toLowerCase().includes('\u03c2')) sigmaContexts.followedBy!.push(cp);
    if ((c + sigma).toLowerCase().endsWith('\u03c2')) sigmaContexts.precededBy!.push(cp);
    if (('A' + sigma + c + 'b').toLowerCase().includes('\u03c2')) sigmaContexts.ignorableAfter!.push(cp);
  }
  const REPL_SEARCH = ['$EVENT_NAME', '{{x}}', 'a', 'ab', '$', '', '😀', '{{rule_name}}', 'aa'];
  const REPL_VALUE = ['', 'v', '$&', '$$', "$'", '$' + String.fromCharCode(96), '$1', '$<n>', '$<', '$0', '$10', '$$$&', 'a$', '$', '$$$', "x$'y$" + String.fromCharCode(96) + 'z$&', '😀', LONE_HIGH, LONE_LOW, '$&$&'];
  const REPL_SUBJECT = ['', 'a', 'aaa', 'abab', '$EVENT_NAME', 'x $EVENT_NAME y $EVENT_NAME', '{{x}} {{x}}', 'pre{{rule_name}}post', '😀a😀', 'a$b$', LONE_HIGH + 'a', 'ab' + LONE_LOW, 'aaaa', '{{x}}'];
  for (const subject of REPL_SUBJECT) for (const search of REPL_SEARCH) for (const rep of REPL_VALUE) {
    cases.replaceAll!.push({ s: subject, search, rep, out: subject.replaceAll(search, rep) });
  }
  const bytesCases: number[][] = [];
  for (const d of BUFFER_DATA) if (d.every((x) => typeof x === 'number' && x >= 0 && x < 256 && Number.isInteger(x))) bytesCases.push(d as number[]);
  const interesting = [0x00, 0x41, 0x7f, 0x80, 0xbf, 0xc0, 0xc1, 0xc2, 0xdf, 0xe0, 0xe1, 0xec, 0xed, 0xee, 0xef, 0xf0, 0xf1, 0xf3, 0xf4, 0xf5, 0xff, 0x9f, 0xa0, 0x8f, 0x90, 0x82, 0xac, 0xbb];
  for (let i = 0; i < 4000; i += 1) bytesCases.push(Array.from({ length: int(7) }, () => pick(interesting)));
  for (let i = 0; i < 300; i += 1) bytesCases.push(Array.from({ length: int(40) }, () => int(256)));
  for (const bytes of bytesCases) {
    cases.decode!.push({ hex: Buffer.from(bytes).toString('hex'), out: Buffer.from(bytes).toString(), noBom: new TextDecoder().decode(Uint8Array.from(bytes)) });
  }
  // Runs of consecutive code points as [first, last], so the file stays small.
  const asRanges = (list: number[]) => {
    const ranges: [number, number][] = [];
    for (const cp of list) {
      const last = ranges[ranges.length - 1];
      if (last && last[1] === cp - 1) last[1] = cp;
      else ranges.push([cp, cp]);
    }
    return ranges;
  };
  const sigmaRanges = Object.fromEntries(Object.entries(sigmaContexts).map(([name, list]) => [name, asRanges(list)]));
  save('strings.json', { cases, trimmedCodePoints: trimmed, loweredCodePoints: lowered, sigmaContexts: sigmaRanges });
}

// ---------------------------------------------------------------- numbers
{
  const parseFloatCases: unknown[] = [];
  const numberCases: unknown[] = [];
  const toStringCases: unknown[] = [];
  const NUM_TOKENS = ['0x1e1000123456789012345678901234567890', '0xffffffffffffffffffffffff', '0b' + '1'.repeat(70), '0o' + '7'.repeat(30), '0x' + 'f'.repeat(300), '0x' + '1' + '0'.repeat(260), '0X1F', '0b', '0x', '0o9', '0xg', '', ' ', '\t', '\n', '\xa0', '\ufeff', '\x85', '\u180e', '\u2028', '+', '-', '0', '1', '9', '00', '0x', '0X1f', '0b101', '0B2', '0o17', '0O8', '.', '.5', '5.', 'e', 'E', 'e5', 'e+', 'e-3', 'Infinity', 'infinity', 'Inf', 'inf', '-Infinity', '+Infinity', 'NaN', 'nan', '_', '1_0', 'px', 'abc', '1e1000', '1e-1000', '١٢٣', '１２３', '0.0000001', '123456789012345678901234567890', '9007199254740993', '1e21', '1e22', '0.1e1', '1.7976931348623157e308', '1.7976931348623159e308', '5e-324', '2e-324', '0.000001', '-0', '+0', '--1', '1 2', '1,5', '12abc', '0.5.5', '1e5e5', '1ee5', 'Infinit', 'Infinityx', '  12  ', '\n\t3.5\r\n'];
  const sNums: string[] = [...NUM_TOKENS];
  for (let i = 0; i < 4000; i += 1) {
    let s = '';
    const n = 1 + int(4);
    for (let j = 0; j < n; j += 1) s += pick(NUM_TOKENS);
    sNums.push(s);
  }
  for (const s of sNums) {
    parseFloatCases.push({ in: s, out: String(parseFloat(s)), neg0: Object.is(parseFloat(s), -0) });
    numberCases.push({ in: s, out: String(Number(s)), neg0: Object.is(Number(s), -0) });
  }
  const buf = new DataView(new ArrayBuffer(8));
  for (let i = 0; i < 3000; i += 1) {
    buf.setUint32(0, int(0x100000000) >>> 0);
    buf.setUint32(4, int(0x100000000) >>> 0);
    const x = buf.getFloat64(0);
    toStringCases.push({ bits: [buf.getUint32(0), buf.getUint32(4)], out: String(x) });
  }
  for (const x of [0, -0, 1, 1.5, 100, 1e20, 1e21, 1.5e21, 123e-7, 1e-6, 1e-7, 0.000001234, NaN, Infinity, -Infinity, 2 ** 31, 2 ** 53, 0.1 + 0.2, 1 / 3]) {
    buf.setFloat64(0, x);
    toStringCases.push({ bits: [buf.getUint32(0), buf.getUint32(4)], out: String(x) });
  }
  save('numbers.json', { parseFloat: parseFloatCases, number: numberCases, toString: toStringCases });
}

// ---------------------------------------------------------------- dates
{
  const cases: unknown[] = [];
  const add = (text: string) => {
    const value = JSON.parse(text);
    const d = new Date(value);
    const invalid = Number.isNaN(d.getTime());
    const iso = attempt(() => d.toISOString());
    const ch = attempt(() => formatClickhouseDate(value));
    cases.push({
      in: text,
      ms: invalid ? null : d.getTime(),
      iso: iso.ok ? iso.v : null,
      json: JSON.stringify(d),
      luxon: DateTime.fromJSDate(d).setZone('UTC').toFormat('yyyy-MM-dd HH:mm:ss.SSS'),
      ch: ch.ok ? ch.v : null,
    });
  };
  const numbers = ['0', '-0', '1', '-1', '1700000000000', '1700000000123', '1700000000123.9', '-1700000000123.9', '0.5', '-0.5', '0.9999', '1e3', '1e21', '8.64e15', '8640000000000001', '-8640000000000000', '-8640000000000001', '8640000000000000.5', '253402300799999', '253402300800000', '-62167219200000', '-62167219200001', '-62198755200000', '-62135596800000', '-62135596800001', '-1e15', '951782400000', '946684800000', '4102444800000', '1e-7', '5e-324', '1.7976931348623157e308', '1e400', '-1e400', '2147483647000', '4294967296000', '-2147483648000', '1700000000', '17e11'];
  for (const n of numbers) add(n);
  const dateStrings = [
    '2024-01-01', '2024-01', '2024', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00.000Z', '2024-01-01T10:20:30.5Z', '2024-01-01T10:20:30.123456Z', '2024-01-01T10:20:30.1234567890Z', '2024-01-01T10:20Z', '2024-01-01T10:20:30',
    '2024-01-01T10:20', '2024-01-01T10:20:30+05:30', '2024-01-01T10:20:30-08:00', '2024-01-01T10:20:30+0530', '2024-01-01T10:20:30+05', '2024-01-01T10:20:30.123+05:30', '2024-01-01T10:20:30z', '2024-01-01t10:20:30Z', '2024-01-01 10:20:30', '2024-01-01 10:20:30Z', '2024-01-01 10:20:30.123', '2024-01-01 10:20:30 +0100',
    '2024-1-1', '2024-1-1T00:00:00Z', '2024-01-1', '2024-13-01', '2024-00-01', '2024-01-00', '2024-01-32', '2024-02-29', '2023-02-29', '2024-02-30', '2024-02-31', '2024-04-31', '2024-06-31', '2024-01-01T24:00:00Z', '2024-01-01T24:00:01Z', '2024-01-01T25:00:00Z', '2024-01-01T23:60:00Z', '2024-01-01T23:59:60Z', '2024-01-01T23:59:59.999Z', '2024-01-01T00:00:00+24:00', '2024-01-01T00:00:00+23:59', '2024-01-01T00:00:00-23:59', '2024-01-01T00:00:00+00:60',
    '+002024-01-01T00:00:00Z', '-000001-01-01T00:00:00Z', '+275760-09-13T00:00:00.000Z', '+275760-09-13T00:00:00.001Z', '-271821-04-20T00:00:00.000Z', '-271821-04-19T23:59:59.999Z', '-000000-01-01T00:00:00Z', '+010000-01-01T00:00:00Z', '0000-01-01T00:00:00Z', '0001-01-01', '0099-12-31T00:00:00Z', '9999-12-31T23:59:59.999Z', '10000-01-01', '12345-01-01T00:00:00Z', '1970-01-01T00:00:00Z', '1969-12-31T23:59:59.999Z',
    '', ' ', 'x', 'null', 'undefined', 'NaN', '1700000000000', '1700000000', '1e3', '0x10', '  2024-01-01', '2024-01-01  ', '2024-01-01T', 'T10:00:00', '2024-01-01Z', '2024-01-01T10Z', '2024-01-01T10:20:30.Z', '2024-01-01T10:20:30,5Z', '2024-01-01T10:20:30 Z', '2024-01-01T10:20:30ZZ', '20240101', '20240101T102030Z', '2024/01/01', '01/02/2024', '1/2/2024', '2024.01.01',
    'Tue, 15 Nov 1994 08:12:31 GMT', 'Tue Nov 15 1994 08:12:31 GMT+0000', 'Jan 1 2024', 'January 1, 2024', '1 Jan 2024', 'Mon, 01 Jan 2024 00:00:00 +0100', 'Sat, 01 Jan 2024 00:00:00 UTC', '2024-01-01T10:20:30.123+05:30[Asia/Kolkata]', 'Thu Jan 01 1970 00:00:00 GMT+0000 (Coordinated Universal Time)', 'Jan 1 2024 10:00 PM', '12:00', '2024-W01-1', '2024-001',
    '2024-01-01T10:20:30.123Z\x00', '\xa02024-01-01', '2024-01-01\u2028', '２０２４-01-01',
  ];
  for (const s of dateStrings) add(JSON.stringify(s));
  const pad = (n: number, w: number) => String(n).padStart(w, '0');
  for (let i = 0; i < 2500; i += 1) {
    const y = pick([0, 1, 99, 100, 999, 1000, 1969, 1970, 1999, 2000, 2024, 2038, 9999, int(10000)]);
    const mo = pick([0, 1, 2, 6, 12, 13, int(14)]);
    const d = pick([0, 1, 28, 29, 30, 31, 32, int(33)]);
    const h = pick([0, 1, 12, 23, 24, 25, int(26)]);
    const mi = pick([0, 30, 59, 60, int(61)]);
    const se = pick([0, 30, 59, 60, int(61)]);
    const fr = pick(['', '.0', '.5', '.123', '.1234', '.123456789', '.']);
    const tz = pick(['Z', 'z', '', '+00:00', '-00:00', '+05:30', '-12:00', '+14:00', '+24:00', '+0530', '+05', 'GMT', ' UTC']);
    const sep = pick(['T', 'T', 'T', ' ', 't']);
    const form = int(5);
    const datePart = form === 0 ? pad(y, 4) : form === 1 ? pad(y, 4) + '-' + pad(mo, 2) : pad(y, 4) + '-' + pad(mo, 2) + '-' + pad(d, 2);
    if (form <= 2 && rand() < 0.5) add(JSON.stringify(datePart));
    else add(JSON.stringify(datePart + sep + pad(h, 2) + ':' + pad(mi, 2) + (rand() < 0.2 ? '' : ':' + pad(se, 2) + fr) + tz));
  }
  for (let i = 0; i < 800; i += 1) {
    const ms = pick([(rand() - 0.5) * 8.64e15 * 2, (rand() - 0.5) * 1e13, rand() * 2e12, Math.floor(rand() * 4e12), (rand() - 0.5) * 2e14]);
    add(String(rand() < 0.5 ? Math.trunc(ms) : ms));
  }
  const parseCases: unknown[] = [];
  const addParse = (s: string) => {
    const dt = DateTime.fromFormat(s, 'yyyy-MM-dd HH:mm:ss.SSS', { zone: 'UTC' });
    const js = dt.toJSDate();
    parseCases.push({ in: s, ms: Number.isNaN(js.getTime()) ? null : js.getTime() });
  };
  for (const s of ['2024-01-01 00:00:00.000', '2024-12-31 23:59:59.999', '2024-02-29 12:00:00.000', '2023-02-29 12:00:00.000', '2024-04-31 00:00:00.000', '2024-13-01 00:00:00.000', '2024-00-01 00:00:00.000', '2024-01-00 00:00:00.000', '2024-01-01 24:00:00.000', '2024-01-01 24:00:00.001', '2024-01-01 25:00:00.000', '2024-01-01 23:60:00.000', '2024-01-01 23:59:60.000', '0000-01-01 00:00:00.000', '0001-01-01 00:00:00.000', '0099-12-31 23:59:59.999', '9999-12-31 23:59:59.999', '1970-01-01 00:00:00.000', '1969-12-31 23:59:59.999', '', ' ', 'x', '2024-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00.00', '2024-01-01 00:00:00.0000', '2024-01-01T00:00:00.000', '2024-01-01 00:00:00.000Z', ' 2024-01-01 00:00:00.000', '2024-01-01 00:00:00.000 ', '2024-1-1 0:0:0.000', '24-01-01 00:00:00.000', '-001-01-01 00:00:00.000', '+2024-01-01 00:00:00.000', '２０２４-01-01 00:00:00.000', '2024-01-01  00:00:00.000', '2024-01-01 00:00:00,000']) addParse(s);
  for (let i = 0; i < 400; i += 1) {
    const ms = pick([(rand() - 0.5) * 1e13, rand() * 2e12, (rand() - 0.5) * 6e14]);
    const text = DateTime.fromMillis(Math.trunc(ms), { zone: 'UTC' }).toFormat('yyyy-MM-dd HH:mm:ss.SSS');
    addParse(text);
    if (i % 4 === 0) addParse(text.replace(/\d/, (d) => String((Number(d) + 1 + int(8)) % 10)));
  }
  save('dates.json', { cases, parseCases });
}

// ---------------------------------------------------------------- urls
const HOSTS = [
  'XN--BCHER-KVA.DE', 'xn--bcher-kva.DE', 'ab--c.com', 'xn--ab--c.com', 'xn--a-.com', 'xn--n3h.com', 'xn--80ak6aa92e.com', 'xn--mnchen-3ya.de', 'xn--fiq228c.com', 'xn--e1afmkfd.xn--p1ai', 'xn--90a3ac.xn--p1ai', 'xn--0zwm56d.com', 'xn--ls8h.la', 'xn--ye8h.com',
  'a'.repeat(63) + '.com', 'a'.repeat(64) + '.com', 'a'.repeat(250) + '.com', Array.from({ length: 40 }, () => 'abcdefg').join('.'), 'ex-ample.com', '-.com', 'a-b-c.d-e', '--a.com', 'a--.com', 'a.-.b',
  'שלום.com', 'שלום1.com', 'abc.שלום', 'مثال.com', 'مثال1.com', 'مثال.إختبار', 'a١٢٣.com', '١٢٣.com', 'ab‌cd.com', 'क‌ष.com', 'क्‌ष.com', 'a‍b.com', '😀.com', '😀😀.xn--n3h', '̈abc.com', 'äbc.com', 'é.com', 'é.com', 'ǆ.com', 'ǅ.com', 'ς.com', 'σ.com', 'ΣΑΣ.com', 'ﬁ.com', 'ℬ.com', 'a⁠b.com', '﹩.com', '㊀.com', '①.com', '№.com', '。.com', 'ａ．ｂ', '…', 'a.b。c',
  'example.com.', 'example..com', '.example.com', 'example.com..', '​.com', 'a­.com', '­.com', 'ṩ.com', 'ṩ.com', 'Å.com', 'Å.com',
  'example.com', 'EXAMPLE.COM', 'Example.Com.', 'www.example.com', 'WWW.GOOGLE.COM', 'google.com', 'www.google.com', 'www.www.google.com', 'wwwxgoogle.com', 'xwww.google.com', 'sub.www.google.com', 'localhost', 'a', 'a.b.c.d.e',
  'bücher.de', 'BÜCHER.DE', '日本語.jp', 'xn--bcher-kva.de', 'xn--', 'xn--a', 'xn--zzzz', 'ß.de', 'faß.de', 'ǅ.com', 'ΣΑΣ.gr', 'İ.com', 'a\u00adb.com', 'a\u200db.com', 'a_b.com', 'a b.com', 'a%20b.com', 'a%2eb.com', '%65xample.com', 'ex%41mple.com', 'a..b', '.', '..', '-a.com', 'a-.com', 'a.b-.com', '1.2.3.4', '127.1', '127.0.0.1', '0x7f.1', '0x7f.0x0.0x0.0x1', '0177.0.0.1', '1.2.3.256', '1.2.3', '4294967295', '4294967296', '256.256.256.256', '1.2.3.4.5', '1.2.3.4.', '0x', '0x.1', '09.1.1.1', '1e3.com', '[::1]', '[::]', '[2001:db8::1]', '[2001:DB8::1]', '[::ffff:1.2.3.4]', '[1:2:3:4:5:6:7:8]', '[1:2:3:4:5:6:7:8:9]', '[::1', '::1]', '[g::1]', '[1::2::3]', '[0:0:0:0:0:0:0:1]', '[::127.0.0.1]', '[]', 'a<b.com', 'a>b.com', 'a^b.com', 'a|b.com', 'a\\b.com', 'a[b.com', 'a]b.com', 'a@b.com', 'user:pw@example.com', 'user@example.com', '@example.com', 'u:p@', ':@example.com', 'ex ample.com', 'ex\tample.com', '\u3002', 'a\u3002b', 'a\uff0eb', 'a\uff61b', 'ＥＸＡＭＰＬＥ.com', 'ex.com\xa0', 'constructor', 'CONSTRUCTOR', '__proto__', 'tostring', 'toString', 'hasOwnProperty', 'valueof',
  'twitter.com', 'www.twitter.com', 't.co', 'm.facebook.com', 'l.facebook.com', 'news.ycombinator.com', 'reddit.com', 'old.reddit.com', 'duckduckgo.com', 'bing.com', 'www.bing.com', 'yandex.ru',
  ...Object.keys(referrers).filter((_, i) => i % 7 === 0).slice(0, 120),
];
const PORTS = ['', ':80', ':443', ':8080', ':0', ':00080', ':65535', ':65536', ':99999', ':-1', ':a', ':', ':8080:80', ':\u0663', ': 80', ':%38%30', ':80x'];
const SCHEMES = ['http', 'https', 'HTTP', 'HtTpS', 'httpx', 'http-', 'http.', 'http+x', 'httpfoo', 'http1', 'http_', 'ftp', 'ws', 'wss', 'file', 'blob', 'data', 'javascript', 'mailto', 'tel', 'about', 'chrome-extension', 'android-app', 'ios-app', 'unknown'];
const SEPS = ['://', ':///', ':////', ':/', ':', ':\\\\', ':/\\', ':\\/', '://', '://', ':\\', ':\t//', ':/ /', ':///\\'];
const PATHS = ['', '/', '/a', '/a/b/c', '/a/', '//', '///', '/a//b', '/./a', '/../a', '/a/./b/../c', '/a/b/..', '/a/b/../', '/a/b/.', '/%2e', '/%2E%2e/x', '/a/%2e%2E/b', '/.%2e/x', '/a/..%2f/b', '/a b', '/a%20b', '/a%2Fb', '/a%zz', '/a%', '/a%4', '/é', '/%C3%A9', '/%c3%a9', '/%ff', '/日本', '/😀', '/\x00', '/\x01\x1f', '/a\tb', '/a\nb', '/a\rb', '/a\\b', '\\a\\b', '/a?b', '/a#b', '/a<b>c', '/a"b', "/a'b", '/a' + String.fromCharCode(96) + 'b', '/a{b}c', '/a|b', '/a^b', '/a[b]', '/a;b=c', '/a:b', '/a@b', '/a,b', '/a$b', '/a&b', '/a=b', '/a+b', '/a*b', '/a(b)', '/a!b', '/a~b', '/%7e', '/a/b.html', '/index.php/', '/' + 'a'.repeat(500), '/' + 'a/'.repeat(100), '/\u200b', '/\ufeff', '/\xa0', '/\u2028', '/' + LONE_HIGH, '/' + LONE_LOW + 'x', '/%E0%A4%A', '/ ', '/  /', '/a/%2e%2e', '/a/%2e%2e/', '/a/%2E', '/a/.%2E', '/a/%2e./b'];
const QUERIES = ['', '?', '?a', '?a=', '?a=b', '?a=b&c=d', '?a=1&a=2', '?a=1&b=2&a=3', '?=', '?=x', '?&&a=1&&', '?a=b=c', '?a+b=c+d', '?a%20b=c%20d', '?a=%', '?a=%4', '?a=%zz', '?a=%ff', '?a=%c3%a9', '?a=%E0%A4%A', '?a=%F0%9F%98%80', '?a=%F0%9F%98', '?%ff=%fe', '?a=é', '?é=1', '?a=😀', '?1=a&0=b&x=c', '?10=a&2=b&b=c&1=d', '?4294967295=a&4294967294=b', '?01=a&1=b', '?-1=a', '?__proto__=x', '?__proto__=x&a=1', '?constructor=x', '?toString=x', '?utm_source=google', '?utm_source=Google', '?utm_source=GOOGLE&ref=x', '?utm_source=&ref=twitter', '?ref=twitter', '?ref=', '?utm_referrer=reddit', '?ref=&utm_referrer=reddit', '?utm_source=constructor', '?utm_source=__proto__', '?utm_source=toString', '?utm_source=Object', '?utm_source=hasownproperty', '?utm_source=%C4%B0', '?utm_source=Facebook', '?utm_source=fb', '?utm_source=yahoo', '?utm_source=bing', '?utm_source=twitter', '?utm_source=t', '?utm_source=newsletter&utm_medium=email', '?a=b#c', '?a=b?c', '?a[]=1&a[]=2', '?a=1;b=2', '?a=\x00', '?a=\t1', '?a= 1', '?a=1\n2', '?a=%00', '?a=%0a', '?q=' + 'x'.repeat(300), '?a=' + LONE_HIGH, '?' + LONE_LOW + '=1', "?a='", '?a="', '?a=<', '?a=' + String.fromCharCode(96), '?a=\\', '?a=#', '?%61=%62', '?+=+', '?%2b=%2b', '?%26=%3d', '?a&b&c', '?a=1&&&b=2', '?=&=', '??a=b'];
const HASHES = ['', '#', '#a', '#a b', '#a%20b', '#é', '#😀', '#/route?x=1', '#a#b', '#\x00', '#a\tb', '#<>', '#' + String.fromCharCode(96), "#'", '#"', '#' + LONE_HIGH, '#%zz', '#%', '#\\'];
const odd = [
  '/', '/a', '//evil.com', '//evil.com/x', '//evil.com:8080/x', '//EVIL.com/x?y=1#z', '/\\evil.com', '\\\\evil.com', '/\\/evil.com', '//', '///', '////x', '//:80', '//@', '//user@host/x', '///host/x', '/ /evil.com', '/\t/evil.com', '//évil.com/x', '//[::1]/x', '//1.2.3/x', '//0x7f.1/x', '//a b/x', '//%/x',
  'http', 'httpfoo', 'http:', 'http:/', 'http://', 'http:///', 'http:/a', 'http:a', 'http:a/b', 'http:/a/b', 'http:\\\\a\\b', 'http:?', 'http:#', 'http:?a#b', 'http:a?b#c', 'http://?', 'http://#', 'http://a', 'http://a/', 'http://a?x', 'http://a#x', 'http://a:80', 'http://a:80/', 'http://a:81', 'https://a:443', 'https://a:80', 'http://a:443', 'http://@', 'http://:@', 'http://:80', 'http://a:b:c@d', 'http://a@b@c', 'http://a b', 'http://a%20b', 'http://%41', 'http://\u00ad', 'http://a/b c', 'http://a/b\tc', 'http://a/\nb', 'http:// a', 'http://a\\b', 'http://a/b\\c', 'http://a//b', 'http://a/./b/../c', 'http://a/%2e%2e/b', 'http://a/..', 'http://a/../..', 'http:/\\a', 'http:\\/a', 'http:////a', 'http:///a', 'HTTP://A.COM', 'Http://a.com/B', 'httpx://a.com', 'httpfoo://a.com/x', 'http-x://a.com/x', 'http+z://a.com/x', 'http:foo', 'httpfoo:bar', 'httpfoo:/bar', 'httpfoo:/bar?x#y', 'httpfoo:', 'httpfoo:?x', 'httpfoo:#x', 'httpfoo://', 'httpfoo:///x', 'httpfoo://h:80/x', 'httpfoo://H/x', 'httpfoo://a b/', 'httpfoo://[::1]/', 'httpfoo://%41/', 'httpfoo:\\\\x', 'httpé://a', 'http\x00://a', 'http\t://a', 'ht\ttp://a.com/x', 'http\n://a', 'http ://a', ' http://a.com', 'http://a.com ', '\thttp://a.com', 'http://a.com\n', '\x01http://a.com', 'http://a.com/\x1f', 'http://a.com/\x00', 'http://\x00', 'httpss://a.com', 'http//a.com', 'http:/a.com', 'http:://a.com', 'http://http://a.com', 'http://a.com:80:80', 'http://a.com:/x', 'http://a.com:?x', 'http://[::1]:80/', 'http://[::1]:8080/', 'http://[::1', 'http://::1/', 'http://1.2.3.4:80/', 'http://0x7f000001/', 'http://2130706433/', 'http://017700000001/', 'http://127.1/', 'http://1.1.1/', 'http://999999999999/', 'http://a.com./', 'http://.a.com/', 'http://a..com/', 'http://-a.com/', 'http://a_b.com/', 'http://a%00b/', 'http://a<b/', 'http://a|b/', 'http://xn--/', 'http://xn--a/', 'http://xn--bcher-kva.de/', 'http://BÜCHER.de/', 'http://faß.de/', 'http://ǅ.com/', 'http://ΣΑΣ.gr/', 'http://İ.com/', 'http://a\u00adb.com/', 'http://a\u200db.com/', 'http://a\u200cb.com/', 'http://a\u3002b/', 'http://\uff21.com/', 'http://😀.com/', 'http://' + LONE_HIGH + '.com/', 'http://a/' + LONE_HIGH, 'http://a/?' + LONE_LOW, 'http://a/#' + LONE_HIGH,
  'file:///etc/passwd', 'file://host/x', 'file:/x', 'file:x', 'file:///C:/x', 'file:///c|/x', 'ws://a/x', 'wss://a:443/x', 'ftp://a:21/x', 'ftp://a:22/x', 'blob:http://a/x', 'data:text/plain,x', 'javascript:alert(1)', 'mailto:a@b', 'about:blank',
  'http://localhost', 'http://localhost:3000/a?b=c#d', 'http://127.0.0.1:5173/x', 'http://[::1]:3000/x', 'https://sub.example.co.uk/a/b?c=d&e=f#g', 'https://www.google.com/search?q=a+b%20c&hl=en', 'https://t.co/AbC123', 'https://l.facebook.com/l.php?u=https%3A%2F%2Fa.com',
  '/a?b=c#d', '/a/../../b', '/a/./b', '/%2e%2e/%2E/x', '/a?b=1&b=2', '/a?%', '/a?a=%ff', '/a?__proto__=x', '/a?x=1#', '/a#', '/a?#', '/a?', '/?', '/#', '/?a#b?c', '/a b?c d#e f',
];
const urlCorpus: string[] = [];
{
  const seen = new Set<string>();
  const addUrl = (u: string) => {
    if (!seen.has(u)) {
      seen.add(u);
      urlCorpus.push(u);
    }
  };
  for (const u of odd) addUrl(u);
  for (const p of PATHS) addUrl(p);
  for (const q of QUERIES) addUrl('/p' + q);
  for (const q of QUERIES) addUrl('https://example.com/x' + q);
  for (const h of HASHES) addUrl('/p' + h);
  for (const h of HOSTS) { addUrl('http://' + h); addUrl('https://' + h + '/'); addUrl('//' + h + '/x'); addUrl('https://' + h + '/a?b=c#d'); addUrl('httpx://' + h + '/a'); }
  for (const s of ['httpx', 'httpfoo', 'http.', 'ws2']) {
    for (const p of PATHS) { addUrl(s + ':' + p); addUrl(s + ':' + p + '?q#h'); addUrl(s + '://h' + p); addUrl(s + '://u@h' + p); }
    for (const u of ['u@', 'u:p@', ':@', '@', '']) for (const tail of ['', '?q', '#', '?', ':80', ':80?q#', '/', '/x', '/.x']) addUrl(s + '://' + u + 'h' + tail);
    for (const p of ['.', '..', '.a', '..a', '.:1', '...', '/.', '/..', './', '../x', '.x/y', '.%2e', '.%61']) { addUrl(s + ':/' + p); addUrl(s + ':' + p); addUrl(s + ':/' + p + '#'); addUrl(s + ':/a/' + p); }
  }
  for (const pt of PORTS) { addUrl('http://example.com' + pt + '/x'); addUrl('https://example.com' + pt); addUrl('httpx://example.com' + pt + '/x'); addUrl('ftp://example.com' + pt + '/x'); addUrl('ws://example.com' + pt + '/x'); }
  for (let i = 0; i < 4000; i += 1) {
    const scheme = pick(SCHEMES.slice(0, rand() < 0.7 ? 7 : SCHEMES.length));
    const sep = rand() < 0.8 ? '://' : pick(SEPS);
    const userinfo = rand() < 0.1 ? pick(['u@', 'u:p@', ':@', '@', 'a%40b@']) : '';
    let url: string;
    const mode = int(10);
    if (mode < 2) url = pick(PATHS) + pick(QUERIES) + pick(HASHES);
    else if (mode < 3) url = '//' + userinfo + pick(HOSTS) + pick(PORTS) + pick(PATHS) + pick(QUERIES) + pick(HASHES);
    else url = scheme + sep + userinfo + pick(HOSTS) + pick(PORTS) + pick(PATHS) + pick(QUERIES) + pick(HASHES);
    if (rand() < 0.05) url = url.replace(/[a-z]/, (c) => c.toUpperCase());
    if (rand() < 0.04) url = pick(['\t', ' ', '\n', '\x01']) + url;
    if (rand() < 0.04) url += pick(['\t', ' ', '\n', '\x01']);
    if (rand() < 0.04) { const pos = int(url.length + 1); url = url.slice(0, pos) + pick(['\t', '\n', '\r']) + url.slice(pos); }
    addUrl(url);
  }
}
{
  const cases: unknown[] = [];
  for (const u of urlCorpus) {
    cases.push({ in: u, out: parsePath(u) });
  }
  const pairs: unknown[] = [];
  for (let i = 0; i < 2500; i += 1) {
    const a = pick(urlCorpus);
    const b = rand() < 0.4 ? a.replace(/^[a-z]+/i, pick(SCHEMES)) : rand() < 0.5 ? pick(urlCorpus) : pick(urlCorpus).replace(/\/.*$/, '');
    pairs.push({ a, b, out: isSameDomain(a, b) });
  }
  for (const [a, b] of [['', 'http://a.com'], ['http://a.com', ''], ['http://a.com/x', 'http://A.COM:80/y'], ['http://a.com', 'https://a.com'], ['http://a.com:81', 'http://a.com'], ['/x', '/x'], ['http://www.a.com', 'http://a.com'], ['a', 'a'], ['http://a', 'http://a'], ['file:///a', 'file:///b'], ['file:///a', 'file://host/b'], ['mailto:a@b', 'mailto:c@d'], ['data:x', 'data:y'], ['http://[::1]/', 'http://[0:0:0:0:0:0:0:1]/'], ['http://1.2.3.4', 'http://0x1.2.3.4'], ['http://bücher.de', 'http://xn--bcher-kva.de']] as const) pairs.push({ a, b, out: isSameDomain(a, b) });
  const refs: unknown[] = [];
  const refInputs = new Set<string>(urlCorpus);
  for (const [i, host] of Object.keys(referrers).entries()) {
    if (i % 3 === 0) refInputs.add('https://' + host + '/x/');
    if (i % 3 === 1) refInputs.add('http://www.' + host);
    if (i % 3 === 2) refInputs.add('https://' + host.toUpperCase() + ':443/path?q=1');
  }
  for (const u of ['', 'x', 'not-a-url', 'https://google.com/search?q=test', 'https://www.twitter.com/user', 'https://unknown-site.com', 'http://constructor/', 'http://www.constructor/x', 'http://__proto__/', 'http://www.__proto__/', 'http://wwwconstructor/', 'http://www.www.google.com/', 'http://xwww.google.com/', 'http://wwww.google.com/', 'http://google.www.com/', 'https://google.com////', 'https://google.com/a//', '////', '/', 'http://t.co/', 'http://t.co/abc///']) refInputs.add(u);
  for (const u of refInputs) {
    const r = attempt(() => parseReferrer(u));
    refs.push({ in: u, out: r.ok ? r.v : null });
  }
  const queryCases: unknown[] = [];
  const sources = new Set<string>(['google', 'Google', 'GOOGLE', 'facebook', 'fb', 'twitter', 'x', 't.co', 'bing', 'yahoo', 'duckduckgo', 'newsletter', 'email', 'constructor', 'Constructor', 'CONSTRUCTOR', '__proto__', 'toString', 'tostring', 'Object', 'object', 'hasOwnProperty', 'valueOf', 'valueof', 'isPrototypeOf', '__defineGetter__', '__definegetter__', '__lookupGetter__', 'toLocaleString', 'propertyIsEnumerable', 'prototype', 'name', 'type', 'İ', 'İstanbul', 'ΣΑΣ', 'Σ', 'ς', '', ' ', 'google ', 'google.com', 'www.google.com', 'a.b', 'ß', 'ǅ', '😀', LONE_HIGH, 'x'.repeat(100)]);
  for (const [i, host] of Object.keys(referrers).entries()) {
    if (i % 3 === 0) sources.add(host);
    if (i % 3 === 1) sources.add(host.replace(/\.com$/, ''));
    if (i % 3 === 2) sources.add(host.toUpperCase());
  }
  for (const r of new Set(Object.values(referrers).map((entry) => entry.name))) { sources.add(r); sources.add(r.toUpperCase()); }
  for (const s of sources) {
    for (const shape of [['utm_source'], ['ref'], ['utm_referrer'], ['utm_source', 'ref'], ['ref', 'utm_source'], ['utm_referrer', 'ref']]) {
      if (shape.length > 1 && rand() < 0.9) continue;
      const q: Record<string, string> = {};
      for (const [idx, key] of shape.entries()) q[key] = idx === 0 ? s : pick(['', 'other', 'google']);
      queryCases.push({ in: q, out: getReferrerWithQuery(q) });
    }
  }
  for (const q of [undefined, {}, { utm_source: '' }, { utm_source: '', ref: 'twitter' }, { ref: '', utm_referrer: 'reddit' }, { x: 'google' }, { utm_source: 'google', ref: 'x', utm_referrer: 'y' }, { constructor: 'google' }]) queryCases.push({ in: q ?? null, out: getReferrerWithQuery(q as never) });
  const protoKeys = Object.getOwnPropertyNames(Object.prototype).map((key) => {
    const m = (referrers as Record<string, { name?: string; type?: string } | undefined>)[key];
    return { key, own: Object.hasOwn(referrers, key), nameKind: typeof m?.name, typeKind: typeof m?.type, name: typeof m?.name === 'string' ? m.name : null, type: typeof m?.type === 'string' ? m.type : null };
  });
  const argTexts = ['null', 'true', 'false', '0', '1', '5', '-1', '1.5', '[]', '["http://a.com/x"]', '["/x"]', '["a","b"]', '[1]', '{}', '{"a":1}', '[[]]', '[null]', '"http://a.com"', '""', '[""]'];
  const valueCases: unknown[] = [];
  for (const text of argTexts) {
    const arg = JSON.parse(text);
    const pp = attempt(() => parsePath(arg));
    const pr = attempt(() => parseReferrer(arg));
    valueCases.push({ in: text, parsePath: pp.ok ? { v: pp.v } : { throws: true }, parseReferrer: pr.ok ? { v: pr.v } : { throws: true } });
  }
  const sameCases: unknown[] = [];
  for (const a of argTexts) for (const b of ['"http://a.com"', '["http://a.com/y"]', '1', 'null', '["x"]']) {
    sameCases.push({ a, b, out: isSameDomain(JSON.parse(a), JSON.parse(b)) });
  }
  save('url.json', { cases });
  save('samedomain.json', { cases: pairs, valueCases: sameCases });
  save('referrer.json', { cases: refs, queryCases, protoKeys, valueCases });
}
