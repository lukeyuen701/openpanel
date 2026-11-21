// Goldens for the Go worker's consumer path (internal/worker/{config,consumer,wire}),
// generated from the real TypeScript code:
//   bun parity/golden-consumer.ts            writes each package's testdata/
//   bun parity/golden-consumer.ts <dir>      writes everything to <dir> instead
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { resolveRequestId } from '../../../packages/core/src/shared/request-id';
import { toDeadLetterRecord } from '../../../packages/redis/dead-letter';
import { loadConfig } from '../../api/src/config/env';

const outDir = process.argv[2];
const WORKER = join(import.meta.dir, '../internal/worker');
const TESTDATA: Record<string, string> = {
  'deadletter.json': 'consumer',
  'utf8.json': 'consumer',
  'jsonparse.json': 'wire',
  'config.json': 'config',
  'requestid.json': 'wire',
};
const save = (name: string, data: unknown) => {
  const path = outDir
    ? join(outDir, name)
    : join(WORKER, TESTDATA[name]!, 'testdata', name);
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(data, null, 1)}\n`);
};
const bytes = (h: string) => Buffer.from(h, 'hex');

// Byte strings that exercise Buffer#toString: valid, truncated and overlong sequences.
const BYTES = [
  '',
  '41',
  '7b22617a223a317d',
  'c3a9',
  'e282ac',
  'f09f9880',
  'ff',
  'c0',
  'c080',
  'c1bf',
  'e0',
  'e080',
  'e0a0',
  'e09f80',
  'eda080',
  'edbfbf',
  'f0',
  'f08080',
  'f0908080',
  'f4908080',
  'f4',
  'f48f',
  'f8888080',
  '80',
  'bf',
  'c3',
  'c328',
  'e282',
  'e28228',
  'f09f98',
  'f09f9828',
  '41ff42',
  '41e28243',
  'efbbbf7b7d',
  'fffe',
  '00',
  'e29c',
  'e29cff',
  'f1808080',
  'f3bfbfbf',
  'f0808080',
];
save(
  'utf8.json',
  BYTES.map((h) => ({ hex: h, text: bytes(h).toString() }))
);

const dead = (
  key: string | null,
  value: string | null,
  headers: [string, string][],
  offset: string,
  reason: string,
  error: string
) => {
  const hmap: Record<string, Buffer> = {};
  for (const [name, h] of headers) {
    hmap[name] = bytes(h);
  }
  return {
    key,
    value,
    headers: headers.map(([name, h]) => ({ name, hex: h })),
    offset,
    reason,
    error,
    out: JSON.stringify(
      toDeadLetterRecord(
        {
          key: key === null ? null : bytes(key),
          value: value === null ? null : bytes(value),
          headers: hmap,
          topic: 'events',
          partition: 3,
          offset,
          reason,
          error,
        },
        new Date('2026-11-21T17:30:22.556Z')
      )
    ),
  };
};
const utf = (s: string) => Buffer.from(s).toString('hex');
save('deadletter.json', [
  dead(
    utf('wparity-dev'),
    utf('{"bad":'),
    [['x-h', utf('hv')]],
    '42',
    'parse_error',
    'Unexpected end'
  ),
  dead(null, null, [], '0', 'parse_error', 'kafka message has no value'),
  dead('', '', [], '1', 'handler_error', ''),
  dead(
    utf('k'),
    utf('{"a":1}'),
    [
      ['b', utf('1')],
      ['10', utf('2')],
      ['2', utf('3')],
      ['a', utf('4')],
      ['b', utf('5')],
      ['01', utf('6')],
      ['4294967294', utf('7')],
      ['4294967295', utf('8')],
    ],
    '9007199254740993',
    'handler_error',
    'quote " and \\ and \n'
  ),
  dead(
    'ff41',
    'c328e282',
    [
      ['h', 'fffe41'],
      ['e', ''],
    ],
    '5',
    'handler_error',
    'invalid bytes'
  ),
  dead(
    utf('é😀'),
    utf('{"x":"\u2028 \u0007 </script>"}'),
    [['ключ', utf('é')]],
    '7',
    'parse_error',
    '\u2028 \u0001'
  ),
]);

const JSON_TEXTS = [
  '{}',
  '[]',
  'null',
  '0',
  '-0',
  '1.5e3',
  '"x"',
  'true',
  ' {"a":1} ',
  '\n\t\r{}',
  '',
  ' ',
  '{',
  '{"a":}',
  '{"a":1,}',
  '[1,]',
  '{a:1}',
  "{'a':1}",
  'NaN',
  'Infinity',
  '-Infinity',
  'undefined',
  '01',
  '1.',
  '.5',
  '+1',
  '0x10',
  '1e',
  '"\\u00e9"',
  '"\\ud800"',
  '"\\udc00x"',
  '"\\ud83d\\ude00"',
  '"\\x41"',
  '"\\\'"',
  '"tab\there"',
  '"line\nbreak"',
  '"\u2028"',
  '\ufeff{}',
  '\u00a0{}',
  '{"a":1}{"b":2}',
  '{"a":1} x',
  '{"a":1,"a":2}',
  '{"__proto__":{"x":1}}',
  '[[[[[[[[[[1]]]]]]]]]]',
  '1e999',
  '-1e999',
  '123456789012345678901234567890',
  '{"a":"\\u0000"}',
  '{"a":"\u0000"}',
  'tru',
  'nul',
  '"abc',
  '\u000b{}',
  '\f{}',
];
save(
  'jsonparse.json',
  JSON_TEXTS.map((text) => {
    try {
      JSON.parse(text);
      return { text, ok: true };
    } catch {
      return { text, ok: false };
    }
  })
);

const REQUEST_IDS: unknown[] = [
  'abc',
  'a b/c',
  '!!!',
  '',
  'x'.repeat(100),
  `${'a'.repeat(63)}!b`,
  'a'.repeat(70),
  'é-_9',
  'ключ',
  'A-Z_a-z0-9',
  0,
  false,
  null,
  5,
  true,
  {},
  [],
  ['a'],
];
// A fresh id is random, so the golden records only that one was minted.
save(
  'requestid.json',
  REQUEST_IDS.map((value) => {
    try {
      const resolved = resolveRequestId(value as string | null);
      const sanitised =
        typeof value === 'string'
          ? value.replace(/[^A-Za-z0-9_-]/g, '').slice(0, 64)
          : '';
      return {
        json: JSON.stringify(value),
        ok: true,
        fresh: sanitised === '',
        id: sanitised === '' ? null : resolved,
      };
    } catch {
      return { json: JSON.stringify(value), ok: false };
    }
  })
);

const BASE = { KAFKA_BROKERS: 'localhost:23092' };
const CONFIG_ENVS: Record<string, string>[] = [
  {},
  { ROLE: 'worker' },
  { ROLE: 'all' },
  { ROLE: 'bogus' },
  { ROLE: ' worker ' },
  { ROLE: '' },
  { ROLE: 'worker', ENABLED_QUEUES: 'events' },
  { ROLE: 'worker', ENABLED_QUEUES: 'events, sessions ,,cron' },
  { ROLE: 'worker', ENABLED_QUEUES: 'events_kafka' },
  { ROLE: 'worker', ENABLED_QUEUES: 'nope' },
  { ROLE: 'worker', ENABLED_QUEUES: ',' },
  { ROLE: 'api', ENABLED_QUEUES: ',' },
  { ROLE: 'worker', ENABLED_QUEUES: 'cohortCompute' },
  { ROLE: 'worker', ENABLED_QUEUES: 'sessions' },
  { ROLE: 'worker', DISABLE_WORKERS: 'false' },
  { ROLE: 'worker', DISABLE_WORKERS: '0' },
  { ROLE: 'worker', DISABLE_WORKERS: '  ' },
  { QUEUE_CLUSTER: '0' },
  { QUEUE_CLUSTER: 'true' },
  { QUEUE_NAMESPACE: 'wparity-x' },
  { QUEUE_NAMESPACE: '  ' },
  { KAFKA_PARTITIONS_CONCURRENT: '4' },
  { KAFKA_PARTITIONS_CONCURRENT: '0' },
  { KAFKA_PARTITIONS_CONCURRENT: '-1' },
  { KAFKA_PARTITIONS_CONCURRENT: '1.5' },
  { KAFKA_PARTITIONS_CONCURRENT: '5000junk' },
  { KAFKA_PARTITIONS_CONCURRENT: '1e2' },
  { KAFKA_PARTITIONS_CONCURRENT: '0x10' },
  { KAFKA_PARTITIONS_CONCURRENT: '010' },
  { KAFKA_PARTITIONS_CONCURRENT: ' 7 ' },
  { KAFKA_PARTITIONS_CONCURRENT: '1_000' },
  { KAFKA_PARTITIONS_CONCURRENT: 'Infinity' },
  { KAFKA_PARTITIONS_CONCURRENT: '+3' },
  { KAFKA_PARTITIONS_CONCURRENT: '3.0' },
  { KAFKA_PARTITIONS_CONCURRENT: '.5e1' },
  { KAFKA_PARTITIONS_CONCURRENT: '0b11' },
  { KAFKA_PARTITIONS_CONCURRENT: '0o17' },
  {
    KAFKA_MIN_MESSAGES: '2',
    KAFKA_MAX_WAIT_MS: '250',
    KAFKA_MAX_MESSAGES_PER_PARTITION: '64',
  },
  { KAFKA_SESSION_TIMEOUT_MS: '10000', KAFKA_HEARTBEAT_INTERVAL_MS: '10000' },
  { KAFKA_SESSION_TIMEOUT_MS: '10000', KAFKA_HEARTBEAT_INTERVAL_MS: '9999' },
  { KAFKA_HEARTBEAT_INTERVAL_MS: '30000' },
  {
    KAFKA_REBALANCE_TIMEOUT_MS: '1000',
    KAFKA_REQUEST_TIMEOUT_MS: '100',
    KAFKA_CONNECTION_TIMEOUT_MS: '50',
  },
  {
    KAFKA_HANDLER_MAX_ATTEMPTS: '5',
    KAFKA_HANDLER_RETRY_INITIAL_MS: '1',
    KAFKA_HANDLER_RETRY_MAX_MS: '2',
  },
  {
    KAFKA_CLIENT_ID: 'c',
    KAFKA_EVENTS_TOPIC: 'wparity-t',
    KAFKA_CONSUMER_GROUP: 'wparity-g',
  },
  { KAFKA_BROKERS: ' a:1 , b:2 ,,' },
  { KAFKA_SSL: 'true' },
  { KAFKA_SSL: '1' },
  { KAFKA_SSL: 'false' },
  { KAFKA_SSL: '0' },
  { KAFKA_SSL: 'yes' },
  { KAFKA_SSL: 'TRUE' },
  { KAFKA_SSL_CA_PATH: '/x.pem' },
  { KAFKA_SSL_CA_PATH: '/x.pem', KAFKA_SSL: 'true' },
  { KAFKA_SSL_REJECT_UNAUTHORIZED: 'false' },
  { KAFKA_SSL_REJECT_UNAUTHORIZED: 'false', KAFKA_SSL: '1' },
  { KAFKA_SASL_USERNAME: 'u' },
  { KAFKA_SASL_PASSWORD: 'p' },
  { KAFKA_SASL_USERNAME: 'u', KAFKA_SASL_PASSWORD: 'p' },
  { KAFKA_SASL_USERNAME: 'u', KAFKA_SASL_PASSWORD: 'p', KAFKA_SSL: 'false' },
  {
    KAFKA_SASL_USERNAME: 'u',
    KAFKA_SASL_PASSWORD: 'p',
    KAFKA_SASL_MECHANISM: 'PLAIN',
  },
  {
    KAFKA_SASL_USERNAME: 'u',
    KAFKA_SASL_PASSWORD: 'p',
    KAFKA_SASL_MECHANISM: 'scram-sha-256',
  },
  {
    KAFKA_SASL_USERNAME: 'u',
    KAFKA_SASL_PASSWORD: 'p',
    KAFKA_SASL_MECHANISM: 'gssapi',
  },
  { KAFKA_SASL_MECHANISM: 'plain' },
  { SHUTDOWN_FORCE_EXIT_MS: '5000' },
  { SHUTDOWN_FORCE_EXIT_MS: '0' },
  { REDIS_CACHE_COMMAND_TIMEOUT_MS: '250' },
  { REDIS_CACHE_COMMAND_TIMEOUT_MS: 'abc' },
  { INGEST_DEAD_LETTER_MAX_ENTRIES: '10' },
  { INGEST_DEAD_LETTER_MAX_ENTRIES: '-2' },
  { API_PORT: '0' },
  { API_PORT: '8080' },
  { API_PORT: '-1' },
  { API_PORT: 'x' },
  { LOG_LEVEL: 'debug' },
  { LOG_LEVEL: '  ' },
  { ROLE: 'worker', ENABLED_QUEUES: 'events', KAFKA_BROKERS: '' },
];
const subset = (c: ReturnType<typeof loadConfig>) => ({
  ROLE: c.ROLE,
  ENABLED_QUEUES: c.ENABLED_QUEUES,
  DISABLE_WORKERS: c.DISABLE_WORKERS,
  QUEUE_CLUSTER: c.QUEUE_CLUSTER,
  QUEUE_NAMESPACE: c.QUEUE_NAMESPACE ?? null,
  SHUTDOWN_FORCE_EXIT_MS: c.SHUTDOWN_FORCE_EXIT_MS,
  REDIS_CACHE_COMMAND_TIMEOUT_MS: c.REDIS_CACHE_COMMAND_TIMEOUT_MS,
  INGEST_DEAD_LETTER_MAX_ENTRIES: c.INGEST_DEAD_LETTER_MAX_ENTRIES,
  API_PORT: c.API_PORT,
  LOG_LEVEL: c.LOG_LEVEL,
  kafka: {
    clientId: c.core.kafka.clientId,
    brokers: c.core.kafka.brokers,
    eventsTopic: c.core.kafka.eventsTopic,
    consumerGroup: c.core.kafka.consumerGroup,
    partitionsConcurrent: c.core.kafka.partitionsConcurrent,
    minMessages: c.core.kafka.minMessages,
    maxWaitMs: c.core.kafka.maxWaitMs,
    maxMessagesPerPartition: c.core.kafka.maxMessagesPerPartition,
    sessionTimeoutMs: c.core.kafka.sessionTimeoutMs,
    heartbeatIntervalMs: c.core.kafka.heartbeatIntervalMs,
    rebalanceTimeoutMs: c.core.kafka.rebalanceTimeoutMs,
    requestTimeoutMs: c.core.kafka.requestTimeoutMs,
    connectionTimeoutMs: c.core.kafka.connectionTimeoutMs,
    handlerMaxAttempts: c.core.kafka.handlerMaxAttempts,
    handlerRetryInitialMs: c.core.kafka.handlerRetryInitialMs,
    handlerRetryMaxMs: c.core.kafka.handlerRetryMaxMs,
    sslEnabled: c.core.kafka.security.ssl.enabled,
    sslCaPath: c.core.kafka.security.ssl.caPath ?? null,
    sslRejectUnauthorized: c.core.kafka.security.ssl.rejectUnauthorized ?? null,
    sasl: c.core.kafka.security.sasl ?? null,
  },
});
save(
  'config.json',
  CONFIG_ENVS.map((env) => {
    const merged: Record<string, string> = { ...BASE, ...env };
    try {
      return { env: merged, ok: true, config: subset(loadConfig(merged)) };
    } catch (err) {
      return {
        env: merged,
        ok: false,
        error: String((err as Error).message).slice(0, 300),
      };
    }
  })
);
console.log('golden written to', outDir ?? 'internal/worker/*/testdata');
