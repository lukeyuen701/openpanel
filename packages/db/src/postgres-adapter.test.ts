import { describe, expect, test } from 'bun:test';
import { postgresAdapterConfig } from './postgres-adapter';

const BASE = 'postgresql://user:p%40ss@db.example:5432/openpanel';
const CPU_COUNT = 4;

const configFor = (query: string) =>
  postgresAdapterConfig(`${BASE}${query}`, CPU_COUNT);

const paramsOf = (query: string) =>
  new URL(configFor(query).pool.connectionString ?? '').searchParams;

describe('postgresAdapterConfig', () => {
  test('keeps Prisma 6 pool defaults when the URL sets none', () => {
    const { pool, schema } = configFor('');
    expect(pool.max).toBe(CPU_COUNT * 2 + 1);
    expect(pool.connectionTimeoutMillis).toBe(10_000);
    expect(schema).toBeUndefined();
  });

  test('reads the schema and pool knobs, and hides them from pg', () => {
    const query =
      '?schema=analytics&connection_limit=25&pool_timeout=30&pgbouncer=true&connect_timeout=5';
    const { pool, schema } = configFor(query);
    expect(schema).toBe('analytics');
    expect(pool.max).toBe(25);
    expect(pool.connectionTimeoutMillis).toBe(30_000);
    expect([...paramsOf(query).keys()]).toEqual([]);
  });

  test('pool_timeout=0 keeps meaning no timeout', () => {
    expect(configFor('?pool_timeout=0').pool.connectionTimeoutMillis).toBe(0);
  });

  test('ignores knobs that are not numbers', () => {
    const { pool } = configFor('?connection_limit=lots&pool_timeout=-1');
    expect(pool.max).toBe(CPU_COUNT * 2 + 1);
    expect(pool.connectionTimeoutMillis).toBe(10_000);
  });

  test('keeps the credentials encoded', () => {
    expect(configFor('?schema=public').pool.connectionString).toBe(BASE);
  });

  test('sslmode=require accepts any certificate, as Prisma 6 did', () => {
    const params = paramsOf('?sslmode=require');
    expect(params.get('sslmode')).toBe('require');
    expect(params.get('uselibpqcompat')).toBe('true');
  });

  test('sslaccept=strict verifies the certificate and the host', () => {
    const params = paramsOf('?sslmode=require&sslaccept=strict');
    expect(params.get('sslmode')).toBe('verify-full');
    expect(params.has('uselibpqcompat')).toBe(false);
    expect(params.has('sslaccept')).toBe(false);
  });

  test('leaves sslmode=disable and an explicit libpq choice alone', () => {
    expect(paramsOf('?sslmode=disable').has('uselibpqcompat')).toBe(false);
    expect(
      paramsOf('?sslmode=require&uselibpqcompat=false').get('uselibpqcompat')
    ).toBe('false');
  });
});
