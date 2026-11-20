import { availableParallelism } from 'node:os';
import { PrismaPg } from '@prisma/adapter-pg';
import type { PoolConfig } from 'pg';

/**
 * Prisma 6's own pool defaults. The pg driver behind Prisma 7 defaults to 10
 * connections and no timeout, so an upgrade would otherwise change capacity
 * and turn a saturated pool into requests that hang.
 */
const DEFAULT_POOL_TIMEOUT_SECONDS = 10;
const CONNECTIONS_PER_CPU = 2;
const MILLISECONDS_PER_SECOND = 1000;

/**
 * Query parameters only Prisma's engine understood. pg reads the rest of the
 * URL itself, so these are translated below and removed.
 */
const PRISMA_ONLY_PARAMS = [
  'schema',
  'connection_limit',
  'pool_timeout',
  'connect_timeout',
  'pgbouncer',
  'statement_cache_size',
  'socket_timeout',
  'sslaccept',
] as const;

const LIBPQ_COMPAT_PARAM = 'uselibpqcompat';
const SSL_MODES_WITHOUT_VERIFICATION = new Set(['prefer', 'require']);

export interface PostgresAdapterConfig {
  pool: PoolConfig;
  schema: string | undefined;
}

function readNonNegativeInteger(value: string | null): number | undefined {
  if (value === null || value.trim() === '') {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

/**
 * Prisma 6 accepted any certificate unless `sslaccept=strict`, like libpq's
 * own `sslmode=require`. pg treats `require` as full verification, so the
 * libpq meaning is asked for explicitly, and `strict` becomes `verify-full`.
 */
function keepPrismaSslSemantics(params: URLSearchParams): void {
  const sslMode = params.get('sslmode');
  if (!sslMode || sslMode === 'disable' || params.has(LIBPQ_COMPAT_PARAM)) {
    return;
  }
  const verifyStrictly = params.get('sslaccept') === 'strict';
  if (verifyStrictly && SSL_MODES_WITHOUT_VERIFICATION.has(sslMode)) {
    params.set('sslmode', 'verify-full');
    return;
  }
  params.set(LIBPQ_COMPAT_PARAM, 'true');
}

/** Translates a Prisma 6 style connection string into pg pool options. */
export function postgresAdapterConfig(
  connectionString: string,
  cpuCount: number = availableParallelism()
): PostgresAdapterConfig {
  const url = new URL(connectionString);
  const params = url.searchParams;

  const schema = params.get('schema') || undefined;
  const max =
    readNonNegativeInteger(params.get('connection_limit')) ||
    cpuCount * CONNECTIONS_PER_CPU + 1;
  // pg has one timeout for both waiting on a busy pool and opening a
  // connection; Prisma's pool_timeout is the one users tuned for load.
  const poolTimeoutSeconds =
    readNonNegativeInteger(params.get('pool_timeout')) ??
    DEFAULT_POOL_TIMEOUT_SECONDS;

  keepPrismaSslSemantics(params);
  for (const param of PRISMA_ONLY_PARAMS) {
    params.delete(param);
  }

  return {
    pool: {
      connectionString: url.toString(),
      max,
      connectionTimeoutMillis: poolTimeoutSeconds * MILLISECONDS_PER_SECOND,
    },
    schema,
  };
}

export function createPostgresAdapter(
  connectionString: string,
  onError?: (error: Error) => void
): PrismaPg {
  const { pool, schema } = postgresAdapterConfig(connectionString);
  return new PrismaPg(pool, {
    schema,
    onPoolError: onError,
    onConnectionError: onError,
  });
}
