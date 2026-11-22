import { afterAll, describe, expect, it } from 'bun:test';
import { createServer, type Server } from 'node:http';
import type { AddressInfo, Socket } from 'node:net';
import type { Readable } from 'node:stream';

// Subject imported directly: it is the round-robin client the buffers get as
// `BufferDeps.ch` (same exemption as clickhouse-failover.test.ts).
const DATABASE = 'openpanel_test';
const DEFAULT_BASE_URL = 'http://localhost:23123';
const ROW_COUNT = 5;
const REQUEST_TIMEOUT_MS = 120_000;

const configured = (process.env.CLICKHOUSE_URL ?? '').split(',')[0]?.trim();
const upstream = new URL(configured || DEFAULT_BASE_URL);
upstream.pathname = '/';
const upstreamBase = upstream.toString();
upstream.pathname = `/${DATABASE}`;
const upstreamWithDatabase = upstream.toString();

async function reachable(): Promise<boolean> {
  try {
    return (await fetch(upstreamBase, { method: 'POST', body: 'SELECT 1' })).ok;
  } catch {
    return false;
  }
}

const originalEnv = {
  CLICKHOUSE_URL: process.env.CLICKHOUSE_URL,
  CLICKHOUSE_REQUEST_TIMEOUT_MS: process.env.CLICKHOUSE_REQUEST_TIMEOUT_MS,
  CLICKHOUSE_UNHEALTHY_MARK_MS: process.env.CLICKHOUSE_UNHEALTHY_MARK_MS,
};

/** Resets the connection after reading the first body bytes of the first insert. */
let resetsLeft = 1;
const proxy: Server = createServer((request, response) => {
  const chunks: Buffer[] = [];
  let wasReset = false;
  request.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    if (resetsLeft > 0 && request.method === 'POST') {
      resetsLeft--;
      wasReset = true;
      request.socket.destroy();
    }
  });
  request.on('end', async () => {
    if (wasReset) {
      return;
    }
    const upstreamResponse = await fetch(
      `${upstreamBase.replace(/\/$/, '')}${request.url}`,
      {
        method: request.method,
        headers: request.headers as Record<string, string>,
        body: Buffer.concat(chunks),
      }
    );
    response.writeHead(
      upstreamResponse.status,
      Object.fromEntries(
        [...upstreamResponse.headers].filter(
          ([name]) => name !== 'content-encoding' && name !== 'content-length'
        )
      )
    );
    response.end(Buffer.from(await upstreamResponse.arrayBuffer()));
  });
});

const sockets = new Set<Socket>();
proxy.on('connection', (socket) => {
  sockets.add(socket);
  socket.on('close', () => sockets.delete(socket));
});

const isReachable = await reachable();
if (isReachable) {
  await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve));
  const { port } = proxy.address() as AddressInfo;
  // Proxy is node 0 (resets), the real server node 1.
  process.env.CLICKHOUSE_URL = `http://127.0.0.1:${port}/${DATABASE},${upstreamWithDatabase}`;
  process.env.CLICKHOUSE_REQUEST_TIMEOUT_MS = String(REQUEST_TIMEOUT_MS);
  process.env.CLICKHOUSE_UNHEALTHY_MARK_MS = '0';
  process.env.CLICKHOUSE_REQUEST_COMPRESSION = 'false';
  const { bootstrapTestDatabases } = await import(
    '../../../../test/bootstrap-databases'
  );
  await bootstrapTestDatabases();
}

afterAll(() => {
  for (const [key, value] of Object.entries(originalEnv)) {
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
  for (const socket of sockets) {
    socket.destroy();
  }
  proxy.close();
});

function buildLines(projectId: string): string[] {
  return Array.from({ length: ROW_COUNT }, (_, index) =>
    JSON.stringify({
      id: crypto.randomUUID(),
      name: `reset_${index}`,
      project_id: projectId,
      device_id: 'reset-test-device',
      created_at: new Date().toISOString().replace('T', ' ').slice(0, 23),
    })
  );
}

describe.skipIf(!isReachable)(
  'ch.insert after a mid-body connection reset',
  () => {
    it('re-sends the whole replayable stream body on the retry', async () => {
      const { ch, chQuery, TABLE_NAMES } = await import(
        '@openpanel/db/src/clickhouse/client'
      );
      const { BaseBuffer } = await import('./base-buffer');
      const projectId = `wparity-reset-${crypto.randomUUID()}`;
      const lines = buildLines(projectId);
      const stream = (
        BaseBuffer.prototype as unknown as {
          jsonEachRowStream: (lines: string[]) => Readable;
        }
      ).jsonEachRowStream.call(Object.create(BaseBuffer.prototype), lines);

      resetsLeft = 1;
      await ch.insert({
        table: TABLE_NAMES.events,
        values: stream,
        format: 'JSONEachRow',
      });
      expect(resetsLeft).toBe(0);

      const rows = await chQuery<{ count: string }>(
        `SELECT count() AS count FROM ${TABLE_NAMES.events} WHERE project_id = '${projectId}'`
      );
      expect(Number(rows[0]?.count)).toBe(ROW_COUNT);
    });
  }
);
