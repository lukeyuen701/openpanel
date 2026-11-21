import {
  chMigrationClient,
  runClickhouseMigrationCommands,
} from '../clickhouse/migration';
import { type CodeMigrationEnv, getIsCluster, writeSqlDump } from './helpers';

/**
 * Writes `events.inserted_at` into the parts that predate it.
 *
 * 22-add-events-inserted-at.ts added the column with `DEFAULT created_at` and
 * a minmax index on it, but never wrote the column into existing parts, so
 * on installs upgraded with data a DELETE that reaches such a part (deleting a
 * project, deleting a profile's events) can fail with
 * `Not found column inserted_at` and stay stuck in system.mutations.
 *
 * MATERIALIZE COLUMN fills the column where a part lacks it and keeps the
 * values new rows were written with, so the export cursor does not move. It
 * rewrites that one column in every part, which on a large table takes long:
 * the migration submits it and does not wait. A delete submitted after it
 * waits behind it, part by part. Progress:
 *
 *   SELECT * FROM system.mutations WHERE command LIKE '%MATERIALIZE COLUMN inserted_at%';
 *
 * A delete that is already stuck blocks the rewrite on its parts too: kill it
 * (`KILL MUTATION WHERE mutation_id = '…'`) and submit it again once the
 * rewrite is done.
 *
 * To run the rewrite off-peak before upgrading, apply it manually; the
 * migration skips it when a completed one is on record (on every replica when
 * clustered):
 *
 *   Non-clustered: ALTER TABLE events MATERIALIZE COLUMN inserted_at;
 *   Clustered:     ALTER TABLE events_replicated ON CLUSTER '{cluster}' MATERIALIZE COLUMN inserted_at;
 *
 * Rerunning it rewrites the column again and changes nothing else.
 */

const COLUMN = 'inserted_at';
// Token-boundary match so a similarly named column (inserted_at_v2) can never
// satisfy the check.
const COMMAND_PATTERN = `MATERIALIZE COLUMN ${COLUMN}($|[^A-Za-z0-9_])`;

async function hasCompletedMaterialization(
  table: string,
  isClustered: boolean
): Promise<boolean> {
  const matchExpr = `match(command, '${COMMAND_PATTERN}') AND is_done = 1`;
  try {
    if (!isClustered) {
      const res = await chMigrationClient.query({
        query: `SELECT count() AS c FROM system.mutations
                WHERE database = currentDatabase() AND table = '${table}'
                  AND ${matchExpr}`,
        format: 'JSONEachRow',
      });
      const [row] = await res.json<{ c: string | number }>();
      return Number(row?.c ?? 0) > 0;
    }

    // A completed mutation on the connected host does not prove the other
    // replicas finished theirs. (hostName(), tcpPort()) because replicas on
    // one machine share a hostname.
    const res = await chMigrationClient.query({
      query: `SELECT
                (SELECT countDistinct((hostName(), tcpPort())) FROM clusterAllReplicas('{cluster}', system.one)) AS total,
                countDistinct((hostName(), tcpPort())) AS done
              FROM clusterAllReplicas('{cluster}', system.mutations)
              WHERE database = currentDatabase() AND table = '${table}'
                AND ${matchExpr}`,
      format: 'JSONEachRow',
    });
    const [row] = await res.json<{
      total: string | number;
      done: string | number;
    }>();
    return (
      Number(row?.total ?? 0) > 0 &&
      Number(row?.done ?? 0) >= Number(row?.total ?? 0)
    );
  } catch {
    // Unverifiable (no grant on system.mutations or the cluster functions):
    // rewriting again is harmless, skipping it is not.
    return false;
  }
}

export async function up(env: CodeMigrationEnv) {
  const isClustered = getIsCluster(env);
  const table = isClustered ? 'events_replicated' : 'events';
  const onCluster = isClustered ? " ON CLUSTER '{cluster}'" : '';

  const sqls = (await hasCompletedMaterialization(table, isClustered))
    ? []
    : [`ALTER TABLE ${table}${onCluster} MATERIALIZE COLUMN ${COLUMN}`];

  writeSqlDump(import.meta.url, sqls);

  if (!process.argv.includes('--dry')) {
    await runClickhouseMigrationCommands(sqls);
  }
}
