import { sql } from '@openpanel/db/src/clickhouse/sql';
import { getRedisCache, type Redis } from '@openpanel/redis';
import { getSafeJson } from '@openpanel/shared';
import { replicatedTarget, TABLE_NAMES } from '../shared/ch-tables';
import { BaseBuffer, type BufferDeps } from './base-buffer';

export interface ProfileBackfillEntry {
  projectId: string;
  sessionId: string;
  profileId: string;
}

// Sessions per UPDATE. Each binds four URL parameters and ClickHouse rejects a
// request with more than http_max_fields (1000) of them ("Too many form
// fields"), so 500 sessions failed on every tick above about 245.
export const BACKFILL_SESSIONS_PER_UPDATE = 200;

const DEFAULT_BATCH_SIZE = 1000;

export class ProfileBackfillBuffer extends BaseBuffer {
  private readonly batchSize =
    this.deps.config.buffers.profileBackfill.batchSize ?? DEFAULT_BATCH_SIZE;

  private readonly redisKey = 'profile-backfill-buffer';
  private redis: Redis;

  constructor(deps: BufferDeps) {
    super(deps, {
      name: 'profile-backfill',
      onFlush: async () => {
        await this.processBuffer();
      },
    });
    this.redis = getRedisCache();
  }

  async add(entry: ProfileBackfillEntry) {
    return this.timeAdd(async () => {
      try {
        this.logger.info({ entry }, 'Adding profile backfill entry');
        await this.redis.rpush(this.redisKey, JSON.stringify(entry));
      } catch (error) {
        this.logger.error(
          { err: error },
          'Failed to add profile backfill entry'
        );
      }
    });
  }

  protected getRedisListKey(): string {
    return this.redisKey;
  }

  async processBuffer() {
    const lrangeStart = performance.now();
    const raw = await this.redis.lrange(this.redisKey, 0, this.batchSize - 1);
    const lrangeMs = performance.now() - lrangeStart;

    if (raw.length === 0) {
      this.reportFlushStats({ rowsProcessed: 0, phases: { lrangeMs } });
      return;
    }

    // Deduplicate by sessionId — last write wins (most recent profileId)
    const seen = new Map<string, ProfileBackfillEntry>();
    for (const r of raw) {
      const parsed = getSafeJson<ProfileBackfillEntry>(r);
      if (parsed) {
        seen.set(parsed.sessionId, parsed);
      }
    }
    const entries = Array.from(seen.values());

    const ch = this.resolveCh();
    // `replicatedTarget`, not `sql.id`: the clustered form carries an
    // `ON CLUSTER` clause and is not a bare identifier.
    const target = replicatedTarget(
      this.deps.config.clickhouseClustered,
      TABLE_NAMES.events
    );

    const chunks = this.chunks(entries, BACKFILL_SESSIONS_PER_UPDATE);

    const chStart = performance.now();
    for (const chunk of chunks) {
      const caseClause = sql.join(
        chunk.map(
          ({ sessionId, profileId }) =>
            sql`WHEN ${sql.string(sessionId)} THEN ${sql.string(profileId)}`
        ),
        '\n'
      );
      const tupleList = sql.join(
        chunk.map(
          ({ projectId, sessionId }) =>
            sql`(${sql.string(projectId)}, ${sql.string(sessionId)})`
        ),
        ', '
      );

      const statement = sql`
        UPDATE ${target}
        SET profile_id = CASE session_id
          ${caseClause}
        END
        WHERE (project_id, session_id) IN (${tupleList})
          AND created_at > now() - INTERVAL 6 HOURS`.toStatement();

      await ch.command({
        query: statement.query,
        query_params: statement.query_params,
        clickhouse_settings: {
          mutations_sync: '0',
          allow_experimental_lightweight_update: '1',
        },
      });
    }
    const chInsertMs = performance.now() - chStart;

    // A failing chunk throws out of the loop above, so nothing is trimmed
    // unless every chunk landed; the UPDATE is idempotent, so the next tick
    // simply sends the whole batch again.
    const trimStart = performance.now();
    await this.redis.ltrim(this.redisKey, raw.length, -1);
    const trimMs = performance.now() - trimStart;

    this.reportFlushStats({
      rowsProcessed: raw.length,
      phases: { lrangeMs, chInsertMs, trimMs },
    });
  }
}
