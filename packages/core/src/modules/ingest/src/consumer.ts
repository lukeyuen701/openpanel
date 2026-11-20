// Delivery contract: per-key serial groups, the ascending contiguous-prefix
// offset walk, at-least-once, bounded in-consumer retry, and a dead-letter on
// exhaustion. The dead-letter destination is a capped Redis list, and the
// event is dropped whether or not that write lands.

import {
  type Consumer,
  type Message,
  type MessagesStream,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
} from '@platformatic/kafka';
import type { IncomingEventPayload } from './incoming-event';
import {
  createPartitionLanes,
  type LaneBatchControl,
  type LaneBatchOutcome,
} from './partition-lanes';

export interface KafkaConsumerHandle {
  /**
   * Stop fetching, then wait for the in-flight batches to finish. Split out
   * from `stop` so a graceful shutdown has a point where nothing new can enter
   * the event buffer but the consumer has not been torn down yet — that is
   * where shutdown flushes whatever was buffered OUTSIDE a Kafka batch. Events
   * buffered inside one are already durable.
   */
  stopConsuming: () => Promise<void>;
  /** Leave the group and close. Safe to call after `stopConsuming`, and without it. */
  stop: () => Promise<void>;
}

// Mark the loop alive every N messages within a batch, so readiness can tell a
// long batch that is moving from one that is wedged.
const ACTIVITY_EVERY = 16;

/** Batches each partition may have waiting behind the one it is running. */
const BUFFERED_BATCHES_PER_LANE = 2;
/** A stream that died is reopened after this, doubling per failure in a row. */
const STREAM_RESTART_INITIAL_MS = 1000;
const STREAM_RESTART_MAX_MS = 30_000;

const RETRY_BACKOFF_FACTOR = 2;

/**
 * Ceiling on the wait a batch takes before it returns from a failed durability
 * flush and lets the broker redeliver it.
 *
 * The wait itself is seeded from `initialRetryMs` and doubled per consecutive
 * failure — the consumer's own retry convention — but it is capped HERE rather
 * than at `maxRetryMs`, which is sized for a flaky handler (1s by default) and
 * not for a dependency that is down. Without any cap a failed flush redelivers
 * in ~90ms and spins, hammering Redis and Kafka at the worst possible moment.
 * 5s keeps a ~15s outage to single-digit redeliveries. The client heartbeats
 * on its own, so the wait cannot cost the consumer its group membership.
 */
const DURABILITY_RETRY_MAX_MS = 5000;

/** Why a message was dead-lettered. A label value — keep the set small. */
export type DeadLetterReason = 'parse_error' | 'handler_error';

/** Header values are raw bytes, as the producer wrote them. */
export type KafkaHeaders = Record<string, Buffer>;

/** One message of a batch: what the handler needs, offsets as strings. */
export interface ConsumedMessage {
  key: Buffer | null;
  value: Buffer | null;
  headers: KafkaHeaders;
  offset: string;
}

/** One partition's slice of the stream, handed to `eachBatch`. */
export interface EventsBatchPayload {
  batch: { topic: string; partition: number; messages: ConsumedMessage[] };
  /** Marks a message handled; the highest one resolved is committed. */
  resolveOffset: (offset: string) => void;
  isRunning: () => boolean;
  /** The partition was taken away by a rebalance. */
  isStale: () => boolean;
}

/**
 * What the consumer hands to the dead-letter sink. The value stays the
 * producer's original bytes, so the record is a faithful copy of what arrived.
 *
 * Implemented by `@openpanel/redis`'s `createDeadLetterRecorder` (the capped
 * list) and `./kafka.ts`'s `produceDeadLetterEvent` (the DLQ topic; a message
 * produced from these bytes is replayable onto the events topic unchanged).
 */
export interface DeadLetterMessage {
  key: Buffer | null;
  value: Buffer | null;
  headers?: KafkaHeaders;
  topic: string;
  partition: number;
  offset: string;
  reason: string;
  error: string;
}

export interface ConsumerLogger {
  info: (obj: object, msg?: string) => void;
  warn: (obj: object, msg?: string) => void;
  error: (obj: object, msg?: string) => void;
}

export interface ConsumerMetrics {
  handlerFailed: (partition: string) => void;
  /** The message was recorded in the dead-letter list and DROPPED. */
  deadLettered: (partition: string, reason: DeadLetterReason) => void;
  /**
   * The message was DROPPED WITHOUT being recorded — the dead-letter write
   * failed. Not "will be retried": nothing retries it.
   */
  deadLetterFailed: (partition: string) => void;
}

export interface EventsBatchHandlerDeps {
  handleEvent: (
    payload: IncomingEventPayload,
    meta: { partition: number; offset: string }
  ) => Promise<unknown>;
  /**
   * Park a poison message somewhere an operator can read it. It may reject —
   * the caller drops the message and resolves the offset regardless.
   */
  sendToDeadLetter: (message: DeadLetterMessage) => Promise<void>;
  /**
   * Opens a durability window over the event buffer and returns the gate that
   * closes it — `EventBuffer.openDurabilityWindow`.
   *
   * Opened BEFORE the batch's first handler runs, and awaited once every
   * message has been handled and BEFORE the first `resolveOffset`, because
   * the resolved offsets are committed as soon as `eachBatch` returns — not
   * at `stopConsuming`.
   *
   * The gate MUST reject when the events THIS batch buffered are not in
   * Redis: events that exist only in the event buffer's in-process array
   * would otherwise be lost with their committed offsets. The window is what
   * makes "this batch's events are durable" answerable, since a failed write
   * drops events instead of keeping them for the broker to redeliver.
   */
  openDurabilityWindow: () => () => Promise<void>;
  logger: ConsumerLogger;
  metrics: ConsumerMetrics;
  onActivity: () => void;
  topic: string;
  maxAttempts: number;
  initialRetryMs: number;
  maxRetryMs: number;
  /** Injected by the tests; production uses the timer below. */
  sleep?: (ms: number) => Promise<void>;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const errorMessage = (err: unknown): string =>
  err instanceof Error ? err.message : String(err);

export interface EventsBatchHandler {
  /** `redeliver`: the batch's events are not durable; nothing was resolved. */
  eachBatch: (payload: EventsBatchPayload) => Promise<LaneBatchOutcome>;
  /**
   * Called on a group join: a new assignment may hand this member different
   * partitions, so the per-partition durability backoff below starts over.
   */
  resetDurabilityBackoff: () => void;
}

/**
 * The `eachBatch` handler, with its dependencies injected so the offset walk,
 * the per-key serial groups and the failure paths are testable without a
 * broker.
 *
 * Delivery contract: at-least-once. An offset is resolved only once its message
 * has been handled, dead-lettered, or deliberately skipped — never merely
 * because it failed. A dead-letter always counts as finished, because the
 * message is dropped either way; the one place an unresolved offset is still
 * correct is a failed DURABILITY flush, which leaves the whole batch for
 * redelivery.
 */
export function createEventsBatchHandler(
  deps: EventsBatchHandlerDeps
): EventsBatchHandler {
  const sleep = deps.sleep ?? defaultSleep;

  /**
   * Park the message and DROP it. The drop is unconditional: whether the record
   * was stored or not, this message's offset is resolved by the caller and the
   * event is gone from the pipeline.
   *
   * There is deliberately no retry, no backoff and no unresolved offset here.
   * The dead-letter destination is Redis, and Redis being unavailable is
   * exactly when handlers fail, so a failed write that held the offset back
   * would recreate the redelivery loop: once something else owns the outcome,
   * a safety net underneath it is the bug.
   * The two counters are what carry the volume, since a capped list makes
   * 50,000 drops look like 12: `deadLettered` = recorded and dropped,
   * `deadLetterFailed` = dropped WITHOUT being recorded. Neither means "will be
   * retried".
   */
  const deadLetter = async (
    message: ConsumedMessage,
    partition: number,
    reason: DeadLetterReason,
    err: unknown
  ): Promise<void> => {
    const label = String(partition);
    try {
      await deps.sendToDeadLetter({
        key: message.key,
        value: message.value,
        headers: message.headers,
        topic: deps.topic,
        partition,
        offset: message.offset,
        reason,
        error: errorMessage(err),
      });
    } catch (dlqErr) {
      deps.metrics.deadLetterFailed(label);
      // The WRITE failure goes in `err` — it is the news, and only that field
      // gets the logger's error serializer. What the message originally did
      // wrong rides along as text.
      deps.logger.error(
        {
          err: dlqErr,
          cause: errorMessage(err),
          partition,
          offset: message.offset,
          reason,
        },
        'kafka message DROPPED without being recorded — dead-letter write failed'
      );
      return;
    }
    deps.metrics.deadLettered(label, reason);
    deps.logger.error(
      { err, partition, offset: message.offset, reason },
      'kafka message dead-lettered — recorded and dropped'
    );
  };

  const runHandlerWithRetry = async (
    payload: IncomingEventPayload,
    partition: number,
    offset: string
  ): Promise<{ ok: true } | { ok: false; err: unknown }> => {
    const label = String(partition);
    let delayMs = deps.initialRetryMs;
    let lastErr: unknown;

    for (let attempt = 1; attempt <= deps.maxAttempts; attempt++) {
      try {
        await deps.handleEvent(payload, { partition, offset });
        return { ok: true };
      } catch (err) {
        lastErr = err;
        deps.metrics.handlerFailed(label);
        deps.logger.warn(
          {
            err,
            partition,
            offset,
            projectId: payload.projectId,
            attempt,
            maxAttempts: deps.maxAttempts,
          },
          'kafka incomingEvent handler failed'
        );
        if (attempt < deps.maxAttempts) {
          await sleep(delayMs);
          delayMs = Math.min(delayMs * RETRY_BACKOFF_FACTOR, deps.maxRetryMs);
        }
      }
    }

    return { ok: false, err: lastErr };
  };

  /**
   * Always returns: the offset is resolvable either way. A message is handled,
   * or it is dead-lettered and dropped — there is no third outcome, which is
   * why the batch loop below has no unresolvable-message branch.
   */
  const processMessage = async (
    message: ConsumedMessage,
    partition: number
  ): Promise<void> => {
    let payload: IncomingEventPayload;
    try {
      if (!message.value) {
        throw new Error('kafka message has no value');
      }
      payload = JSON.parse(message.value.toString()) as IncomingEventPayload;
    } catch (err) {
      deps.logger.error(
        { err, partition, offset: message.offset },
        'kafka message parse failed'
      );
      // Deterministic: retrying identical bytes cannot succeed.
      await deadLetter(message, partition, 'parse_error', err);
      return;
    }

    const result = await runHandlerWithRetry(
      payload,
      partition,
      message.offset
    );
    if (result.ok) {
      return;
    }
    await deadLetter(message, partition, 'handler_error', result.err);
  };

  // Consecutive durability failures per `topic-partition`, for the backoff
  // below. Cleared the moment a batch's events are durable again.
  const durabilityFailures = new Map<string, number>();

  /**
   * @returns false when the flush failed, which resolves no offset and runs
   * the whole batch again.
   */
  const bufferedEventsAreDurable = async (
    partition: number,
    closeDurabilityWindow: () => Promise<void>
  ): Promise<boolean> => {
    try {
      await closeDurabilityWindow();
      return true;
    } catch (err) {
      deps.logger.error(
        { err, partition },
        'buffered events could not be made durable — batch left unresolved for redelivery'
      );
      return false;
    }
  };

  /**
   * Hold the partition before returning, so the redelivery this batch just
   * asked for does not run in ~90ms and fail the same way.
   */
  const waitForRedelivery = async (
    partitionKey: string,
    partition: number
  ): Promise<void> => {
    const failures = (durabilityFailures.get(partitionKey) ?? 0) + 1;
    durabilityFailures.set(partitionKey, failures);
    const delayMs = Math.min(
      deps.initialRetryMs * RETRY_BACKOFF_FACTOR ** (failures - 1),
      DURABILITY_RETRY_MAX_MS
    );
    // A member that is deliberately waiting must not look like one that has
    // died.
    deps.onActivity();
    deps.logger.warn(
      { partition, consecutiveFailures: failures, delayMs },
      'waiting before running the batch again'
    );
    await sleep(delayMs);
  };

  const eachBatch = async ({
    batch,
    resolveOffset,
    isRunning,
    isStale,
  }: EventsBatchPayload): Promise<LaneBatchOutcome> => {
    if (batch.messages.length === 0) {
      return 'done';
    }

    const pk = `${batch.topic}-${batch.partition}`;
    // Opened before the first handler buffers anything, so the gate answers for
    // THIS batch's events and not for whatever else was pending when it
    // finished.
    const closeDurabilityWindow = deps.openDurabilityWindow();

    // Group by partition key (= deviceId or `${projectId}:${profileId}`).
    // Same-key messages stay serial so sessionBuffer/session-end-job state
    // can't race; different keys run in parallel via Promise.all.
    // Keyless messages get their own singleton group.
    const groups = new Map<string, ConsumedMessage[]>();
    for (const m of batch.messages) {
      const key = m.key ? m.key.toString() : `__no_key__:${m.offset}`;
      const arr = groups.get(key);
      if (arr) {
        arr.push(m);
      } else {
        groups.set(key, [m]);
      }
    }

    // Offsets that finished processing this batch. We resolve them AFTER all
    // groups complete, in strict ascending order (see the loop below).
    //
    // Why: the highest resolved offset is what gets committed. If we resolved
    // inside the concurrent per-key loop, a higher offset finishing before a
    // lower one would commit past a message that has not been handled yet.
    // Resolving the contiguous ascending prefix at the end avoids both
    // duplicates (never regress) and loss (stop at the first gap).
    const processed = new Set<string>();
    let processedCount = 0;

    await Promise.all(
      [...groups.values()].map(async (msgs) => {
        for (const m of msgs) {
          if (!isRunning() || isStale()) {
            return;
          }

          // No "unresolvable" branch: a message is handled or dropped, and a
          // dropped one is still finished. The only gap this walk can now stop
          // at is the isRunning/isStale early return above, and the only
          // offsets left uncommitted are a whole batch whose durability flush
          // failed — see below.
          await processMessage(m, batch.partition);

          processed.add(m.offset);
          processedCount += 1;
          if (processedCount % ACTIVITY_EVERY === 0) {
            deps.onActivity();
          }
        }
      })
    );

    // Durability before commit. Handled is not the same as safe: the events
    // are in an in-process array until they are pushed to Redis, and every
    // offset resolved below is committed the instant this handler returns. If
    // that push will not land, resolve NOTHING and run the whole batch again —
    // a duplicate is recoverable, a loss is not.
    const durable = await bufferedEventsAreDurable(
      batch.partition,
      closeDurabilityWindow
    );
    if (durable) {
      durabilityFailures.delete(pk);
      // Resolve in strict ascending offset order, stopping at the first offset
      // that did not finish (e.g. an isStale/isRunning early-return mid-batch).
      // batch.messages is already ordered by offset.
      for (const m of batch.messages) {
        if (!processed.has(m.offset)) {
          break;
        }
        resolveOffset(m.offset);
      }
    } else if (isRunning() && !isStale()) {
      // Not while shutting down: the redelivery is the next process's problem
      // and the wait would only eat the shutdown budget.
      await waitForRedelivery(pk, batch.partition);
    }

    deps.onActivity();
    return durable ? 'done' : 'redeliver';
  };

  return {
    eachBatch,
    resetDurabilityBackoff: () => {
      durabilityFailures.clear();
    },
  };
}

/** The broker-facing half, supplied by whoever owns the Kafka client. */
export interface EventsConsumerDeps {
  /** Already configured with the group id, timeouts and fetch bounds. */
  createConsumer: (options?: { groupId?: string }) => Consumer;
  /** Lifecycle / rebalance visibility. */
  logger: ConsumerLogger;
  /** The "consumer running" line, on the kafka-scoped logger. */
  kafkaLogger: ConsumerLogger;
  topic: string;
  partitionsConsumedConcurrently: number;
  /** Messages per batch, and so the most one commit covers. */
  maxMessagesPerBatch: number;
  batch: Omit<EventsBatchHandlerDeps, 'topic'>;
  /** Injected by the tests; production uses the timer. */
  sleep?: (ms: number) => Promise<void>;
}

interface StreamedMessage {
  offset: bigint;
  leaderEpoch: number;
  message: ConsumedMessage;
}

const toConsumedMessage = (message: Message): ConsumedMessage => {
  const headers: KafkaHeaders = {};
  for (const [name, value] of message.headers) {
    headers[name.toString()] = value;
  }
  return {
    key: message.key ?? null,
    value: message.value ?? null,
    headers,
    offset: String(message.offset),
  };
};

const assignedPartitions = (
  topic: string,
  assignments: { topic: string; partitions: number[] }[] | null | undefined
): number[] | null => {
  if (!assignments) {
    return null;
  }
  return assignments
    .filter((assignment) => assignment.topic === topic)
    .flatMap((assignment) => assignment.partitions);
};

export async function startKafkaEventsConsumer(
  deps: EventsConsumerDeps
): Promise<KafkaConsumerHandle> {
  const { logger, topic } = deps;
  const sleep = deps.sleep ?? defaultSleep;
  const consumer = deps.createConsumer();
  const handler = createEventsBatchHandler({ ...deps.batch, topic });

  let stream: MessagesStream<Buffer, Buffer, Buffer, Buffer> | null = null;
  let stopping = false;
  let restarting = false;
  let consecutiveRestarts = 0;
  let consumingStopped: Promise<void> | null = null;
  // Set when the current stream fails, so a reopen that was still joining
  // the group does not report success with a stream that already died.
  let streamFailed = false;

  const lanes = createPartitionLanes<StreamedMessage>({
    concurrency: deps.partitionsConsumedConcurrently,
    maxBatchSize: deps.maxMessagesPerBatch,
    maxBuffered:
      deps.partitionsConsumedConcurrently *
      deps.maxMessagesPerBatch *
      BUFFERED_BATCHES_PER_LANE,
    runBatch: (partition, items, control: LaneBatchControl) =>
      handler.eachBatch({
        batch: {
          topic,
          partition,
          messages: items.map((item) => item.message),
        },
        resolveOffset: (offset) => control.resolveOffset(BigInt(offset)),
        isRunning: control.isRunning,
        isStale: control.isStale,
      }),
    commit: (partition, offset, leaderEpoch) =>
      consumer.commit({ offsets: [{ topic, partition, offset, leaderEpoch }] }),
    pauseSource: () => {
      stream?.pause();
    },
    resumeSource: () => {
      stream?.resume();
    },
    logger,
    sleep: deps.sleep,
  });

  const closeStream = async (): Promise<void> => {
    const current = stream;
    stream = null;
    await lanes.quiesce();
    await current?.close().catch(() => undefined);
    lanes.reset();
  };

  // `consume()` joins the group only for a consumer that never had a member
  // id. One whose membership was lost keeps a stale id, so its new stream
  // would fetch the old partitions from outside the group.
  const ensureMembership = async (): Promise<void> => {
    if (!consumer.isActive()) {
      await consumer.joinGroup();
    }
  };

  // A stream that errored is unusable, but the consumer is not: open a new one
  // from the committed offsets, back in the group.
  const restartStream = async (): Promise<void> => {
    if (stopping || restarting) {
      return;
    }
    restarting = true;
    await closeStream();
    while (!stopping) {
      const delayMs = Math.min(
        STREAM_RESTART_INITIAL_MS * RETRY_BACKOFF_FACTOR ** consecutiveRestarts,
        STREAM_RESTART_MAX_MS
      );
      consecutiveRestarts += 1;
      logger.warn(
        { delayMs, attempt: consecutiveRestarts },
        'reopening the kafka events stream'
      );
      await sleep(delayMs);
      if (stopping) {
        break;
      }
      try {
        await openStream();
        await ensureMembership();
        if (streamFailed) {
          throw new Error('the reopened stream failed while rejoining');
        }
        logger.info(
          { attempt: consecutiveRestarts },
          'kafka events stream reopened'
        );
        break;
      } catch (err) {
        logger.error({ err }, 'kafka events stream could not be reopened');
        await closeStream();
      }
    }
    restarting = false;
  };

  const openStream = async (): Promise<void> => {
    // Only decides where a partition WITHOUT a committed offset starts: a new
    // install, a new partition or a renamed group. From the newest message it
    // would skip everything produced before this consumer first joined; from
    // the oldest it can at worst re-read the topic's retention as duplicates.
    const opened = await consumer.consume({
      topics: [topic],
      mode: MessagesStreamModes.COMMITTED,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST,
      autocommit: false,
    });
    opened.on('data', (message) => {
      consecutiveRestarts = 0;
      lanes.push(message.partition, {
        offset: message.offset,
        leaderEpoch: message.leaderEpoch,
        message: toConsumedMessage(message),
      });
    });
    // An idle partition set still fetches every `maxWaitTime`. Busy lanes
    // report progress themselves, so a wedged one is not covered up here, and
    // a stream fetching from outside the group is not a healthy consumer.
    opened.on('fetch', () => {
      if (lanes.activeCount() === 0 && consumer.isActive()) {
        deps.batch.onActivity();
      }
    });
    opened.on('error', (err) => {
      if (stream === opened) {
        streamFailed = true;
      }
      logger.error(
        { err, groupId: consumer.groupId },
        'kafka consumer crashed'
      );
      restartStream().catch(() => undefined);
    });
    stream = opened;
    streamFailed = false;
    lanes.setAssignment(assignedPartitions(topic, consumer.assignments));
  };

  // Lifecycle / rebalance logging: a rebalance storm (a common source of
  // at-least-once duplicates) would otherwise be invisible.
  consumer.on('consumer:group:join', (payload) => {
    // A new assignment means partitions may have moved between members, so a
    // consecutive-failure count carried over from the old one would pick the
    // wrong backoff for the new one.
    handler.resetDurabilityBackoff();
    lanes.setAssignment(assignedPartitions(topic, payload.assignments));
    logger.info(
      {
        memberId: payload.memberId,
        groupId: payload.groupId,
        generationId: payload.generationId,
        isLeader: payload.isLeader,
        assignments: payload.assignments,
      },
      'kafka consumer joined group (rebalance complete)'
    );
  });
  consumer.on('consumer:group:rebalance', (payload) => {
    logger.warn(
      { memberId: consumer.memberId, groupId: payload.groupId },
      'kafka consumer rebalancing'
    );
  });
  consumer.on('consumer:heartbeat:stalled', (payload) => {
    logger.warn(
      { err: payload.lastError, stalledForMs: payload.stalledFor },
      'kafka consumer heartbeats are failing'
    );
  });
  consumer.on('consumer:group:lost', (payload) => {
    logger.error(
      {
        err: payload.error,
        groupId: payload.groupId,
        operation: payload.operation,
      },
      'kafka consumer lost its group membership'
    );
    restartStream().catch(() => undefined);
  });
  // A rejoin that failed for good is reported here, and nothing reschedules it:
  // without a listener the error is thrown and takes the process down.
  consumer.on('error', (err) => {
    logger.error(
      { err, groupId: consumer.groupId },
      'kafka consumer failed to rejoin its group'
    );
    restartStream().catch(() => undefined);
  });
  consumer.on('client:close', () => {
    logger.warn({}, 'kafka consumer disconnected');
  });

  // Fails the boot, like a refused connect did: a wrong broker list or
  // credential has to stop the deploy, not surface later as a stale probe.
  await openStream();

  deps.kafkaLogger.info(
    {
      topic,
      partitionsConsumedConcurrently: deps.partitionsConsumedConcurrently,
    },
    'kafka events consumer running'
  );

  const stopConsuming = (): Promise<void> => {
    consumingStopped ??= (async () => {
      stopping = true;
      stream?.pause();
      await lanes.stop();
      await stream?.close().catch((err) => {
        logger.warn({ err }, 'kafka events stream close failed');
      });
      stream = null;
    })();
    return consumingStopped;
  };

  return {
    stopConsuming,
    stop: async () => {
      await stopConsuming();
      await consumer.close(true);
    },
  };
}
