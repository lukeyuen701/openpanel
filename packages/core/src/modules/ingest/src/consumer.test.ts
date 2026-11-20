/**
 * Delivery contract of the Kafka events consumer: at-least-once. An offset is
 * resolved once its message has been handled, dead-lettered, or deliberately
 * skipped — never merely because it failed.
 *
 * The batch handler is exercised directly with injected dependencies; the
 * broker, the metrics registry and `incomingEvent` are all out of the picture —
 * which is why no `mock.module` appears here.
 */

import { describe, expect, mock, test } from 'bun:test';
import { EventEmitter } from 'node:events';
import {
  createDeadLetterRecorder,
  type DeadLetterMulti,
  type DeadLetterRecord,
  type DeadLetterRedisClient,
} from '@openpanel/redis';
import type { Consumer } from '@platformatic/kafka';
import {
  type ConsumedMessage,
  createEventsBatchHandler,
  type DeadLetterMessage,
  type EventsBatchHandlerDeps,
  type EventsBatchPayload,
  startKafkaEventsConsumer,
} from './consumer';

const PARTITION = 7;
const TOPIC = 'events';

const payloadFor = (offset: number) => ({
  projectId: `project-${offset}`,
  event: { name: 'screen_view' },
});

const message = (
  offset: number,
  key: string | null,
  value: string = JSON.stringify(payloadFor(offset))
): ConsumedMessage => ({
  key: key === null ? null : Buffer.from(key),
  value: value === '' ? null : Buffer.from(value),
  headers: {},
  offset: String(offset),
});

function makeBatch(
  messages: ConsumedMessage[],
  options: { isRunning?: () => boolean; isStale?: () => boolean } = {}
) {
  const resolveOffset = mock((_offset: string) => undefined);
  const payload: EventsBatchPayload = {
    batch: { topic: TOPIC, partition: PARTITION, messages },
    resolveOffset,
    isRunning: options.isRunning ?? (() => true),
    isStale: options.isStale ?? (() => false),
  };
  return { payload, resolveOffset };
}

interface MockCalls {
  mock: { calls: unknown[][] };
}

const resolvedOffsets = (resolveOffset: MockCalls): string[] =>
  resolveOffset.mock.calls.map(([offset]) => offset as string);

type DepOverrides = Partial<
  Omit<EventsBatchHandlerDeps, 'openDurabilityWindow'>
> & {
  /** Sugar: the gate every window in this test closes with. */
  flushBufferedEvents?: () => Promise<void>;
};

function makeDeps(overrides: DepOverrides = {}) {
  const metrics: EventsBatchHandlerDeps['metrics'] = {
    handlerFailed: mock(() => undefined),
    deadLettered: mock(() => undefined),
    deadLetterFailed: mock(() => undefined),
  };
  const handleEvent = mock<EventsBatchHandlerDeps['handleEvent']>(
    async () => undefined
  );
  const sendToDeadLetter = mock<EventsBatchHandlerDeps['sendToDeadLetter']>(
    async () => undefined
  );
  const sleep = mock(async (_ms: number) => undefined);
  // The gate a durability window closes with. The tests drive it directly;
  // `openDurabilityWindow` below hands the same one to every batch, which is
  // also what makes "opened before the first handler" assertable.
  const flushBufferedEvents =
    overrides.flushBufferedEvents ?? mock(async (): Promise<void> => undefined);
  const openDurabilityWindow = mock(() => flushBufferedEvents);
  const { flushBufferedEvents: _gate, ...depOverrides } = overrides;
  const deps: EventsBatchHandlerDeps = {
    handleEvent,
    sendToDeadLetter,
    openDurabilityWindow,
    logger: {
      info: mock(() => undefined),
      warn: mock(() => undefined),
      error: mock(() => undefined),
    },
    onActivity: mock(() => undefined),
    topic: TOPIC,
    maxAttempts: 3,
    initialRetryMs: 10,
    maxRetryMs: 25,
    sleep,
    ...depOverrides,
    metrics: { ...metrics, ...overrides.metrics },
  };
  return {
    deps,
    metrics: deps.metrics as typeof metrics,
    handleEvent,
    sendToDeadLetter: (overrides.sendToDeadLetter ??
      sendToDeadLetter) as typeof sendToDeadLetter,
    flushBufferedEvents,
    openDurabilityWindow,
    sleep,
  };
}

describe('offset walk', () => {
  test('resolves the whole contiguous prefix in ascending order', async () => {
    const { deps } = makeDeps();
    const handler = createEventsBatchHandler(deps);
    // Two interleaved key-groups: they run concurrently, so completion order
    // is not offset order — the walk must still resolve 0..3 ascending.
    const { payload, resolveOffset } = makeBatch([
      message(0, 'a'),
      message(1, 'b'),
      message(2, 'a'),
      message(3, 'b'),
    ]);

    await handler.eachBatch(payload);

    expect(resolvedOffsets(resolveOffset)).toEqual(['0', '1', '2', '3']);
  });

  test('stops at the first gap, even when later offsets did finish', async () => {
    // Keys 'a' (0, 2, 4) and 'b' (1, 3, 5) run concurrently. 'a' is held on
    // its first message until 'b' has finished all of its own, then the
    // consumer stops — so 3 and 5 are processed while 2 never runs. Resolving
    // them would ack offset 2 as well (last-write-wins) and lose it.
    let running = true;
    let releaseA = (): void => undefined;
    const bFinished = new Promise<void>((resolve) => {
      releaseA = resolve;
    });
    const { deps, handleEvent } = makeDeps();
    handleEvent.mockImplementation(async (p: { projectId: string }) => {
      if (p.projectId === 'project-0') {
        await bFinished;
        running = false;
      }
      if (p.projectId === 'project-5') {
        releaseA();
      }
    });
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch(
      [
        message(0, 'a'),
        message(1, 'b'),
        message(2, 'a'),
        message(3, 'b'),
        message(4, 'a'),
        message(5, 'b'),
      ],
      { isRunning: () => running }
    );

    await handler.eachBatch(payload);

    const handled = handleEvent.mock.calls.map(([p]) => p.projectId);
    expect(handled).toContain('project-5');
    expect(handled).not.toContain('project-2');
    expect(resolvedOffsets(resolveOffset)).toEqual(['0', '1']);
  });

  test('resolves nothing when the batch is stale before the first message', async () => {
    const { deps, handleEvent } = makeDeps();
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch(
      [message(0, 'a'), message(1, 'a')],
      { isStale: () => true }
    );

    await handler.eachBatch(payload);

    expect(handleEvent).not.toHaveBeenCalled();
    expect(resolveOffset).not.toHaveBeenCalled();
  });
});

describe('kill consumer mid-batch', () => {
  test('loses no event: every unresolved offset is left for redelivery', async () => {
    const KILL_AFTER = 3;
    const TOTAL = 8;
    let handled = 0;
    let running = true;
    const { deps, handleEvent } = makeDeps();
    handleEvent.mockImplementation(async () => {
      handled += 1;
      if (handled >= KILL_AFTER) {
        running = false;
      }
    });
    const handler = createEventsBatchHandler(deps);
    // One key: strictly serial, so "killed after N" is deterministic.
    const messages = Array.from({ length: TOTAL }, (_, i) => message(i, 'a'));
    const { payload, resolveOffset } = makeBatch(messages, {
      isRunning: () => running,
    });

    await handler.eachBatch(payload);

    expect(handled).toBe(KILL_AFTER);
    expect(resolvedOffsets(resolveOffset)).toEqual(['0', '1', '2']);
    // The tail is not acked, so the broker redelivers it from offset 3.
    const unresolved = messages
      .slice(KILL_AFTER)
      .map((m) => m.offset)
      .filter((offset) => !resolvedOffsets(resolveOffset).includes(offset));
    expect(unresolved).toEqual(['3', '4', '5', '6', '7']);
  });
});

describe('handler failure', () => {
  test('retries with bounded backoff before giving up', async () => {
    const { deps, handleEvent, sleep, sendToDeadLetter } = makeDeps({
      maxAttempts: 4,
    });
    handleEvent.mockImplementation(() => Promise.reject(new Error('boom')));
    const handler = createEventsBatchHandler(deps);
    const { payload } = makeBatch([message(0, 'a')]);

    await handler.eachBatch(payload);

    expect(handleEvent).toHaveBeenCalledTimes(4);
    // 10 → 20 → 25 (clamped at maxRetryMs), and no sleep after the last try.
    expect(sleep.mock.calls.map(([ms]) => ms)).toEqual([10, 20, 25]);
    expect(sendToDeadLetter).toHaveBeenCalledTimes(1);
  });

  test('stops retrying as soon as the handler succeeds', async () => {
    const { deps, handleEvent, sendToDeadLetter, metrics } = makeDeps();
    let attempts = 0;
    handleEvent.mockImplementation(() => {
      attempts += 1;
      return attempts === 1
        ? Promise.reject(new Error('transient'))
        : Promise.resolve(undefined);
    });
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([message(0, 'a')]);

    await handler.eachBatch(payload);

    expect(handleEvent).toHaveBeenCalledTimes(2);
    expect(metrics.handlerFailed).toHaveBeenCalledTimes(1);
    expect(sendToDeadLetter).not.toHaveBeenCalled();
    expect(resolvedOffsets(resolveOffset)).toEqual(['0']);
  });

  test('dead-letters the raw message, counts it, and keeps the partition moving', async () => {
    const poison = 'poison-payload';
    const { deps, handleEvent, sendToDeadLetter, metrics } = makeDeps();
    handleEvent.mockImplementation(async (p: { projectId: string }) => {
      if (p.projectId === 'project-1') {
        throw new Error('handler exploded');
      }
    });
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([
      message(0, 'a'),
      message(1, 'a', JSON.stringify({ projectId: 'project-1', poison })),
      message(2, 'a'),
    ]);

    await handler.eachBatch(payload);

    const dlq = sendToDeadLetter.mock.calls[0]?.[0] as DeadLetterMessage;
    expect(dlq.reason).toBe('handler_error');
    expect(dlq.offset).toBe('1');
    expect(dlq.partition).toBe(PARTITION);
    expect(dlq.topic).toBe(TOPIC);
    // The raw producer bytes travel untouched, so the message is replayable.
    expect(JSON.parse(dlq.value?.toString() ?? '')).toMatchObject({ poison });

    expect(metrics.deadLettered).toHaveBeenCalledWith(
      String(PARTITION),
      'handler_error'
    );
    expect(metrics.handlerFailed).toHaveBeenCalledTimes(3);
    // The partition keeps moving: the poison offset and everything after it
    // are still resolved.
    expect(resolvedOffsets(resolveOffset)).toEqual(['0', '1', '2']);
  });

  test('resolves the offset even when the dead-letter write fails', async () => {
    const { deps, handleEvent, metrics } = makeDeps({
      sendToDeadLetter: mock(() => Promise.reject(new Error('redis down'))),
    });
    handleEvent.mockImplementation(async (p: { projectId: string }) => {
      if (p.projectId === 'project-1') {
        throw new Error('handler exploded');
      }
    });
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([
      message(0, 'a'),
      message(1, 'a'),
      message(2, 'a'),
    ]);

    await handler.eachBatch(payload);

    // Dropped WITHOUT being recorded — and still acked. Holding the offset
    // back here is what rebuilds the redelivery loop.
    expect(metrics.deadLetterFailed).toHaveBeenCalledWith(String(PARTITION));
    expect(metrics.deadLettered).not.toHaveBeenCalled();
    expect(resolvedOffsets(resolveOffset)).toEqual(['0', '1', '2']);
  });
});

describe('unparseable messages', () => {
  test('dead-letters instead of dropping, without retrying', async () => {
    const { deps, handleEvent, sendToDeadLetter, metrics, sleep } = makeDeps();
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([
      message(0, 'a', 'not json at all'),
    ]);

    await handler.eachBatch(payload);

    expect(handleEvent).not.toHaveBeenCalled();
    expect(sleep).not.toHaveBeenCalled();
    expect(metrics.deadLettered).toHaveBeenCalledWith(
      String(PARTITION),
      'parse_error'
    );
    expect(
      (
        sendToDeadLetter.mock.calls[0]?.[0] as DeadLetterMessage
      ).value?.toString()
    ).toBe('not json at all');
    expect(resolvedOffsets(resolveOffset)).toEqual(['0']);
  });

  test('dead-letters a message with no value at all', async () => {
    const { deps, sendToDeadLetter, metrics } = makeDeps();
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([message(0, 'a', '')]);

    await handler.eachBatch(payload);

    expect(sendToDeadLetter).toHaveBeenCalledTimes(1);
    expect(metrics.deadLettered).toHaveBeenCalledWith(
      String(PARTITION),
      'parse_error'
    );
    expect(resolvedOffsets(resolveOffset)).toEqual(['0']);
  });
});

/**
 * A redelivery must resolve and commit exactly what the resolve loop marks:
 * no watermark suppresses or alters it, so duplicates are never silently
 * skipped.
 */
describe('a redelivery after the watermark was deleted', () => {
  test('resolves every offset again, in ascending order', async () => {
    const { deps } = makeDeps();
    const handler = createEventsBatchHandler(deps);
    const messages = [message(0, 'a'), message(1, 'a')];

    const first = makeBatch(messages);
    await handler.eachBatch(first.payload);
    expect(resolvedOffsets(first.resolveOffset)).toEqual(['0', '1']);

    const redelivered = makeBatch(messages);
    await handler.eachBatch(redelivered.payload);
    expect(resolvedOffsets(redelivered.resolveOffset)).toEqual(['0', '1']);
  });

  test('hands the redelivered message to the handler again', async () => {
    const { deps, handleEvent } = makeDeps();
    const handler = createEventsBatchHandler(deps);
    const messages = [message(0, 'a')];

    await handler.eachBatch(makeBatch(messages).payload);
    await handler.eachBatch(makeBatch(messages).payload);

    // At-least-once: the consumer does not suppress a redelivery. Whether one
    // is a duplicate is decided by the marker on the event id
    // (incoming-event-handler.ts).
    expect(handleEvent).toHaveBeenCalledTimes(2);
  });

  test('still resolves a redelivered offset that was dead-lettered', async () => {
    const { deps, handleEvent } = makeDeps();
    handleEvent.mockImplementation(() => Promise.reject(new Error('boom')));
    const handler = createEventsBatchHandler(deps);
    const messages = [message(0, 'a')];

    const first = makeBatch(messages);
    await handler.eachBatch(first.payload);
    expect(resolvedOffsets(first.resolveOffset)).toEqual(['0']);

    const redelivered = makeBatch(messages);
    await handler.eachBatch(redelivered.payload);
    expect(resolvedOffsets(redelivered.resolveOffset)).toEqual(['0']);
  });

  test('resetDurabilityBackoff on GROUP_JOIN changes no offset behaviour', async () => {
    const { deps } = makeDeps();
    const handler = createEventsBatchHandler(deps);
    const messages = [message(0, 'a'), message(1, 'b')];

    await handler.eachBatch(makeBatch(messages).payload);
    handler.resetDurabilityBackoff();

    const afterRebalance = makeBatch(messages);
    await handler.eachBatch(afterRebalance.payload);
    expect(resolvedOffsets(afterRebalance.resolveOffset)).toEqual(['0', '1']);
  });
});

/**
 * The regression test for a real data-loss incident: a graceful restart lost
 * 2 of 44,075 events.
 *
 * The event a handler accepted lives in an in-process array until the buffer
 * pushes it to Redis, and the consumer commits whatever `resolveOffset` marked
 * the moment `eachBatch` returns, so `stopConsuming` is NOT the commit point.
 * Resolving an offset for
 * an event that is still only in memory is exactly how those events were
 * lost: the offsets were committed, the process exited, and nothing was
 * redelivered.
 *
 * `resolveOffset` is therefore the commit in these tests. It is the only thing
 * that decides what gets committed.
 */
describe('durability before commit', () => {
  test('flushes the buffered events before it resolves any offset', async () => {
    const { payload, resolveOffset } = makeBatch([
      message(0, 'a'),
      message(1, 'b'),
    ]);
    let handled = 0;
    let resolvedWhenFlushed = -1;
    let handledWhenFlushed = -1;

    const { deps } = makeDeps({
      handleEvent: mock(async () => {
        handled += 1;
        return undefined;
      }),
      flushBufferedEvents: mock(async () => {
        resolvedWhenFlushed = resolveOffset.mock.calls.length;
        handledWhenFlushed = handled;
      }),
    });
    const handler = createEventsBatchHandler(deps);

    await handler.eachBatch(payload);

    // Nothing committed yet, and every message already handled — so the flush
    // covers the whole batch and none of it was committed behind its back.
    expect(resolvedWhenFlushed).toBe(0);
    expect(handledWhenFlushed).toBe(2);
    expect(resolvedOffsets(resolveOffset)).toEqual(['0', '1']);
  });

  test('resolves NO offset when the flush fails', async () => {
    const logger = {
      info: mock(() => undefined),
      warn: mock(() => undefined),
      error: mock(() => undefined),
    };
    const onActivity = mock(() => undefined);
    const { deps } = makeDeps({
      flushBufferedEvents: mock(() =>
        Promise.reject(new Error('redis is unreachable'))
      ),
      logger,
      onActivity,
    });
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([
      message(0, 'a'),
      message(1, 'b'),
    ]);

    const outcome = await handler.eachBatch(payload);

    // The whole batch stays uncommitted and is asked for again. A duplicate
    // is recoverable; a loss is not.
    expect(outcome).toBe('redeliver');
    expect(resolvedOffsets(resolveOffset)).toEqual([]);
    expect(logger.error).toHaveBeenCalledTimes(1);
    // Still visibly alive while it waits: readiness must not read a Redis
    // outage as a wedged consumer.
    expect(onActivity).toHaveBeenCalled();
  });

  test('redelivery after a failed flush commits', async () => {
    let redisIsDown = true;
    const { deps } = makeDeps({
      flushBufferedEvents: mock(async () => {
        if (redisIsDown) {
          throw new Error('redis is unreachable');
        }
      }),
    });
    const handler = createEventsBatchHandler(deps);

    const first = makeBatch([message(0, 'a'), message(1, 'b')]);
    await handler.eachBatch(first.payload);
    expect(resolvedOffsets(first.resolveOffset)).toEqual([]);

    redisIsDown = false;
    const redelivered = makeBatch([message(0, 'a'), message(1, 'b')]);
    await handler.eachBatch(redelivered.payload);

    expect(resolvedOffsets(redelivered.resolveOffset)).toEqual(['0', '1']);
  });

  test('opens the durability window before the first handler buffers anything', async () => {
    // The window's lower bound is what makes the gate answer for THIS batch: a
    // failed write drops the events it knows the broker will redeliver, so
    // "everything queued is settled" is no longer "my events are in Redis".
    let handled = 0;
    let handledWhenOpened = -1;
    const { deps, openDurabilityWindow } = makeDeps({
      handleEvent: mock(async () => {
        handled += 1;
        return undefined;
      }),
    });
    openDurabilityWindow.mockImplementation(() => {
      handledWhenOpened = handled;
      return async () => undefined;
    });
    const handler = createEventsBatchHandler(deps);
    const { payload } = makeBatch([message(0, 'a'), message(1, 'b')]);

    await handler.eachBatch(payload);

    expect(openDurabilityWindow).toHaveBeenCalledTimes(1);
    expect(handledWhenOpened).toBe(0);
  });

  test('a failed flush resolves NOTHING, so a redelivery is guaranteed', async () => {
    // Asserted directly: with the re-queue gone, the only thing standing
    // between a failed flush and a lost batch is that its offsets are never
    // resolved — every message, not just the tail, and regardless of how
    // many of them the handlers finished.
    const { deps, handleEvent } = makeDeps({
      flushBufferedEvents: mock(() =>
        Promise.reject(new Error('redis is unreachable'))
      ),
    });
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([
      message(0, 'a'),
      message(1, 'b'),
      message(2, 'a'),
    ]);

    await handler.eachBatch(payload);

    expect(handleEvent).toHaveBeenCalledTimes(3);
    expect(resolveOffset).not.toHaveBeenCalled();
  });

  test('backs off before the redelivery, and the wait grows and then caps', async () => {
    // Observable as a growing delay, not as a fixed attempt count: the point
    // is that a ~15s outage costs single-digit redeliveries instead of the
    // ~162 laps an unbacked retry loop would cost at ~90ms each.
    const OUTAGE_LAPS = 8;
    const { deps, sleep } = makeDeps({
      flushBufferedEvents: mock(() =>
        Promise.reject(new Error('redis is unreachable'))
      ),
      initialRetryMs: 100,
      maxRetryMs: 1000,
    });
    const handler = createEventsBatchHandler(deps);

    for (let lap = 0; lap < OUTAGE_LAPS; lap++) {
      const { payload, resolveOffset } = makeBatch([message(0, 'a')]);
      await handler.eachBatch(payload);
      expect(resolvedOffsets(resolveOffset)).toEqual([]);
    }

    const waits = sleep.mock.calls.map(([ms]) => ms as number);
    expect(waits.length).toBe(OUTAGE_LAPS);
    expect(waits[0]).toBe(100);
    for (let lap = 1; lap < OUTAGE_LAPS; lap++) {
      expect(waits[lap]).toBeGreaterThanOrEqual(waits[lap - 1]!);
    }
    // Grown, and bounded — `maxRetryMs` is the flaky-handler bound, not this
    // one, and the cap stays far below the 30s session timeout.
    expect(waits.at(-1)).toBeGreaterThan(waits[0]!);
    expect(Math.max(...waits)).toBe(5000);
    // Single digit for an outage of this length, against ~162 without it.
    expect(waits.reduce((total, ms) => total + ms, 0)).toBeGreaterThan(15_000);
  });

  test('forgets the backoff once the batch is durable again', async () => {
    let redisIsDown = true;
    const { deps, sleep } = makeDeps({
      flushBufferedEvents: mock(async () => {
        if (redisIsDown) {
          throw new Error('redis is unreachable');
        }
      }),
      initialRetryMs: 100,
    });
    const handler = createEventsBatchHandler(deps);

    for (let lap = 0; lap < 3; lap++) {
      await handler.eachBatch(makeBatch([message(lap, 'a')]).payload);
    }
    redisIsDown = false;
    await handler.eachBatch(makeBatch([message(3, 'a')]).payload);
    redisIsDown = true;
    await handler.eachBatch(makeBatch([message(4, 'a')]).payload);

    const waits = sleep.mock.calls.map(([ms]) => ms as number);
    expect(waits).toEqual([100, 200, 400, 100]);
  });

  test('does not hold the partition while the consumer is shutting down', async () => {
    const { deps, sleep } = makeDeps({
      flushBufferedEvents: mock(() =>
        Promise.reject(new Error('redis is unreachable'))
      ),
    });
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([message(0, 'a')], {
      isRunning: () => false,
    });

    await handler.eachBatch(payload);

    expect(resolvedOffsets(resolveOffset)).toEqual([]);
    expect(sleep).not.toHaveBeenCalled();
  });

  test('flushes what a shutdown-truncated batch handled, before resolving it', async () => {
    // SIGTERM lands mid-batch, `isRunning` goes false, and the handler
    // resolves only the prefix it finished. That prefix must be durable too —
    // it is the window a graceful restart used to lose.
    let handled = 0;
    const flushBufferedEvents = mock(async () => undefined);
    const { deps } = makeDeps({
      flushBufferedEvents,
      handleEvent: mock(async () => {
        handled += 1;
        return undefined;
      }),
    });
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch(
      [message(0, 'a'), message(1, 'a'), message(2, 'a')],
      { isRunning: () => handled < 2 }
    );

    await handler.eachBatch(payload);

    expect(resolvedOffsets(resolveOffset)).toEqual(['0', '1']);
    expect(flushBufferedEvents).toHaveBeenCalledTimes(1);
  });
});

/**
 * End to end: the consumer wired to the REAL capped-list recorder
 * (`@openpanel/redis`'s `createDeadLetterRecorder`) instead of a mock, against
 * an in-memory stand-in for the one MULTI it issues. Redis itself is out of the
 * picture; the LPUSH/LTRIM arithmetic is not.
 */
describe('dead letter to a capped redis list', () => {
  const LIST_KEY = 'dead_letter:events-test';
  const MAX_ENTRIES = 3;
  const OK: [Error | null, unknown] = [null, 1];

  /** Enough of a Redis client for one MULTI, with real list semantics. */
  function fakeRedis(execError?: Error) {
    const entries: string[] = [];
    let roundTrips = 0;
    const client: DeadLetterRedisClient = {
      multi() {
        const queued: (() => void)[] = [];
        const chain: DeadLetterMulti = {
          lpush(_key, value) {
            queued.push(() => entries.unshift(value));
            return chain;
          },
          ltrim(_key, start, stop) {
            queued.push(() => entries.splice(stop + 1 - start));
            return chain;
          },
          async exec() {
            roundTrips += 1;
            if (execError) {
              throw execError;
            }
            for (const run of queued) {
              run();
            }
            return queued.map(() => OK);
          },
        };
        return chain;
      },
    };
    return {
      client,
      records: () => entries.map((raw) => JSON.parse(raw) as DeadLetterRecord),
      roundTrips: () => roundTrips,
    };
  }

  function makeRecordingDeps(execError?: Error) {
    const redis = fakeRedis(execError);
    const built = makeDeps({
      sendToDeadLetter: createDeadLetterRecorder({
        client: redis.client,
        maxEntries: MAX_ENTRIES,
        key: LIST_KEY,
      }),
    });
    return { ...built, redis };
  }

  test('a handler that exhausts its attempts leaves ONE record and a RESOLVED offset', async () => {
    const { deps, handleEvent, metrics, redis } = makeRecordingDeps();
    handleEvent.mockImplementation(() => Promise.reject(new Error('boom')));
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([message(0, 'a')]);

    await handler.eachBatch(payload);

    expect(handleEvent).toHaveBeenCalledTimes(3);
    const records = redis.records();
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      reason: 'handler_error',
      error: 'boom',
      offset: '0',
      partition: PARTITION,
      topic: TOPIC,
      key: 'a',
    });
    expect(JSON.parse(records[0]?.value ?? '')).toMatchObject({
      projectId: 'project-0',
    });
    // One MULTI, so one round trip per dropped event.
    expect(redis.roundTrips()).toBe(1);
    expect(metrics.deadLettered).toHaveBeenCalledWith(
      String(PARTITION),
      'handler_error'
    );
    expect(resolvedOffsets(resolveOffset)).toEqual(['0']);
  });

  test('a DEAD REDIS still resolves the offset and records nothing', async () => {
    // The dependency that fails the handler is the same dependency the dead
    // letter is written to. If this offset is left unresolved the
    // redelivery loop is back, in a new place.
    const { deps, handleEvent, metrics, redis } = makeRecordingDeps(
      new Error(
        "Stream isn't writeable and enableOfflineQueue options is false"
      )
    );
    handleEvent.mockImplementation(() => Promise.reject(new Error('boom')));
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([
      message(0, 'a'),
      message(1, 'a'),
      message(2, 'a'),
    ]);

    await handler.eachBatch(payload);

    expect(redis.records()).toEqual([]);
    expect(metrics.deadLetterFailed).toHaveBeenCalledTimes(3);
    expect(metrics.deadLettered).not.toHaveBeenCalled();
    expect(resolvedOffsets(resolveOffset)).toEqual(['0', '1', '2']);
  });

  test('a per-command error inside the MULTI counts as not recorded', async () => {
    // WRONGTYPE is a real Redis fault shape, and a MULTI reports it in
    // `exec()`'s results rather than by rejecting.
    const redis = fakeRedis();
    const wrongType: [Error | null, unknown] = [
      new Error(
        'WRONGTYPE Operation against a key holding the wrong kind of value'
      ),
      null,
    ];
    const failing: DeadLetterRedisClient = {
      multi() {
        const chain: DeadLetterMulti = {
          lpush: () => chain,
          ltrim: () => chain,
          exec: () => Promise.resolve([wrongType, OK]),
        };
        return chain;
      },
    };
    const { deps, handleEvent, metrics } = makeDeps({
      sendToDeadLetter: createDeadLetterRecorder({
        client: failing,
        maxEntries: MAX_ENTRIES,
        key: LIST_KEY,
      }),
    });
    handleEvent.mockImplementation(() => Promise.reject(new Error('boom')));
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([message(0, 'a')]);

    await handler.eachBatch(payload);

    expect(redis.records()).toEqual([]);
    expect(metrics.deadLetterFailed).toHaveBeenCalledWith(String(PARTITION));
    expect(metrics.deadLettered).not.toHaveBeenCalled();
    expect(resolvedOffsets(resolveOffset)).toEqual(['0']);
  });

  test('the list never exceeds N and keeps the most recent drops', async () => {
    const { deps, handleEvent, redis } = makeRecordingDeps();
    handleEvent.mockImplementation(() => Promise.reject(new Error('boom')));
    const handler = createEventsBatchHandler(deps);
    // One key, so one serial group: the drop order is the offset order.
    const { payload, resolveOffset } = makeBatch(
      [0, 1, 2, 3, 4].map((offset) => message(offset, 'a'))
    );

    await handler.eachBatch(payload);

    const records = redis.records();
    expect(records).toHaveLength(MAX_ENTRIES);
    // Newest first, and the two oldest drops fell off the tail.
    expect(records.map((record) => record.offset)).toEqual(['4', '3', '2']);
    expect(resolvedOffsets(resolveOffset)).toEqual(['0', '1', '2', '3', '4']);
  });

  test('a parse failure takes the same path as a handler failure', async () => {
    const { deps, handleEvent, metrics, redis } = makeRecordingDeps();
    const handler = createEventsBatchHandler(deps);
    const { payload, resolveOffset } = makeBatch([
      message(0, 'a', 'not json at all'),
    ]);

    await handler.eachBatch(payload);

    expect(handleEvent).not.toHaveBeenCalled();
    const records = redis.records();
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      reason: 'parse_error',
      offset: '0',
      value: 'not json at all',
    });
    expect(metrics.deadLettered).toHaveBeenCalledWith(
      String(PARTITION),
      'parse_error'
    );
    expect(resolvedOffsets(resolveOffset)).toEqual(['0']);
  });
});

/**
 * The consumer wired to a stand-in for the client: a stream of messages, the
 * group events, and `commit`. What is asserted is what reaches the broker.
 */
describe('startKafkaEventsConsumer', () => {
  const LEADER_EPOCH = 1;

  class FakeStream extends EventEmitter {
    pause = mock(() => this);
    resume = mock(() => this);
    close = mock(async () => undefined);
  }

  function fakeClient() {
    const streams: FakeStream[] = [];
    const client = Object.assign(new EventEmitter(), {
      groupId: 'openpanel-events',
      memberId: 'member-1',
      assignments: [{ topic: TOPIC, partitions: [0, 1] }],
      consume: mock(async (_options: Record<string, unknown>) => {
        const stream = new FakeStream();
        streams.push(stream);
        return stream;
      }),
      commit: mock(async (_options: unknown) => undefined),
      close: mock(async (_force?: boolean) => undefined),
      isActive: mock(() => true),
      joinGroup: mock(async () => 'member-1'),
    });
    return { client, streams };
  }

  const streamed = (partition: number, offset: number, key = 'a') => ({
    topic: TOPIC,
    partition,
    offset: BigInt(offset),
    leaderEpoch: LEADER_EPOCH,
    key: Buffer.from(key),
    value: Buffer.from(JSON.stringify(payloadFor(offset))),
    headers: new Map([[Buffer.from('source'), Buffer.from('sdk')]]),
  });

  const settle = async (rounds = 30): Promise<void> => {
    for (let i = 0; i < rounds; i++) {
      await Promise.resolve();
    }
  };

  async function start(overrides: DepOverrides = {}) {
    const { client, streams } = fakeClient();
    const built = makeDeps(overrides);
    const { topic: _topic, ...batch } = built.deps;
    const sleep = mock(async (_ms: number) => undefined);
    const handle = await startKafkaEventsConsumer({
      createConsumer: () => client as unknown as Consumer,
      logger: built.deps.logger,
      kafkaLogger: built.deps.logger,
      topic: TOPIC,
      partitionsConsumedConcurrently: 2,
      maxMessagesPerBatch: 10,
      batch,
      sleep,
    });
    return { ...built, client, streams, handle, sleep };
  }

  const committed = (commit: { mock: { calls: unknown[][] } }) =>
    commit.mock.calls.map(([options]) => options);

  test('reads from the committed offsets, the oldest when there are none, and commits itself', async () => {
    const { client } = await start();
    expect(client.consume).toHaveBeenCalledTimes(1);
    expect(client.consume.mock.calls[0]?.[0]).toEqual({
      topics: [TOPIC],
      mode: 'committed',
      fallbackMode: 'earliest',
      autocommit: false,
    });
  });

  test("hands a partition's messages to the handler and commits past the last", async () => {
    const handleEvent = mock(async (p: { projectId: string }) => {
      if (p.projectId === 'project-1') {
        throw new Error('poison');
      }
    });
    const { client, streams, sendToDeadLetter } = await start({ handleEvent });
    streams[0]?.emit('data', streamed(0, 0));
    streams[0]?.emit('data', streamed(0, 1));
    await settle();

    // Offset 0 handled; offset 1 failed every attempt and was dead-lettered.
    expect(handleEvent).toHaveBeenCalledTimes(4);
    expect(committed(client.commit)).toEqual([
      { offsets: [{ topic: TOPIC, partition: 0, offset: 2n, leaderEpoch: 1 }] },
    ]);
    // Headers reach the dead letter as name -> bytes.
    const dlq = sendToDeadLetter.mock.calls[0]?.[0] as DeadLetterMessage;
    expect(dlq.offset).toBe('1');
    expect(dlq.headers?.source?.toString()).toBe('sdk');
  });

  test('a failed flush runs the batch again, and commits only once it is durable', async () => {
    let failures = 2;
    const onActivity = mock(() => undefined);
    const { client, streams, handleEvent } = await start({
      onActivity,
      flushBufferedEvents: mock(async () => {
        if (failures > 0) {
          failures -= 1;
          throw new Error('redis is unreachable');
        }
      }),
    });
    streams[0]?.emit('data', streamed(0, 0));
    await settle(60);

    expect(handleEvent).toHaveBeenCalledTimes(3);
    expect(committed(client.commit)).toEqual([
      { offsets: [{ topic: TOPIC, partition: 0, offset: 1n, leaderEpoch: 1 }] },
    ]);
    // Every attempt, failed or not, counts as the loop being alive.
    expect(onActivity.mock.calls.length).toBeGreaterThanOrEqual(3);
  });

  test('a rebalance that takes a partition away stops handing over its messages', async () => {
    const { client, streams, handleEvent } = await start();
    client.emit('consumer:group:join', {
      groupId: 'openpanel-events',
      memberId: 'member-1',
      generationId: 2,
      assignments: [{ topic: TOPIC, partitions: [1] }],
    });
    streams[0]?.emit('data', streamed(0, 0));
    streams[0]?.emit('data', streamed(1, 0));
    await settle();

    expect(handleEvent).toHaveBeenCalledTimes(1);
    expect(committed(client.commit)).toEqual([
      { offsets: [{ topic: TOPIC, partition: 1, offset: 1n, leaderEpoch: 1 }] },
    ]);
  });

  test('a stream that dies is reopened from the committed offsets', async () => {
    const { client, streams, sleep, deps } = await start();
    streams[0]?.emit('error', new Error('broker went away'));
    await settle();

    expect(deps.logger.error).toHaveBeenCalled();
    expect(streams[0]?.close).toHaveBeenCalledTimes(1);
    expect(sleep).toHaveBeenCalledTimes(1);
    expect(client.consume).toHaveBeenCalledTimes(2);

    // The new stream is the live one.
    streams[1]?.emit('data', streamed(0, 5));
    await settle();
    expect(committed(client.commit)).toEqual([
      { offsets: [{ topic: TOPIC, partition: 0, offset: 6n, leaderEpoch: 1 }] },
    ]);
  });

  test('a failed rejoin is logged and reopens the stream instead of crashing', async () => {
    const { client, streams, deps } = await start();
    client.emit('error', new Error('JoinGroup timed out'));
    await settle();
    expect(deps.logger.error).toHaveBeenCalled();
    expect(streams[0]?.close).toHaveBeenCalledTimes(1);
    expect(client.consume).toHaveBeenCalledTimes(2);
  });

  test('a stream that cannot open at boot fails the start', async () => {
    const { client } = fakeClient();
    client.consume = mock(async (_options: Record<string, unknown>) => {
      throw new Error('SASL authentication failed');
    });
    const built = makeDeps();
    const { topic: _topic, ...batch } = built.deps;
    await expect(
      startKafkaEventsConsumer({
        createConsumer: () => client as unknown as Consumer,
        logger: built.deps.logger,
        kafkaLogger: built.deps.logger,
        topic: TOPIC,
        partitionsConsumedConcurrently: 2,
        maxMessagesPerBatch: 10,
        batch,
      })
    ).rejects.toThrow('SASL authentication failed');
  });

  test('a stream reopened after the membership was lost rejoins the group first', async () => {
    const { client, streams } = await start();
    client.isActive.mockImplementation(() => false);
    client.emit('consumer:group:lost', {
      groupId: 'openpanel-events',
      memberId: 'member-1',
      generationId: 2,
      error: new Error('joinGroup failed 4 times.'),
      operation: 'joinGroup',
    });
    await settle();
    expect(streams[0]?.close).toHaveBeenCalledTimes(1);
    expect(client.consume).toHaveBeenCalledTimes(2);
    expect(client.joinGroup).toHaveBeenCalledTimes(1);
  });

  test('a rejoin that fails closes the new stream and tries again', async () => {
    const { client, streams, deps } = await start();
    let failures = 1;
    client.isActive.mockImplementation(() => false);
    client.joinGroup.mockImplementation(async () => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('coordinator not available');
      }
      return 'member-2';
    });
    streams[0]?.emit('error', new Error('fetch failed 4 times.'));
    await settle(60);
    expect(deps.logger.error).toHaveBeenCalled();
    expect(streams[1]?.close).toHaveBeenCalledTimes(1);
    expect(client.consume).toHaveBeenCalledTimes(3);
    expect(client.joinGroup).toHaveBeenCalledTimes(2);
  });

  test('a stream that fails while the reopen is still rejoining is reopened again', async () => {
    const { client, streams } = await start();
    client.isActive.mockImplementation(() => false);
    let joins = 0;
    client.joinGroup.mockImplementation(async () => {
      joins += 1;
      if (joins === 1) {
        // The stream the reopen just opened dies before the join answers.
        streams[1]?.emit('error', new Error('fetch failed 4 times.'));
      }
      return 'member-2';
    });
    streams[0]?.emit('error', new Error('fetch failed 4 times.'));
    await settle(80);
    expect(streams[1]?.close).toHaveBeenCalledTimes(1);
    expect(client.consume).toHaveBeenCalledTimes(3);
  });

  test('an idle fetch outside the group does not mark the loop alive', async () => {
    const onActivity = mock(() => undefined);
    const { client, streams } = await start({ onActivity });
    client.isActive.mockImplementation(() => false);
    streams[0]?.emit('fetch');
    expect(onActivity).not.toHaveBeenCalled();
  });

  test('an idle fetch marks the loop alive', async () => {
    const onActivity = mock(() => undefined);
    const { streams } = await start({ onActivity });
    streams[0]?.emit('fetch');
    expect(onActivity).toHaveBeenCalledTimes(1);
  });

  test('a fetch while a batch is running does not cover for it', async () => {
    let release = (): void => undefined;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const onActivity = mock(() => undefined);
    const { streams } = await start({
      onActivity,
      handleEvent: mock(() => held),
    });
    streams[0]?.emit('data', streamed(0, 0));
    await settle();
    streams[0]?.emit('fetch');
    expect(onActivity).not.toHaveBeenCalled();
    release();
    await settle();
  });

  test('stopConsuming waits for the running batch; stop then leaves the group', async () => {
    let release = (): void => undefined;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { client, streams, handle } = await start({
      handleEvent: mock(() => held),
    });
    streams[0]?.emit('data', streamed(0, 0));
    await settle();

    let stopped = false;
    const stopping = handle.stopConsuming().then(() => {
      stopped = true;
    });
    await settle();
    expect(stopped).toBe(false);
    expect(streams[0]?.pause).toHaveBeenCalled();

    release();
    await stopping;
    expect(streams[0]?.close).toHaveBeenCalledTimes(1);
    // The message finished before the stop, so it is committed.
    expect(committed(client.commit)).toEqual([
      { offsets: [{ topic: TOPIC, partition: 0, offset: 1n, leaderEpoch: 1 }] },
    ]);

    await handle.stop();
    expect(client.close).toHaveBeenCalledWith(true);
  });
});
