/**
 * The lanes are the consumer's commit discipline: one batch per partition at a
 * time, commits in offset order, nothing committed that was not handled, and a
 * batch whose events are not durable is run again instead of skipped — the
 * client never refetches an uncommitted offset by itself.
 */

import { describe, expect, mock, test } from 'bun:test';
import {
  createPartitionLanes,
  type LaneBatchControl,
  type LaneBatchOutcome,
  type PartitionLanesOptions,
} from './partition-lanes';

interface Item {
  offset: bigint;
  leaderEpoch: number;
}

const LEADER_EPOCH = 3;
const item = (offset: number): Item => ({
  offset: BigInt(offset),
  leaderEpoch: LEADER_EPOCH,
});

/** Lets queued promise callbacks run. */
const flush = async (rounds = 5): Promise<void> => {
  for (let i = 0; i < rounds; i++) {
    await Promise.resolve();
  }
};

interface Deferred {
  promise: Promise<void>;
  resolve: () => void;
}

const deferred = (): Deferred => {
  let resolve = (): void => undefined;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
};

type RunBatch = PartitionLanesOptions<Item>['runBatch'];

/** Resolves every offset it is handed. */
const handleAll: RunBatch = async (_partition, items, control) => {
  for (const it of items) {
    control.resolveOffset(it.offset);
  }
  return 'done';
};

function makeLanes(overrides: Partial<PartitionLanesOptions<Item>> = {}) {
  const commits: [number, bigint, number][] = [];
  const commit = mock(
    async (partition: number, offset: bigint, leaderEpoch: number) => {
      commits.push([partition, offset, leaderEpoch]);
    }
  );
  const pauseSource = mock(() => undefined);
  const resumeSource = mock(() => undefined);
  const logger = { warn: mock(() => undefined), error: mock(() => undefined) };
  const sleep = mock(async (_ms: number) => undefined);
  const options: PartitionLanesOptions<Item> = {
    concurrency: 2,
    maxBatchSize: 10,
    maxBuffered: 1000,
    runBatch: handleAll,
    commit,
    pauseSource,
    resumeSource,
    logger,
    sleep,
    ...overrides,
  };
  const lanes = createPartitionLanes(options);
  return { lanes, commits, commit, pauseSource, resumeSource, logger, sleep };
}

describe('scheduling', () => {
  test('commits one past the last handled offset, with its leader epoch', async () => {
    const { lanes, commits } = makeLanes();
    lanes.push(0, item(5));
    lanes.push(0, item(6));
    await flush();
    expect(commits).toEqual([[0, 7n, LEADER_EPOCH]]);
  });

  test('never runs more partitions at once than the concurrency allows', async () => {
    let running = 0;
    let peak = 0;
    const gate = deferred();
    const { lanes } = makeLanes({
      concurrency: 2,
      runBatch: async (partition, items, control) => {
        running += 1;
        peak = Math.max(peak, running);
        await gate.promise;
        running -= 1;
        return handleAll(partition, items, control);
      },
    });
    for (const partition of [0, 1, 2, 3]) {
      lanes.push(partition, item(0));
    }
    await flush();
    expect(lanes.activeCount()).toBe(2);
    gate.resolve();
    await flush(20);
    expect(peak).toBe(2);
    expect(lanes.activeCount()).toBe(0);
  });

  test('runs one batch per partition at a time, in offset order', async () => {
    const seen: bigint[][] = [];
    const gate = deferred();
    const { lanes, commits } = makeLanes({
      maxBatchSize: 2,
      runBatch: async (partition, items, control) => {
        seen.push(items.map((it) => it.offset));
        await gate.promise;
        return handleAll(partition, items, control);
      },
    });
    for (const offset of [0, 1, 2, 3, 4]) {
      lanes.push(0, item(offset));
    }
    await flush();
    // The first batch holds the partition; the rest waits behind it.
    expect(seen).toEqual([[0n, 1n]]);
    gate.resolve();
    await flush(30);
    expect(seen).toEqual([[0n, 1n], [2n, 3n], [4n]]);
    expect(commits.map(([, offset]) => offset)).toEqual([2n, 4n, 5n]);
  });

  test('a busy partition does not starve the others', async () => {
    const order: number[] = [];
    const { lanes } = makeLanes({
      concurrency: 1,
      maxBatchSize: 1,
      runBatch: async (partition, items, control) => {
        order.push(partition);
        return handleAll(partition, items, control);
      },
    });
    for (const offset of [0, 1, 2]) {
      lanes.push(0, item(offset));
      lanes.push(1, item(offset));
    }
    await flush(40);
    expect(order.slice(0, 4)).toEqual([0, 1, 0, 1]);
  });

  test('ignores an offset it has already queued, as after a rewind', async () => {
    const seen: bigint[] = [];
    const { lanes } = makeLanes({
      runBatch: async (partition, items, control) => {
        seen.push(...items.map((it) => it.offset));
        return handleAll(partition, items, control);
      },
    });
    lanes.push(0, item(0));
    lanes.push(0, item(1));
    lanes.push(0, item(0));
    lanes.push(0, item(2));
    await flush(20);
    expect(seen).toEqual([0n, 1n, 2n]);
  });
});

describe('a batch whose events are not durable', () => {
  test('runs the same batch again and commits only once it succeeds', async () => {
    let attempts = 0;
    const commitsSeenPerAttempt: number[] = [];
    const { lanes, commits } = makeLanes({
      runBatch: async (
        partition,
        items,
        control
      ): Promise<LaneBatchOutcome> => {
        attempts += 1;
        commitsSeenPerAttempt.push(commits.length);
        if (attempts < 3) {
          return 'redeliver';
        }
        return handleAll(partition, items, control);
      },
    });
    lanes.push(0, item(0));
    lanes.push(0, item(1));
    await flush(30);
    expect(attempts).toBe(3);
    expect(commitsSeenPerAttempt).toEqual([0, 0, 0]);
    expect(commits).toEqual([[0, 2n, LEADER_EPOCH]]);
  });

  test('is abandoned, uncommitted, when the consumer stops', async () => {
    const { lanes, commits } = makeLanes({
      runBatch: async () => 'redeliver',
    });
    lanes.push(0, item(0));
    await flush();
    await lanes.stop();
    expect(commits).toEqual([]);
    expect(lanes.activeCount()).toBe(0);
  });

  test('is abandoned, uncommitted, when the partition is revoked', async () => {
    let attempts = 0;
    const { lanes, commits } = makeLanes({
      runBatch: async () => {
        attempts += 1;
        if (attempts === 2) {
          lanes.setAssignment([1]);
        }
        return 'redeliver';
      },
    });
    lanes.setAssignment([0, 1]);
    lanes.push(0, item(0));
    await flush(20);
    expect(attempts).toBe(2);
    expect(commits).toEqual([]);
    expect(lanes.activeCount()).toBe(0);
  });

  test('a handler that throws is retried after a pause, not dropped', async () => {
    let attempts = 0;
    const { lanes, commits, sleep, logger } = makeLanes({
      runBatch: async (partition, items, control) => {
        attempts += 1;
        if (attempts === 1) {
          throw new Error('bug');
        }
        return handleAll(partition, items, control);
      },
    });
    lanes.push(0, item(0));
    await flush(20);
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(sleep).toHaveBeenCalledTimes(1);
    expect(commits).toEqual([[0, 1n, LEADER_EPOCH]]);
  });
});

describe('rebalance', () => {
  test('drops what is queued for a partition that was taken away', async () => {
    const gate = deferred();
    const seen: number[] = [];
    const { lanes } = makeLanes({
      concurrency: 1,
      runBatch: async (partition, items, control) => {
        seen.push(partition);
        await gate.promise;
        return handleAll(partition, items, control);
      },
    });
    lanes.setAssignment([0, 1]);
    lanes.push(0, item(0));
    lanes.push(1, item(0));
    await flush();
    lanes.setAssignment([0]);
    gate.resolve();
    await flush(20);
    expect(seen).toEqual([0]);
  });

  test('accepts nothing for a partition this member does not own', async () => {
    const runBatch = mock(handleAll);
    const { lanes } = makeLanes({ runBatch });
    lanes.setAssignment([0]);
    lanes.push(1, item(0));
    await flush();
    expect(runBatch).not.toHaveBeenCalled();
  });

  test('a running batch sees the revocation and commits only what it finished', async () => {
    const { lanes, commits } = makeLanes({
      runBatch: async (_partition, items, control: LaneBatchControl) => {
        for (const it of items) {
          if (control.isStale()) {
            break;
          }
          control.resolveOffset(it.offset);
          if (it.offset === 1n) {
            lanes.setAssignment([]);
          }
        }
        return 'done';
      },
    });
    lanes.setAssignment([0]);
    for (const offset of [0, 1, 2, 3]) {
      lanes.push(0, item(offset));
    }
    await flush(20);
    expect(commits).toEqual([[0, 2n, LEADER_EPOCH]]);
  });
});

describe('backpressure', () => {
  test('pauses the source when full and resumes it once drained', async () => {
    const gate = deferred();
    const { lanes, pauseSource, resumeSource } = makeLanes({
      concurrency: 1,
      maxBatchSize: 2,
      maxBuffered: 4,
      runBatch: async (partition, items, control) => {
        await gate.promise;
        return handleAll(partition, items, control);
      },
    });
    for (const offset of [0, 1, 2, 3]) {
      lanes.push(0, item(offset));
    }
    expect(pauseSource).toHaveBeenCalledTimes(1);
    expect(resumeSource).not.toHaveBeenCalled();
    gate.resolve();
    await flush(30);
    expect(resumeSource).toHaveBeenCalledTimes(1);
  });
});

describe('shutdown', () => {
  test('stop lets the running batch finish its prefix and commits it', async () => {
    const { lanes, commits } = makeLanes({
      runBatch: async (_partition, items, control) => {
        for (const it of items) {
          if (!control.isRunning()) {
            break;
          }
          control.resolveOffset(it.offset);
          if (it.offset === 1n) {
            stopping = lanes.stop();
          }
        }
        return 'done';
      },
    });
    let stopping: Promise<void> | null = null;
    for (const offset of [0, 1, 2, 3]) {
      lanes.push(0, item(offset));
    }
    await flush(10);
    await stopping;
    expect(commits).toEqual([[0, 2n, LEADER_EPOCH]]);
    // Nothing is accepted after stop.
    lanes.push(0, item(4));
    await flush();
    expect(commits).toHaveLength(1);
  });

  test('a commit that fails is logged and the partition keeps moving', async () => {
    let failNext = true;
    const commit = mock(async () => {
      if (failNext) {
        failNext = false;
        throw new Error('coordinator moved');
      }
    });
    const { lanes, logger } = makeLanes({ commit, maxBatchSize: 1 });
    lanes.push(0, item(0));
    lanes.push(0, item(1));
    await flush(20);
    expect(logger.warn).toHaveBeenCalledTimes(1);
    expect(commit).toHaveBeenCalledTimes(2);
  });

  test('quiesce waits for running batches; reset forgets the queue', async () => {
    const gate = deferred();
    const runBatch = mock(
      async (partition: number, items: Item[], control: LaneBatchControl) => {
        await gate.promise;
        return handleAll(partition, items, control);
      }
    );
    const { lanes, commits } = makeLanes({
      concurrency: 1,
      maxBatchSize: 1,
      runBatch,
    });
    lanes.push(0, item(0));
    lanes.push(0, item(1));
    await flush();
    let quiesced = false;
    const quiescing = lanes.quiesce().then(() => {
      quiesced = true;
    });
    await flush();
    expect(quiesced).toBe(false);
    gate.resolve();
    await quiescing;
    // The running batch finished and committed; the queued one never started.
    expect(commits).toEqual([[0, 1n, LEADER_EPOCH]]);
    expect(runBatch).toHaveBeenCalledTimes(1);
    lanes.reset();
    // After a reset the source starts over from the committed offset.
    lanes.push(0, item(1));
    await flush(20);
    expect(commits).toEqual([
      [0, 1n, LEADER_EPOCH],
      [0, 2n, LEADER_EPOCH],
    ]);
  });
});
