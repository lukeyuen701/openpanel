// One lane per partition: a partition's messages are handled one batch at a
// time and its offsets committed in order, while up to `concurrency` partitions
// run side by side. The client delivers one stream for every assigned
// partition and never refetches what was not committed, so this is where both
// kafkajs's `partitionsConsumedConcurrently` and its redelivery now live.

export interface LaneItem {
  offset: bigint;
  leaderEpoch: number;
}

export interface LaneBatchControl {
  isRunning: () => boolean;
  /** The partition was taken away by a rebalance. */
  isStale: () => boolean;
  /** Marks an offset handled. Called in ascending order; the highest is committed. */
  resolveOffset: (offset: bigint) => void;
}

/** `redeliver`: nothing in the batch may be committed; run it again. */
export type LaneBatchOutcome = 'done' | 'redeliver';

export interface LaneLogger {
  warn: (obj: object, msg?: string) => void;
  error: (obj: object, msg?: string) => void;
}

export interface PartitionLanesOptions<TItem extends LaneItem> {
  concurrency: number;
  maxBatchSize: number;
  /** Above this many buffered messages the source is paused. */
  maxBuffered: number;
  runBatch: (
    partition: number,
    items: TItem[],
    control: LaneBatchControl
  ) => Promise<LaneBatchOutcome>;
  /** `offset` is the next one to consume, as Kafka expects. */
  commit: (
    partition: number,
    offset: bigint,
    leaderEpoch: number
  ) => Promise<void>;
  pauseSource: () => void;
  resumeSource: () => void;
  logger: LaneLogger;
  /** Injected by the tests; production uses the timer. */
  sleep?: (ms: number) => Promise<void>;
}

export interface PartitionLanes<TItem extends LaneItem> {
  push: (partition: number, item: TItem) => void;
  /** `null` until the first assignment is known: accept everything. */
  setAssignment: (partitions: number[] | null) => void;
  /** Stops starting batches; resolves once the running ones are done. */
  quiesce: () => Promise<void>;
  /** Forgets everything queued, for a source that restarts from the committed offsets. */
  reset: () => void;
  /** Running batches stop between messages and commit what they finished. */
  stop: () => Promise<void>;
  activeCount: () => number;
}

/** A batch handler that throws is a bug; this keeps the partition from spinning on it. */
const FAILED_BATCH_RETRY_MS = 1000;
/** The source resumes once the backlog falls to this share of `maxBuffered`. */
const RESUME_BELOW_SHARE = 0.5;

interface Lane<TItem> {
  queue: TItem[];
  running: boolean;
  /** One past the highest offset accepted, so a source that rewinds cannot queue a message twice. */
  nextOffset: bigint | null;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

export function createPartitionLanes<TItem extends LaneItem>(
  options: PartitionLanesOptions<TItem>
): PartitionLanes<TItem> {
  const sleep = options.sleep ?? defaultSleep;
  const lanes = new Map<number, Lane<TItem>>();
  let assigned: Set<number> | null = null;
  let running = true;
  let holding = false;
  let paused = false;
  let active = 0;
  let buffered = 0;
  let idleWaiters: (() => void)[] = [];
  let scheduled = false;

  const laneFor = (partition: number): Lane<TItem> => {
    let lane = lanes.get(partition);
    if (!lane) {
      lane = { queue: [], running: false, nextOffset: null };
      lanes.set(partition, lane);
    }
    return lane;
  };

  const isAssigned = (partition: number): boolean =>
    assigned === null || assigned.has(partition);

  const dropQueue = (lane: Lane<TItem>): void => {
    buffered -= lane.queue.length;
    lane.queue = [];
    lane.nextOffset = null;
  };

  const updateBackpressure = (): void => {
    if (!paused && buffered >= options.maxBuffered) {
      paused = true;
      options.pauseSource();
      return;
    }
    if (paused && buffered <= options.maxBuffered * RESUME_BELOW_SHARE) {
      paused = false;
      options.resumeSource();
    }
  };

  const notifyIdle = (): void => {
    if (active > 0) {
      return;
    }
    const waiters = idleWaiters;
    idleWaiters = [];
    for (const resolve of waiters) {
      resolve();
    }
  };

  const whenIdle = (): Promise<void> =>
    active === 0
      ? Promise.resolve()
      : new Promise((resolve) => {
          idleWaiters.push(resolve);
        });

  const commitHandled = async (
    partition: number,
    last: TItem
  ): Promise<void> => {
    try {
      await options.commit(partition, last.offset + 1n, last.leaderEpoch);
    } catch (err) {
      // Not fatal: the handled events are in Redis, and a later commit on the
      // partition covers these offsets too. If none comes — the partition
      // moved, the process died — they are redelivered, as duplicates.
      options.logger.warn(
        { err, partition, offset: String(last.offset) },
        'kafka offset commit failed'
      );
    }
  };

  const runOnce = async (
    partition: number,
    items: TItem[],
    control: LaneBatchControl
  ): Promise<LaneBatchOutcome> => {
    try {
      return await options.runBatch(partition, items, control);
    } catch (err) {
      options.logger.error(
        { err, partition },
        'kafka batch handler threw — running the batch again'
      );
      await sleep(FAILED_BATCH_RETRY_MS);
      return 'redeliver';
    }
  };

  const runLane = async (partition: number, lane: Lane<TItem>) => {
    const items = lane.queue.splice(0, options.maxBatchSize);
    let handled: TItem | null = null;
    const byOffset = new Map(items.map((item) => [item.offset, item]));
    const control: LaneBatchControl = {
      isRunning: () => running,
      isStale: () => !isAssigned(partition),
      resolveOffset: (offset) => {
        handled = byOffset.get(offset) ?? handled;
      },
    };

    // A failed durability flush resolves nothing and asks for the batch again;
    // the handler has already waited out its backoff by then.
    let outcome = await runOnce(partition, items, control);
    while (
      outcome === 'redeliver' &&
      control.isRunning() &&
      !control.isStale()
    ) {
      outcome = await runOnce(partition, items, control);
    }

    const last = handled as TItem | null;
    if (last) {
      await commitHandled(partition, last);
    }
    const unfinished = last
      ? items.filter((item) => item.offset > last.offset)
      : items;
    buffered -= items.length - unfinished.length;
    if (unfinished.length === 0) {
      return;
    }
    if (control.isRunning() && !control.isStale()) {
      lane.queue.unshift(...unfinished);
      return;
    }
    // Shutdown or revocation: never committed, so the next owner reads them.
    buffered -= unfinished.length;
  };

  const schedule = (): void => {
    if (!running || holding) {
      return;
    }
    for (const [partition, lane] of lanes) {
      if (active >= options.concurrency) {
        return;
      }
      if (lane.running || lane.queue.length === 0) {
        continue;
      }
      lane.running = true;
      active += 1;
      runLane(partition, lane).finally(() => {
        lane.running = false;
        active -= 1;
        // Round robin: a partition that just ran goes to the back.
        if (lanes.get(partition) === lane) {
          lanes.delete(partition);
          lanes.set(partition, lane);
        }
        updateBackpressure();
        notifyIdle();
        schedule();
      });
    }
  };

  // The source hands over a fetch response one message at a time, in one
  // synchronous run: starting a lane on the first of them would split the
  // response into a batch of one and a remainder.
  const scheduleSoon = (): void => {
    if (scheduled) {
      return;
    }
    scheduled = true;
    queueMicrotask(() => {
      scheduled = false;
      schedule();
    });
  };

  return {
    push: (partition, item) => {
      if (!(running && isAssigned(partition))) {
        return;
      }
      const lane = laneFor(partition);
      if (lane.nextOffset !== null && item.offset < lane.nextOffset) {
        return;
      }
      lane.nextOffset = item.offset + 1n;
      lane.queue.push(item);
      buffered += 1;
      updateBackpressure();
      scheduleSoon();
    },
    setAssignment: (partitions) => {
      assigned = partitions === null ? null : new Set(partitions);
      for (const [partition, lane] of lanes) {
        if (!isAssigned(partition)) {
          dropQueue(lane);
        }
      }
      updateBackpressure();
    },
    quiesce: () => {
      holding = true;
      return whenIdle();
    },
    reset: () => {
      for (const lane of lanes.values()) {
        dropQueue(lane);
      }
      holding = false;
      paused = false;
      scheduleSoon();
    },
    stop: async () => {
      running = false;
      await whenIdle();
      for (const lane of lanes.values()) {
        dropQueue(lane);
      }
    },
    activeCount: () => active,
  };
}
