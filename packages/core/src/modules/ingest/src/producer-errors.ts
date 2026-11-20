// The producer's error classification, split out of `kafka.ts` so the batching
// path can be tested against the same recovery wrapper without a broker.

// Kafka error codes that mean the producer's PID/sequence state is
// permanently out of sync with the broker for some partition — only a
// fresh PID (new producer instance) can recover.
//   45 OUT_OF_ORDER_SEQUENCE_NUMBER
//   46 DUPLICATE_SEQUENCE_NUMBER
//   47 INVALID_PRODUCER_EPOCH
//   65 UNKNOWN_PRODUCER_ID
const FATAL_PRODUCER_ERROR_CODES = new Set<number>([45, 46, 47, 65]);

const AGGREGATE_ERROR_CODE = 'PLT_KFK_MULTIPLE';
/** How the client words an operation it stopped retrying. */
const RETRIES_EXHAUSTED_MESSAGE = /failed \d+ times\.$/;

interface ClientErrorFields {
  apiCode?: unknown;
  code?: unknown;
  message?: unknown;
  errors?: unknown;
  cause?: unknown;
}

/**
 * The client reports a failed produce as a tree: retries aggregate their
 * attempts, a response aggregates its per-partition errors, and the broker's
 * code sits on a leaf.
 */
function* errorTree(
  err: unknown,
  seen = new Set<unknown>()
): Generator<ClientErrorFields> {
  if (!err || typeof err !== 'object' || seen.has(err)) {
    return;
  }
  seen.add(err);
  const node = err as ClientErrorFields;
  yield node;
  if (Array.isArray(node.errors)) {
    for (const child of node.errors) {
      yield* errorTree(child, seen);
    }
  }
  yield* errorTree(node.cause, seen);
}

const isRetriesExhausted = (node: ClientErrorFields): boolean =>
  node.code === AGGREGATE_ERROR_CODE &&
  typeof node.message === 'string' &&
  RETRIES_EXHAUSTED_MESSAGE.test(node.message);

export const isFatalProducerError = (err: unknown): boolean => {
  for (const node of errorTree(err)) {
    if (
      typeof node.apiCode === 'number' &&
      FATAL_PRODUCER_ERROR_CODES.has(node.apiCode)
    ) {
      return true;
    }
    // Retries-exceeded leaves the idempotent producer's sequence state
    // suspect (broker may have persisted a batch we gave up on), so treat
    // it as fatal-for-this-producer too.
    if (isRetriesExhausted(node)) {
      return true;
    }
  }
  return false;
};

/**
 * Runs a produce call and hands a fatal producer failure to `onFatal` before
 * rethrowing. The error always propagates — `onFatal` is the recovery side
 * effect (drop and rebuild the producer), never a swallow.
 */
export const sendWithFatalRecovery = async (
  produce: () => Promise<void>,
  onFatal: (err: unknown) => void
): Promise<void> => {
  try {
    await produce();
  } catch (err) {
    if (isFatalProducerError(err)) {
      onFatal(err);
    }
    throw err;
  }
};
