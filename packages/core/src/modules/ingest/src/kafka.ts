// NOTHING here constructs a Kafka client at import time: the clients are lazy
// and only `assertKafkaConfigured` reads the broker list eagerly, so core stays
// importable with no broker and `bun test` still runs offline.

import {
  Admin,
  type BaseOptions,
  Consumer,
  ListOffsetTimestamps,
  type MessageToProduce,
  Producer,
  type RetryDelayGetter,
} from '@platformatic/kafka';
import type { CoreConfig, KafkaConfig } from '../../../config';
import { createLogger, type ILogger } from '../../../pino-logger';
// One definition, not two: the consumer already declares the dead-letter
// message shape it hands to this producer.
import type { DeadLetterMessage, KafkaHeaders } from './consumer';
import type { IncomingEventPayload } from './incoming-event';
import {
  describeKafkaSecurity,
  type ResolvedKafkaSecurity,
  resolveKafkaSecurity,
} from './kafka-security';
import { eventsPartitioner } from './partitioner';
import {
  createProducerBatcher,
  type ProducerBatcher,
} from './producer-batcher';
import { sendWithFatalRecovery } from './producer-errors';
import {
  type ResolvedProducerTuning,
  resolveProducerTuning,
} from './producer-tuning';

export type { Admin } from '@platformatic/kafka';

// Approx size of one event payload (observed range ~0.9–1.3 KiB).
// We size fetch knobs in messages and convert to bytes via this constant.
const KAFKA_BYTES_PER_MESSAGE = 1024;

/**
 * A JoinGroup is held by the broker until every member has rejoined, for up to
 * the rebalance timeout — and a member that died without leaving never does.
 * Every consumer request therefore waits that long plus the usual request
 * timeout; a shorter wait abandons a join the broker was about to answer.
 */
const consumerRequestTimeoutMs = (kafka: KafkaConfig): number =>
  kafka.rebalanceTimeoutMs + kafka.requestTimeoutMs;

const PRODUCER_RETRY_BACKOFF_FACTOR = 2;

/**
 * The group protocol kafkajs advertised. Members of one consumer group must
 * share a protocol name, so a member that advertised the client's own
 * `roundrobin` could not join a group that still holds a kafkajs member — a
 * rolling upgrade, or a rollback, would stall on `INCONSISTENT_GROUP_PROTOCOL`.
 * The name is only a label; the assignment is round robin either way.
 */
export const EVENTS_GROUP_PROTOCOLS = [
  { name: 'RoundRobinAssigner', version: 0 },
];

// One logger per process, built on first use from the config the caller was
// handed. `loadConfig` runs once at boot, so the memo cannot serve one caller
// another caller's brokers.
let kafkaLoggerInstance: ILogger | null = null;
export const kafkaLogger = (config: CoreConfig): ILogger => {
  kafkaLoggerInstance ??= createLogger({ name: 'kafka', config });
  return kafkaLoggerInstance;
};

// Kafka/Redpanda is the sole events transport: there is no fallback, so an
// unset KAFKA_BROKERS must fail loudly at boot rather than quietly at the first
// event.
export const assertKafkaConfigured = (config: KafkaConfig): void => {
  if (config.brokers.length === 0) {
    throw new Error(
      'KAFKA_BROKERS is not set. Kafka/Redpanda is the only events transport — set KAFKA_BROKERS to a comma-separated broker list.'
    );
  }
  // Reads the CA file, so an unreadable KAFKA_SSL_CA_PATH fails here at boot
  // rather than on the first produce/consume.
  getKafkaSecurity(config);
};

// Memoised: the CA file is read once per process.
let kafkaSecurity: ResolvedKafkaSecurity | null = null;
const getKafkaSecurity = (config: KafkaConfig): ResolvedKafkaSecurity => {
  kafkaSecurity ??= resolveKafkaSecurity(config.security);
  return kafkaSecurity;
};

/** What every client — producer, consumer, admin — connects with. */
const clientOptions = (config: CoreConfig): BaseOptions => {
  assertKafkaConfigured(config.kafka);
  const security = getKafkaSecurity(config.kafka);
  return {
    clientId: config.kafka.clientId,
    bootstrapBrokers: config.kafka.brokers,
    connectTimeout: config.kafka.connectionTimeoutMs,
    // Both: `requestTimeout` is how long we wait for a reply, `timeout` the
    // budget a request hands the broker (a produce's ack wait).
    requestTimeout: config.kafka.requestTimeoutMs,
    timeout: config.kafka.requestTimeoutMs,
    tls: security.tls,
    sasl: security.sasl,
  };
};

/** Exponential from `initialMs`, capped at `maxMs`. */
const exponentialRetryDelay =
  (initialMs: number, maxMs: number): RetryDelayGetter =>
  (_client, _operationId, attempt) =>
    Math.min(initialMs * PRODUCER_RETRY_BACKOFF_FACTOR ** (attempt - 1), maxMs);

let producer: Producer | null = null;
let producerConnectPromise: Promise<Producer> | null = null;

const getProducer = async (config: CoreConfig): Promise<Producer> => {
  if (producer) {
    return producer;
  }
  if (!producerConnectPromise) {
    const tuning = resolveProducerTuning(config.kafka);
    const p = new Producer({
      ...clientOptions(config),
      idempotent: true,
      // Stays 1 (not 5) to avoid in-flight reordering after a transient broker
      // hiccup: with idempotency on and low retries, reordered batches trip
      // OUT_OF_ORDER_SEQUENCE_NUMBER and stick the producer per-partition.
      // Raising it gained only a couple percent of throughput and was
      // rejected; batching is what amortises the round-trip.
      // KAFKA_PRODUCER_MAX_IN_FLIGHT still raises it for a measurement run, but it
      // is coupled to the retry policy.
      maxInflights: tuning.maxInFlight,
      autocreateTopics: true,
      partitioner: eventsPartitioner,
      retries: config.kafka.producerRetries,
      retryDelay: exponentialRetryDelay(
        config.kafka.producerInitialRetryMs,
        config.kafka.producerMaxRetryMs
      ),
    });
    producerConnectPromise = p
      .connectToBrokers()
      .then(() => {
        producer = p;
        kafkaLogger(config).info(
          {
            brokers: config.kafka.brokers,
            topic: config.kafka.eventsTopic,
            // `ssl: bool` and the mechanism only — never a credential.
            ...describeKafkaSecurity(getKafkaSecurity(config.kafka)),
            // The throughput knobs, logged once, so a measurement run can be
            // tied to the configuration that produced it.
            maxInFlight: tuning.maxInFlight,
            batchSize: tuning.batchSize,
            batchLingerMs: tuning.lingerMs,
            batching: tuning.batchingEnabled,
          },
          'kafka producer connected'
        );
        return p;
      })
      .catch((err) => {
        producerConnectPromise = null;
        p.close().catch(() => undefined);
        throw err;
      });
  }
  return producerConnectPromise;
};

const resetProducer = (config: CoreConfig, broken: Producer): void => {
  if (producer !== broken) {
    return;
  }
  producer = null;
  producerConnectPromise = null;
  broken.close().catch((err) => {
    kafkaLogger(config).warn(
      { err },
      'kafka producer close after fatal error failed'
    );
  });
};

interface OutgoingMessage {
  key: Buffer | null;
  value: Buffer | null;
  headers?: KafkaHeaders;
}

const toProducedMessage = (
  topic: string,
  message: OutgoingMessage
): MessageToProduce => ({
  topic,
  key: message.key ?? undefined,
  value: message.value ?? undefined,
  headers: message.headers,
});

// One send, one or many messages. The batching path (below) and the
// unbatched path are the same call, so a batch that fails resets the producer
// exactly like a lone message did.
const sendMessages = (
  config: CoreConfig,
  topic: string,
  messages: OutgoingMessage[]
): Promise<void> =>
  getProducer(config).then((p) =>
    sendWithFatalRecovery(
      async () => {
        await p.send({
          messages: messages.map((message) =>
            toProducedMessage(topic, message)
          ),
        });
      },
      (err) => {
        kafkaLogger(config).warn(
          { err },
          'kafka producer in fatal state; resetting for next call'
        );
        resetProducer(config, p);
      }
    )
  );

// Built on the first batched produce, from the tuning the config carries.
// Only the events topic batches: the dead-letter path is low volume and its
// messages are produced one poison record at a time, so it has no round-trip
// to amortise.
let eventsBatcher: ProducerBatcher<OutgoingMessage> | null = null;

const getEventsBatcher = (
  config: CoreConfig,
  tuning: ResolvedProducerTuning
): ProducerBatcher<OutgoingMessage> => {
  eventsBatcher ??= createProducerBatcher<OutgoingMessage>({
    batchSize: tuning.batchSize,
    lingerMs: tuning.lingerMs,
    send: (messages) =>
      sendMessages(config, config.kafka.eventsTopic, messages),
  });
  return eventsBatcher;
};

export const produceIncomingEvent = async (
  config: CoreConfig,
  payload: IncomingEventPayload,
  partitionKey: string
): Promise<void> => {
  const message: OutgoingMessage = {
    key: Buffer.from(partitionKey),
    value: Buffer.from(JSON.stringify(payload)),
  };
  const tuning = resolveProducerTuning(config.kafka);
  if (!tuning.batchingEnabled) {
    await sendMessages(config, config.kafka.eventsTopic, [message]);
    return;
  }
  await getEventsBatcher(config, tuning).enqueue(message);
};

// Why the reason/error/coordinates travel as headers and not in the value: the
// value stays the producer's original bytes, so a DLQ message can be replayed
// onto the events topic unchanged.
export const produceDeadLetterEvent = async (
  config: CoreConfig,
  message: DeadLetterMessage
): Promise<void> =>
  sendMessages(config, config.kafka.eventsDlqTopic, [
    {
      key: message.key,
      value: message.value,
      headers: {
        ...message.headers,
        'dlq-source-topic': Buffer.from(message.topic),
        'dlq-source-partition': Buffer.from(String(message.partition)),
        'dlq-source-offset': Buffer.from(message.offset),
        'dlq-reason': Buffer.from(message.reason),
        'dlq-error': Buffer.from(message.error),
        'dlq-at': Buffer.from(new Date().toISOString()),
      },
    },
  ]);

const consumers = new Set<Consumer>();

export const createKafkaEventsConsumer = (
  config: CoreConfig,
  options?: {
    groupId?: string;
  }
): Consumer => {
  const consumer = new Consumer({
    ...clientOptions(config),
    requestTimeout: consumerRequestTimeoutMs(config.kafka),
    groupId: options?.groupId || config.kafka.consumerGroup,
    protocols: EVENTS_GROUP_PROTOCOLS,
    // A worker may start before the first event has created the topic.
    autocreateTopics: true,
    sessionTimeout: config.kafka.sessionTimeoutMs,
    heartbeatInterval: config.kafka.heartbeatIntervalMs,
    rebalanceTimeout: config.kafka.rebalanceTimeoutMs,
    minBytes: config.kafka.minMessages * KAFKA_BYTES_PER_MESSAGE,
    maxWaitTime: config.kafka.maxWaitMs,
    maxBytesPerPartition:
      config.kafka.maxMessagesPerPartition * KAFKA_BYTES_PER_MESSAGE,
    // The consumer commits each partition's handled prefix itself.
    autocommit: false,
  });
  consumers.add(consumer);
  // Whoever opens closes: a consumer that closed on its own (the events
  // handle's `stop()`) drops out of the set so shutdown never closes it twice
  // and the set never retains a dead consumer.
  consumer.once('client:close', () => {
    consumers.delete(consumer);
  });
  return consumer;
};

// Consumer-group lag (backpressure visibility)
// A fast producer can hide a lagging consumer entirely — throughput numbers
// alone don't show it. `sampleConsumerGroupLag` reads the same end-offset-
// minus-committed-offset a broker-side tool (e.g. `rpk group describe`) would
// report, via the admin API this package already depends on, so a caller can
// poll it on an interval without shelling out to broker tooling.
export interface PartitionLag {
  partition: number;
  endOffset: number;
  committedOffset: number;
  lag: number;
}

export interface ConsumerGroupLag {
  sampledAt: number;
  totalLag: number;
  partitions: PartitionLag[];
}

export const createKafkaAdmin = (config: CoreConfig): Admin =>
  new Admin(clientOptions(config));

const listPartitionOffsets = async (
  admin: Admin,
  topic: string,
  partitionIndexes: number[],
  timestamp: bigint
): Promise<Map<number, number>> => {
  const [listed] = await admin.listOffsets({
    topics: [
      {
        name: topic,
        partitions: partitionIndexes.map((partitionIndex) => ({
          partitionIndex,
          timestamp,
        })),
      },
    ],
  });
  return new Map(
    (listed?.partitions ?? []).map((p) => [p.partitionIndex, Number(p.offset)])
  );
};

export const sampleConsumerGroupLag = async (
  admin: Admin,
  topic: string,
  groupId: string
): Promise<ConsumerGroupLag> => {
  const metadata = await admin.metadata({ topics: [topic] });
  const partitionCount = metadata.topics.get(topic)?.partitionsCount ?? 0;
  const partitionIndexes = Array.from({ length: partitionCount }, (_, i) => i);
  const [endOffsets, lowWatermarks, committedByGroup] = await Promise.all([
    listPartitionOffsets(
      admin,
      topic,
      partitionIndexes,
      ListOffsetTimestamps.LATEST
    ),
    listPartitionOffsets(
      admin,
      topic,
      partitionIndexes,
      ListOffsetTimestamps.EARLIEST
    ),
    admin.listConsumerGroupOffsets({
      groups: [{ groupId, topics: [{ name: topic, partitionIndexes }] }],
    }),
  ]);
  const committed = new Map<number, number>();
  const committedTopic = committedByGroup[0]?.topics.find(
    (t) => t.name === topic
  );
  for (const p of committedTopic?.partitions ?? []) {
    committed.set(p.partitionIndex, Number(p.committedOffset));
  }
  const partitions: PartitionLag[] = partitionIndexes.map((partition) => {
    const endOffset = endOffsets.get(partition) ?? 0;
    const lowWatermark = lowWatermarks.get(partition) ?? 0;
    const rawCommitted = committed.get(partition);
    // -1 means the group has never committed on this partition; treat the
    // whole backlog down to the low watermark as lag rather than computing
    // a bogus `endOffset - (-1)`.
    const committedOffset =
      rawCommitted === undefined || rawCommitted < 0
        ? lowWatermark
        : rawCommitted;
    return {
      partition,
      endOffset,
      committedOffset,
      lag: Math.max(0, endOffset - committedOffset),
    };
  });
  return {
    sampledAt: Date.now(),
    totalLag: partitions.reduce((sum, p) => sum + p.lag, 0),
    partitions,
  };
};

/** The group's state (`Stable`, `PreparingRebalance`, …) and its member count. */
export const describeConsumerGroup = async (
  admin: Admin,
  groupId: string
): Promise<{ state: string; members: number }> => {
  const groups = await admin.describeGroups({ groups: [groupId] });
  const group = groups.get(groupId);
  return {
    state: group?.state ?? 'Unknown',
    members: group?.members.size ?? 0,
  };
};

export const disconnectKafka = async (config: CoreConfig): Promise<void> => {
  // Whatever is still accumulating goes out before the producer closes, so a
  // shutdown inside the linger window cannot strand an accepted event.
  if (eventsBatcher) {
    const batcher = eventsBatcher;
    eventsBatcher = null;
    await batcher.flush().catch((err) => {
      kafkaLogger(config).error({ err }, 'kafka producer batch flush failed');
    });
  }
  const tasks: Promise<unknown>[] = [];
  for (const c of consumers) {
    tasks.push(
      c.close(true).catch((err) => {
        kafkaLogger(config).error({ err }, 'kafka consumer close error');
      })
    );
  }
  consumers.clear();
  const pending = producerConnectPromise;
  producer = null;
  producerConnectPromise = null;
  if (pending) {
    tasks.push(
      pending
        .then((p) => p.close())
        .catch((err) => {
          kafkaLogger(config).error({ err }, 'kafka producer close error');
        })
    );
  }
  await Promise.all(tasks);
};
