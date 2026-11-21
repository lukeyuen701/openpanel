package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"openpanel/ingest/internal/kafka"
)

const bytesPerKiB = 1024

// KafkaConfig is the franz-go client's settings, from KAFKA_*.
type KafkaConfig struct {
	Brokers           []string
	ClientID          string
	Topic             string
	Group             string
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration
	RequestTimeout    time.Duration
	ConnectionTimeout time.Duration
	MaxWait           time.Duration
	// MinMessages and MaxMessagesPerPartition are in KiB, as the Bun worker
	// multiplies them: fetch min bytes and max bytes per partition.
	MinMessages             int
	MaxMessagesPerPartition int
	TLS                     kafka.TLS
	SASL                    *kafka.SASL
}

// KafkaBroker is the Broker over franz-go.
type KafkaBroker struct {
	client *kgo.Client
	topic  string
}

// NewKafkaBrokerFactory returns the BrokerFactory for cfg.
func NewKafkaBrokerFactory(cfg KafkaConfig) BrokerFactory {
	return func(ctx context.Context, hooks GroupHooks) (Broker, error) {
		return newKafkaBroker(ctx, cfg, hooks)
	}
}

func newKafkaBroker(ctx context.Context, cfg KafkaConfig, hooks GroupHooks) (*KafkaBroker, error) {
	topic := cfg.Topic
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(topic),
		// Decides only where a partition WITHOUT a committed offset starts.
		// From the newest message it would skip everything produced before the
		// consumer first joined; from the oldest it re-reads at worst.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kafka.EventsGroupBalancer()),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.HeartbeatInterval(cfg.HeartbeatInterval),
		kgo.RebalanceTimeout(cfg.RebalanceTimeout),
		kgo.RequestTimeoutOverhead(cfg.RequestTimeout),
		kgo.DialTimeout(cfg.ConnectionTimeout),
		kgo.FetchMaxWait(cfg.MaxWait),
		kgo.FetchMinBytes(int32(cfg.MinMessages * bytesPerKiB)),
		kgo.FetchMaxPartitionBytes(int32(cfg.MaxMessagesPerPartition * bytesPerKiB)),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.AllowAutoTopicCreation(),
		// A revocation waits for the poll's records to be queued, so none
		// reach the lanes after the partition left.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			hooks.Assigned(assigned[topic])
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			hooks.Revoked(revoked[topic])
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			hooks.Lost(lost[topic])
		}),
	}
	if cfg.TLS.Enabled {
		tlsConfig, err := kafka.TLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if cfg.SASL != nil {
		mechanism, err := kafka.SASLMechanism(*cfg.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka: brokers unreachable: %w", err)
	}
	return &KafkaBroker{client: client, topic: topic}, nil
}

// Poll implements Broker.
func (b *KafkaBroker) Poll(ctx context.Context) ([]Record, error) {
	fetches := b.client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return nil, ErrBrokerClosed
	}
	var records []Record
	fetches.EachRecord(func(r *kgo.Record) { records = append(records, convertRecord(r)) })
	var errs []error
	for _, fetchErr := range fetches.Errors() {
		errs = append(errs, fmt.Errorf("%s/%d: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err))
	}
	return records, errors.Join(errs...)
}

func convertRecord(r *kgo.Record) Record {
	record := Record{Partition: r.Partition, Offset: r.Offset, LeaderEpoch: r.LeaderEpoch, Key: r.Key, Value: r.Value}
	if len(r.Headers) > 0 {
		record.Headers = make([]Header, len(r.Headers))
		for i, h := range r.Headers {
			record.Headers[i] = Header{Key: h.Key, Value: h.Value}
		}
	}
	return record
}

// AllowRebalance implements Broker.
func (b *KafkaBroker) AllowRebalance() { b.client.AllowRebalance() }

// PauseFetch implements Broker.
func (b *KafkaBroker) PauseFetch() { b.client.PauseFetchTopics(b.topic) }

// ResumeFetch implements Broker.
func (b *KafkaBroker) ResumeFetch() { b.client.ResumeFetchTopics(b.topic) }

// Commit implements Broker. One partition per request, no commit metadata.
func (b *KafkaBroker) Commit(ctx context.Context, partition int32, offset int64, leaderEpoch int32) error {
	var result error
	b.client.CommitOffsetsSync(ctx,
		map[string]map[int32]kgo.EpochOffset{b.topic: {partition: {Epoch: leaderEpoch, Offset: offset}}},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				result = err
				return
			}
			for _, topic := range resp.Topics {
				for _, p := range topic.Partitions {
					if p.ErrorCode != 0 {
						result = kerr.ErrorForCode(p.ErrorCode)
						return
					}
				}
			}
		})
	return result
}

// Close implements Broker: leave the group and disconnect.
func (b *KafkaBroker) Close() { b.client.CloseAllowingRebalance() }
