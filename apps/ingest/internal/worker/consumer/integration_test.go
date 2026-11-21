package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"openpanel/ingest/internal/worker/wire"
)

const (
	integrationEnv        = "WPARITY_KAFKA"
	integrationPartitions = 2
	integrationRecords    = 6
	integrationTimeout    = 90 * time.Second
)

// brokerRig is a wparity-prefixed topic and group on a real broker.
type brokerRig struct {
	brokers []string
	topic   string
	group   string
	admin   *kgo.Client
}

func newBrokerRig(t *testing.T) *brokerRig {
	t.Helper()
	address := os.Getenv(integrationEnv)
	if address == "" {
		t.Skipf("%s is not set (e.g. localhost:23092)", integrationEnv)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	rig := &brokerRig{brokers: strings.Split(address, ","), topic: "wparity-consumer-" + suffix, group: "wparity-consumer-" + suffix}
	client, err := kgo.NewClient(kgo.SeedBrokers(rig.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	rig.admin = client
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	create := kmsg.NewCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic, topic.NumPartitions, topic.ReplicationFactor = rig.topic, integrationPartitions, 1
	create.Topics = append(create.Topics, topic)
	response, err := create.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	for _, created := range response.Topics {
		if created.ErrorCode != 0 {
			t.Fatalf("create %s: error code %d", created.Topic, created.ErrorCode)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		deleteTopic := kmsg.NewDeleteTopicsRequest()
		entry := kmsg.NewDeleteTopicsRequestTopic()
		entry.Topic = &rig.topic
		deleteTopic.Topics = append(deleteTopic.Topics, entry)
		deleteTopic.TopicNames = []string{rig.topic}
		_, _ = deleteTopic.RequestWith(ctx, client)
		deleteGroup := kmsg.NewDeleteGroupsRequest()
		deleteGroup.Groups = []string{rig.group}
		_, _ = deleteGroup.RequestWith(ctx, client)
		client.Close()
	})
	return rig
}

func (r *brokerRig) produce(t *testing.T) {
	t.Helper()
	producer, err := kgo.NewClient(kgo.SeedBrokers(r.brokers...), kgo.DefaultProduceTopic(r.topic), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	records := make([]*kgo.Record, integrationRecords)
	for i := range records {
		records[i] = &kgo.Record{
			Partition: int32(i % integrationPartitions),
			Key:       []byte(fmt.Sprint("device-", i)),
			Value:     []byte(payload(int64(i))),
		}
	}
	if err := producer.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// committed is how many records of the topic the group has committed past.
func (r *brokerRig) committed(t *testing.T) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	request := kmsg.NewOffsetFetchRequest()
	request.Group = r.group
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = r.group
	partitions := make([]int32, integrationPartitions)
	for i := range partitions {
		partitions[i] = int32(i)
	}
	legacy := kmsg.NewOffsetFetchRequestTopic()
	legacy.Topic, legacy.Partitions = r.topic, partitions
	grouped := kmsg.NewOffsetFetchRequestGroupTopic()
	grouped.Topic, grouped.Partitions = r.topic, partitions
	request.Topics = append(request.Topics, legacy)
	group.Topics = append(group.Topics, grouped)
	request.Groups = append(request.Groups, group)
	response, err := request.RequestWith(ctx, r.admin)
	if err != nil {
		t.Logf("offset fetch: %v", err)
		return 0
	}
	// The response carries the answer in Groups from version 8 and in Topics
	// before; the client fills both for the version it used.
	var total int64
	for _, topic := range response.Topics {
		for _, p := range topic.Partitions {
			total += max(p.Offset, 0)
		}
	}
	return total
}

type brokerConsumer struct {
	consumer *Consumer
	mu       sync.Mutex
	handled  map[string]int
	total    atomic.Int64
}

func (r *brokerRig) start(t *testing.T, gate func() error) *brokerConsumer {
	t.Helper()
	bc := &brokerConsumer{handled: map[string]int{}}
	cfg := KafkaConfig{
		Brokers: r.brokers, ClientID: "wparity-consumer-test", Topic: r.topic, Group: r.group,
		SessionTimeout: 10 * time.Second, HeartbeatInterval: time.Second, RebalanceTimeout: 20 * time.Second,
		RequestTimeout: 5 * time.Second, ConnectionTimeout: 5 * time.Second, MaxWait: 200 * time.Millisecond,
		MinMessages: 1, MaxMessagesPerPartition: 256,
	}
	durability := &fakeDurability{gate: gate}
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	consumer, err := Start(ctx, Config{
		NewBroker: NewKafkaBrokerFactory(cfg),
		Handler: func(_ context.Context, _ wire.IncomingEventPayload, d Delivery) error {
			bc.mu.Lock()
			bc.handled[fmt.Sprintf("%d/%d", d.Partition, d.Offset)]++
			bc.mu.Unlock()
			bc.total.Add(1)
			return nil
		},
		DeadLetter: &memoryDeadLetter{}, Durability: durability, Logger: integrationLogger(), Metrics: NopMetrics{},
		Clock: SystemClock{}, OnActivity: func() {},
		Topic: r.topic, PartitionsConcurrent: 2, MaxMessagesPerBatch: 256, PollTimeout: 200 * time.Millisecond,
		MaxAttempts: 3, InitialRetry: 50 * time.Millisecond, MaxRetry: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	bc.consumer = consumer
	t.Cleanup(consumer.Stop)
	return bc
}

// integrationLogger is quiet unless WPARITY_DEBUG is set.
func integrationLogger() *slog.Logger {
	if os.Getenv("WPARITY_DEBUG") == "" {
		return quietLogger()
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(integrationTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestBrokerCommitsOnlyAfterTheBatchIsDurable: while the durability gate fails
// the batch is handled again and again and nothing is committed; once the
// gate opens the whole topic is committed.
func TestBrokerCommitsOnlyAfterTheBatchIsDurable(t *testing.T) {
	rig := newBrokerRig(t)
	rig.produce(t)
	var open atomic.Bool
	bc := rig.start(t, func() error {
		if open.Load() {
			return nil
		}
		return errRedisDown
	})
	waitFor(t, "the batches to be handled more than once", func() bool { return bc.total.Load() >= 2*integrationRecords })
	if got := rig.committed(t); got != 0 {
		t.Fatalf("committed %d offsets while the gate was failing", got)
	}
	open.Store(true)
	waitFor(t, "every offset to be committed", func() bool { return rig.committed(t) == integrationRecords })
	bc.consumer.Stop()
}

// TestBrokerRedeliversWhatWasNeverCommitted: a consumer that handled the
// records but never made them durable leaves the group without committing; the
// next member of the group reads the same records again and commits them.
func TestBrokerRedeliversWhatWasNeverCommitted(t *testing.T) {
	rig := newBrokerRig(t)
	rig.produce(t)
	first := rig.start(t, func() error { return errRedisDown })
	waitFor(t, "the first consumer to handle the records", func() bool { return first.total.Load() >= integrationRecords })
	first.consumer.Stop()
	if got := rig.committed(t); got != 0 {
		t.Fatalf("committed %d offsets without durability", got)
	}

	second := rig.start(t, nil)
	waitFor(t, "the second consumer to commit everything", func() bool { return rig.committed(t) == integrationRecords })
	second.mu.Lock()
	defer second.mu.Unlock()
	if len(second.handled) != integrationRecords {
		t.Fatalf("redelivered %d distinct records, want %d: %v", len(second.handled), integrationRecords, second.handled)
	}
	second.consumer.Stop()

	// A third member starts from the committed offsets: nothing is delivered again.
	third := rig.start(t, nil)
	time.Sleep(3 * time.Second)
	if third.total.Load() != 0 {
		t.Fatalf("a committed record was delivered again: %v", third.handled)
	}
}
