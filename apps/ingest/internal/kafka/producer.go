// Package kafka produces incoming events to the topic the worker consumes,
// with the producer guarantees the Bun API's has: idempotent, acks=all, one
// request in flight per broker, no compression, and kafkajs's key-to-partition
// mapping so one device's events keep landing on one partition while both
// producers run side by side.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// SASL is the optional SASL login.
type SASL struct {
	Mechanism string // plain, scram-sha-256, scram-sha-512
	Username  string
	Password  string
}

// TLS is the optional TLS setup.
type TLS struct {
	Enabled            bool
	CAPath             string
	RejectUnauthorized *bool
}

// Config is the producer's half of the API's KAFKA_* settings.
type Config struct {
	Brokers           []string
	ClientID          string
	Topic             string
	RequestTimeout    time.Duration
	ConnectionTimeout time.Duration
	Retries           int
	InitialRetry      time.Duration
	MaxRetry          time.Duration
	MaxInFlight       int
	Linger            time.Duration
	MaxMessageBytes   int
	TLS               TLS
	SASL              *SASL
}

// Producer sends events and waits for their acknowledgement.
type Producer struct {
	client *kgo.Client
	topic  string
}

// Partitioner is the key-to-partition mapping kafkajs's default partitioner
// used: murmur2 of the key, made positive, modulo the partition count.
func Partitioner() kgo.Partitioner {
	return kgo.StickyKeyPartitioner(nil)
}

// New connects lazily; the first produce dials the brokers.
func New(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: KAFKA_BROKERS is not set")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RecordPartitioner(Partitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.MaxProduceRequestsInflightPerBroker(max(1, cfg.MaxInFlight)),
		kgo.ProducerLinger(cfg.Linger),
		kgo.ProduceRequestTimeout(cfg.RequestTimeout),
		kgo.RecordRetries(cfg.Retries),
		kgo.RetryBackoffFn(func(attempt int) time.Duration {
			backoff := cfg.InitialRetry << max(0, attempt-1)
			return min(backoff, cfg.MaxRetry)
		}),
		kgo.DialTimeout(cfg.ConnectionTimeout),
		kgo.AllowAutoTopicCreation(),
	}
	if cfg.MaxMessageBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(cfg.MaxMessageBytes)))
	}
	if cfg.TLS.Enabled {
		tlsConfig, err := tlsConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if cfg.SASL != nil {
		mechanism, err := saslMechanism(*cfg.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return &Producer{client: client, topic: cfg.Topic}, nil
}

func tlsConfig(cfg TLS) (*tls.Config, error) {
	out := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAPath != "" {
		pem, err := os.ReadFile(cfg.CAPath)
		if err != nil {
			return nil, fmt.Errorf("kafka: failed to read KAFKA_SSL_CA_PATH %q: %w", cfg.CAPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka: KAFKA_SSL_CA_PATH %q holds no certificate", cfg.CAPath)
		}
		out.RootCAs = pool
	}
	if cfg.RejectUnauthorized != nil && !*cfg.RejectUnauthorized {
		out.InsecureSkipVerify = true
	}
	return out, nil
}

func saslMechanism(cfg SASL) (sasl.Mechanism, error) {
	switch cfg.Mechanism {
	case "plain":
		return plain.Auth{User: cfg.Username, Pass: cfg.Password}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha256Mechanism(), nil
	case "", "scram-sha-512":
		return scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha512Mechanism(), nil
	}
	return nil, fmt.Errorf("kafka: unsupported KAFKA_SASL_MECHANISM %q", cfg.Mechanism)
}

// Produce sends one event and returns once the broker acknowledged it.
// Concurrent calls share batches: the client lingers for company.
func (p *Producer) Produce(ctx context.Context, key, value []byte) error {
	record := &kgo.Record{Key: key, Value: value, Topic: p.topic}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce: %w", err)
	}
	return nil
}

// Ping checks a broker answers, for readiness.
func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Close flushes whatever is still lingering, then disconnects, so a shutdown
// inside the linger window cannot strand an accepted event.
func (p *Producer) Close(ctx context.Context) error {
	err := p.client.Flush(ctx)
	p.client.Close()
	if err != nil {
		return fmt.Errorf("kafka: flush on close: %w", err)
	}
	return nil
}
