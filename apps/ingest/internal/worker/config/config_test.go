package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func env(pairs map[string]string) func(string) string {
	return func(name string) string { return pairs[name] }
}

type goldenKafka struct {
	ClientID              string   `json:"clientId"`
	Brokers               []string `json:"brokers"`
	EventsTopic           string   `json:"eventsTopic"`
	ConsumerGroup         string   `json:"consumerGroup"`
	PartitionsConcurrent  int      `json:"partitionsConcurrent"`
	MinMessages           int      `json:"minMessages"`
	MaxWaitMs             int      `json:"maxWaitMs"`
	MaxMessagesPerPart    int      `json:"maxMessagesPerPartition"`
	SessionTimeoutMs      int      `json:"sessionTimeoutMs"`
	HeartbeatIntervalMs   int      `json:"heartbeatIntervalMs"`
	RebalanceTimeoutMs    int      `json:"rebalanceTimeoutMs"`
	RequestTimeoutMs      int      `json:"requestTimeoutMs"`
	ConnectionTimeoutMs   int      `json:"connectionTimeoutMs"`
	HandlerMaxAttempts    int      `json:"handlerMaxAttempts"`
	HandlerRetryInitialMs int      `json:"handlerRetryInitialMs"`
	HandlerRetryMaxMs     int      `json:"handlerRetryMaxMs"`
	SSLEnabled            bool     `json:"sslEnabled"`
	SSLCaPath             *string  `json:"sslCaPath"`
	SSLRejectUnauthorized *bool    `json:"sslRejectUnauthorized"`
	SASL                  *struct {
		Mechanism string `json:"mechanism"`
		Username  string `json:"username"`
		Password  string `json:"password"`
	} `json:"sasl"`
}

type goldenConfig struct {
	Role                     string      `json:"ROLE"`
	EnabledQueues            []string    `json:"ENABLED_QUEUES"`
	DisableWorkers           bool        `json:"DISABLE_WORKERS"`
	QueueCluster             bool        `json:"QUEUE_CLUSTER"`
	QueueNamespace           *string     `json:"QUEUE_NAMESPACE"`
	ShutdownForceExitMs      int         `json:"SHUTDOWN_FORCE_EXIT_MS"`
	RedisCacheCommandTimeout int         `json:"REDIS_CACHE_COMMAND_TIMEOUT_MS"`
	DeadLetterMaxEntries     int         `json:"INGEST_DEAD_LETTER_MAX_ENTRIES"`
	APIPort                  int         `json:"API_PORT"`
	LogLevel                 string      `json:"LOG_LEVEL"`
	Kafka                    goldenKafka `json:"kafka"`
}

// TestLoadMatchesTheTypeScriptEnvLoader runs every case of
// parity/golden-consumer.ts: loadConfig's verdict and values for the same env.
func TestLoadMatchesTheTypeScriptEnvLoader(t *testing.T) {
	raw, err := os.ReadFile("testdata/config.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Env    map[string]string `json:"env"`
		OK     bool              `json:"ok"`
		Config goldenConfig      `json:"config"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		name, _ := json.Marshal(c.Env)
		got, err := Load(env(c.Env))
		if !c.OK {
			if err == nil {
				t.Errorf("%s: TypeScript rejects it, Go accepted", name)
			}
			continue
		}
		if err != nil {
			// The TS loader leaves a missing broker list to the consumer's boot.
			if strings.Contains(err.Error(), "KAFKA_BROKERS is not set") && len(c.Config.Kafka.Brokers) == 0 {
				continue
			}
			t.Errorf("%s: Go rejects it: %v", name, err)
			continue
		}
		compare(t, string(name), got, c.Config)
	}
}

func compare(t *testing.T, name string, got Config, want goldenConfig) {
	t.Helper()
	k, w := got.Kafka, want.Kafka
	ms := func(d time.Duration) int { return int(d.Milliseconds()) }
	checks := []struct {
		field     string
		got, want any
	}{
		{"ROLE", got.Role, want.Role},
		{"ENABLED_QUEUES", strings.Join(got.EnabledQueues, ","), strings.Join(want.EnabledQueues, ",")},
		{"DISABLE_WORKERS", got.DisableWorkers, want.DisableWorkers},
		{"QUEUE_CLUSTER", got.QueueCluster, want.QueueCluster},
		{"QUEUE_NAMESPACE", got.QueueNamespace, derefOr(want.QueueNamespace, "")},
		{"SHUTDOWN_FORCE_EXIT_MS", ms(got.ShutdownForceExit), want.ShutdownForceExitMs},
		{"REDIS_CACHE_COMMAND_TIMEOUT_MS", ms(got.RedisCacheCommandTimeout), want.RedisCacheCommandTimeout},
		{"INGEST_DEAD_LETTER_MAX_ENTRIES", got.DeadLetterMaxEntries, want.DeadLetterMaxEntries},
		{"API_PORT", got.APIPort, want.APIPort},
		{"LOG_LEVEL", got.LogLevel, want.LogLevel},
		{"clientId", k.ClientID, w.ClientID},
		{"brokers", strings.Join(k.Brokers, ","), strings.Join(w.Brokers, ",")},
		{"eventsTopic", k.EventsTopic, w.EventsTopic},
		{"consumerGroup", k.ConsumerGroup, w.ConsumerGroup},
		{"partitionsConcurrent", k.PartitionsConcurrent, w.PartitionsConcurrent},
		{"minMessages", k.MinMessages, w.MinMessages},
		{"maxWaitMs", ms(k.MaxWait), w.MaxWaitMs},
		{"maxMessagesPerPartition", k.MaxMessagesPerPartition, w.MaxMessagesPerPart},
		{"sessionTimeoutMs", ms(k.SessionTimeout), w.SessionTimeoutMs},
		{"heartbeatIntervalMs", ms(k.HeartbeatInterval), w.HeartbeatIntervalMs},
		{"rebalanceTimeoutMs", ms(k.RebalanceTimeout), w.RebalanceTimeoutMs},
		{"requestTimeoutMs", ms(k.RequestTimeout), w.RequestTimeoutMs},
		{"connectionTimeoutMs", ms(k.ConnectionTimeout), w.ConnectionTimeoutMs},
		{"handlerMaxAttempts", k.HandlerMaxAttempts, w.HandlerMaxAttempts},
		{"handlerRetryInitialMs", ms(k.HandlerRetryInitial), w.HandlerRetryInitialMs},
		{"handlerRetryMaxMs", ms(k.HandlerRetryMax), w.HandlerRetryMaxMs},
		{"sslEnabled", k.TLS.Enabled, w.SSLEnabled},
		{"sslCaPath", k.TLS.CAPath, derefOr(w.SSLCaPath, "")},
		{"sslRejectUnauthorized", fmtBoolPtr(k.TLS.RejectUnauthorized), fmtBoolPtr(w.SSLRejectUnauthorized)},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: %s = %v, TypeScript %v", name, c.field, c.got, c.want)
		}
	}
	switch {
	case w.SASL == nil && k.SASL != nil, w.SASL != nil && k.SASL == nil:
		t.Errorf("%s: sasl = %+v, TypeScript %+v", name, k.SASL, w.SASL)
	case w.SASL != nil && (k.SASL.Mechanism != w.SASL.Mechanism || k.SASL.Username != w.SASL.Username || k.SASL.Password != w.SASL.Password):
		t.Errorf("%s: sasl = %+v, TypeScript %+v", name, k.SASL, w.SASL)
	}
}

func derefOr(s *string, fallback string) string {
	if s == nil {
		return fallback
	}
	return *s
}

func fmtBoolPtr(b *bool) string {
	switch {
	case b == nil:
		return "unset"
	case *b:
		return "true"
	}
	return "false"
}

func TestConsumesEventsNeedsAConsumingRoleWorkersOnAndTheEventsToken(t *testing.T) {
	base := map[string]string{"KAFKA_BROKERS": "b:1"}
	with := func(extra map[string]string) Config {
		merged := map[string]string{}
		for k, v := range base {
			merged[k] = v
		}
		for k, v := range extra {
			merged[k] = v
		}
		cfg, err := Load(env(merged))
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	cases := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{"api role", nil, false},
		{"worker, ENABLED_QUEUES unset", map[string]string{"ROLE": "worker"}, false},
		{"all role, ENABLED_QUEUES unset", map[string]string{"ROLE": "all"}, false},
		{"all role, events", map[string]string{"ROLE": "all", "ENABLED_QUEUES": "events"}, true},
		{"workers disabled", map[string]string{"ROLE": "worker", "DISABLE_WORKERS": "false"}, false},
		{"no events token", map[string]string{"ROLE": "worker", "ENABLED_QUEUES": "sessions,cron"}, false},
		{"events only", map[string]string{"ROLE": "worker", "ENABLED_QUEUES": "events"}, true},
	}
	for _, c := range cases {
		if got := with(c.env).ConsumesEvents(); got != c.want {
			t.Errorf("%s: ConsumesEvents = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestLoadDefaultsOfTheVariablesTheGoldensDoNotCover(t *testing.T) {
	cfg, err := Load(env(map[string]string{"ROLE": "worker", "KAFKA_BROKERS": "b:1"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RedisURL != "redis://localhost:23379" || cfg.SessionTimeout != 30*time.Minute || cfg.EventBufferMicroBatchSize != 0 {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestLoadReportsEveryProblemAtOnce(t *testing.T) {
	_, err := Load(env(map[string]string{"ROLE": "bogus", "KAFKA_MIN_MESSAGES": "0", "KAFKA_SSL": "maybe"}))
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"ROLE", "KAFKA_MIN_MESSAGES", "KAFKA_SSL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s in %v", want, err)
		}
	}
}

func TestOptionalKnobsFallBackWhenMalformed(t *testing.T) {
	cfg, err := Load(env(map[string]string{"SESSION_TIMEOUT_MS": "abc", "EVENT_BUFFER_MICRO_BATCH_SIZE": "-1", "EVENT_BUFFER_MICRO_BATCH_MS": "5"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SessionTimeout != 30*time.Minute || cfg.EventBufferMicroBatchSize != 0 || cfg.EventBufferMicroBatchWait != 5*time.Millisecond {
		t.Fatalf("cfg = %+v", cfg)
	}
}
