// Package config reads the worker's environment with the names, defaults and
// parsing rules of apps/api/src/config/env.ts, so one environment file starts
// either implementation. A malformed boot-critical value fails the load;
// every problem is reported at once.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"openpanel/ingest/internal/kafka"
)

// Roles a process can have.
const (
	RoleAPI    = "api"
	RoleWorker = "worker"
	RoleAll    = "all"
)

// EventsQueueToken in ENABLED_QUEUES starts the Kafka events consumer.
const EventsQueueToken = "events"

var enabledQueueValues = []string{EventsQueueToken, "sessions", "cron", "notification", "import", "insights", "gsc", "cohortCompute"}

var renamedQueueTokens = map[string]string{"events_kafka": EventsQueueToken}

const (
	defaultRole                     = RoleAPI
	defaultAPIPort                  = 3000
	defaultLogLevel                 = "info"
	defaultShutdownForceExit        = 20_000
	defaultRedisCacheCommandTimeout = 5000
	defaultRedisURL                 = "redis://localhost:23379"
	defaultDeadLetterMaxEntries     = 1000
	defaultKafkaClientID            = "openpanel"
	defaultKafkaEventsTopic         = "events"
	defaultKafkaConsumerGroup       = "openpanel-events"
	defaultKafkaPartitionsConc      = 8
	defaultKafkaMinMessages         = 1
	defaultKafkaMaxWaitMs           = 500
	defaultKafkaMaxMessagesPerPart  = 256
	defaultKafkaSessionTimeoutMs    = 30_000
	defaultKafkaHeartbeatMs         = 3000
	defaultKafkaRebalanceTimeoutMs  = 60_000
	defaultKafkaRequestTimeoutMs    = 5000
	defaultKafkaConnectionTimeoutMs = 2000
	defaultKafkaHandlerMaxAttempts  = 3
	defaultKafkaHandlerRetryInitial = 100
	defaultKafkaHandlerRetryMax     = 1000
	defaultSessionTimeoutMs         = 1_800_000
	defaultSASLMechanism            = "scram-sha-512"
)

// Config is the worker's environment, parsed.
type Config struct {
	Role           string
	EnabledQueues  []string
	DisableWorkers bool
	QueueCluster   bool
	QueueNamespace string

	ShutdownForceExit        time.Duration
	RedisCacheCommandTimeout time.Duration
	DeadLetterMaxEntries     int

	LogLevel    string
	NodeEnv     string
	APIPort     int
	RedisURL    string
	DatabaseURL string
	// ClickHouseURLs is the CLICKHOUSE_URL comma list as given.
	ClickHouseURLs []string

	Kafka KafkaConfig

	// SessionTimeout is SESSION_TIMEOUT_MS: the idle window of a session.
	SessionTimeout time.Duration
	// EventBufferMicroBatchSize and EventBufferMicroBatchWait are 0 when unset,
	// leaving the module's own defaults (100 and 10 ms) to the buffer.
	EventBufferMicroBatchSize int
	EventBufferMicroBatchWait time.Duration
}

// KafkaConfig is every KAFKA_* setting of the consumer path.
type KafkaConfig struct {
	Brokers       []string
	ClientID      string
	EventsTopic   string
	ConsumerGroup string

	PartitionsConcurrent    int
	MinMessages             int
	MaxWait                 time.Duration
	MaxMessagesPerPartition int

	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration
	RequestTimeout    time.Duration
	ConnectionTimeout time.Duration

	HandlerMaxAttempts  int
	HandlerRetryInitial time.Duration
	HandlerRetryMax     time.Duration

	TLS  kafka.TLS
	SASL *kafka.SASL
}

// ConsumesEvents reports whether this process runs the Kafka events consumer:
// a consuming role, workers not disabled, and the events token enabled.
func (c Config) ConsumesEvents() bool {
	if c.Role == RoleAPI || c.DisableWorkers {
		return false
	}
	for _, queue := range c.EnabledQueues {
		if queue == EventsQueueToken {
			return true
		}
	}
	return false
}

// FromEnvironment is Load(os.Getenv).
func FromEnvironment() (Config, error) { return Load(os.Getenv) }

type loader struct {
	getenv   func(string) string
	problems []error
}

func (l *loader) problem(format string, args ...any) {
	l.problems = append(l.problems, fmt.Errorf(format, args...))
}

// value is blankToUndefined: unset and whitespace-only are the same.
func (l *loader) value(name string) (string, bool) {
	v := strings.TrimSpace(l.getenv(name))
	return v, v != ""
}

func (l *loader) text(name, fallback string) string {
	if v, ok := l.value(name); ok {
		return v
	}
	return fallback
}

func (l *loader) optionalText(name string) string {
	v, _ := l.value(name)
	return v
}

func (l *loader) tokens(name string) []string {
	v, _ := l.value(name)
	var out []string
	for _, token := range strings.Split(v, ",") {
		if token = strings.TrimSpace(token); token != "" {
			out = append(out, token)
		}
	}
	return out
}

// positiveInt is positiveIntWithDefault: a malformed value fails the load.
func (l *loader) positiveInt(name string, fallback int) int {
	v, ok := l.value(name)
	if !ok {
		return fallback
	}
	n, ok := positiveInteger(v)
	if !ok {
		l.problem("%s: must be a positive integer (got %q)", name, v)
		return fallback
	}
	return n
}

// optionalPositiveInt is a tuning knob: 0 when unset or malformed.
func (l *loader) optionalPositiveInt(name string) int {
	v, ok := l.value(name)
	if !ok {
		return 0
	}
	n, _ := positiveInteger(v)
	return n
}

// nonNegativeInt is z.coerce.number().int().min(0).
func (l *loader) nonNegativeInt(name string, fallback int) int {
	v, ok := l.value(name)
	if !ok {
		return fallback
	}
	n, ok := jsNumber(v)
	if !ok || n != math.Trunc(n) || n < 0 || n > maxSafeInteger {
		l.problem("%s: must be a non-negative integer (got %q)", name, v)
		return fallback
	}
	return int(n)
}

// definedIsTrue: any non-blank value is on, including "false" and "0".
func (l *loader) definedIsTrue(name string) bool {
	_, ok := l.value(name)
	return ok
}

// strictBool accepts true, 1, false, 0; anything else fails the load.
func (l *loader) strictBool(name string) *bool {
	v, ok := l.value(name)
	if !ok {
		return nil
	}
	var out bool
	switch v {
	case "true", "1":
		out = true
	case "false", "0":
		out = false
	default:
		l.problem("%s: must be \"true\" or \"false\" (got %q)", name, v)
		return nil
	}
	return &out
}

const maxSafeInteger = 1<<53 - 1

var (
	decimalNumber = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)
	prefixedInt   = regexp.MustCompile(`^0([xX][0-9a-fA-F]+|[oO][0-7]+|[bB][01]+)$`)
)

// jsNumber is Number(text) for the strings Number accepts as finite.
func jsNumber(text string) (float64, bool) {
	switch {
	case decimalNumber.MatchString(text):
		n, err := strconv.ParseFloat(text, 64)
		return n, err == nil && !math.IsInf(n, 0)
	case prefixedInt.MatchString(text):
		n, err := strconv.ParseUint(text, 0, 64)
		return float64(n), err == nil
	}
	return 0, false
}

func positiveInteger(text string) (int, bool) {
	n, ok := jsNumber(text)
	if !ok || n != math.Trunc(n) || n <= 0 || n > maxSafeInteger {
		return 0, false
	}
	return int(n), true
}

func millis(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// Load reads the configuration from getenv.
func Load(getenv func(string) string) (Config, error) {
	l := &loader{getenv: getenv}
	cfg := Config{
		Role:                      l.role(),
		EnabledQueues:             l.enabledQueues(),
		DisableWorkers:            l.definedIsTrue("DISABLE_WORKERS"),
		QueueCluster:              l.definedIsTrue("QUEUE_CLUSTER"),
		QueueNamespace:            l.optionalText("QUEUE_NAMESPACE"),
		ShutdownForceExit:         millis(l.positiveInt("SHUTDOWN_FORCE_EXIT_MS", defaultShutdownForceExit)),
		RedisCacheCommandTimeout:  millis(l.positiveInt("REDIS_CACHE_COMMAND_TIMEOUT_MS", defaultRedisCacheCommandTimeout)),
		DeadLetterMaxEntries:      l.positiveInt("INGEST_DEAD_LETTER_MAX_ENTRIES", defaultDeadLetterMaxEntries),
		LogLevel:                  l.text("LOG_LEVEL", defaultLogLevel),
		NodeEnv:                   l.optionalText("NODE_ENV"),
		APIPort:                   l.nonNegativeInt("API_PORT", defaultAPIPort),
		RedisURL:                  l.text("REDIS_URL", defaultRedisURL),
		DatabaseURL:               l.optionalText("DATABASE_URL"),
		ClickHouseURLs:            l.tokens("CLICKHOUSE_URL"),
		SessionTimeout:            millis(orDefault(l.optionalPositiveInt("SESSION_TIMEOUT_MS"), defaultSessionTimeoutMs)),
		EventBufferMicroBatchSize: l.optionalPositiveInt("EVENT_BUFFER_MICRO_BATCH_SIZE"),
		EventBufferMicroBatchWait: millis(l.optionalPositiveInt("EVENT_BUFFER_MICRO_BATCH_MS")),
		Kafka:                     l.kafka(),
	}
	if len(cfg.EnabledQueues) == 0 && cfg.Role != RoleAPI {
		l.problem("ROLE=%s consumes queues but ENABLED_QUEUES names none — this process would boot and do nothing.", cfg.Role)
	}
	if cfg.Kafka.HeartbeatInterval >= cfg.Kafka.SessionTimeout {
		l.problem("KAFKA_HEARTBEAT_INTERVAL_MS (%d) must be below KAFKA_SESSION_TIMEOUT_MS (%d) — otherwise every consumer is evicted before its first heartbeat.",
			cfg.Kafka.HeartbeatInterval.Milliseconds(), cfg.Kafka.SessionTimeout.Milliseconds())
	}
	if cfg.ConsumesEvents() && len(cfg.Kafka.Brokers) == 0 {
		l.problem("KAFKA_BROKERS is not set. Kafka/Redpanda is the only events transport — set KAFKA_BROKERS to a comma-separated broker list")
	}
	if len(l.problems) > 0 {
		return Config{}, errors.Join(l.problems...)
	}
	return cfg, nil
}

func orDefault(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func (l *loader) role() string {
	role := l.text("ROLE", defaultRole)
	switch role {
	case RoleAPI, RoleWorker, RoleAll:
		return role
	}
	l.problem("ROLE: unknown value %q. Accepted values: api, worker, all.", role)
	return defaultRole
}

func (l *loader) enabledQueues() []string {
	if _, ok := l.value("ENABLED_QUEUES"); !ok {
		return append([]string(nil), enabledQueueValues...)
	}
	tokens := l.tokens("ENABLED_QUEUES")
	for _, token := range tokens {
		if contains(enabledQueueValues, token) {
			continue
		}
		if renamed, ok := renamedQueueTokens[token]; ok {
			l.problem("ENABLED_QUEUES: %q was renamed to %q — update ENABLED_QUEUES to use the new name.", token, renamed)
			continue
		}
		l.problem("ENABLED_QUEUES: unknown queue %q. Accepted values: %s.", token, strings.Join(enabledQueueValues, ", "))
	}
	return tokens
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func (l *loader) kafka() KafkaConfig {
	k := KafkaConfig{
		Brokers:                 l.tokens("KAFKA_BROKERS"),
		ClientID:                l.text("KAFKA_CLIENT_ID", defaultKafkaClientID),
		EventsTopic:             l.text("KAFKA_EVENTS_TOPIC", defaultKafkaEventsTopic),
		ConsumerGroup:           l.text("KAFKA_CONSUMER_GROUP", defaultKafkaConsumerGroup),
		PartitionsConcurrent:    l.positiveInt("KAFKA_PARTITIONS_CONCURRENT", defaultKafkaPartitionsConc),
		MinMessages:             l.positiveInt("KAFKA_MIN_MESSAGES", defaultKafkaMinMessages),
		MaxWait:                 millis(l.positiveInt("KAFKA_MAX_WAIT_MS", defaultKafkaMaxWaitMs)),
		MaxMessagesPerPartition: l.positiveInt("KAFKA_MAX_MESSAGES_PER_PARTITION", defaultKafkaMaxMessagesPerPart),
		SessionTimeout:          millis(l.positiveInt("KAFKA_SESSION_TIMEOUT_MS", defaultKafkaSessionTimeoutMs)),
		HeartbeatInterval:       millis(l.positiveInt("KAFKA_HEARTBEAT_INTERVAL_MS", defaultKafkaHeartbeatMs)),
		RebalanceTimeout:        millis(l.positiveInt("KAFKA_REBALANCE_TIMEOUT_MS", defaultKafkaRebalanceTimeoutMs)),
		RequestTimeout:          millis(l.positiveInt("KAFKA_REQUEST_TIMEOUT_MS", defaultKafkaRequestTimeoutMs)),
		ConnectionTimeout:       millis(l.positiveInt("KAFKA_CONNECTION_TIMEOUT_MS", defaultKafkaConnectionTimeoutMs)),
		HandlerMaxAttempts:      l.positiveInt("KAFKA_HANDLER_MAX_ATTEMPTS", defaultKafkaHandlerMaxAttempts),
		HandlerRetryInitial:     millis(l.positiveInt("KAFKA_HANDLER_RETRY_INITIAL_MS", defaultKafkaHandlerRetryInitial)),
		HandlerRetryMax:         millis(l.positiveInt("KAFKA_HANDLER_RETRY_MAX_MS", defaultKafkaHandlerRetryMax)),
	}
	l.kafkaSecurity(&k)
	return k
}

func (l *loader) kafkaSecurity(k *KafkaConfig) {
	username, password := l.optionalText("KAFKA_SASL_USERNAME"), l.optionalText("KAFKA_SASL_PASSWORD")
	mechanism := strings.ToLower(l.optionalText("KAFKA_SASL_MECHANISM"))
	if mechanism != "" && !contains([]string{"plain", "scram-sha-256", "scram-sha-512"}, mechanism) {
		l.problem("Unsupported KAFKA_SASL_MECHANISM %q. Supported: plain, scram-sha-256, scram-sha-512", mechanism)
	}
	saslEnabled := username != "" || password != ""
	switch {
	case saslEnabled && (username == "" || password == ""):
		missing := "KAFKA_SASL_PASSWORD"
		if username == "" {
			missing = "KAFKA_SASL_USERNAME"
		}
		l.problem("Kafka SASL is partially configured: %s is missing (both KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required)", missing)
	case saslEnabled:
		if mechanism == "" {
			mechanism = defaultSASLMechanism
		}
		k.SASL = &kafka.SASL{Mechanism: mechanism, Username: username, Password: password}
	case mechanism != "":
		l.problem("KAFKA_SASL_MECHANISM is set but KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are missing")
	}

	ssl := l.strictBool("KAFKA_SSL")
	k.TLS = kafka.TLS{Enabled: saslEnabled, CAPath: l.optionalText("KAFKA_SSL_CA_PATH"), RejectUnauthorized: l.strictBool("KAFKA_SSL_REJECT_UNAUTHORIZED")}
	if ssl != nil {
		k.TLS.Enabled = *ssl
	}
	if !k.TLS.Enabled && (k.TLS.CAPath != "" || k.TLS.RejectUnauthorized != nil) {
		l.problem("KAFKA_SSL_CA_PATH / KAFKA_SSL_REJECT_UNAUTHORIZED require TLS; set KAFKA_SSL=true")
	}
}
