// Package config reads the receiver's environment. It shares every name it
// can with apps/api/src/config/env.ts, so one .env serves both processes;
// the INGEST_* names are its own.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"openpanel/ingest/internal/kafka"
)

const (
	defaultPort                = 3000
	defaultAdminPort           = 3001
	defaultRedisURL            = "redis://localhost:23379"
	defaultSessionTimeout      = 30 * time.Minute
	defaultMaxNativeBody       = 4 << 20
	defaultKafkaClientID       = "openpanel"
	defaultKafkaTopic          = "events"
	defaultRequestTimeoutMs    = 5000
	defaultConnectTimeoutMs    = 2000
	defaultMaxMessageBytes     = 1_048_576
	defaultProducerRetries     = 2
	defaultInitialRetryMs      = 100
	defaultMaxRetryMs          = 1000
	defaultMaxInFlight         = 1
	defaultBatchLingerMs       = 5
	defaultRedisCommandTimeout = 1 * time.Second
)

// Config is the whole receiver configuration.
type Config struct {
	Host               string
	Port               int
	AdminPort          int
	UpstreamURL        *url.URL
	GeoDir             string
	MaxNativeBodyBytes int64
	RedisURL           string
	RedisTimeout       time.Duration
	DatabaseURL        string
	SessionTimeout     time.Duration
	IPHeaderOrder      []string
	SelfHosted         bool
	Production         bool
	LogLevel           string
	Kafka              kafka.Config
}

// Load reads the configuration from getenv (os.Getenv in production).
func Load(getenv func(string) string) (Config, error) {
	var problems []error
	env := func(name string) string { return strings.TrimSpace(getenv(name)) }
	intEnv := func(name string, fallback int) int {
		raw := env(name)
		if raw == "" {
			return fallback
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			problems = append(problems, fmt.Errorf("%s must be a non-negative integer, got %q", name, raw))
			return fallback
		}
		return n
	}
	boolEnv := func(name string) *bool {
		switch env(name) {
		case "":
			return nil
		case "true":
			v := true
			return &v
		case "false":
			v := false
			return &v
		}
		problems = append(problems, fmt.Errorf("%s must be true or false", name))
		return nil
	}

	cfg := Config{
		Host:               env("INGEST_HOST"),
		Port:               intEnv("INGEST_PORT", defaultPort),
		AdminPort:          intEnv("INGEST_ADMIN_PORT", defaultAdminPort),
		GeoDir:             env("INGEST_GEO_DIR"),
		MaxNativeBodyBytes: int64(intEnv("INGEST_MAX_BODY_BYTES", defaultMaxNativeBody)),
		RedisURL:           env("REDIS_URL"),
		RedisTimeout:       time.Duration(intEnv("INGEST_REDIS_TIMEOUT_MS", int(defaultRedisCommandTimeout/time.Millisecond))) * time.Millisecond,
		DatabaseURL:        env("DATABASE_URL"),
		SessionTimeout:     defaultSessionTimeout,
		IPHeaderOrder:      tokenList(env("IP_HEADER_ORDER")),
		SelfHosted:         env("SELF_HOSTED") == "true" || env("SELF_HOSTED") == "1",
		Production:         env("NODE_ENV") == "production",
		LogLevel:           strings.ToLower(env("LOG_LEVEL")),
	}
	if cfg.GeoDir == "" {
		cfg.GeoDir = "."
	}
	if cfg.RedisURL == "" {
		cfg.RedisURL = defaultRedisURL
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if raw := env("SESSION_TIMEOUT_MS"); raw != "" {
		cfg.SessionTimeout = time.Duration(intEnv("SESSION_TIMEOUT_MS", 0)) * time.Millisecond
		if cfg.SessionTimeout <= 0 {
			problems = append(problems, errors.New("SESSION_TIMEOUT_MS must be positive"))
		}
	}

	upstream := env("INGEST_UPSTREAM_URL")
	if upstream == "" {
		problems = append(problems, errors.New("INGEST_UPSTREAM_URL is not set: the receiver hands every request it does not handle itself to the Bun API at this URL"))
	} else if parsed, err := url.Parse(upstream); err != nil || parsed.Scheme == "" || parsed.Host == "" {
		problems = append(problems, fmt.Errorf("INGEST_UPSTREAM_URL %q is not an absolute URL", upstream))
	} else {
		cfg.UpstreamURL = parsed
	}
	if cfg.DatabaseURL == "" {
		problems = append(problems, errors.New("DATABASE_URL is not set"))
	}

	cfg.Kafka = kafka.Config{
		Brokers:           tokenList(env("KAFKA_BROKERS")),
		ClientID:          withDefault(env("KAFKA_CLIENT_ID"), defaultKafkaClientID),
		Topic:             withDefault(env("KAFKA_EVENTS_TOPIC"), defaultKafkaTopic),
		RequestTimeout:    time.Duration(intEnv("KAFKA_REQUEST_TIMEOUT_MS", defaultRequestTimeoutMs)) * time.Millisecond,
		ConnectionTimeout: time.Duration(intEnv("KAFKA_CONNECTION_TIMEOUT_MS", defaultConnectTimeoutMs)) * time.Millisecond,
		Retries:           intEnv("KAFKA_PRODUCER_RETRIES", defaultProducerRetries),
		InitialRetry:      time.Duration(intEnv("KAFKA_PRODUCER_INITIAL_RETRY_MS", defaultInitialRetryMs)) * time.Millisecond,
		MaxRetry:          time.Duration(intEnv("KAFKA_PRODUCER_MAX_RETRY_MS", defaultMaxRetryMs)) * time.Millisecond,
		MaxInFlight:       intEnv("KAFKA_PRODUCER_MAX_IN_FLIGHT", defaultMaxInFlight),
		Linger:            time.Duration(intEnv("KAFKA_PRODUCER_BATCH_LINGER_MS", defaultBatchLingerMs)) * time.Millisecond,
		MaxMessageBytes:   intEnv("KAFKA_MAX_MESSAGE_BYTES", defaultMaxMessageBytes),
	}
	// KAFKA_PRODUCER_BATCH_SIZE below 2 turns batching off in the API.
	if intEnv("KAFKA_PRODUCER_BATCH_SIZE", 2) < 2 {
		cfg.Kafka.Linger = 0
	}
	if len(cfg.Kafka.Brokers) == 0 {
		problems = append(problems, errors.New("KAFKA_BROKERS is not set. Kafka/Redpanda is the only events transport — set KAFKA_BROKERS to a comma-separated broker list"))
	}

	username, password := env("KAFKA_SASL_USERNAME"), env("KAFKA_SASL_PASSWORD")
	saslEnabled := username != "" || password != ""
	switch {
	case saslEnabled && (username == "" || password == ""):
		problems = append(problems, errors.New("Kafka SASL is partially configured: both KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required"))
	case saslEnabled:
		cfg.Kafka.SASL = &kafka.SASL{Mechanism: withDefault(env("KAFKA_SASL_MECHANISM"), "scram-sha-512"), Username: username, Password: password}
	case env("KAFKA_SASL_MECHANISM") != "":
		problems = append(problems, errors.New("KAFKA_SASL_MECHANISM is set but KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are missing"))
	}
	ssl := boolEnv("KAFKA_SSL")
	cfg.Kafka.TLS = kafka.TLS{Enabled: saslEnabled, CAPath: env("KAFKA_SSL_CA_PATH"), RejectUnauthorized: boolEnv("KAFKA_SSL_REJECT_UNAUTHORIZED")}
	if ssl != nil {
		cfg.Kafka.TLS.Enabled = *ssl
	}
	if !cfg.Kafka.TLS.Enabled && (cfg.Kafka.TLS.CAPath != "" || cfg.Kafka.TLS.RejectUnauthorized != nil) {
		problems = append(problems, errors.New("KAFKA_SSL_CA_PATH / KAFKA_SSL_REJECT_UNAUTHORIZED require TLS; set KAFKA_SSL=true"))
	}

	if len(problems) > 0 {
		return Config{}, errors.Join(problems...)
	}
	return cfg, nil
}

// FromEnvironment is Load(os.Getenv).
func FromEnvironment() (Config, error) { return Load(os.Getenv) }

func withDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func tokenList(raw string) []string {
	var out []string
	for _, token := range strings.Split(raw, ",") {
		if token = strings.TrimSpace(token); token != "" {
			out = append(out, token)
		}
	}
	return out
}

// PostgresURL turns a Prisma connection string into one pgx accepts: Prisma's
// own query parameters are removed and ?schema= becomes the search_path.
func PostgresURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("config: DATABASE_URL: %w", err)
	}
	query := u.Query()
	schema := query.Get("schema")
	for _, prismaOnly := range []string{"schema", "connection_limit", "pool_timeout", "pgbouncer", "socket_timeout", "statement_cache_size", "sslaccept", "sslidentity", "sslpassword", "connect_timeout"} {
		query.Del(prismaOnly)
	}
	if schema != "" && schema != "public" {
		query.Set("search_path", schema)
	}
	u.RawQuery = query.Encode()
	return u.String(), nil
}
