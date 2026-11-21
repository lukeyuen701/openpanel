package config

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// CronJobNames are the ids of the Bun worker's cron queue
// (Object.keys(queues.cron.jobs), the values DISABLED_CRON_JOBS accepts), in
// registry order. A test compares them with the TypeScript registry.
var CronJobNames = []string{
	"salt", "flushEvents", "flushBots", "flushProfiles", "flushProfileBackfill",
	"flushGroups", "flushAiEvents", "insightsDaily", "insightCleanup", "weeklyDigest",
	"gscSync", "cohortRefresh", "delete", "windDown", "onboarding", "flushSessions",
	"flushReplay", "sessionReaper", "sessionVacuum", "ping", "dataHealth", "flushExports",
}

const (
	defaultInsertConcurrency        = 5
	defaultClickHouseMaxOpen        = 50
	defaultClickHouseRequestTimeout = 30_000
	minClickHouseRequestTimeoutMs   = 1000
	defaultClickHouseUnhealthyMark  = 5000

	defaultEventBatch          = 4000
	defaultEventChunk          = 1000
	defaultSessionBatch        = 10_000
	defaultProfileBatch        = 200
	defaultProfileChunk        = 1000
	defaultProfileFetchChunk   = 50
	defaultProfileTTL          = 3600
	defaultProfileBackfillSize = 1000
	defaultBotBatch            = 1000
	defaultReplayBatch         = 500
	defaultReplayChunk         = 500
	defaultGroupBatch          = 200
	defaultGroupChunk          = 1000
	defaultGroupTTL            = 3600
	defaultAiEventBatch        = 1000
	defaultAiEventChunk        = 500
)

// Setting is one ClickHouse setting as it goes on the wire.
type Setting struct{ Key, Value string }

// ClickHouseConfig is the CLICKHOUSE_* tuning of packages/db's client.ts.
type ClickHouseConfig struct {
	// Cluster is CLICKHOUSE_CLUSTER=true|1.
	Cluster bool
	// Settings is the CLICKHOUSE_SETTINGS object in key order, scalars only.
	Settings []Setting
	// RemoveConvertAnyJoin drops query_plan_convert_any_join_to_semi_or_anti_join.
	RemoveConvertAnyJoin bool
	// RequestCompression gzips insert bodies.
	RequestCompression bool
	MaxOpenConnections int
	RequestTimeout     time.Duration
	// UnhealthyMark is how long a node that refused a connection is skipped.
	UnhealthyMark time.Duration
}

// Sizing is a buffer's batch (rows read per LRANGE) and chunk (rows per insert).
type Sizing struct{ Batch, Chunk int }

// BuffersConfig is every buffer knob of the flush path (P2.8).
type BuffersConfig struct {
	// AsyncInserts is BUFFER_ASYNC_INSERTS: any non-blank value turns it on,
	// "false" and "0" included.
	AsyncInserts      bool
	InsertConcurrency int

	Event   Sizing
	Session Sizing
	// SessionSquash is SESSION_BUFFER_SQUASH, on unless "false" or "0".
	SessionSquash bool

	Profile             Sizing
	ProfileFetchChunk   int
	ProfileTTL          time.Duration
	ProfileBackfillSize int

	Bot    Sizing
	Replay Sizing
	Group  Sizing
	// GroupTTL is GROUP_BUFFER_TTL_IN_SECONDS.
	GroupTTL time.Duration
	AiEvent  Sizing
}

func (l *loader) clickhouse() ClickHouseConfig {
	return ClickHouseConfig{
		Cluster:              rawIn(l.getenv("CLICKHOUSE_CLUSTER"), "true", "1"),
		Settings:             l.clickhouseSettings(),
		RemoveConvertAnyJoin: l.getenv("CLICKHOUSE_SETTINGS_REMOVE_CONVERT_ANY_JOIN") != "",
		RequestCompression:   !rawIn(l.getenv("CLICKHOUSE_REQUEST_COMPRESSION"), "false", "0"),
		MaxOpenConnections:   l.jsParsedInt("CLICKHOUSE_MAX_OPEN_CONNECTIONS", 1, defaultClickHouseMaxOpen),
		RequestTimeout:       millis(l.jsParsedInt("CLICKHOUSE_REQUEST_TIMEOUT_MS", minClickHouseRequestTimeoutMs, defaultClickHouseRequestTimeout)),
		UnhealthyMark:        millis(l.jsParsedInt("CLICKHOUSE_UNHEALTHY_MARK_MS", 0, defaultClickHouseUnhealthyMark)),
	}
}

func rawIn(value string, accepted ...string) bool {
	for _, candidate := range accepted {
		if value == candidate {
			return true
		}
	}
	return false
}

// jsParsedInt is `env ? Math.max(min, Number.parseInt(env, 10)) : fallback`: a
// value parseInt cannot read keeps the default instead of becoming NaN.
func (l *loader) jsParsedInt(name string, min, fallback int) int {
	raw := l.getenv(name)
	if raw == "" {
		return fallback
	}
	parsed, ok := parseIntPrefix(raw)
	if !ok {
		return fallback
	}
	return max(min, parsed)
}

// parseIntPrefix is Number.parseInt(text, 10): leading whitespace, an
// optional sign, then the longest run of digits.
func parseIntPrefix(text string) (int, bool) {
	text = strings.TrimLeftFunc(text, unicode.IsSpace)
	end := 0
	if end < len(text) && (text[end] == '+' || text[end] == '-') {
		end++
	}
	digitsStart := end
	for end < len(text) && text[end] >= '0' && text[end] <= '9' {
		end++
	}
	if end == digitsStart {
		return 0, false
	}
	n, err := strconv.Atoi(text[:end])
	return n, err == nil
}

// clickhouseSettings is getSafeJson(CLICKHOUSE_SETTINGS) || {}, in object
// order: anything but a JSON object is {}, and a value that is not a string,
// number or boolean is skipped (the Node client throws on it).
func (l *loader) clickhouseSettings() []Setting {
	raw := l.getenv("CLICKHOUSE_SETTINGS")
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	opening, err := decoder.Token()
	if delim, isDelim := opening.(json.Delim); err != nil || !isDelim || delim != '{' {
		return nil
	}
	var settings []Setting
	indexByKey := map[string]int{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, isString := keyToken.(string)
		if err != nil || !isString {
			return nil
		}
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil
		}
		text, ok := settingText(value)
		if !ok {
			continue
		}
		// A repeated key keeps its first position and takes the last value,
		// as JSON.parse and object spread do.
		if at, seen := indexByKey[key]; seen {
			settings[at].Value = text
			continue
		}
		indexByKey[key] = len(settings)
		settings = append(settings, Setting{Key: key, Value: text})
	}
	if _, err := decoder.Token(); err != nil {
		return nil
	}
	return settings
}

func settingText(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case bool:
		if typed {
			return "1", true
		}
		return "0", true
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), true
	}
	return "", false
}

func (l *loader) sizing(batchName, chunkName string, defaultBatch, defaultChunk int) Sizing {
	batch := orDefault(l.optionalPositiveInt(batchName), defaultBatch)
	chunkDefault := defaultChunk
	if chunkDefault == 0 {
		chunkDefault = batch
	}
	return Sizing{Batch: batch, Chunk: orDefault(l.optionalPositiveInt(chunkName), chunkDefault)}
}

func (l *loader) buffers() BuffersConfig {
	return BuffersConfig{
		AsyncInserts:        l.definedIsTrue("BUFFER_ASYNC_INSERTS"),
		InsertConcurrency:   orDefault(l.optionalPositiveInt("BUFFER_CH_INSERT_CONCURRENCY"), defaultInsertConcurrency),
		Event:               l.sizing("EVENT_BUFFER_BATCH_SIZE", "EVENT_BUFFER_CHUNK_SIZE", defaultEventBatch, defaultEventChunk),
		Session:             l.sizing("SESSION_BUFFER_BATCH_SIZE", "SESSION_BUFFER_CHUNK_SIZE", defaultSessionBatch, 0),
		SessionSquash:       !rawIn(l.getenv("SESSION_BUFFER_SQUASH"), "false", "0"),
		Profile:             l.sizing("PROFILE_BUFFER_BATCH_SIZE", "PROFILE_BUFFER_CHUNK_SIZE", defaultProfileBatch, defaultProfileChunk),
		ProfileFetchChunk:   orDefault(l.optionalPositiveInt("PROFILE_BUFFER_FETCH_CHUNK_SIZE"), defaultProfileFetchChunk),
		ProfileTTL:          time.Duration(orDefault(l.optionalPositiveInt("PROFILE_BUFFER_TTL_IN_SECONDS"), defaultProfileTTL)) * time.Second,
		ProfileBackfillSize: orDefault(l.optionalPositiveInt("PROFILE_BACKFILL_BUFFER_BATCH_SIZE"), defaultProfileBackfillSize),
		Bot:                 Sizing{Batch: orDefault(l.optionalPositiveInt("BOT_BUFFER_BATCH_SIZE"), defaultBotBatch)},
		Replay:              l.sizing("REPLAY_BUFFER_BATCH_SIZE", "REPLAY_BUFFER_CHUNK_SIZE", defaultReplayBatch, defaultReplayChunk),
		Group:               l.sizing("GROUP_BUFFER_BATCH_SIZE", "GROUP_BUFFER_CHUNK_SIZE", defaultGroupBatch, defaultGroupChunk),
		GroupTTL:            time.Duration(orDefault(l.optionalPositiveInt("GROUP_BUFFER_TTL_IN_SECONDS"), defaultGroupTTL)) * time.Second,
		AiEvent:             l.sizing("AI_EVENT_BUFFER_BATCH_SIZE", "AI_EVENT_BUFFER_CHUNK_SIZE", defaultAiEventBatch, defaultAiEventChunk),
	}
}

// goWorkerCronJobs is GO_WORKER_CRON_JOBS, the cron jobs this process runs.
// The operator lists the same names in the Bun worker's DISABLED_CRON_JOBS.
// Empty (the default) runs none, so a Go worker started by mistake takes no
// flush from the Bun one. An unknown name fails the load, like DISABLED_CRON_JOBS.
func (l *loader) goWorkerCronJobs() []string {
	tokens := l.tokens("GO_WORKER_CRON_JOBS")
	for _, token := range tokens {
		if !contains(CronJobNames, token) {
			l.problem("GO_WORKER_CRON_JOBS: unknown cron job %q. Accepted values: %s.", token, strings.Join(CronJobNames, ", "))
		}
	}
	return tokens
}
