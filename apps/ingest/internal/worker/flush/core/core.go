// Package core holds the four buffers whose flush transforms what it reads:
// event, session, profile and profile-backfill. The others (bot, ai-event,
// replay, group) are plain passthroughs in package simple.
//
//   - event: raw JSONEachRow lines to events, then one pub/sub message per
//     project for the dashboard's live view, then the trim.
//   - session: the rows are parsed, squashed per version and re-serialised.
//   - profile: the rows are merged with each other and with the profile already
//     stored (the cache first, ClickHouse for the misses), inserted, and the
//     merged row is cached for an hour in the same MULTI that trims the list.
//   - profile-backfill: one UPDATE of events.profile_id per 500 sessions.
//
// Every flush trims its list only after the ClickHouse call for the whole batch
// returned, so a failure leaves the rows for the next tick (at-least-once).
// The bodies are rebuilt on every attempt by chclient, so a retry on another
// node cannot send an empty one.
//
// Buffers returns the four flushes with their cron job ids, as
// simple.Buffer values, so a worker concatenates them with simple.Buffers and
// builds all runners with simple.Jobs.
package core

import (
	"context"
	"log/slog"
	"time"

	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
	"openpanel/ingest/internal/worker/flush/simple"
)

// Names and list keys, as in the TypeScript buffers.
const (
	EventName           = "event"
	EventKey            = "event_buffer:queue"
	SessionName         = "session"
	SessionKey          = "session-buffer"
	ProfileName         = "profile"
	ProfileKey          = "profile-buffer"
	ProfileBackfillName = "profile-backfill"
	ProfileBackfillKey  = "profile-backfill-buffer"

	// ProfileCachePrefix is shared with the API, which reads the cache
	// (fetchFromCache) and with the consumer's isFromEvent check.
	ProfileCachePrefix = "profile-cache:"
	// EventsBatchChannel is getSubscribeChannel('events', 'batch').
	EventsBatchChannel = "events:batch"
)

// trimTimeout bounds the trim that follows a ClickHouse call which already
// landed; it runs even when the flush's context was cancelled, so shutdown
// never leaves a landed batch to be inserted twice. publishTimeout bounds all
// the pub/sub messages of a batch together.
const (
	trimTimeout    = 10 * time.Second
	publishTimeout = 5 * time.Second
)

// ClickHouse is what the flushes use of the client: *chclient.Client.
type ClickHouse interface {
	chclient.Inserter
	// Query is chQuery: a SELECT answered as JSON.
	Query(ctx context.Context, statement string, params []chclient.Param, settings chclient.Settings) (*chclient.Result, error)
	// CommandWithParams is ch.command with query_params.
	CommandWithParams(ctx context.Context, statement string, params []chclient.Param, settings chclient.Settings) error
}

// Deps are what the buffers read from outside. Logger may be nil.
type Deps struct {
	Redis  Redis
	CH     ClickHouse
	Logger *slog.Logger
	// Clustered is CLICKHOUSE_CLUSTER: only the backfill's UPDATE names the
	// replicated table.
	Clustered bool
	// Rejects records the rows a flush cannot read before it trims them.
	Rejects Rejects
}

func (d Deps) logger(buffer string) *slog.Logger {
	logger := d.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return logger.With("buffer", buffer)
}

// Overrides rename a buffer's list or lock, for a test that must not touch the
// real keys. The zero value keeps the production names.
type Overrides struct {
	Name    string
	ListKey string
}

func (o Overrides) name(fallback string) string {
	if o.Name != "" {
		return o.Name
	}
	return fallback
}

func (o Overrides) key(fallback string) string {
	if o.ListKey != "" {
		return o.ListKey
	}
	return fallback
}

// Buffers are the four flushes with their cron job ids, in the order of the
// TypeScript registry.
func Buffers(cfg config.BuffersConfig, deps Deps) []simple.Buffer {
	return []simple.Buffer{
		{Job: flush.JobFlushEvents, Flusher: NewEvent(cfg, deps, Overrides{})},
		{Job: flush.JobFlushProfiles, Flusher: NewProfile(cfg, deps, Overrides{})},
		{Job: flush.JobFlushSessions, Flusher: NewSession(cfg, deps, Overrides{})},
		{Job: flush.JobFlushProfileBackfill, Flusher: NewProfileBackfill(cfg, deps, Overrides{})},
	}
}

// insertLines sends lines to table in chunks of chunkSize (all at once when it
// is below 1), at most concurrency in flight, and returns the first error
// after the running inserts settled.
func insertLines(ctx context.Context, ch chclient.Inserter, table string, lines []string, chunkSize, concurrency int, settings chclient.Settings) error {
	return flush.ParallelLimit(concurrency, flush.Chunks(lines, chunkSize), func(_ int, chunk []string) error {
		return ch.Insert(ctx, table, chunk, settings)
	})
}

// trimHead removes count rows from the head of key, after they landed.
func trimHead(ctx context.Context, redis flush.Redis, key string, count int) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), trimTimeout)
	defer cancel()
	return redis.LTrim(ctx, key, int64(count), -1)
}
