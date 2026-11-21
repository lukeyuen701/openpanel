// Package simple holds the four buffers whose flush is a passthrough from a
// Redis list to ClickHouse: bot, ai-event, replay and group. Each Redis entry
// is already a JSONEachRow line (written by the Bun API or the consumer), so
// the bytes read are the bytes inserted, and the list is trimmed only after the
// insert for the whole batch succeeded: at-least-once, never a row removed
// that did not land.
package simple

import (
	"context"
	"time"

	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
)

// Names, list keys and tables, as in the TypeScript buffers.
const (
	BotName    = "bot"
	BotKey     = "bot-events-buffer"
	AiName     = "ai-event"
	AiKey      = "ai-event-buffer"
	ReplayKey  = "replay-buffer"
	ReplayName = "replay"
	GroupName  = "group"
	GroupKey   = "group-buffer"
)

// trimTimeout bounds the LTRIM that follows an insert which already landed; it
// runs even when the flush's context was cancelled, so shutdown never leaves a
// landed batch to be inserted twice.
const trimTimeout = 10 * time.Second

// Deps are what the buffers read from outside.
type Deps struct {
	Redis    flush.Redis
	Inserter chclient.Inserter
	// AsyncInserts is BUFFER_ASYNC_INSERTS; InsertConcurrency is BUFFER_CH_INSERT_CONCURRENCY.
	AsyncInserts      bool
	InsertConcurrency int
}

// Overrides rename a buffer's list or lock, for a test that must not touch
// the real keys. The zero value keeps the production names.
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

// passthrough is the flush shared by bot, ai-event and group: LRANGE a batch,
// insert it in chunks, LTRIM it.
type passthrough struct {
	name    string
	listKey string
	table   string
	batch   int
	// chunk is the rows per insert; 0 sends the whole batch in one.
	chunk int
	deps  Deps
}

func (p *passthrough) Name() string    { return p.name }
func (p *passthrough) ListKey() string { return p.listKey }

// Flush implements flush.Flusher.
func (p *passthrough) Flush(ctx context.Context, run *flush.Run) error {
	return run.Drain(ctx, p.batch, func(ctx context.Context) (int, error) { return p.processBatch(ctx, run) })
}

func (p *passthrough) processBatch(ctx context.Context, run *flush.Run) (int, error) {
	lrangeStart := run.Now()
	items, err := p.deps.Redis.LRange(ctx, p.listKey, 0, int64(p.batch)-1)
	if err != nil {
		return 0, err
	}
	lrange := run.Since(lrangeStart)
	if len(items) == 0 {
		run.Report(0, flush.Phases{LRange: lrange})
		return 0, nil
	}

	if err := run.EnsureLock(); err != nil {
		return 0, err
	}
	insertStart := run.Now()
	if err := insertChunks(ctx, p.deps, p.table, items, p.chunk); err != nil {
		return 0, err
	}
	insert := run.Since(insertStart)

	trimStart := run.Now()
	if err := trimHead(ctx, p.deps.Redis, p.listKey, len(items)); err != nil {
		return 0, err
	}
	run.Report(len(items), flush.Phases{LRange: lrange, CHInsert: insert, Trim: run.Since(trimStart)})
	return len(items), nil
}

// insertChunks sends items to table in chunks of chunkSize rows (all at once
// when it is 0), at most InsertConcurrency in flight.
func insertChunks(ctx context.Context, deps Deps, table string, items []string, chunkSize int) error {
	settings := chclient.AsyncInsertSettings(deps.AsyncInserts)
	return flush.ParallelLimit(deps.InsertConcurrency, flush.Chunks(items, chunkSize), func(_ int, chunk []string) error {
		return deps.Inserter.Insert(ctx, table, chunk, settings)
	})
}

func trimHead(ctx context.Context, redis flush.Redis, key string, count int) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), trimTimeout)
	defer cancel()
	return redis.LTrim(ctx, key, int64(count), -1)
}

// NewBot is the flush of bot-events-buffer into events_bots: one insert of the
// whole batch.
func NewBot(cfg config.BuffersConfig, deps Deps, overrides Overrides) flush.Flusher {
	return &passthrough{
		name: overrides.name(BotName), listKey: overrides.key(BotKey), table: chclient.TableBots,
		batch: cfg.Bot.Batch, deps: deps,
	}
}

// NewAiEvent is the flush of ai-event-buffer into ai_events.
func NewAiEvent(cfg config.BuffersConfig, deps Deps, overrides Overrides) flush.Flusher {
	return &passthrough{
		name: overrides.name(AiName), listKey: overrides.key(AiKey), table: chclient.TableAiEvents,
		batch: cfg.AiEvent.Batch, chunk: cfg.AiEvent.Chunk, deps: deps,
	}
}

// NewGroup is the flush of group-buffer into groups. The merge with the
// stored group happened when the Bun API added the row, not here.
func NewGroup(cfg config.BuffersConfig, deps Deps, overrides Overrides) flush.Flusher {
	return &passthrough{
		name: overrides.name(GroupName), listKey: overrides.key(GroupKey), table: chclient.TableGroups,
		batch: cfg.Group.Batch, chunk: cfg.Group.Chunk, deps: deps,
	}
}
