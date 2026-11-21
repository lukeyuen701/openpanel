package simple

import (
	"context"

	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
)

const (
	// replayReadSlice is the items per LRANGE: one count-sized read once asked
	// Redis for 260 MB, timed out and let the list grow.
	replayReadSlice = 10
	// replayMaxBatchBytes bounds a batch in bytes as well as rows; chunks
	// average ~270 KB and reach 1 MB. It is checked before each slice, so a batch
	// can exceed it by up to nine items.
	replayMaxBatchBytes = 8 * 1024 * 1024
)

// replay is the flush of replay-buffer into session_replay_chunks.
type replay struct {
	name    string
	listKey string
	batch   int
	chunk   int
	deps    Deps
}

// NewReplay builds the replay flush.
func NewReplay(cfg config.BuffersConfig, deps Deps, overrides Overrides) flush.Flusher {
	return &replay{
		name: overrides.name(ReplayName), listKey: overrides.key(ReplayKey),
		batch: cfg.Replay.Batch, chunk: cfg.Replay.Chunk, deps: deps,
	}
}

func (r *replay) Name() string    { return r.name }
func (r *replay) ListKey() string { return r.listKey }

// Flush implements flush.Flusher. Its own loop, with the drain budget of the
// others: read a byte-bounded batch in small slices, insert it, trim it, until a
// slice comes back short or the budget is spent.
//
// Unlike the TypeScript flush it stops once the lock is lost: two flushes
// reading the same list insert one batch twice and then trim one nobody
// inserted, which loses rows.
func (r *replay) Flush(ctx context.Context, run *flush.Run) (err error) {
	deadline := run.Deadline(run.Now())
	var rows int
	var phases flush.Phases
	defer func() { run.Report(rows, phases) }()

	for run.Now().Before(deadline) && run.ShouldContinue(ctx) {
		lrangeStart := run.Now()
		items, reachedEnd, err := r.readBatch(ctx)
		if err != nil {
			return err
		}
		phases = phases.Add(flush.Phases{LRange: run.Since(lrangeStart)})

		if len(items) > 0 {
			insertStart := run.Now()
			if err := insertChunks(ctx, r.deps, chclient.TableReplayChunks, items, r.chunk); err != nil {
				return err
			}
			insert := run.Since(insertStart)

			trimStart := run.Now()
			if err := trimHead(ctx, r.deps.Redis, r.listKey, len(items)); err != nil {
				return err
			}
			phases = phases.Add(flush.Phases{CHInsert: insert, Trim: run.Since(trimStart)})
			rows += len(items)
		}
		if reachedEnd {
			break
		}
	}
	return nil
}

// readBatch is the head of the list, up to batch items or replayMaxBatchBytes.
func (r *replay) readBatch(ctx context.Context) (items []string, reachedEnd bool, err error) {
	bytes := 0
	for len(items) < r.batch && bytes < replayMaxBatchBytes {
		requested := min(replayReadSlice, r.batch-len(items))
		slice, err := r.deps.Redis.LRange(ctx, r.listKey, int64(len(items)), int64(len(items)+requested-1))
		if err != nil {
			return nil, false, err
		}
		for _, item := range slice {
			items = append(items, item)
			bytes += len(item)
		}
		if len(slice) < requested {
			return items, true, nil
		}
	}
	return items, false, nil
}
