package core

import (
	"context"
	"strings"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
	"openpanel/ingest/internal/worker/jsport"
)

const projectIDNeedle = `"project_id":"`

// ExtractProjectID is extractProjectId of event-buffer.ts: the top-level
// project_id of a JSONEachRow event line without parsing it. A nested
// project_id inside a string value is escaped and cannot match; two or more
// hits mean a real nested key, which falls back to a parse. The text up to the
// next quote is returned as is, escapes included; an empty value is none.
func ExtractProjectID(line string) (string, bool) {
	first := strings.Index(line, projectIDNeedle)
	if first < 0 {
		return "", false
	}
	valueStart := first + len(projectIDNeedle)
	if strings.Contains(line[valueStart:], projectIDNeedle) {
		parsed, err := jsport.ParseJSON(line)
		if err != nil {
			return "", false
		}
		projectID := parsed.Get("project_id")
		if projectID == nil || projectID.Kind != jsjson.String {
			return "", false
		}
		return projectID.Str, true
	}
	valueLength := strings.IndexByte(line[valueStart:], '"')
	if valueLength <= 0 {
		return "", false
	}
	return line[valueStart : valueStart+valueLength], true
}

type projectCount struct {
	projectID string
	count     int
}

// countByProject counts the lines of each project, projects in first-seen order.
func countByProject(lines []string) []projectCount {
	var counts []projectCount
	index := map[string]int{}
	for _, line := range lines {
		projectID, ok := ExtractProjectID(line)
		if !ok {
			continue
		}
		if at, seen := index[projectID]; seen {
			counts[at].count++
			continue
		}
		index[projectID] = len(counts)
		counts = append(counts, projectCount{projectID: projectID, count: 1})
	}
	return counts
}

// batchMessage is setSuperJson({ projectId, count }): superjson adds no meta
// to a plain object of a string and a number.
func batchMessage(projectID string, count int) string {
	return `{"json":{"projectId":` + jsport.StringifyString(projectID) + `,"count":` + jsjson.FormatNumber(float64(count)) + `}}`
}

// event is the flush of event_buffer:queue into events.
type event struct {
	name        string
	listKey     string
	batch       int
	chunk       int
	concurrency int
	settings    chclient.Settings
	deps        Deps
}

// NewEvent builds the event flush: LRANGE a batch of JSONEachRow lines, insert
// them untouched in chunks, publish the per-project counts, LTRIM.
func NewEvent(cfg config.BuffersConfig, deps Deps, overrides Overrides) flush.Flusher {
	return &event{
		name: overrides.name(EventName), listKey: overrides.key(EventKey),
		batch: cfg.Event.Batch, chunk: cfg.Event.Chunk, concurrency: cfg.InsertConcurrency,
		settings: chclient.AsyncInsertSettings(cfg.AsyncInserts), deps: deps,
	}
}

func (e *event) Name() string    { return e.name }
func (e *event) ListKey() string { return e.listKey }

// Flush implements flush.Flusher.
func (e *event) Flush(ctx context.Context, run *flush.Run) error {
	return run.Drain(ctx, e.batch, func(ctx context.Context) (int, error) { return e.processBatch(ctx, run) })
}

func (e *event) processBatch(ctx context.Context, run *flush.Run) (int, error) {
	lrangeStart := run.Now()
	lines, err := e.deps.Redis.LRange(ctx, e.listKey, 0, int64(e.batch)-1)
	if err != nil {
		return 0, err
	}
	lrange := run.Since(lrangeStart)
	if len(lines) == 0 {
		run.Report(0, flush.Phases{LRange: lrange})
		return 0, nil
	}

	counts := countByProject(lines)

	insertStart := run.Now()
	if err := insertLines(ctx, e.deps.CH, chclient.TableEvents, lines, e.chunk, e.concurrency, e.settings); err != nil {
		return 0, err
	}
	insert := run.Since(insertStart)

	e.publish(ctx, counts)

	if err := checkLock(run, e.name); err != nil {
		return 0, err
	}
	trimStart := run.Now()
	if err := trimHead(ctx, e.deps.Redis, e.listKey, len(lines)); err != nil {
		return 0, err
	}
	run.Report(len(lines), flush.Phases{LRange: lrange, CHInsert: insert, Trim: run.Since(trimStart)})
	return len(lines), nil
}

// publish tells the dashboard's live view how many events each project just
// received. It never fails the flush: the rows are in ClickHouse already, and
// a message that does not go out costs one live-view update.
func (e *event) publish(ctx context.Context, counts []projectCount) {
	logger := e.deps.logger(e.name)
	// One deadline for all of them, so many projects or a slow Redis cannot hold
	// the flush lock for long.
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	for _, entry := range counts {
		if err := e.deps.Redis.Publish(publishCtx, EventsBatchChannel, batchMessage(entry.projectID, entry.count)); err != nil {
			logger.Warn("Failed to publish the events batch", "err", err, "projectId", entry.projectID)
		}
	}
}
