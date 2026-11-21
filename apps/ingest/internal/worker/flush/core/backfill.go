package core

import (
	"context"
	"strconv"
	"strings"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
)

// backfillChunkSize is the sessions per UPDATE. The TypeScript buffer sends 500,
// but each session binds four parameters as URL fields and ClickHouse refuses a
// request with more than http_max_fields (1000) of them ("Too many form
// fields"): a batch of more than about 245 distinct sessions fails on every tick
// and the list never advances. 200 sessions are 800 parameters, which leaves
// room for the settings on the same URL. The rows that result are the same.
const backfillChunkSize = 200

// The UPDATE as the TypeScript renders it, whitespace of its template literal
// included. It reaches six hours back on the server's clock; the table needs
// enable_block_number_column and enable_block_offset_column (migration 8) for
// lightweight updates.
const (
	backfillUpdateHead = "\n        UPDATE "
	backfillSetHead    = "\n        SET profile_id = CASE session_id\n          "
	backfillWhereHead  = "\n        END\n        WHERE (project_id, session_id) IN ("
	backfillTail       = ")\n          AND created_at > now() - INTERVAL 6 HOURS"
)

func paramName(index int) string { return "p" + strconv.Itoa(index) }

// backfillSettings are the per-call settings of the UPDATE: it returns before
// the mutation is applied.
var backfillSettings = chclient.Settings{
	{Key: "mutations_sync", Value: "0"},
	{Key: "allow_experimental_lightweight_update", Value: "1"},
}

// BackfillEntry is one {projectId, sessionId, profileId} the session-end job
// queued.
type BackfillEntry struct{ ProjectID, SessionID, ProfileID string }

// BackfillStatement is the UPDATE of one chunk and its parameters: p1... are the
// (sessionId, profileId) pairs of the CASE, then the (projectId, sessionId) pairs
// of the IN list. Clustered, it names the replicated table ON CLUSTER.
func BackfillStatement(clustered bool, chunk []BackfillEntry) (string, []chclient.Param) {
	params := make([]chclient.Param, 0, len(chunk)*4)
	next := func(value string) string {
		name := paramName(len(params) + 1)
		params = append(params, chclient.Param{Name: name, Value: chclient.FormatString(value)})
		return "{" + name + ":String}"
	}
	cases := make([]string, len(chunk))
	for i, entry := range chunk {
		session := next(entry.SessionID)
		cases[i] = "WHEN " + session + " THEN " + next(entry.ProfileID)
	}
	tuples := make([]string, len(chunk))
	for i, entry := range chunk {
		project := next(entry.ProjectID)
		tuples[i] = "(" + project + ", " + next(entry.SessionID) + ")"
	}
	return backfillUpdateHead + chclient.ReplicatedTarget(clustered, chclient.TableEvents) +
		backfillSetHead + strings.Join(cases, "\n") + backfillWhereHead + strings.Join(tuples, ", ") + backfillTail, params
}

// parseBackfillEntries is the deduplication of the flush: by sessionId, a later
// entry replacing the profileId (and projectId) of an earlier one at the
// earlier one's position. A row that is not an object of three strings is
// skipped; the TypeScript would bind undefined and fail the whole batch.
func parseBackfillEntries(raw []string) []BackfillEntry {
	var entries []BackfillEntry
	position := map[string]int{}
	for _, text := range raw {
		row, ok := parseObject(text)
		if !ok {
			continue
		}
		projectID, sessionID, profileID := row.Get("projectId"), row.Get("sessionId"), row.Get("profileId")
		if !isString(projectID) || !isString(sessionID) || !isString(profileID) {
			continue
		}
		entry := BackfillEntry{ProjectID: projectID.Str, SessionID: sessionID.Str, ProfileID: profileID.Str}
		if at, seen := position[entry.SessionID]; seen {
			entries[at] = entry
			continue
		}
		position[entry.SessionID] = len(entries)
		entries = append(entries, entry)
	}
	return entries
}

func isString(value *jsjson.Value) bool { return value != nil && value.Kind == jsjson.String }

// profileBackfill is the flush of profile-backfill-buffer: events.profile_id of
// the last six hours is rewritten for the sessions whose identity became known
// at session end.
type profileBackfill struct {
	name      string
	listKey   string
	batch     int
	clustered bool
	deps      Deps
}

// NewProfileBackfill builds the profile-backfill flush. It reads one batch per
// tick and does not drain, as in the TypeScript; the UPDATE is idempotent, so
// a failed batch is simply sent again.
func NewProfileBackfill(cfg config.BuffersConfig, deps Deps, overrides Overrides) flush.Flusher {
	return &profileBackfill{
		name: overrides.name(ProfileBackfillName), listKey: overrides.key(ProfileBackfillKey),
		batch: cfg.ProfileBackfillSize, clustered: deps.Clustered, deps: deps,
	}
}

func (b *profileBackfill) Name() string    { return b.name }
func (b *profileBackfill) ListKey() string { return b.listKey }

// Flush implements flush.Flusher.
func (b *profileBackfill) Flush(ctx context.Context, run *flush.Run) error {
	lrangeStart := run.Now()
	raw, err := b.deps.Redis.LRange(ctx, b.listKey, 0, int64(b.batch)-1)
	if err != nil {
		return err
	}
	lrange := run.Since(lrangeStart)
	if len(raw) == 0 {
		run.Report(0, flush.Phases{LRange: lrange})
		return nil
	}

	if err := run.EnsureLock(); err != nil {
		return err
	}
	updateStart := run.Now()
	for _, chunk := range flush.Chunks(parseBackfillEntries(raw), backfillChunkSize) {
		statement, params := BackfillStatement(b.clustered, chunk)
		if err := b.deps.CH.CommandWithParams(ctx, statement, params, backfillSettings); err != nil {
			return err
		}
	}
	update := run.Since(updateStart)

	trimStart := run.Now()
	if err := trimHead(ctx, b.deps.Redis, b.listKey, len(raw)); err != nil {
		return err
	}
	run.Report(len(raw), flush.Phases{LRange: lrange, CHInsert: update, Trim: run.Since(trimStart)})
	return nil
}
