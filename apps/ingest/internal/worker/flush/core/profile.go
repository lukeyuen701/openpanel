package core

import (
	"context"
	"sync"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
	"openpanel/ingest/internal/worker/jsport"
)

// profileFetchConcurrency matches the two lookups (identified, anonymous) that
// used to run side by side.
const profileFetchConcurrency = 2

// profileLookupColumns, profileLookupFrom and profileLookupTail are the lookup
// as the TypeScript renders it, whitespace of its template literal included.
// The query is argMax by last_seen_at, the same value FINAL would produce
// without merging parts at query time; p.last_seen_at is qualified because the
// SELECT also exposes a last_seen_at alias, which ClickHouse would otherwise
// bind inside the aggregate (ILLEGAL_AGGREGATION). There is deliberately no
// recency window: an anonymous profile can be weeks old.
const (
	profileLookupColumns = "id, project_id, argMax(first_name, p.last_seen_at) AS first_name, " +
		"argMax(last_name, p.last_seen_at) AS last_name, argMax(email, p.last_seen_at) AS email, " +
		"argMax(avatar, p.last_seen_at) AS avatar, argMax(properties, p.last_seen_at) AS properties, " +
		"argMax(is_external, p.last_seen_at) AS is_external, argMax(groups, p.last_seen_at) AS groups, " +
		"min(created_at) AS created_at, max(p.last_seen_at) AS last_seen_at"
	profileLookupFrom = "\n            FROM profiles AS p\n            WHERE (p.id, p.project_id) IN ("
	profileLookupTail = ")\n            GROUP BY p.id, p.project_id"
)

// profileKey is `${project_id}:${id}`, with JavaScript's String() of a missing
// or non-string field.
func profileKey(profile *jsjson.Obj) string {
	return jsjson.JSString(profile.Get("project_id")) + ":" + jsjson.JSString(profile.Get("id"))
}

func profileCacheKey(profile *jsjson.Obj) string {
	return ProfileCachePrefix + profileKey(profile)
}

// lookupStatement is the query for one chunk: p1, p2 are the first profile's id
// and project, and so on.
func lookupStatement(chunk []*jsjson.Obj) (string, []chclient.Param) {
	statement := "SELECT " + profileLookupColumns + profileLookupFrom
	params := make([]chclient.Param, 0, len(chunk)*2)
	for i, profile := range chunk {
		if i > 0 {
			statement += ", "
		}
		idName, projectName := paramName(len(params)+1), paramName(len(params)+2)
		statement += "({" + idName + ":String}, {" + projectName + ":String})"
		params = append(params,
			chclient.Param{Name: idName, Value: chclient.FormatString(jsjson.JSString(profile.Get("id")))},
			chclient.Param{Name: projectName, Value: chclient.FormatString(jsjson.JSString(profile.Get("project_id")))},
		)
	}
	return statement + profileLookupTail, params
}

// profile is the flush of profile-buffer into profiles.
type profile struct {
	name        string
	listKey     string
	batch       int
	chunk       int
	fetchChunk  int
	ttl         time.Duration
	concurrency int
	settings    chclient.Settings
	deps        Deps
}

// NewProfile builds the profile flush. The ReplacingMergeTree keeps the row
// with the newest last_seen_at, so what is inserted is each profile merged with
// the one already stored.
func NewProfile(cfg config.BuffersConfig, deps Deps, overrides Overrides) flush.Flusher {
	return &profile{
		name: overrides.name(ProfileName), listKey: overrides.key(ProfileKey),
		batch: cfg.Profile.Batch, chunk: cfg.Profile.Chunk, fetchChunk: cfg.ProfileFetchChunk, ttl: cfg.ProfileTTL,
		concurrency: cfg.InsertConcurrency, settings: chclient.AsyncInsertSettings(cfg.AsyncInserts), deps: deps,
	}
}

func (p *profile) Name() string    { return p.name }
func (p *profile) ListKey() string { return p.listKey }

// Flush implements flush.Flusher.
func (p *profile) Flush(ctx context.Context, run *flush.Run) error {
	return run.Drain(ctx, p.batch, func(ctx context.Context) (int, error) { return p.processBatch(ctx, run) })
}

func (p *profile) processBatch(ctx context.Context, run *flush.Run) (int, error) {
	lrangeStart := run.Now()
	raw, err := p.deps.Redis.LRange(ctx, p.listKey, 0, int64(p.batch)-1)
	if err != nil {
		return 0, err
	}
	lrange := run.Since(lrangeStart)
	if len(raw) == 0 {
		run.Report(0, flush.Phases{LRange: lrange})
		return 0, nil
	}

	unique := mergeWithinBatch(raw)
	if len(unique) == 0 {
		// Nothing but unreadable rows: MGET without keys is an error, and the
		// rows would block the list for good.
		if err := trimHead(ctx, p.deps.Redis, p.listKey, len(raw)); err != nil {
			return 0, err
		}
		run.Report(len(raw), flush.Phases{LRange: lrange})
		return len(raw), nil
	}

	existing, misses, err := p.existingFromCache(ctx, unique)
	if err != nil {
		return 0, err
	}
	var fetch flush.Phase
	if len(misses) > 0 {
		fetchStart := run.Now()
		fetched, err := p.fetchFromClickHouse(ctx, misses)
		if err != nil {
			return 0, err
		}
		fetch = run.Since(fetchStart)
		for key, stored := range fetched {
			existing[key] = stored
		}
	}

	lines := make([]string, len(unique))
	writes := make([]CacheWrite, len(unique))
	for i, incoming := range unique {
		merged := mergeProfiles(existing[profileKey(incoming)], incoming)
		line, err := jsport.Stringify(jsjson.NewObject(merged))
		if err != nil {
			return 0, err
		}
		lines[i] = line
		writes[i] = CacheWrite{Key: profileCacheKey(incoming), Value: line}
	}

	if err := run.EnsureLock(); err != nil {
		return 0, err
	}
	insertStart := run.Now()
	if err := insertLines(ctx, p.deps.CH, chclient.TableProfiles, lines, p.chunk, p.concurrency, p.settings); err != nil {
		return 0, err
	}
	insert := run.Since(insertStart)

	trimStart := run.Now()
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), trimTimeout)
	defer cancel()
	if err := p.deps.Redis.CommitProfiles(commitCtx, writes, p.ttl, p.listKey, len(raw)); err != nil {
		return 0, err
	}
	run.Report(len(raw), flush.Phases{LRange: lrange, CHFetch: fetch, CHInsert: insert, Trim: run.Since(trimStart)})
	return len(raw), nil
}

// mergeWithinBatch folds the rows of one profile into one, in first-appearance
// order. A row that is not a JSON object is skipped.
func mergeWithinBatch(raw []string) []*jsjson.Obj {
	var order []string
	merged := map[string]*jsjson.Obj{}
	for _, text := range raw {
		incoming, ok := parseObject(text)
		if !ok {
			continue
		}
		key := profileKey(incoming)
		previous, seen := merged[key]
		if !seen {
			order = append(order, key)
		}
		merged[key] = mergeProfiles(previous, incoming)
	}
	unique := make([]*jsjson.Obj, len(order))
	for i, key := range order {
		unique[i] = merged[key]
	}
	return unique
}

// existingFromCache reads profile-cache:* for the batch's profiles: what it
// holds is the last merged row a flush wrote. A key that is missing or does not
// hold a JSON object is a miss.
func (p *profile) existingFromCache(ctx context.Context, unique []*jsjson.Obj) (existing map[string]*jsjson.Obj, misses []*jsjson.Obj, err error) {
	keys := make([]string, len(unique))
	for i, profile := range unique {
		keys[i] = profileCacheKey(profile)
	}
	cached, err := p.deps.Redis.MGet(ctx, keys)
	if err != nil {
		return nil, nil, err
	}
	existing = make(map[string]*jsjson.Obj, len(unique))
	for i, profile := range unique {
		if cached[i] != "" {
			if stored, ok := parseObject(cached[i]); ok {
				existing[profileKey(profile)] = stored
				continue
			}
		}
		misses = append(misses, profile)
	}
	return existing, misses, nil
}

// fetchFromClickHouse is batchFetchFromClickhouse: the stored profile of each
// miss, in chunks, two at a time. A failing chunk is logged and its profiles
// are merged against nothing, as in the TypeScript flush, except when the
// flush itself was cancelled: shutdown must leave the rows for the next tick,
// not write them unmerged.
func (p *profile) fetchFromClickHouse(ctx context.Context, misses []*jsjson.Obj) (map[string]*jsjson.Obj, error) {
	var mutex sync.Mutex
	stored := map[string]*jsjson.Obj{}
	logger := p.deps.logger(p.name)
	err := flush.ParallelLimit(profileFetchConcurrency, flush.Chunks(misses, p.fetchChunk), func(_ int, chunk []*jsjson.Obj) error {
		statement, params := lookupStatement(chunk)
		result, err := p.deps.CH.Query(ctx, statement, params, nil)
		if err == nil {
			err = collectProfiles(result, &mutex, stored)
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		logger.Warn("Failed to batch fetch profiles from Clickhouse, proceeding without existing data", "err", err, "chunkSize", len(chunk))
		return nil
	})
	return stored, err
}

func collectProfiles(result *chclient.Result, mutex *sync.Mutex, into map[string]*jsjson.Obj) error {
	rows := make([]*jsjson.Obj, 0, len(result.Data))
	for _, rawRow := range result.Data {
		row, ok := parseObject(string(rawRow))
		if !ok {
			continue
		}
		rows = append(rows, row)
	}
	mutex.Lock()
	defer mutex.Unlock()
	for _, row := range rows {
		into[profileKey(row)] = row
	}
	return nil
}
