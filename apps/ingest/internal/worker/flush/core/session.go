package core

import (
	"context"
	"math"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
	"openpanel/ingest/internal/worker/jsport"
)

// parseObject is getSafeJson for a row that must be an object: anything else,
// a parse error included, is skipped (and trimmed with the batch). The
// TypeScript flush throws on a truthy non-object and so blocks the list.
func parseObject(raw string) (*jsjson.Obj, bool) {
	value, err := jsport.ParseJSON(raw)
	if err != nil || !value.IsObject() {
		return nil, false
	}
	return value.Obj, true
}

// clampDuration is `s.duration = Math.max(0, s.duration || 0)`: a falsy value is
// 0, anything else goes through Number(), and a key that is missing is added at
// the end of the row.
func clampDuration(row *jsjson.Obj) {
	current := row.Get("duration")
	seconds := 0.0
	if isTruthy(current) {
		seconds = jsport.ToNumberValue(current)
	}
	if !math.IsNaN(seconds) {
		seconds = math.Max(0, seconds)
	}
	row.Set("duration", jsjson.NewNumber(seconds))
}

// isTruthy is JavaScript truthiness of a parsed JSON value; nil is undefined.
func isTruthy(value *jsjson.Value) bool {
	if value == nil {
		return false
	}
	switch value.Kind {
	case jsjson.Null:
		return false
	case jsjson.Bool:
		return value.Bool
	case jsjson.Number:
		return value.Num != 0 && !math.IsNaN(value.Num)
	case jsjson.String:
		return value.Str != ""
	}
	return true
}

// mapKey stands for a JavaScript Map key: equal for equal primitives, and
// different for different kinds (1 and "1" are two keys).
func mapKey(value *jsjson.Value) string {
	if value == nil {
		return "u"
	}
	return string(rune('a'+int(value.Kind))) + jsjson.JSString(value)
}

type versionNet struct {
	net      float64
	pos, neg *jsjson.Obj
}

// SquashSessions is squashSessionsByVersion: the rows of each session id
// reduced, version by version, to the rows ClickHouse needs to collapse to the
// same state. A (+1, V) only collapses against a (-1, V) of the same version,
// so the signs are summed per version, a version that nets to zero emits
// nothing and the others emit |net| copies of the last row with that sign. Ids
// and versions keep their first-appearance order; a session with one row in the
// batch is passed through, a lone -1 included.
func SquashSessions(rows []*jsjson.Obj) []*jsjson.Obj {
	if len(rows) <= 1 {
		return rows
	}
	var order []string
	grouped := map[string][]*jsjson.Obj{}
	for _, row := range rows {
		key := mapKey(row.Get("id"))
		if _, seen := grouped[key]; !seen {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], row)
	}

	out := make([]*jsjson.Obj, 0, len(rows))
	for _, key := range order {
		entries := grouped[key]
		if len(entries) == 1 {
			out = append(out, entries[0])
			continue
		}
		var versions []string
		nets := map[string]*versionNet{}
		for _, entry := range entries {
			versionKey := mapKey(entry.Get("version"))
			slot, seen := nets[versionKey]
			if !seen {
				slot = &versionNet{}
				nets[versionKey] = slot
				versions = append(versions, versionKey)
			}
			sign := entry.Get("sign")
			if sign != nil && sign.Kind != jsjson.String && sign.Kind != jsjson.Array && sign.Kind != jsjson.Object {
				slot.net += jsport.ToNumberValue(sign)
			} else {
				// undefined, a string or a structure: NaN, which emits no row.
				slot.net = math.NaN()
			}
			switch {
			case sign != nil && sign.Kind == jsjson.Number && sign.Num == 1:
				slot.pos = entry
			case sign != nil && sign.Kind == jsjson.Number && sign.Num == -1:
				slot.neg = entry
			}
		}
		for _, versionKey := range versions {
			slot := nets[versionKey]
			if slot.net == 0 {
				continue
			}
			row := slot.neg
			if slot.net > 0 {
				row = slot.pos
			}
			if row == nil {
				continue
			}
			for copies := 0.0; copies < math.Abs(slot.net); copies++ {
				out = append(out, row)
			}
		}
	}
	return out
}

// session is the flush of session-buffer into sessions.
type session struct {
	name        string
	listKey     string
	batch       int
	chunk       int
	squash      bool
	concurrency int
	deps        Deps
}

// NewSession builds the session flush. Its inserts never carry the async
// settings: a session's +1 and -1 rows must be in a part when the flush
// returns. With SESSION_BUFFER_CHUNK_SIZE unset a batch is one insert, because
// a chunk that landed and is inserted again after a later chunk failed would
// leave a duplicated sign row, which corrupts the collapse.
func NewSession(cfg config.BuffersConfig, deps Deps, overrides Overrides) flush.Flusher {
	return &session{
		name: overrides.name(SessionName), listKey: overrides.key(SessionKey),
		batch: cfg.Session.Batch, chunk: cfg.Session.Chunk, squash: cfg.SessionSquash,
		concurrency: cfg.InsertConcurrency, deps: deps,
	}
}

func (s *session) Name() string    { return s.name }
func (s *session) ListKey() string { return s.listKey }

// Flush implements flush.Flusher.
func (s *session) Flush(ctx context.Context, run *flush.Run) error {
	return run.Drain(ctx, s.batch, func(ctx context.Context) (int, error) { return s.processBatch(ctx, run) })
}

func (s *session) processBatch(ctx context.Context, run *flush.Run) (int, error) {
	lrangeStart := run.Now()
	raw, err := s.deps.Redis.LRange(ctx, s.listKey, 0, int64(s.batch)-1)
	if err != nil {
		return 0, err
	}
	lrange := run.Since(lrangeStart)
	if len(raw) == 0 {
		run.Report(0, flush.Phases{LRange: lrange})
		return 0, nil
	}

	transformStart := run.Now()
	parsed := make([]*jsjson.Obj, 0, len(raw))
	for _, text := range raw {
		row, ok := parseObject(text)
		if !ok {
			continue
		}
		clampDuration(row)
		parsed = append(parsed, row)
	}
	rows := parsed
	if s.squash {
		rows = SquashSessions(parsed)
	}
	lines, err := serialize(rows)
	if err != nil {
		return 0, err
	}
	transform := run.Since(transformStart)

	if err := run.EnsureLock(); err != nil {
		return 0, err
	}
	insertStart := run.Now()
	if err := insertLines(ctx, s.deps.CH, chclient.TableSessions, lines, s.chunk, s.concurrency, nil); err != nil {
		return 0, err
	}
	insert := run.Since(insertStart)

	trimStart := run.Now()
	if err := trimHead(ctx, s.deps.Redis, s.listKey, len(raw)); err != nil {
		return 0, err
	}
	run.Report(len(raw), flush.Phases{LRange: lrange, Transform: transform, CHInsert: insert, Trim: run.Since(trimStart)})
	return len(raw), nil
}

// serialize is JSON.stringify of each row; a row that was squashed into
// several copies is written once per copy.
func serialize(rows []*jsjson.Obj) ([]string, error) {
	lines := make([]string, len(rows))
	var previous *jsjson.Obj
	for i, row := range rows {
		if row == previous {
			lines[i] = lines[i-1]
			continue
		}
		line, err := jsport.Stringify(jsjson.NewObject(row))
		if err != nil {
			return nil, err
		}
		lines[i], previous = line, row
	}
	return lines, nil
}
