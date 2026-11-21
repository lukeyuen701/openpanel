// Package projects is what the event consumer reads about a project: the row
// (cached like the API's getProjectByIdCached), the project's event filters,
// the AI-content opt-in, and the first-event mark.
package projects

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/notify"
)

const (
	projectCacheName = "getProjectById"
	aiStoreCacheName = "getProjectAiStoreContent"
	localCacheTTL    = 60 * time.Second
	localCacheMax    = 1000
	eventFilterType  = "event"
)

// Cache is the Redis surface: the API's cachable entries are read, and the
// project's is deleted once its first event is recorded. Nothing is written.
type Cache interface {
	// Get returns the value and whether the key exists.
	Get(ctx context.Context, key string) (string, bool, error)
	// Del deletes the key.
	Del(ctx context.Context, key string) error
}

// DB is the Postgres surface; *pgxpool.Pool and pgx.Tx satisfy it.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Clock is the wall clock of the first-event timestamps.
type Clock interface{ Now() time.Time }

// Project is the part of the project row the consumer reads.
type Project struct {
	ID   string
	Name string
	// HasFirstEvent is whether firstEventAt is set.
	HasFirstEvent bool
	// Filters is the filters array as stored (nil when there are none).
	Filters *jsjson.Value
}

// Store reads and updates projects.
type Store struct {
	redis   Cache
	db      DB
	clock   Clock
	matcher *notify.Matcher
	logger  *slog.Logger
	local   *expirable.LRU[string, *Project]
}

// NewStore builds a Store; a nil logger discards.
func NewStore(redis Cache, db DB, clock Clock, matcher *notify.Matcher, logger *slog.Logger) *Store {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Store{
		redis:   redis,
		db:      db,
		clock:   clock,
		matcher: matcher,
		logger:  logger,
		local:   expirable.NewLRU[string, *Project](localCacheMax, nil, localCacheTTL),
	}
}

// GetCached is getProjectByIdCached: the process-local copy, then the API's
// Redis entry, then Postgres. It returns nil for an unknown project, which is
// never cached, so every call for one reads Postgres. A Redis error is
// returned, as the TypeScript read does not catch it.
func (s *Store) GetCached(ctx context.Context, projectID string) (*Project, error) {
	key := notify.CacheKey(projectCacheName, projectID)
	if project, ok := s.local.Get(key); ok {
		return project, nil
	}
	raw, found, err := s.redis.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if found && raw != "" {
		if project, ok := parseCachedProject(raw); ok {
			s.local.Add(key, project)
			return project, nil
		}
	}
	project, err := s.load(ctx, projectID)
	if err != nil || project == nil {
		return nil, err
	}
	s.local.Add(key, project)
	return project, nil
}

// ProjectName implements notify.ProjectNames.
func (s *Store) ProjectName(ctx context.Context, projectID string) (string, error) {
	project, err := s.GetCached(ctx, projectID)
	if err != nil || project == nil {
		return "", err
	}
	return project.Name, nil
}

// parseCachedProject accepts what the cache accepts: any parse result that is
// not null, an empty object, an empty array or an empty string. A cached
// object may lack fields (the dev stack holds a {firstEventAt, filters} one).
func parseCachedProject(raw string) (*Project, bool) {
	parsed, err := jsjson.Parse([]byte(raw))
	if err != nil {
		return nil, false
	}
	switch parsed.Kind {
	case jsjson.Null:
		return nil, false
	case jsjson.String:
		return &Project{}, parsed.Str != ""
	case jsjson.Array:
		return &Project{}, len(parsed.Arr) > 0
	case jsjson.Object:
		if parsed.Obj.Len() == 0 {
			return nil, false
		}
		project := &Project{
			HasFirstEvent: isTruthy(parsed.Get("firstEventAt")),
			Filters:       parsed.Get("filters"),
		}
		if name := parsed.Get("name"); name != nil && name.Kind == jsjson.String {
			project.Name = name.Str
		}
		if id := parsed.Get("id"); id != nil && id.Kind == jsjson.String {
			project.ID = id.Str
		}
		return project, true
	}
	return &Project{}, true
}

func isTruthy(v *jsjson.Value) bool {
	if v == nil {
		return false
	}
	switch v.Kind {
	case jsjson.Null:
		return false
	case jsjson.Bool:
		return v.Bool
	case jsjson.Number:
		return v.Num != 0
	case jsjson.String:
		return v.Str != ""
	}
	return true
}

func (s *Store) load(ctx context.Context, projectID string) (*Project, error) {
	var project Project
	var filters string
	err := s.db.QueryRow(ctx, `SELECT id, name, "firstEventAt" IS NOT NULL, filters::text FROM projects WHERE id = $1`, projectID).
		Scan(&project.ID, &project.Name, &project.HasFirstEvent, &filters)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("projects: read project %s: %w", projectID, err)
	}
	if project.Filters, err = jsjson.Parse([]byte(filters)); err != nil {
		return nil, fmt.Errorf("projects: project %s filters: %w", projectID, err)
	}
	return &project, nil
}

// GetAiStoreContent is getProjectAiStoreContentCached: nil when the project
// is unknown. It has no process-local copy, so an opt-out reaches every
// process at once; a cached false is an answer.
func (s *Store) GetAiStoreContent(ctx context.Context, projectID string) (*bool, error) {
	key := notify.CacheKey(aiStoreCacheName, projectID)
	raw, found, err := s.redis.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if found && raw != "" {
		if parsed, err := jsjson.Parse([]byte(raw)); err == nil && parsed.Kind == jsjson.Bool {
			return &parsed.Bool, nil
		}
	}
	var value bool
	err = s.db.QueryRow(ctx, `SELECT "aiStoreContent" FROM projects WHERE id = $1`, projectID).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("projects: read aiStoreContent of %s: %w", projectID, err)
	}
	return &value, nil
}

// IsExcluded is isEventExcludedByProjectFilter: whether any of the project's
// event filters matches the payload. An error is a TypeError the TypeScript
// would throw (a filter reading a non-string property); it fails the message.
func (s *Store) IsExcluded(ctx context.Context, projectID string, payload *jsjson.Value) (bool, error) {
	project, err := s.GetCached(ctx, projectID)
	if err != nil || project == nil {
		return false, err
	}
	filters := project.Filters
	if filters == nil || filters.Kind == jsjson.Null {
		return false, nil
	}
	if filters.Kind != jsjson.Array {
		return false, fmt.Errorf("%w: project.filters.filter is not a function", notify.ErrTypeError)
	}
	var eventFilters []*jsjson.Value
	for _, filter := range filters.Arr {
		if filter == nil || filter.Kind == jsjson.Null {
			return false, fmt.Errorf("%w: Cannot read properties of null (reading 'type')", notify.ErrTypeError)
		}
		if kind := filter.Get("type"); kind != nil && kind.Kind == jsjson.String && kind.Str == eventFilterType {
			eventFilters = append(eventFilters, filter)
		}
	}
	for _, filter := range eventFilters {
		matched, err := s.matcher.MatchEvent(payload, filter)
		if err != nil || matched {
			return matched, err
		}
	}
	return false, nil
}

// MarkFirstEvent records the project's first event, once: nothing happens for
// an unknown project or one whose cached row already has firstEventAt. The
// UPDATE is conditional, so concurrent workers are idempotent, and the cache
// entry is deleted after it. Callers ignore the error, as the TypeScript does.
func (s *Store) MarkFirstEvent(ctx context.Context, projectID string) error {
	project, err := s.GetCached(ctx, projectID)
	if err != nil || project == nil || project.HasFirstEvent {
		return err
	}
	// Prisma fills updatedAt with a second clock read.
	firstEventAt := s.clock.Now().UTC().Truncate(time.Millisecond)
	updatedAt := s.clock.Now().UTC().Truncate(time.Millisecond)
	if _, err := s.db.Exec(ctx, `UPDATE "projects" SET "firstEventAt" = $1, "updatedAt" = $2 WHERE ("projects"."id" = $3 AND "projects"."firstEventAt" IS NULL)`,
		firstEventAt, updatedAt, projectID); err != nil {
		return fmt.Errorf("projects: mark first event of %s: %w", projectID, err)
	}
	key := notify.CacheKey(projectCacheName, projectID)
	s.local.Remove(key)
	if err := s.redis.Del(ctx, key); err != nil {
		return err
	}
	s.logger.Info("Project received its first event", "projectId", projectID)
	return nil
}
