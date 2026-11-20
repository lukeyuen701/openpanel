// Package lookups holds the cached reads /track makes besides the client:
// the device-id salts and the organization's wind-down step. Each reads the
// API's Redis cache entry first and Postgres on a miss, and keeps its own
// in-process copy for as long as the API's does; neither writes Redis.
package lookups

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"openpanel/ingest/internal/kv"
)

const (
	localTTL    = 60 * time.Second
	orgCacheMax = 10_000
	saltsKey    = "cachable:op:salt:[]"
)

// Salts are the current and previous device-id salts.
type Salts struct {
	Current  string `json:"current"`
	Previous string `json:"previous"`
}

// ErrNoSalt is the API's "No salt found": the bootstrap never ran.
var ErrNoSalt = errors.New("lookups: no salt found")

// SaltStore is getSalts.
type SaltStore struct {
	Redis kv.Store
	DB    *pgxpool.Pool

	mu      sync.Mutex
	cached  Salts
	expires time.Time
}

// Get returns the salts.
func (s *SaltStore) Get(ctx context.Context) (Salts, error) {
	s.mu.Lock()
	if time.Now().Before(s.expires) {
		salts := s.cached
		s.mu.Unlock()
		return salts, nil
	}
	s.mu.Unlock()

	salts, err := s.load(ctx)
	if err != nil {
		return Salts{}, err
	}
	s.mu.Lock()
	s.cached, s.expires = salts, time.Now().Add(localTTL)
	s.mu.Unlock()
	return salts, nil
}

func (s *SaltStore) load(ctx context.Context) (Salts, error) {
	if raw, ok, err := s.Redis.Get(ctx, saltsKey); err == nil && ok {
		var salts Salts
		if json.Unmarshal([]byte(raw), &salts) == nil && salts.Current != "" {
			return salts, nil
		}
	}
	rows, err := s.DB.Query(ctx, `SELECT salt FROM salts ORDER BY "createdAt" DESC LIMIT 2`)
	if err != nil {
		return Salts{}, fmt.Errorf("lookups: read salts: %w", err)
	}
	values, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return Salts{}, fmt.Errorf("lookups: read salts: %w", err)
	}
	if len(values) == 0 {
		return Salts{}, ErrNoSalt
	}
	salts := Salts{Current: values[0], Previous: values[0]}
	if len(values) > 1 {
		salts.Previous = values[1]
	}
	return salts, nil
}

// Organization is the slice of the organization row wind-down reads.
type Organization struct {
	ID           string  `json:"id"`
	WindDownStep *string `json:"windDownStep"`
}

// OrganizationStore is getOrganizationByProjectIdCached.
type OrganizationStore struct {
	Redis kv.Store
	DB    *pgxpool.Pool
	local *expirable.LRU[string, *Organization]
}

// NewOrganizationStore builds the store.
func NewOrganizationStore(rdb kv.Store, db *pgxpool.Pool) *OrganizationStore {
	return &OrganizationStore{Redis: rdb, DB: db, local: expirable.NewLRU[string, *Organization](orgCacheMax, nil, localTTL)}
}

// ByProject returns the project's organization; an unknown project is an
// error, as findUniqueOrThrow makes it.
func (s *OrganizationStore) ByProject(ctx context.Context, projectID string) (*Organization, error) {
	if cached, ok := s.local.Get(projectID); ok {
		return cached, nil
	}
	key := "cachable:getOrganizationByProjectId:[" + projectID + "]"
	if raw, ok, err := s.Redis.Get(ctx, key); err == nil && ok {
		var org Organization
		if json.Unmarshal([]byte(raw), &org) == nil && org.ID != "" {
			s.local.Add(projectID, &org)
			return &org, nil
		}
	}
	var org Organization
	err := s.DB.QueryRow(ctx, `SELECT o.id, o."windDownStep" FROM projects p JOIN organizations o ON o.id = p."organizationId" WHERE p.id = $1`, projectID).Scan(&org.ID, &org.WindDownStep)
	if err != nil {
		return nil, fmt.Errorf("lookups: organization of project %s: %w", projectID, err)
	}
	s.local.Add(projectID, &org)
	return &org, nil
}

// WoundDownSteps are the wind-down steps that stop ingestion.
var WoundDownSteps = map[string]bool{"blocked": true, "final_warning": true}
