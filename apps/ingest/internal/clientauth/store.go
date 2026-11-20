// Package clientauth is the ingest tier of client authentication: which
// client sent the request, whether its secret verified, and whether the
// project's filters, revenue rule and CORS list let it through.
package clientauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"openpanel/ingest/internal/kv"
)

const (
	// localCacheTTL matches the API's in-process copy: a cleared client may
	// be served for up to this long, as on every other API node.
	localCacheTTL  = 60 * time.Second
	localCacheSize = 10_000
)

// Client is the slice of Prisma's Client-with-project the ingest tier reads.
type Client struct {
	ID                  string   `json:"id"`
	OrganizationID      string   `json:"organizationId"`
	Type                string   `json:"type"`
	Secret              *string  `json:"secret"`
	ProjectID           *string  `json:"projectId"`
	IgnoreCorsAndSecret bool     `json:"ignoreCorsAndSecret"`
	Project             *Project `json:"project"`
}

// Project is the slice of the project row the ingest tier reads.
type Project struct {
	ID                         string          `json:"id"`
	Cors                       []string        `json:"cors"`
	Filters                    json.RawMessage `json:"filters"`
	AllowUnsafeRevenueTracking bool            `json:"allowUnsafeRevenueTracking"`
}

// Store finds clients: this process's copy, then the API's Redis cache entry
// (cachable::[<id>]), then Postgres. It never writes the Redis entry, whose
// full shape other API routes read back.
type Store struct {
	Redis kv.Store
	DB    *pgxpool.Pool
	local *expirable.LRU[string, *Client]
}

// NewStore builds a Store.
func NewStore(rdb kv.Store, db *pgxpool.Pool) *Store {
	return &Store{Redis: rdb, DB: db, local: expirable.NewLRU[string, *Client](localCacheSize, nil, localCacheTTL)}
}

func clientCacheKey(id string) string { return "cachable::[" + id + "]" }

// ClientByID returns nil, nil for an unknown client.
func (s *Store) ClientByID(ctx context.Context, id string) (*Client, error) {
	if cached, ok := s.local.Get(id); ok {
		return cached, nil
	}
	if client := s.fromRedis(ctx, id); client != nil {
		s.local.Add(id, client)
		return client, nil
	}
	client, err := s.fromPostgres(ctx, id)
	if err != nil || client == nil {
		return nil, err
	}
	s.local.Add(id, client)
	return client, nil
}

// fromRedis reads the API's cached row. A miss, an error or an entry that
// does not decode all fall through to Postgres, as an empty hit does there.
func (s *Store) fromRedis(ctx context.Context, id string) *Client {
	raw, ok, err := s.Redis.Get(ctx, clientCacheKey(id))
	if err != nil || !ok || raw == "" {
		return nil
	}
	var client Client
	if err := json.Unmarshal([]byte(raw), &client); err != nil || client.ID == "" {
		return nil
	}
	return &client
}

const clientQuery = `
SELECT c.id, c."organizationId", c.type::text, c.secret, c."projectId", c."ignoreCorsAndSecret",
       p.id, p.cors, p.filters::text, p."allowUnsafeRevenueTracking"
FROM clients c
LEFT JOIN projects p ON p.id = c."projectId"
WHERE c.id = $1`

func (s *Store) fromPostgres(ctx context.Context, id string) (*Client, error) {
	var client Client
	var projectID, filters *string
	var cors []string
	var allowUnsafe *bool
	err := s.DB.QueryRow(ctx, clientQuery, id).Scan(
		&client.ID, &client.OrganizationID, &client.Type, &client.Secret, &client.ProjectID, &client.IgnoreCorsAndSecret,
		&projectID, &cors, &filters, &allowUnsafe,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("clientauth: load client %s: %w", id, err)
	}
	if projectID != nil {
		client.Project = &Project{ID: *projectID, Cors: cors, AllowUnsafeRevenueTracking: allowUnsafe != nil && *allowUnsafe}
		if filters != nil {
			client.Project.Filters = json.RawMessage(*filters)
		}
	}
	return &client, nil
}
