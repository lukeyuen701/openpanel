package projects

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/notify"
)

type fakeCache struct {
	values  map[string]string
	deleted []string
	gets    int
	err     error
}

func newFakeCache() *fakeCache { return &fakeCache{values: map[string]string{}} }

func (c *fakeCache) Get(_ context.Context, key string) (string, bool, error) {
	c.gets++
	if c.err != nil {
		return "", false, c.err
	}
	value, ok := c.values[key]
	return value, ok, nil
}

func (c *fakeCache) Del(_ context.Context, key string) error {
	c.deleted = append(c.deleted, key)
	delete(c.values, key)
	return nil
}

// tickingClock advances a millisecond per read, so two reads differ.
type tickingClock struct{ at time.Time }

func (c *tickingClock) Now() time.Time {
	c.at = c.at.Add(time.Millisecond)
	return c.at
}

func openTx(t *testing.T) pgx.Tx {
	t.Helper()
	url := os.Getenv("WPARITY_PG")
	if url == "" {
		t.Skip("set WPARITY_PG to a Postgres URL to run the live test")
	}
	url, _, _ = strings.Cut(url, "?")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback(ctx) })
	for _, statement := range []string{
		`INSERT INTO organizations (id, name) VALUES ('wparity-org', 'WParity')`,
		`INSERT INTO projects (id, name, "organizationId", filters, "aiStoreContent")
		  VALUES ('wparity-proj', 'WParity Project', 'wparity-org',
		  '[{"type":"ip","ip":"1.2.3.4"},{"type":"event","name":"ignored","filters":[]}]', false)`,
	} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v", err)
		}
	}
	return tx
}

func newStore(cache Cache, db DB, clock Clock) *Store {
	return NewStore(cache, db, clock, notify.NewMatcher(nil), nil)
}

func parse(t *testing.T, raw string) *jsjson.Value {
	t.Helper()
	value, err := jsjson.Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func projectKey(id string) string { return notify.CacheKey(projectCacheName, id) }

func TestGetCachedReadsRedisFirst(t *testing.T) {
	cache := newFakeCache()
	cache.values[projectKey("p")] = `{"id":"p","name":"Cached","firstEventAt":"2026-01-01T00:00:00.000Z","filters":[]}`
	store := newStore(cache, nil, nil)
	got, err := store.GetCached(context.Background(), "p")
	if err != nil || got == nil || got.Name != "Cached" || !got.HasFirstEvent {
		t.Fatalf("got %+v %v", got, err)
	}
	if _, err := store.GetCached(context.Background(), "p"); err != nil || cache.gets != 1 {
		t.Errorf("second read should come from the process: %d gets, %v", cache.gets, err)
	}
}

func TestGetCachedAcceptsPartialRows(t *testing.T) {
	cache := newFakeCache()
	cache.values[projectKey("p")] = `{"firstEventAt":"2026-01-01T00:00:00.000Z","filters":[]}`
	got, err := newStore(cache, nil, nil).GetCached(context.Background(), "p")
	if err != nil || got == nil || got.Name != "" || !got.HasFirstEvent {
		t.Fatalf("got %+v %v", got, err)
	}
}

func TestGetCachedPropagatesRedisErrors(t *testing.T) {
	cache := newFakeCache()
	cache.err = errors.New("down")
	if _, err := newStore(cache, nil, nil).GetCached(context.Background(), "p"); !errors.Is(err, cache.err) {
		t.Fatalf("got %v", err)
	}
}

func TestGetCachedFallsBackToPostgres(t *testing.T) {
	tx := openTx(t)
	for _, miss := range []string{"", "{}", "null", "not json", `""`, "[]"} {
		cache := newFakeCache()
		if miss != "" {
			cache.values[projectKey("wparity-proj")] = miss
		}
		got, err := newStore(cache, tx, nil).GetCached(context.Background(), "wparity-proj")
		if err != nil || got == nil || got.Name != "WParity Project" || got.HasFirstEvent || got.ID != "wparity-proj" {
			t.Errorf("cached %q: got %+v %v", miss, got, err)
		}
		if wantKeys := map[bool]int{true: 1, false: 0}[miss != ""]; len(cache.values) != wantKeys {
			t.Errorf("cached %q: Redis must not be written, holds %v", miss, cache.values)
		}
	}
	cache := newFakeCache()
	store := newStore(cache, tx, nil)
	for range 2 {
		if got, err := store.GetCached(context.Background(), "wparity-unknown"); got != nil || err != nil {
			t.Fatalf("unknown project: %+v %v", got, err)
		}
	}
	if cache.gets != 2 {
		t.Errorf("an unknown project is never cached, so Redis is asked each time: %d", cache.gets)
	}
}

func TestIsExcluded(t *testing.T) {
	payload := parse(t, `{"name":"purchase","profileId":"u","deviceId":"d","properties":{"value":5,"plan":"pro"},"path":"/x"}`)
	tests := []struct {
		name    string
		filters string
		want    bool
		wantErr bool
	}{
		{"no filters", `[]`, false, false},
		{"null filters", `null`, false, false},
		{"name match", `[{"type":"event","name":"purchase","filters":[]}]`, true, false},
		{"other name", `[{"type":"event","name":"signup","filters":[]}]`, false, false},
		{"ip and profile_id filters are ignored", `[{"type":"ip","ip":"1.1.1.1"},{"type":"profile_id","profileId":"u"}]`, false, false},
		{"property filter", `[{"type":"event","name":"*","filters":[{"name":"properties.plan","operator":"is","value":["pro"]}]}]`, true, false},
		{"regex filter", `[{"type":"event","name":"*","filters":[{"name":"path","operator":"regex","value":["/^\\/x$/"]}]}]`, true, false},
		{"non-string property throws", `[{"type":"event","name":"purchase","filters":[{"name":"properties.value","operator":"is","value":["5"]}]}]`, false, true},
		{"a match before the throwing filter wins", `[{"type":"event","name":"purchase","filters":[]},{"type":"event","name":"purchase","filters":[{"name":"properties.value","operator":"is","value":["5"]}]}]`, true, false},
		{"null entry throws", `[null]`, false, true},
		{"filters that are not an array throw", `{"type":"event"}`, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newFakeCache()
			cache.values[projectKey("p")] = `{"id":"p","name":"n","filters":` + tt.filters + `}`
			got, err := newStore(cache, nil, nil).IsExcluded(context.Background(), "p", payload)
			if tt.wantErr != errors.Is(err, notify.ErrTypeError) || (!tt.wantErr && err != nil) {
				t.Fatalf("error %v, want error %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("excluded %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsExcludedUnknownProject(t *testing.T) {
	tx := openTx(t)
	excluded, err := newStore(newFakeCache(), tx, nil).IsExcluded(context.Background(), "wparity-unknown", parse(t, `{"name":"x"}`))
	if excluded || err != nil {
		t.Fatalf("%v %v", excluded, err)
	}
}

func TestMarkFirstEvent(t *testing.T) {
	tx := openTx(t)
	ctx := context.Background()
	cache := newFakeCache()
	clock := &tickingClock{at: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)}
	store := newStore(cache, tx, clock)

	if err := store.MarkFirstEvent(ctx, "wparity-proj"); err != nil {
		t.Fatal(err)
	}
	var firstEventAt, updatedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT "firstEventAt", "updatedAt" FROM projects WHERE id = 'wparity-proj'`).Scan(&firstEventAt, &updatedAt); err != nil {
		t.Fatal(err)
	}
	if want := clock.at.Add(-time.Millisecond); !firstEventAt.Equal(want) || !updatedAt.Equal(clock.at) {
		t.Errorf("firstEventAt %v updatedAt %v: two clock reads expected, last %v", firstEventAt, updatedAt, clock.at)
	}
	if len(cache.deleted) != 1 || cache.deleted[0] != "cachable:getProjectById:[wparity-proj]" {
		t.Errorf("deleted %v", cache.deleted)
	}

	// The next event sees the row through Postgres and does nothing.
	before := clock.at
	if err := store.MarkFirstEvent(ctx, "wparity-proj"); err != nil || !clock.at.Equal(before) || len(cache.deleted) != 1 {
		t.Errorf("second call: %v, clock moved %v, deleted %v", err, !clock.at.Equal(before), cache.deleted)
	}
}

func TestMarkFirstEventSkipsKnownAndUnknown(t *testing.T) {
	tx := openTx(t)
	ctx := context.Background()
	cache := newFakeCache()
	cache.values[projectKey("wparity-proj")] = `{"id":"wparity-proj","firstEventAt":"2026-01-01T00:00:00.000Z"}`
	clock := &tickingClock{at: time.Now()}
	store := newStore(cache, tx, clock)
	if err := store.MarkFirstEvent(ctx, "wparity-proj"); err != nil || len(cache.deleted) != 0 {
		t.Fatalf("known: %v %v", err, cache.deleted)
	}
	var stored *time.Time
	if err := tx.QueryRow(ctx, `SELECT "firstEventAt" FROM projects WHERE id = 'wparity-proj'`).Scan(&stored); err != nil || stored != nil {
		t.Fatalf("the row must stay untouched: %v %v", stored, err)
	}
	if err := store.MarkFirstEvent(ctx, "wparity-unknown"); err != nil || len(cache.deleted) != 0 {
		t.Fatalf("unknown: %v %v", err, cache.deleted)
	}
}

func TestMarkFirstEventIsConditional(t *testing.T) {
	tx := openTx(t)
	ctx := context.Background()
	if _, err := tx.Exec(ctx, `UPDATE projects SET "firstEventAt" = '2020-01-01' WHERE id = 'wparity-proj'`); err != nil {
		t.Fatal(err)
	}
	// A stale cached copy without firstEventAt: the UPDATE must not overwrite.
	cache := newFakeCache()
	cache.values[projectKey("wparity-proj")] = `{"id":"wparity-proj","name":"n"}`
	store := newStore(cache, tx, &tickingClock{at: time.Now()})
	if err := store.MarkFirstEvent(ctx, "wparity-proj"); err != nil {
		t.Fatal(err)
	}
	var stored time.Time
	if err := tx.QueryRow(ctx, `SELECT "firstEventAt" FROM projects WHERE id = 'wparity-proj'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored.Year() != 2020 {
		t.Errorf("firstEventAt overwritten: %v", stored)
	}
	if len(cache.deleted) != 1 {
		t.Errorf("the cache entry is still deleted: %v", cache.deleted)
	}
}

func TestGetAiStoreContent(t *testing.T) {
	tx := openTx(t)
	ctx := context.Background()
	cache := newFakeCache()
	key := notify.CacheKey(aiStoreCacheName, "wparity-proj")
	store := newStore(cache, tx, nil)

	got, err := store.GetAiStoreContent(ctx, "wparity-proj")
	if err != nil || got == nil || *got {
		t.Fatalf("from Postgres: %v %v", got, err)
	}
	cache.values[key] = "true"
	if got, err = store.GetAiStoreContent(ctx, "wparity-proj"); err != nil || got == nil || !*got {
		t.Fatalf("a cached true wins: %v %v", got, err)
	}
	cache.values[key] = "false"
	if got, err = store.GetAiStoreContent(ctx, "wparity-proj"); err != nil || got == nil || *got {
		t.Fatalf("a cached false is an answer: %v %v", got, err)
	}
	before := cache.gets
	_, _ = store.GetAiStoreContent(ctx, "wparity-proj")
	if cache.gets != before+1 {
		t.Error("there is no process-local copy: every read asks Redis")
	}
	if got, err = store.GetAiStoreContent(ctx, "wparity-unknown"); err != nil || got != nil {
		t.Fatalf("unknown project: %v %v", got, err)
	}
}

func TestProjectNameForNotifications(t *testing.T) {
	cache := newFakeCache()
	cache.values[projectKey("p")] = `{"id":"p","name":"Shop"}`
	var names notify.ProjectNames = newStore(cache, nil, nil)
	if got, err := names.ProjectName(context.Background(), "p"); err != nil || got != "Shop" {
		t.Fatalf("%q %v", got, err)
	}
}
