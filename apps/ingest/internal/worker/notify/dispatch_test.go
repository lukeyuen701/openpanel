package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"openpanel/ingest/internal/jsjson"
)

type memCache map[string]string

func (m memCache) Get(_ context.Context, key string) (string, bool, error) {
	value, ok := m[key]
	return value, ok, nil
}

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

type fixedName string

func (n fixedName) ProjectName(context.Context, string) (string, error) { return string(n), nil }

type fakeProfileRows struct{ row *jsjson.Value }

func (f fakeProfileRows) ProfileRow(context.Context, string, string) (*jsjson.Value, error) {
	return f.row, nil
}

type effect struct {
	op   string
	data string
}

// recorder is the Enqueuer and the DB at once, so the order of effects is
// observable.
type recorder struct {
	effects     []effect
	inserts     [][]any
	failEnqueue func(*jsjson.Value) error
	raw         []string
}

func (r *recorder) Add(_ context.Context, queue, name string, data []byte, opts JobOptions) (string, error) {
	if queue != QueueName || name != JobName || opts != (JobOptions{}) {
		return "", fmt.Errorf("unexpected job %s/%s %+v", queue, name, opts)
	}
	envelope, err := jsjson.Parse(data)
	if err != nil {
		return "", err
	}
	if r.failEnqueue != nil {
		if err := r.failEnqueue(envelope); err != nil {
			return "", err
		}
	}
	r.effects = append(r.effects, effect{"add", string(jsjson.Stringify(envelope.Get("payload")))})
	r.raw = append(r.raw, string(data))
	return "1", nil
}

func (r *recorder) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	r.inserts = append(r.inserts, args)
	r.effects = append(r.effects, effect{op: "insert"})
	return pgconn.CommandTag{}, nil
}

func (r *recorder) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected query")
}

func TestDispatchGolden(t *testing.T) {
	var golden struct {
		Keys struct {
			Project string `json:"projectKey"`
			Rules   string `json:"rulesKey"`
			Profile string `json:"profileKey"`
		} `json:"keys"`
		Scenarios []struct {
			Scenario   string                 `json:"scenario"`
			Rules      json.RawMessage        `json:"rules"`
			Project    *struct{ Name string } `json:"project"`
			Profile    *string                `json:"profile"`
			ProfileRow json.RawMessage        `json:"profileRow"`
			Payload    json.RawMessage        `json:"payload"`
			Effects    []struct {
				Op   string          `json:"op"`
				Data json.RawMessage `json:"data"`
			} `json:"effects"`
			Error        *string         `json:"error"`
			FinalPayload json.RawMessage `json:"finalPayload"`
		} `json:"scenarios"`
	}
	readGolden(t, "dispatch.json", &golden)
	if got := CacheKey(rulesCacheName, "wparity-proj"); got != golden.Keys.Rules {
		t.Fatalf("rules key %q, TypeScript %q", got, golden.Keys.Rules)
	}
	now := time.Date(2026, 1, 1, 0, 2, 0, 123456789, time.UTC)

	for _, s := range golden.Scenarios {
		t.Run(s.Scenario, func(t *testing.T) {
			cache := memCache{golden.Keys.Rules: string(s.Rules)}
			rec := &recorder{}
			rows := fakeProfileRows{}
			switch {
			case s.Profile != nil && *s.Profile == "cache":
				cache[golden.Keys.Profile] = string(s.ProfileRow)
			case s.Profile != nil && *s.Profile == "clickhouse":
				rows.row = mustParse(t, s.ProfileRow)
			}
			name := ""
			if s.Project != nil {
				name = s.Project.Name
			}
			dispatcher := &Dispatcher{
				Matcher:  NewMatcher(nil),
				Projects: fixedName(name),
				Rules:    NewRuleStore(cache, rec),
				Profiles: NewProfileStore(cache, rows),
				DB:       rec,
				Queue:    rec,
				Clock:    fixedClock{now},
			}
			payload := mustParse(t, s.Payload)
			err := dispatcher.Check(context.Background(), "wparity-req", payload)

			if (s.Error != nil) != (err != nil) || (err != nil && !errors.Is(err, ErrTypeError)) {
				t.Fatalf("error: got %v, TypeScript %v", err, s.Error)
			}
			if len(rec.effects) != len(s.Effects) {
				t.Fatalf("%d effects %v, TypeScript %d", len(rec.effects), rec.effects, len(s.Effects))
			}
			inserts := 0
			for i, want := range s.Effects {
				got := rec.effects[i]
				if got.op != want.Op {
					t.Fatalf("effect %d: %s, TypeScript %s", i, got.op, want.Op)
				}
				if want.Op == "add" {
					wantText := string(jsjson.Stringify(mustParse(t, want.Data)))
					if got.data != wantText {
						t.Errorf("effect %d job data:\n got %s\nwant %s", i, got.data, wantText)
					}
					continue
				}
				checkInsert(t, rec.inserts[inserts], mustParse(t, want.Data), now)
				inserts++
			}
			wantFinal := string(jsjson.Stringify(mustParse(t, s.FinalPayload)))
			if got := string(jsjson.Stringify(payload)); got != wantFinal {
				t.Errorf("payload after the check:\n got %s\nwant %s", got, wantFinal)
			}
		})
	}
}

// checkInsert holds a notifications INSERT's bind parameters to what Prisma
// was asked to create.
func checkInsert(t *testing.T, args []any, want *jsjson.Value, now time.Time) {
	t.Helper()
	text := func(key string) string { return jsjson.JSString(want.Get(key)) }
	got := func(i int) string { return fmt.Sprint(args[i]) }
	checks := []struct {
		name      string
		got, want string
	}{
		{"projectId", got(0), text("projectId")},
		{"title", got(1), text("title")},
		{"message", got(2), text("message")},
		{"createdAt", got(3), now.Truncate(time.Millisecond).String()},
		{"updatedAt", got(4), now.Truncate(time.Millisecond).String()},
		{"sendToApp", got(5), text("sendToApp")},
		{"sendToEmail", got(6), text("sendToEmail")},
		{"notificationRuleId", got(8), text("notificationRuleId")},
		{"payload", got(9), string(jsjson.Stringify(want.Get("payload")))},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("insert %s: got %q want %q", c.name, c.got, c.want)
		}
	}
	if id, _ := args[7].(*string); want.Get("integrationId").Kind == jsjson.String {
		if id == nil || *id != want.Get("integrationId").Str {
			t.Errorf("insert integrationId: got %v want %s", id, want.Get("integrationId").Str)
		}
	} else if id != nil {
		t.Errorf("insert integrationId: got %q want null", *id)
	}
}

func TestDeliveryFailureDoesNotCancelOthers(t *testing.T) {
	rules := `[{"id":"00000000-0000-0000-0000-0000000000a1","name":"r","sendToApp":true,"sendToEmail":true,"config":{"type":"events","events":[{"name":"x","filters":[]}]},"template":null,"integrations":[{"id":"00000000-0000-0000-0000-0000000000b1"}]}]`
	cache := memCache{CacheKey(rulesCacheName, "p"): rules}
	rec := &recorder{}
	boom := errors.New("redis down")
	rec.failEnqueue = func(data *jsjson.Value) error {
		if data.Path("payload", "notification", "integrationId").Kind == jsjson.String {
			return boom
		}
		return nil
	}
	dispatcher := &Dispatcher{
		Matcher: NewMatcher(nil), Projects: fixedName("P"), Rules: NewRuleStore(cache, rec),
		Profiles: NewProfileStore(cache, fakeProfileRows{}), DB: rec, Queue: rec, Clock: fixedClock{time.Now()},
	}
	payload := mustParse(t, json.RawMessage(`{"name":"x","projectId":"p","profileId":"d","deviceId":"d"}`))
	err := dispatcher.Check(context.Background(), "r", payload)
	if !errors.Is(err, boom) {
		t.Fatalf("want the enqueue error, got %v", err)
	}
	if len(rec.effects) != 3 {
		t.Fatalf("email job, row and app job should still happen: %v", rec.effects)
	}
}

// TestJobDataEnvelope holds the stored job data to the bytes captured from the
// TypeScript producer (contracts P1.9.2): the zod key order, the nulls, the
// envelope and its requestId.
func TestJobDataEnvelope(t *testing.T) {
	rules := `[{"id":"00000000-0000-0000-0000-0000000000a1","name":"r","sendToApp":false,"sendToEmail":true,"config":{"type":"events","events":[{"name":"purchase","filters":[]}]},"template":"Bought $EVENT_NAME by {{profileId}}","integrations":[]}]`
	cache := memCache{CacheKey(rulesCacheName, "wparity-proj"): rules}
	rec := &recorder{}
	dispatcher := &Dispatcher{
		Matcher: NewMatcher(nil), Projects: fixedName("WParity Project"), Rules: NewRuleStore(cache, rec),
		Profiles: NewProfileStore(cache, fakeProfileRows{}), DB: rec, Queue: rec, Clock: fixedClock{time.Now()},
	}
	payload := mustParse(t, json.RawMessage(`{"name":"purchase","profileId":"wparity-user","projectId":"wparity-proj","deviceId":"wparity-dev"}`))
	if err := dispatcher.Check(context.Background(), "wparity-req", payload); err != nil {
		t.Fatal(err)
	}
	want := `{"payload":{"notification":{"projectId":"wparity-proj","title":"Bought purchase by wparity-user","message":"Project: WParity Project","sendToApp":false,"sendToEmail":true,"integrationId":null,"notificationRuleId":"00000000-0000-0000-0000-0000000000a1","payload":{"type":"event","event":{"name":"purchase","profileId":"wparity-user","projectId":"wparity-proj","deviceId":"wparity-dev"}}}},"meta":{"requestId":"wparity-req"}}`
	if len(rec.raw) != 1 || rec.raw[0] != want {
		t.Errorf("job data:\n got %v\nwant %s", rec.raw, want)
	}
}
