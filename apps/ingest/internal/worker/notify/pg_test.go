package notify

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"openpanel/ingest/internal/jsjson"
)

const (
	fixtureProject     = "wparity-proj"
	fixtureRuleID      = "00000000-0000-0000-0000-0000000000a1"
	fixtureIntegration = "00000000-0000-0000-0000-0000000000b1"
)

// openTx opens a transaction on the dev database that is always rolled back,
// with an organization, project, rule and integration inside it. The test is
// skipped when there is no reachable database.
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
	statements := []string{
		`INSERT INTO organizations (id, name) VALUES ('wparity-org', 'WParity')`,
		`INSERT INTO projects (id, name, "organizationId") VALUES ('wparity-proj', 'WParity Project', 'wparity-org')`,
		`INSERT INTO notification_rules (id, name, "projectId", "sendToApp", "sendToEmail", config, template, "updatedAt")
		  VALUES ('` + fixtureRuleID + `', 'Purchases', 'wparity-proj', true, false,
		  '{"type":"events","events":[{"id":"e1","name":"purchase","filters":[]}]}', 'Bought $EVENT_NAME', now())`,
		`INSERT INTO integrations (id, name, config, "organizationId", "projectId")
		  VALUES ('` + fixtureIntegration + `', 'hook', '{"type":"webhook"}', 'wparity-org', 'wparity-proj')`,
		`INSERT INTO "_IntegrationToNotificationRule" ("A", "B") VALUES ('` + fixtureIntegration + `', '` + fixtureRuleID + `')`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v", err)
		}
	}
	return tx
}

func TestRuleStoreReadsPostgres(t *testing.T) {
	tx := openTx(t)
	store := NewRuleStore(memCache{}, tx)
	rules, err := store.ByProject(context.Background(), fixtureProject)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 1 {
		t.Fatalf("%d rules", len(rules))
	}
	rule := rules[0]
	if rule.ID != fixtureRuleID || rule.Name != "Purchases" || !rule.SendToApp || rule.SendToEmail || rule.Template != "Bought $EVENT_NAME" {
		t.Errorf("rule %+v", rule)
	}
	if len(rule.IntegrationIDs) != 1 || rule.IntegrationIDs[0] != fixtureIntegration {
		t.Errorf("integrations %v", rule.IntegrationIDs)
	}
	if got := string(jsjson.Stringify(rule.Config)); got != `{"type":"events","events":[{"id":"e1","name":"purchase","filters":[]}]}` {
		t.Errorf("config %s", got)
	}
	none, err := store.ByProject(context.Background(), "wparity-none")
	if err != nil || none == nil || len(none) != 0 {
		t.Errorf("no rules: %v %v", none, err)
	}
}

func TestDispatchInsertsNotificationRow(t *testing.T) {
	tx := openTx(t)
	ctx := context.Background()
	queue := &recorder{}
	now := time.Date(2026, 1, 1, 0, 2, 0, 123456789, time.UTC)
	dispatcher := &Dispatcher{
		Matcher:  NewMatcher(nil),
		Projects: fixedName("WParity Project"),
		Rules:    NewRuleStore(memCache{}, tx),
		Profiles: NewProfileStore(memCache{}, fakeProfileRows{}),
		DB:       tx,
		Queue:    queue,
		Clock:    fixedClock{now},
	}
	payload := mustParse(t, json.RawMessage(`{"name":"purchase","profileId":"u","deviceId":"d","projectId":"wparity-proj","properties":{},"createdAt":"2026-01-01T00:01:00.000Z"}`))
	payload.Get("properties").Obj.Set("note", jsjson.NewString("a\x00b"))
	if err := dispatcher.Check(ctx, "wparity-req", payload); err != nil {
		t.Fatal(err)
	}

	var (
		id, title, message, ruleID, payloadText string
		createdAt, updatedAt                    time.Time
		sendToApp, sendToEmail                  bool
		integrationID                           *string
	)
	err := tx.QueryRow(ctx, `SELECT id::text, title, message, "createdAt", "updatedAt", "sendToApp", "sendToEmail", "integrationId"::text, "notificationRuleId"::text, payload::text
FROM notifications WHERE "projectId" = 'wparity-proj'`).Scan(&id, &title, &message, &createdAt, &updatedAt, &sendToApp, &sendToEmail, &integrationID, &ruleID, &payloadText)
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 36 || title != "Bought purchase" || message != "Project: WParity Project" || !sendToApp || sendToEmail || integrationID != nil || ruleID != fixtureRuleID {
		t.Errorf("row %s %q %q app=%v email=%v integration=%v rule=%s", id, title, message, sendToApp, sendToEmail, integrationID, ruleID)
	}
	wantTime := time.Date(2026, 1, 1, 0, 2, 0, 123000000, time.UTC)
	if !createdAt.Equal(wantTime) || !updatedAt.Equal(wantTime) {
		t.Errorf("timestamps %v %v, want %v", createdAt, updatedAt, wantTime)
	}
	var stored map[string]any
	if err := json.Unmarshal([]byte(payloadText), &stored); err != nil {
		t.Fatal(err)
	}
	event := stored["event"].(map[string]any)
	if stored["type"] != "event" || event["properties"].(map[string]any)["note"] != "ab" {
		t.Errorf("payload %s", payloadText)
	}
	if len(queue.effects) != 2 {
		t.Fatalf("jobs %v: want the integration job and the app job", queue.effects)
	}
}
