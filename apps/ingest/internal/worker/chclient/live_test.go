package chclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/clock"
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush/chtest"
)

// Live tests need WPARITY_CH (see internal/worker/flush/chtest) and run in a
// database of their own, openpanel_wparity_flush_chclient, built from the
// repository's schema and dropped afterwards.

func liveClient(t *testing.T, urls ...string) (*Client, string, string) {
	t.Helper()
	base := chtest.BaseURL(t)
	database := chtest.Database(t, base, "chclient")
	cfg := defaultTestConfig()
	cfg.RequestTimeout = 30 * time.Second
	live := chtest.URLWithDatabase(base, database)
	client, err := New(Config{URLs: append(urls, live), ClickHouseConfig: cfg}, Deps{Clock: clock.System{}})
	if err != nil {
		t.Fatal(err)
	}
	return client, base, database
}

func botRow(n int) string {
	return fmt.Sprintf(`{"id":"00000000-0000-4000-8000-%012d","name":"bot é %d","type":"crawler","project_id":"wparity-project","path":"/p/%d","created_at":"2026-10-08 09:00:%02d"}`, n, n, n, n%60)
}

func countRows(t *testing.T, base, database, where string) int {
	t.Helper()
	var count int
	out := strings.TrimSpace(chtest.Exec(t, base, database, "SELECT count() FROM events_bots WHERE "+where))
	if _, err := fmt.Sscanf(out, "%d", &count); err != nil {
		t.Fatalf("count: %q", out)
	}
	return count
}

func TestLiveInsertAndQueryRoundTrip(t *testing.T) {
	client, base, database := liveClient(t)
	ctx := context.Background()
	rows := []string{botRow(1), botRow(2), botRow(3)}
	for _, settings := range []Settings{nil, AsyncInsertSettings(false), nil} {
		if err := client.Insert(ctx, TableBots, rows, settings); err != nil {
			t.Fatal(err)
		}
	}
	if got := countRows(t, base, database, "project_id = 'wparity-project'"); got != 9 {
		t.Fatalf("%d rows landed", got)
	}

	result, err := client.Query(ctx, "SELECT name, path FROM events_bots WHERE project_id = {p:String} AND name IN {names:Array(String)} ORDER BY path LIMIT 1",
		[]Param{{"p", FormatString("wparity-project")}, {"names", FormatStringArray([]string{"bot é 2", "it's"})}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var first struct{ Name, Path string }
	if err := json.Unmarshal(result.Data[0], &first); err != nil {
		t.Fatal(err)
	}
	if result.Rows != 1 || first.Name != "bot é 2" || first.Path != "/p/2" || result.Meta[0].Name != "name" {
		t.Fatalf("result %+v %s", result, result.Data)
	}

	// Awkward characters survive the parameter encoding.
	nasty := "a\tb\nc'd\\e é 😀"
	echo, err := client.Query(ctx, "SELECT {s:String} AS s", []Param{{"s", FormatString(nasty)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var row struct {
		S string `json:"s"`
	}
	if err := json.Unmarshal(echo.Data[0], &row); err != nil || row.S != nasty {
		t.Fatalf("echo %q (%v)", row.S, err)
	}
}

func TestLiveAsyncInsertSettingsAreAccepted(t *testing.T) {
	client, base, database := liveClient(t)
	if err := client.Insert(context.Background(), TableBots, []string{botRow(1)}, AsyncInsertSettings(true)); err != nil {
		t.Fatal(err)
	}
	chtest.Exec(t, base, database, "SYSTEM FLUSH ASYNC INSERT QUEUE")
	if got := countRows(t, base, database, "project_id = 'wparity-project'"); got != 1 {
		t.Fatalf("%d rows landed", got)
	}
}

// The first node refuses connections: the insert goes to the live one, as in
// clickhouse-failover.test.ts, and the query does too.
func TestLiveFailoverFromARefusedNode(t *testing.T) {
	dead := refusedURL(t)
	client, base, database := liveClient(t, dead)
	ctx := context.Background()
	if err := client.Insert(ctx, TableBots, []string{botRow(7)}, nil); err != nil {
		t.Fatal(err)
	}
	result, err := client.Query(ctx, "SELECT 1 AS value", nil, nil)
	if err != nil || result.Rows != 1 {
		t.Fatalf("query: %v %+v", err, result)
	}
	if got := countRows(t, base, database, "project_id = 'wparity-project'"); got != 1 {
		t.Fatalf("%d rows landed, want exactly one (the refused node took none)", got)
	}
}

// A line ClickHouse cannot parse fails the whole call and nothing lands, so
// the caller keeps the batch in Redis rather than trimming rows that never
// landed.
func TestLivePoisonLineFailsTheInsertAndLandsNothing(t *testing.T) {
	client, base, database := liveClient(t)
	rows := []string{botRow(1), botRow(2), `{"id": not json`, botRow(3)}
	err := client.Insert(context.Background(), TableBots, rows, AsyncInsertSettings(false))
	var server *ServerError
	if !errors.As(err, &server) || Classify(err) != ClassServer {
		t.Fatalf("err = %v (class %s)", err, Classify(err))
	}
	t.Logf("server answered: HTTP %d code %d: %.120s", server.Status, server.Code, server.Message)
	if got := countRows(t, base, database, "project_id = 'wparity-project'"); got != 0 {
		t.Fatalf("%d rows of a failed insert landed", got)
	}
}

func TestLiveCommandAndUnknownTable(t *testing.T) {
	client, _, _ := liveClient(t)
	ctx := context.Background()
	if err := client.Command(ctx, "CREATE TABLE IF NOT EXISTS wparity_scratch (x UInt8) ENGINE = Memory", nil); err != nil {
		t.Fatal(err)
	}
	if err := client.Command(ctx, "DROP TABLE wparity_scratch", nil); err != nil {
		t.Fatal(err)
	}
	err := client.Insert(ctx, "table_that_does_not_exist", []string{"{}"}, nil)
	var server *ServerError
	if !errors.As(err, &server) || server.Code != 60 {
		t.Fatalf("err = %v", err)
	}
}

func TestLiveConfigFromEnvironmentBuildsAWorkingClient(t *testing.T) {
	base := chtest.BaseURL(t)
	database := chtest.Database(t, base, "chclient")
	cfg, err := config.Load(func(name string) string {
		switch name {
		case "ROLE":
			return "worker"
		case "ENABLED_QUEUES":
			return "cron"
		case "CLICKHOUSE_URL":
			return chtest.URLWithDatabase(base, database)
		case "CLICKHOUSE_SETTINGS":
			return `{"max_threads":2}`
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(FromWorkerConfig(cfg), Deps{Clock: clock.System{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Insert(context.Background(), TableBots, []string{botRow(1)}, nil); err != nil {
		t.Fatal(err)
	}
}
