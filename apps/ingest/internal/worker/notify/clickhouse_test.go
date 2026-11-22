package notify

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"openpanel/ingest/internal/jsjson"
)

func TestClickHouseHTTPRequestAndResponse(t *testing.T) {
	var seen *http.Request
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r
		body = make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		_, _ = w.Write([]byte(`{"meta":[],"data":[{"id":"u","created_at":"2026-01-02 03:04:05.678"}],"rows":1}`))
	}))
	defer server.Close()

	client, err := NewClickHouseHTTP(server.URL + "/openpanel")
	if err != nil {
		t.Fatal(err)
	}
	row, err := client.ProfileRow(context.Background(), "proj", "user 1&x\t'\\")
	if err != nil {
		t.Fatal(err)
	}
	query := seen.URL.Query()
	if query.Get("param_p1") != "user 1&x\\t\\'\\\\" || query.Get("param_p2") != "proj" || query.Get("database") != "openpanel" {
		t.Errorf("query %v", query)
	}
	if string(body) != profileQuery {
		t.Errorf("body %q", body)
	}
	if got := jsjson.JSString(row.Get("id")); got != "u" {
		t.Errorf("row %s", jsjson.Stringify(row))
	}
}

func TestClickHouseHTTPEmptyAndError(t *testing.T) {
	status := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()
	client := &ClickHouseHTTP{URL: server.URL}
	if row, err := client.ProfileRow(context.Background(), "p", "u"); err != nil || row != nil {
		t.Errorf("no row: %v %v", row, err)
	}
	status = http.StatusInternalServerError
	if _, err := client.ProfileRow(context.Background(), "p", "u"); err == nil {
		t.Error("a 500 must be an error")
	}
}

// TestClickHouseLive runs the real query against WPARITY_CH when set.
func TestClickHouseLive(t *testing.T) {
	url := os.Getenv("WPARITY_CH")
	if url == "" {
		t.Skip("set WPARITY_CH to a ClickHouse URL to run the live test")
	}
	client, err := NewClickHouseHTTP(url)
	if err != nil {
		t.Fatal(err)
	}
	row, err := client.ProfileRow(context.Background(), "wparity-none", "wparity-none")
	if err != nil || row != nil {
		t.Skipf("live ClickHouse not usable: %v %v", row, err)
	}
	response, err := http.Post(client.URL+"/?database="+client.Database, "text/plain", strings.NewReader("SELECT project_id, id FROM profiles LIMIT 1 FORMAT JSON"))
	if err != nil {
		t.Skipf("live ClickHouse not usable: %v", err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	listing, err := jsjson.Parse(raw)
	if err != nil || listing.Get("data") == nil || len(listing.Get("data").Arr) == 0 {
		t.Skip("no profile in the live ClickHouse")
	}
	existing := listing.Get("data").Arr[0]
	found, err := client.ProfileRow(context.Background(), jsjson.JSString(existing.Get("project_id")), jsjson.JSString(existing.Get("id")))
	if err != nil || found == nil {
		t.Fatalf("lookup by id: %v %v", found, err)
	}
	profile, err := transformProfile(found)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Get("createdAt").Kind != jsjson.String {
		t.Errorf("profile %s", jsjson.Stringify(profile))
	}
}

// TestClickHouseLiveParamEscaping sends the characters the JavaScript client
// escapes and expects ClickHouse to hand them back unchanged.
func TestClickHouseLiveParamEscaping(t *testing.T) {
	url := os.Getenv("WPARITY_CH")
	if url == "" {
		t.Skip("set WPARITY_CH to a ClickHouse URL to run the live test")
	}
	client, err := NewClickHouseHTTP(url)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"plain", "tab\there", "line\nbreak\r", "it's", "back\\slash", "\\t literal", "a\\'b", "ключ 😀"} {
		body, err := client.run(context.Background(), "SELECT {p1:String} AS v FORMAT JSONCompact", map[string]string{"p1": value})
		if err != nil {
			t.Skipf("live ClickHouse not usable: %v", err)
		}
		parsed, err := jsjson.Parse(body)
		if err != nil {
			t.Fatal(err)
		}
		if got := parsed.Get("data").Arr[0].Arr[0].Str; got != value {
			t.Errorf("sent %q, ClickHouse read %q", value, got)
		}
	}
}
