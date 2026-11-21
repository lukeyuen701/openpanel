package chclient

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/clock"
	"openpanel/ingest/internal/worker/config"
)

type wireCall struct {
	Op       string            `json:"op"`
	Table    string            `json:"table"`
	Lines    []string          `json:"lines"`
	Objects  []json.RawMessage `json:"objects"`
	Query    string            `json:"query"`
	Settings [][2]any          `json:"settings"`
	Params   [][2]any          `json:"params"`
}

type wireRequest struct {
	Method          string      `json:"method"`
	Path            string      `json:"path"`
	Pairs           [][2]string `json:"pairs"`
	Authorization   *string     `json:"authorization"`
	ContentEncoding *string     `json:"contentEncoding"`
	Body            string      `json:"body"`
}

type wireScenario struct {
	Name     string            `json:"name"`
	Env      map[string]string `json:"env"`
	URL      string            `json:"url"`
	Calls    []wireCall        `json:"calls"`
	Requests []wireRequest     `json:"requests"`
}

// recorder answers 200 to everything and keeps what it was sent.
type recorder struct{ requests []wireRequest }

func (r *recorder) RoundTrip(request *http.Request) (*http.Response, error) {
	raw, _ := io.ReadAll(request.Body)
	body := string(raw)
	encoding := request.Header.Get("Content-Encoding")
	if encoding == "gzip" {
		reader, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		plain, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		body = string(plain)
	}
	pairs := [][2]string{}
	for _, part := range strings.Split(request.URL.RawQuery, "&") {
		name, value, _ := strings.Cut(part, "=")
		name, value = formDecode(name), formDecode(value)
		if name == "query_id" {
			value = "<id>"
		}
		pairs = append(pairs, [2]string{name, value})
	}
	captured := wireRequest{Method: request.Method, Path: request.URL.Path, Pairs: pairs, Body: body}
	if auth := request.Header.Get("Authorization"); auth != "" {
		captured.Authorization = &auth
	}
	if encoding != "" {
		captured.ContentEncoding = &encoding
	}
	r.requests = append(r.requests, captured)
	answer := ""
	if request.URL.Query().Has("default_format") {
		answer = `{"meta":[],"data":[],"rows":0}`
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(answer)), Header: http.Header{}, Request: request}, nil
}

func formDecode(text string) string {
	var out strings.Builder
	for i := 0; i < len(text); i++ {
		switch {
		case text[i] == '+':
			out.WriteByte(' ')
		case text[i] == '%' && i+2 < len(text)+0:
			var value byte
			for _, digit := range []byte{text[i+1], text[i+2]} {
				value <<= 4
				switch {
				case digit >= '0' && digit <= '9':
					value |= digit - '0'
				case digit >= 'A' && digit <= 'F':
					value |= digit - 'A' + 10
				case digit >= 'a' && digit <= 'f':
					value |= digit - 'a' + 10
				}
			}
			out.WriteByte(value)
			i += 2
		default:
			out.WriteByte(text[i])
		}
	}
	return out.String()
}

func settingValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return strings.TrimSuffix(strings.TrimSuffix(jsonNumber(typed), ".0"), ".")
	}
	panic("unsupported setting")
}

func jsonNumber(value float64) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

func paramText(t *testing.T, value any) string {
	t.Helper()
	switch typed := value.(type) {
	case string:
		return FormatString(typed)
	case []any:
		items := make([]string, len(typed))
		for i, item := range typed {
			items[i], _ = item.(string)
		}
		return FormatStringArray(items)
	}
	t.Fatalf("unsupported param %v", value)
	return ""
}

// TestWireMatchesTheNodeClient replays the calls of parity/golden-chclient.ts
// and compares URL pairs (in order), credentials, encoding and decompressed
// body with what the TypeScript client sent for the same calls and env.
func TestWireMatchesTheNodeClient(t *testing.T) {
	raw, err := os.ReadFile("testdata/wire.json")
	if err != nil {
		t.Fatal(err)
	}
	var scenarios []wireScenario
	if err := json.Unmarshal(raw, &scenarios); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			cfg, err := config.Load(func(name string) string {
				switch name {
				case "ROLE":
					return "worker"
				case "ENABLED_QUEUES":
					return "cron"
				}
				return scenario.Env[name]
			})
			if err != nil {
				t.Fatal(err)
			}
			userinfo := "u:p@"
			if scenario.Name == "no-credentials-no-database" {
				userinfo = ""
			}
			rec := &recorder{}
			client, err := New(Config{URLs: []string{"http://" + userinfo + "127.0.0.1:1" + scenario.URL}, ClickHouseConfig: cfg.ClickHouse},
				Deps{Clock: clock.System{}, Transport: rec})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			for _, call := range scenario.Calls {
				var settings Settings
				for _, pair := range call.Settings {
					settings = append(settings, Setting{Key: pair[0].(string), Value: settingValue(pair[1])})
				}
				switch call.Op {
				case "insert":
					lines := call.Lines
					for _, object := range call.Objects {
						var compact bytes.Buffer
						if err := json.Compact(&compact, object); err != nil {
							t.Fatal(err)
						}
						lines = append(lines, compact.String())
					}
					err = client.Insert(ctx, call.Table, lines, settings)
				case "command":
					err = client.Command(ctx, call.Query, settings)
				case "query":
					var params []Param
					for _, pair := range call.Params {
						params = append(params, Param{Name: pair[0].(string), Value: paramText(t, pair[1])})
					}
					_, err = client.Query(ctx, call.Query, params, settings)
				}
				if err != nil {
					t.Fatalf("%s: %v", call.Op, err)
				}
			}
			if len(rec.requests) != len(scenario.Requests) {
				t.Fatalf("sent %d requests, TypeScript sent %d", len(rec.requests), len(scenario.Requests))
			}
			for i, want := range scenario.Requests {
				got := rec.requests[i]
				gotJSON, _ := json.Marshal(got)
				wantJSON, _ := json.Marshal(want)
				if string(gotJSON) != string(wantJSON) {
					t.Errorf("request %d differs\n go: %s\n ts: %s", i, gotJSON, wantJSON)
				}
			}
		})
	}
}

func TestQueryDecodesTheJSONAnswer(t *testing.T) {
	server := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		answer := `{"meta":[{"name":"n","type":"UInt64"}],"data":[{"n":"7"},{"n":"8"}],"rows":2,"statistics":{}}`
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(answer)), Header: http.Header{}}, nil
	})
	client, err := New(Config{URLs: []string{"http://127.0.0.1:1/db"}, ClickHouseConfig: defaultTestConfig()}, Deps{Clock: clock.System{}, Transport: server})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Query(context.Background(), "SELECT 1", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 2 || len(result.Data) != 2 || result.Meta[0].Type != "UInt64" || string(result.Data[1]) != `{"n":"8"}` {
		t.Fatalf("unexpected result %+v", result)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func defaultTestConfig() config.ClickHouseConfig {
	return config.ClickHouseConfig{
		RequestCompression: true, MaxOpenConnections: 5, RequestTimeout: 5 * time.Second, UnhealthyMark: 0,
	}
}

func TestNewRejectsAnUnusableSetup(t *testing.T) {
	if _, err := New(Config{ClickHouseConfig: defaultTestConfig()}, Deps{Clock: clock.System{}}); err == nil {
		t.Error("no CLICKHOUSE_URL must fail")
	}
	if _, err := New(Config{URLs: []string{"not a url"}, ClickHouseConfig: defaultTestConfig()}, Deps{Clock: clock.System{}}); err == nil {
		t.Error("a malformed URL must fail")
	}
}
