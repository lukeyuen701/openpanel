package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/scrypt"

	"openpanel/ingest/internal/bots"
	"openpanel/ingest/internal/clientauth"
	"openpanel/ingest/internal/config"
	"openpanel/ingest/internal/dedupe"
	"openpanel/ingest/internal/geo"
	"openpanel/ingest/internal/identity"
	"openpanel/ingest/internal/kv"
	"openpanel/ingest/internal/lookups"
	"openpanel/ingest/internal/track"
	"openpanel/ingest/internal/useragent"
)

const (
	testClientID = "0f0f0000-0000-4000-8000-0000000000aa"
	testSecret   = "sec_test"
	chromeUA     = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

type message struct{ key, value string }

type fakeProducer struct {
	mu       sync.Mutex
	messages []message
}

func (f *fakeProducer) Produce(_ context.Context, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, message{string(key), string(value)})
	return nil
}

type noGeo struct{}

func (noGeo) Location(string) geo.Location { return geo.Location{} }
func (noGeo) ASN(string) geo.ASN           { return geo.ASN{} }

type upstreamCall struct {
	method, path string
	header       http.Header
	body         string
}

type harness struct {
	handler  http.Handler
	producer *fakeProducer
	upstream *[]upstreamCall
}

func newHarness(t *testing.T) harness {
	t.Helper()
	var mu sync.Mutex
	calls := []upstreamCall{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		calls = append(calls, upstreamCall{r.Method, r.URL.Path, r.Header.Clone(), string(body)})
		mu.Unlock()
		w.Header().Set("Content-Type", "text/plain;charset=utf-8")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("from bun"))
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, _ := url.Parse(upstream.URL)

	hash, err := scrypt.Key([]byte(testSecret), []byte("salt"), 16384, 8, 1, 32)
	if err != nil {
		t.Fatal(err)
	}
	store := kv.NewMemory()
	client := map[string]any{
		"id": testClientID, "organizationId": "org", "type": "write", "secret": "salt." + hex.EncodeToString(hash),
		"projectId": "proj", "ignoreCorsAndSecret": false,
		"project": map[string]any{"id": "proj", "cors": []string{"https://ok.example"}, "filters": []any{}, "allowUnsafeRevenueTracking": false},
	}
	encoded, _ := json.Marshal(client)
	store.Set("cachable::["+testClientID+"]", string(encoded))
	store.Set("cachable:op:salt:[]", `{"current":"current-salt","previous":"previous-salt"}`)

	parser, err := useragent.Default()
	if err != nil {
		t.Fatal(err)
	}
	detector, err := bots.New()
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	producer := &fakeProducer{}
	cfg := config.Config{UpstreamURL: upstreamURL, MaxNativeBodyBytes: 4 << 20, SelfHosted: true}
	cfg.Kafka.MaxMessageBytes = 1 << 20
	srv := &Server{
		Config:  cfg,
		Auth:    &clientauth.Authenticator{Store: clientauth.NewStore(store, nil), Verifier: clientauth.NewVerifier(store)},
		Dedupe:  dedupe.Checker{Redis: store},
		Orgs:    lookups.NewOrganizationStore(store, nil),
		Bots:    detector,
		Logger:  logger,
		Metrics: NewMetrics(),
		Pipeline: &track.Pipeline{
			UA: parser, Bots: detector, Geo: noGeo{}, Salts: &lookups.SaltStore{Redis: store},
			Sessions: identity.RedisSessions{Client: store}, TimeoutMs: 30 * 60 * 1000,
			Producer: producer, Logger: logger, NewUUID: func() string { return "uuid" },
		},
	}
	return harness{handler: srv.Handler(), producer: producer, upstream: &calls}
}

func (h harness) do(method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func browser(extra map[string]string) map[string]string {
	h := map[string]string{
		"content-type": "application/json", "openpanel-client-id": testClientID, "origin": "https://ok.example",
		"user-agent": chromeUA, "x-client-ip": "81.2.69.142", "sec-ch-ua": "x", "sec-fetch-mode": "cors", "accept-language": "en",
	}
	for k, v := range extra {
		h[k] = v
	}
	return h
}

func TestTrackProducesTheEvent(t *testing.T) {
	h := newHarness(t)
	rec := h.do("POST", "/track", `{"type":"track","payload":{"name":"page_view","properties":{"__path":"/a","b":1,"a":2},"profileId":7}}`, browser(nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var answer struct{ DeviceID, SessionID string }
	if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil || answer.DeviceID == "" || answer.SessionID == "" {
		t.Fatalf("answer %s", rec.Body)
	}
	if got := rec.Header().Get("Content-Type"); got != contentTypeJSON {
		t.Errorf("content type %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://ok.example" {
		t.Errorf("allow origin %q", got)
	}
	if len(h.producer.messages) != 1 {
		t.Fatalf("%d messages", len(h.producer.messages))
	}
	m := h.producer.messages[0]
	if m.key != answer.DeviceID {
		t.Errorf("key %q, want the device id", m.key)
	}
	want := `"event":{"name":"page_view","properties":{"__path":"/a","b":1,"a":2},"profileId":"7","groups":[],`
	if !strings.Contains(m.value, want) {
		t.Errorf("value %s\nmissing %s", m.value, want)
	}
	if !strings.Contains(m.value, `"uaInfo":{"os":"macOS","osVersion":"10.15.7","browser":"Chrome","browserVersion":"128.0.0.0","device":"desktop","brand":"Apple","model":"Macintosh","isServer":false}`) {
		t.Errorf("uaInfo in %s", m.value)
	}
	if len(*h.upstream) != 0 {
		t.Errorf("forwarded %d requests", len(*h.upstream))
	}
}

func TestRepeatedRequestIsADuplicate(t *testing.T) {
	h := newHarness(t)
	body := `{"type":"track","payload":{"name":"twice"}}`
	first := h.do("POST", "/track", body, browser(nil))
	second := h.do("POST", "/track", body, browser(nil))
	if first.Code != http.StatusOK || second.Body.String() != `{"duplicate":true}` {
		t.Fatalf("first %d %s, second %d %s", first.Code, first.Body, second.Code, second.Body)
	}
	if len(h.producer.messages) != 1 {
		t.Errorf("%d messages", len(h.producer.messages))
	}
}

func TestRefusalsAreText401(t *testing.T) {
	h := newHarness(t)
	cases := map[string]struct {
		headers map[string]string
		want    string
	}{
		"wrong origin":     {browser(map[string]string{"origin": "https://evil.example"}), "Ingestion: Invalid cors or secret"},
		"no client":        {browser(map[string]string{"openpanel-client-id": ""}), "Ingestion: Missing client id"},
		"malformed client": {browser(map[string]string{"openpanel-client-id": "nope"}), "Ingestion: Client ID must be a valid UUIDv4"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			rec := h.do("POST", "/track", `{"type":"track","payload":{"name":"`+name+`"}}`, c.headers)
			if rec.Code != http.StatusUnauthorized || rec.Body.String() != c.want || rec.Header().Get("Content-Type") != contentTypeText {
				t.Errorf("got %d %q %q", rec.Code, rec.Body, rec.Header().Get("Content-Type"))
			}
		})
	}
	if rec := h.do("POST", "/track", `{"type":"track","payload":{"name":"secret"}}`, browser(map[string]string{"origin": "https://evil.example", "openpanel-client-secret": testSecret})); rec.Code != http.StatusOK {
		t.Errorf("a verified secret overrides cors: %d %s", rec.Code, rec.Body)
	}
}

func TestForwardsWhatItDoesNotModel(t *testing.T) {
	cases := map[string]struct {
		method, path, body string
		headers            map[string]string
	}{
		"identify":     {"POST", "/track", `{"type":"identify","payload":{"profileId":"p"}}`, browser(nil)},
		"__identify":   {"POST", "/track", `{"type":"track","payload":{"name":"x","properties":{"__identify":{"profileId":"p"}}}}`, browser(nil)},
		"invalid":      {"POST", "/track", `{"type":"track","payload":{"name":""}}`, browser(nil)},
		"bad json":     {"POST", "/track", `{"type":`, browser(nil)},
		"text body":    {"POST", "/track", `{"type":"track","payload":{"name":"x"}}`, browser(map[string]string{"content-type": "text/plain"})},
		"bot":          {"POST", "/track", `{"type":"track","payload":{"name":"x"}}`, browser(map[string]string{"user-agent": "Googlebot/2.1 (+http://www.google.com/bot.html)"})},
		"local time":   {"POST", "/track", `{"type":"track","payload":{"name":"x","properties":{"__timestamp":"2026-11-20 10:00:00"}}}`, browser(nil)},
		"odd origin":   {"POST", "/track", `{"type":"track","payload":{"name":"x"}}`, browser(map[string]string{"origin": "chrome-extension://abc"})},
		"other route":  {"GET", "/healthcheck", "", nil},
		"preflight":    {"OPTIONS", "/track", "", map[string]string{"origin": "https://ok.example", "access-control-request-method": "POST"}},
		"legacy array": {"POST", "/event", `[1]`, browser(nil)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			rec := h.do(c.method, c.path, c.body, c.headers)
			if rec.Code != http.StatusTeapot || rec.Body.String() != "from bun" {
				t.Fatalf("got %d %q, want the upstream's answer", rec.Code, rec.Body)
			}
			calls := *h.upstream
			if len(calls) != 1 || calls[0].body != c.body || calls[0].path != c.path {
				t.Fatalf("upstream saw %+v", calls)
			}
			if calls[0].header.Get("X-Forwarded-For") != "" || calls[0].header.Get("Accept-Encoding") != "" {
				t.Errorf("proxy added headers: %v", calls[0].header)
			}
			if len(h.producer.messages) != 0 {
				t.Errorf("produced %d", len(h.producer.messages))
			}
		})
	}
}

func TestDeviceID(t *testing.T) {
	h := newHarness(t)
	headers := browser(nil)
	delete(headers, "content-type")
	rec := h.do("GET", "/track/device-id", "", headers)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"sessionId":"","message":"No session exists for this device id"`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
	delete(headers, "user-agent")
	headers["openpanel-client-secret"] = testSecret
	headers["origin"] = "https://other.example"
	if rec := h.do("GET", "/track/device-id", "", headers); rec.Code != http.StatusBadRequest || rec.Body.String() != "Missing header: user-agent" {
		t.Errorf("missing ua: %d %s", rec.Code, rec.Body)
	}
}

func TestLegacyEvent(t *testing.T) {
	h := newHarness(t)
	rec := h.do("POST", "/event", `{"name":"legacy","profileId":42,"properties":{"a":1}}`, browser(nil))
	if rec.Code != http.StatusAccepted || rec.Body.String() != "ok" {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
	if len(h.producer.messages) != 1 || !strings.Contains(h.producer.messages[0].value, `"event":{"name":"legacy","profileId":"42","properties":{"a":1},"groups":[],`) {
		t.Fatalf("messages %+v", h.producer.messages)
	}
	if rec := h.do("POST", "/event", `{"name":"  "}`, browser(map[string]string{"x-client-ip": "8.8.8.8"})); rec.Code != http.StatusAccepted || len(h.producer.messages) != 1 {
		t.Errorf("a nameless event is accepted and dropped: %d, %d messages", rec.Code, len(h.producer.messages))
	}
}

func TestOversizedBodyAfterAuth(t *testing.T) {
	h := newHarness(t)
	body := `{"type":"track","payload":{"name":"big","properties":{"blob":"` + strings.Repeat("x", 1<<20) + `"}}}`
	rec := h.do("POST", "/track", body, browser(nil))
	if rec.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rec.Body.String(), "the limit is 1048576.") {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}
