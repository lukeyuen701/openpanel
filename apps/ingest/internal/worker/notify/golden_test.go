package notify

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"openpanel/ingest/internal/jsjson"
)

func readGolden(t *testing.T, name string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if errors.Is(err, os.ErrNotExist) {
		raw = readGzipped(t, filepath.Join("testdata", name+".gz"))
	} else if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

// readGzipped reads a golden written by parity/golden-notify.ts; the small
// hand-kept ones stay plain JSON.
func readGzipped(t *testing.T, path string) []byte {
	t.Helper()
	compressed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return raw
}

func mustParse(t *testing.T, raw json.RawMessage) *jsjson.Value {
	t.Helper()
	value, err := jsjson.Parse(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}
	return value
}

// outcome is the golden of a call that may throw: {ok: v} or {throws: kind}.
type outcome struct {
	OK     json.RawMessage `json:"ok"`
	Throws string          `json:"throws"`
}

func (o outcome) throws() bool { return o.Throws != "" }

type payloadsFile struct {
	Payloads []json.RawMessage `json:"payloads"`
}

func TestMatchEventGolden(t *testing.T) {
	var golden struct {
		payloadsFile
		Cases []struct {
			Payload int             `json:"payload"`
			Event   json.RawMessage `json:"event"`
			Result  outcome         `json:"result"`
		} `json:"cases"`
	}
	readGolden(t, "match.json", &golden)
	matcher := NewMatcher(nil)
	var failures int
	for i, c := range golden.Cases {
		payload := mustParse(t, golden.Payloads[c.Payload])
		got, err := matcher.MatchEvent(payload, mustParse(t, c.Event))
		switch {
		case c.Result.throws() && !errors.Is(err, ErrTypeError):
			t.Errorf("case %d %s: want TypeError, got %v, %v", i, c.Event, got, err)
		case !c.Result.throws() && err != nil:
			t.Errorf("case %d %s: unexpected %v", i, c.Event, err)
		case !c.Result.throws() && string(c.Result.OK) != jsonBool(got):
			t.Errorf("case %d %s on %s: want %s, got %v", i, c.Event, golden.Payloads[c.Payload], c.Result.OK, got)
		default:
			continue
		}
		if failures++; failures > 20 {
			t.Fatal("too many failures")
		}
	}
	if len(golden.Cases) < 1000 {
		t.Fatalf("only %d golden cases", len(golden.Cases))
	}
}

func jsonBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func TestRegexGolden(t *testing.T) {
	var golden []struct {
		Pattern string  `json:"pattern"`
		Input   string  `json:"input"`
		Result  outcome `json:"result"`
	}
	readGolden(t, "regex.json", &golden)
	var known []struct {
		Pattern string `json:"pattern"`
		Reason  string `json:"reason"`
	}
	readGolden(t, "regex-divergences.json", &known)
	allowed := map[string]string{}
	for _, k := range known {
		allowed[k.Pattern] = k.Reason
	}

	matcher := NewMatcher(nil)
	diverged := map[string]bool{}
	for _, c := range golden {
		got := matcher.TestFilterPattern(stripSlashes(c.Pattern), c.Input)
		if string(c.Result.OK) == jsonBool(got) {
			continue
		}
		diverged[c.Pattern] = true
		if _, ok := allowed[c.Pattern]; !ok {
			t.Errorf("pattern %q on %q: RE2J says %s, Go says %v (not in regex-divergences.json)", c.Pattern, c.Input, c.Result.OK, got)
		}
	}
	for pattern, reason := range allowed {
		if !diverged[pattern] {
			t.Errorf("regex-divergences.json lists %q (%s) but Go now agrees: remove it", pattern, reason)
		}
	}
}

func stripSlashes(s string) string {
	start, end := 0, len(s)
	for start < end && s[start] == '/' {
		start++
	}
	for end > start && s[end-1] == '/' {
		end--
	}
	return s[start:end]
}

func TestTemplateEventGolden(t *testing.T) {
	var golden struct {
		payloadsFile
		Cases []struct {
			Template *string `json:"template"`
			RuleName string  `json:"ruleName"`
			Payload  int     `json:"payload"`
			Result   outcome `json:"result"`
		} `json:"cases"`
	}
	readGolden(t, "template.json", &golden)
	for i, c := range golden.Cases {
		rule := Rule{Name: c.RuleName}
		if c.Template != nil {
			rule.Template = *c.Template
		}
		got := TemplateEvent(mustParse(t, golden.Payloads[c.Payload]), rule)
		if c.Result.throws() {
			t.Fatalf("case %d: TypeScript throws %s; TemplateEvent has no error to match", i, c.Result.Throws)
		}
		var wantText string
		if err := json.Unmarshal(c.Result.OK, &wantText); err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if got != wantText {
			t.Errorf("case %d template %v rule %q:\n got %q\nwant %q", i, c.Template, c.RuleName, got, wantText)
		}
	}
}

func TestTransformProfileGolden(t *testing.T) {
	var golden []struct {
		Row    json.RawMessage `json:"row"`
		Result outcome         `json:"result"`
	}
	readGolden(t, "profile.json", &golden)
	for i, c := range golden {
		got, err := transformProfile(mustParse(t, c.Row))
		if c.Result.throws() {
			if !errors.Is(err, ErrTypeError) {
				t.Errorf("case %d: want TypeError, got %v", i, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("case %d: %v", i, err)
			continue
		}
		want := string(jsjson.Stringify(mustParse(t, c.Result.OK)))
		if have := string(jsjson.Stringify(got)); have != want {
			t.Errorf("case %d:\n got %s\nwant %s", i, have, want)
		}
	}
}

func TestCacheKeyGolden(t *testing.T) {
	var golden []struct {
		ID      string `json:"id"`
		Project string `json:"project"`
		Rules   string `json:"rules"`
	}
	readGolden(t, "keys.json", &golden)
	for _, c := range golden {
		if got := CacheKey("getProjectById", c.ID); got != c.Project {
			t.Errorf("project key of %q: got %q want %q", c.ID, got, c.Project)
		}
		if got := CacheKey(rulesCacheName, c.ID); got != c.Rules {
			t.Errorf("rules key of %q: got %q want %q", c.ID, got, c.Rules)
		}
	}
}
