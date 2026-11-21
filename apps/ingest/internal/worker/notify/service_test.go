package notify

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"openpanel/ingest/internal/jsjson"
)

// These port notification.service.test.ts and filter-regex.test.ts.

func event(t *testing.T, raw string) *jsjson.Value {
	t.Helper()
	return mustParse(t, []byte(raw))
}

func TestMatchEventName(t *testing.T) {
	m := NewMatcher(nil)
	click := event(t, `{"name":"click","filters":[]}`)
	wildcard := event(t, `{"name":"*","filters":[]}`)
	tests := []struct {
		name    string
		payload string
		def     *jsjson.Value
		want    bool
	}{
		{"exact name", `{"name":"click"}`, click, true},
		{"other name", `{"name":"purchase"}`, click, false},
		{"wildcard", `{"name":"anything"}`, wildcard, true},
	}
	for _, tt := range tests {
		got, err := m.MatchEvent(event(t, tt.payload), tt.def)
		if err != nil || got != tt.want {
			t.Errorf("%s: %v %v", tt.name, got, err)
		}
	}
}

func TestMatchEventFilters(t *testing.T) {
	m := NewMatcher(nil)
	filters := func(raw string) []*jsjson.Value { return event(t, raw).Arr }
	tests := []struct {
		name    string
		payload string
		filters string
		want    bool
	}{
		{"an empty value list always matches", `{}`, `[{"name":"country","operator":"is","value":[]}]`, true},
		{"is matches", `{"country":"SE"}`, `[{"name":"country","operator":"is","value":["SE"]}]`, true},
		{"is does not match", `{"country":"SE"}`, `[{"name":"country","operator":"is","value":["US"]}]`, false},
		{"regex with slashes stripped", `{"path":"/product/42"}`, `[{"name":"path","operator":"regex","value":["/^\\/product\\/\\d+$/"]}]`, true},
		{"regex that does not match", `{"path":"/product/42"}`, `[{"name":"path","operator":"regex","value":["^/cart"]}]`, false},
		{"has_profile true, identified", `{"profileId":"p1","deviceId":"d1"}`, `[{"name":"has_profile","operator":"is","value":["true"]}]`, true},
		{"has_profile true, anonymous", `{"profileId":"d1","deviceId":"d1"}`, `[{"name":"has_profile","operator":"is","value":["true"]}]`, false},
		{"has_profile false, anonymous", `{"profileId":"d1","deviceId":"d1"}`, `[{"name":"has_profile","operator":"is","value":["false"]}]`, true},
	}
	for _, tt := range tests {
		got, err := m.MatchEventFilters(event(t, tt.payload), filters(tt.filters))
		if err != nil || got != tt.want {
			t.Errorf("%s: %v %v", tt.name, got, err)
		}
	}
}

func TestUncompilablePatternNeverMatchesAndWarnsOnce(t *testing.T) {
	var logs bytes.Buffer
	m := NewMatcher(slog.New(slog.NewTextHandler(&logs, nil)))
	payload := event(t, `{"path":"/product/42"}`)
	lookahead := []*jsjson.Value{event(t, `{"name":"path","operator":"regex","value":["(?=/product)/p"]}`)}
	for range 2 {
		if got, err := m.MatchEventFilters(payload, lookahead); err != nil || got {
			t.Fatalf("a pattern RE2 cannot compile must not match: %v %v", got, err)
		}
	}
	if warnings := strings.Count(logs.String(), "level=WARN"); warnings != 1 {
		t.Errorf("%d warnings, want exactly one:\n%s", warnings, logs.String())
	}
}

func TestBacktrackingPatternStaysFast(t *testing.T) {
	m := NewMatcher(nil)
	payload := jsjson.NewObj()
	payload.Set("name", jsjson.NewString(strings.Repeat("a", 50_000)+"!"))
	filter := event(t, `{"name":"name","operator":"regex","value":["(a+)+$"]}`)
	started := time.Now()
	got, err := m.MatchEventFilters(jsjson.NewObject(payload), []*jsjson.Value{filter})
	if err != nil || got {
		t.Fatalf("%v %v", got, err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Errorf("took %v", elapsed)
	}
}

func TestPatternsRE2RejectsNeverMatch(t *testing.T) {
	m := NewMatcher(nil)
	for _, pattern := range []string{"(?=foo)bar", "(a)\\1", "((a{100}){100}){100}", "[a-"} {
		if m.TestFilterPattern(pattern, strings.Repeat("a", 10)+"foobar") {
			t.Errorf("%q matched", pattern)
		}
	}
	if !m.TestFilterPattern("product", "/shop/product/1") || m.TestFilterPattern("^product", "/shop/product/1") {
		t.Error("a pattern matches anywhere in the input, like RegExp.test")
	}
}

func TestTemplateEvent(t *testing.T) {
	payload := event(t, `{"name":"click","properties":{"country":"SE"}}`)
	if got := TemplateEvent(payload, Rule{Name: "Rule"}); got != `You received a new "click" event` {
		t.Errorf("default title %q", got)
	}
	rule := Rule{Name: "My Rule", Template: "$RULE_NAME saw $EVENT_NAME from {{properties.country}}"}
	if got := TemplateEvent(payload, rule); got != "My Rule saw click from SE" {
		t.Errorf("title %q", got)
	}
}
