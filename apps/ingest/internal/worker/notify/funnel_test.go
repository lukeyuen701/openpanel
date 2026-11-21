package notify

import (
	"context"
	"errors"
	"strings"
	"testing"

	"openpanel/ingest/internal/jsjson"
)

type staticRules []Rule

func (s staticRules) ByProject(context.Context, string) ([]Rule, error) { return s, nil }

func parseValue(t *testing.T, text string) *jsjson.Value {
	t.Helper()
	value, err := jsjson.Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sessionEvent(t *testing.T, name string, at float64, extra string) SessionEvent {
	t.Helper()
	text := `{"name":"` + name + `","projectId":"p1"` + extra + `}`
	return SessionEvent{Value: parseValue(t, text), CreatedAtMs: at}
}

func funnelDispatcher(rules []Rule, recorder *recorder) *Dispatcher {
	return &Dispatcher{
		Matcher: NewMatcher(nil), Projects: fixedName("Acme"), Rules: staticRules(rules),
		DB: recorder, Queue: recorder, Clock: fixedClock{},
	}
}

func TestCheckSessionEndCompletesAFunnelInOrder(t *testing.T) {
	recorder := &recorder{}
	rule := Rule{
		ID: "r1", Name: "Checkout", SendToEmail: true,
		Config: parseValue(t, `{"type":"funnel","events":[{"name":"screen_view","filters":[]},{"name":"purchase","filters":[]}]}`),
	}
	dispatcher := funnelDispatcher([]Rule{rule}, recorder)
	// Handed over newest first, as the session job builds them.
	events := []SessionEvent{
		sessionEvent(t, "session_end", 40, ""),
		sessionEvent(t, "purchase", 30, ""),
		sessionEvent(t, "screen_view", 10, ""),
	}
	if err := dispatcher.CheckSessionEnd(context.Background(), "req1", events); err != nil {
		t.Fatal(err)
	}
	if len(recorder.effects) != 1 {
		t.Fatalf("effects = %+v", recorder.effects)
	}
	data := recorder.effects[0].data
	for _, want := range []string{`"title":"Funnel \"Checkout\" completed"`, `"type":"funnel"`, `"message":"Project: Acme"`, `"sendToEmail":true`} {
		if !strings.Contains(data, want) {
			t.Errorf("job data %s lacks %s", data, want)
		}
	}
	if strings.Index(data, `"screen_view"`) > strings.Index(data, `"purchase"`) {
		t.Errorf("matched events are not oldest first: %s", data)
	}
}

func TestCheckSessionEndIncompleteFunnelNotifiesNothing(t *testing.T) {
	recorder := &recorder{}
	rule := Rule{ID: "r1", Name: "Checkout", SendToApp: true, Config: parseValue(t, `{"type":"funnel","events":[{"name":"purchase","filters":[]},{"name":"screen_view","filters":[]}]}`)}
	events := []SessionEvent{sessionEvent(t, "purchase", 30, ""), sessionEvent(t, "screen_view", 10, "")}
	if err := funnelDispatcher([]Rule{rule}, recorder).CheckSessionEnd(context.Background(), "req1", events); err != nil {
		t.Fatal(err)
	}
	if len(recorder.effects) != 0 {
		t.Fatalf("effects = %+v", recorder.effects)
	}
}

func TestCheckSessionEndTemplateAndOrderOfDestinations(t *testing.T) {
	recorder := &recorder{}
	rule := Rule{
		ID: "r1", Name: "R", SendToApp: true, SendToEmail: true, IntegrationIDs: []string{"i1"}, Template: "$RULE_NAME: $EVENT_NAME",
		Config: parseValue(t, `{"type":"funnel","events":[{"name":"*","filters":[]},{"name":"b","filters":[]}]}`),
	}
	events := []SessionEvent{sessionEvent(t, "b", 2, ""), sessionEvent(t, "a", 1, "")}
	if err := funnelDispatcher([]Rule{rule}, recorder).CheckSessionEnd(context.Background(), "req1", events); err != nil {
		t.Fatal(err)
	}
	ops := make([]string, len(recorder.effects))
	for i, e := range recorder.effects {
		ops[i] = e.op
	}
	if got := strings.Join(ops, ","); got != "add,add,insert,add" {
		t.Fatalf("effects = %s (integration and email first, then the dashboard row and its job)", got)
	}
	if !strings.Contains(recorder.effects[0].data, `"title":"R: a -> b"`) {
		t.Errorf("title = %s", recorder.effects[0].data)
	}
}

func TestCheckSessionEndThrowingFilterKeepsEarlierRulesAndReports(t *testing.T) {
	recorder := &recorder{}
	good := Rule{ID: "r1", Name: "Good", SendToEmail: true, Config: parseValue(t, `{"type":"funnel","events":[{"name":"a","filters":[]}]}`)}
	// is_bounce is a boolean on the synthetic screen_view: trim() throws.
	bad := Rule{ID: "r2", Name: "Bad", SendToEmail: true, Config: parseValue(t, `{"type":"funnel","events":[{"name":"a","filters":[{"name":"properties.is_bounce","operator":"is","value":["x"]}]}]}`)}
	after := Rule{ID: "r3", Name: "After", SendToEmail: true, Config: parseValue(t, `{"type":"funnel","events":[{"name":"a","filters":[]}]}`)}
	events := []SessionEvent{sessionEvent(t, "a", 1, `,"properties":{"is_bounce":false}`)}
	err := funnelDispatcher([]Rule{good, bad, after}, recorder).CheckSessionEnd(context.Background(), "req1", events)
	if !errors.Is(err, ErrTypeError) {
		t.Fatalf("err = %v", err)
	}
	if len(recorder.effects) != 1 || !strings.Contains(recorder.effects[0].data, "Good") {
		t.Fatalf("effects = %+v", recorder.effects)
	}
}

func TestHasFunnelRules(t *testing.T) {
	funnel := Rule{Config: parseValue(t, `{"type":"funnel","events":[]}`)}
	events := Rule{Config: parseValue(t, `{"type":"events","events":[]}`)}
	if has, err := HasFunnelRules([]Rule{events, funnel}); err != nil || !has {
		t.Fatalf("has = %v, %v", has, err)
	}
	if has, err := HasFunnelRules([]Rule{events}); err != nil || has {
		t.Fatalf("has = %v, %v", has, err)
	}
	if _, err := HasFunnelRules([]Rule{{}}); !errors.Is(err, ErrTypeError) {
		t.Fatalf("err = %v", err)
	}
}
