package wire

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func readGolden[T any](t *testing.T, name string) []T {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var cases []T
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

// TestDecodeAcceptsWhatJSONParseAccepts compares with JSON.parse in the real runtime.
func TestDecodeAcceptsWhatJSONParseAccepts(t *testing.T) {
	type golden struct {
		Text string `json:"text"`
		OK   bool   `json:"ok"`
	}
	for _, c := range readGolden[golden](t, "jsonparse.json") {
		_, err := Decode([]byte(c.Text))
		if (err == nil) != c.OK {
			t.Errorf("%q: Decode error = %v, JSON.parse ok = %v", c.Text, err, c.OK)
		}
	}
}

type fixedIDs struct{}

func (fixedIDs) NewID(length int) string { return strings.Repeat("F", length) }

func TestResolveRequestIDMatchesTheTypeScriptHandler(t *testing.T) {
	type golden struct {
		JSON  string  `json:"json"`
		OK    bool    `json:"ok"`
		Fresh bool    `json:"fresh"`
		ID    *string `json:"id"`
	}
	for _, c := range readGolden[golden](t, "requestid.json") {
		got, err := ResolveRequestID(json.RawMessage(c.JSON), fixedIDs{})
		if (err == nil) != c.OK {
			t.Errorf("%s: error = %v, want ok = %v", c.JSON, err, c.OK)
			continue
		}
		switch {
		case !c.OK:
		case c.Fresh:
			if got != strings.Repeat("F", RequestIDLength) {
				t.Errorf("%s: got %q, want a fresh id", c.JSON, got)
			}
		default:
			if got != *c.ID {
				t.Errorf("%s: got %q, want %q", c.JSON, got, *c.ID)
			}
		}
	}
}

func TestResolveRequestIDAbsentHeaderGetsAFreshID(t *testing.T) {
	got, err := ResolveRequestID(nil, fixedIDs{})
	if err != nil || got != strings.Repeat("F", RequestIDLength) {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestRandomIDsHaveTheRequestedLengthAndAlphabet(t *testing.T) {
	id := RandomIDs{}.NewID(RequestIDLength)
	if len(id) != RequestIDLength {
		t.Fatalf("id = %q", id)
	}
	for i := range id {
		if !isRequestIDByte(id[i]) {
			t.Fatalf("id = %q", id)
		}
	}
}

const fullPayload = `{"id":"e1","projectId":"p","event":{"name":"screen_view","properties":{"b":1,"a":[1.50,"x"]},"profileId":42,"groups":null,"timestamp":1700000000000,"isTimestampFromThePast":true},"uaInfo":{"isServer":false,"os":"Mac","browser":"Chrome"},"geo":{"country":"SE","longitude":12.50},"headers":{"request-id":"r1","openpanel-sdk-name":"web"},"deviceId":"d","sessionId":"s"}`

func TestDecodeKeepsRawJSONForTheFieldsTheHandlerPassesOn(t *testing.T) {
	d, err := Decode([]byte(fullPayload))
	if err != nil || d.ShapeErr != nil {
		t.Fatalf("err = %v, shape = %v", err, d.ShapeErr)
	}
	p := d.Payload
	if p.ID == nil || *p.ID != "e1" || p.ProjectID != "p" || p.DeviceID != "d" || p.SessionID != "s" || p.Event.Name != "screen_view" {
		t.Fatalf("payload = %+v", p)
	}
	if string(p.Event.Properties) != `{"b":1,"a":[1.50,"x"]}` || string(p.Event.ProfileID) != "42" || string(p.Event.Groups) != "null" || string(p.Event.Timestamp) != "1700000000000" {
		t.Fatalf("raw fields = %s %s %s %s", p.Event.Properties, p.Event.ProfileID, p.Event.Groups, p.Event.Timestamp)
	}
	if !p.Event.IsTimestampFromThePast || p.UAInfo.IsServer || *p.UAInfo.OS != "Mac" || p.UAInfo.Device != nil {
		t.Fatalf("event flags / uaInfo = %+v %+v", p.Event, p.UAInfo)
	}
	if *p.Geo.Country != "SE" || *p.Geo.Longitude != 12.5 || p.Geo.Latitude != nil {
		t.Fatalf("geo = %+v", p.Geo)
	}
	headers, ok := p.HeaderMap()
	if !ok || string(headers["request-id"]) != `"r1"` {
		t.Fatalf("headers = %v, %v", headers, ok)
	}
	if p.AI != nil {
		t.Fatalf("ai = %s", p.AI)
	}
}

func TestDecodeKeysAreCaseSensitiveAndTheLastDuplicateWins(t *testing.T) {
	d, _ := Decode([]byte(`{"ProjectId":"upper","projectId":"first","projectId":"last"}`))
	if d.Payload.ProjectID != "last" {
		t.Fatalf("projectId = %q", d.Payload.ProjectID)
	}
	d, _ = Decode([]byte(`{"ProjectId":"upper"}`))
	if d.Payload.ProjectID != "" {
		t.Fatalf("a differently cased key must not match: %q", d.Payload.ProjectID)
	}
}

func TestDecodeAbsentVersusNullRawFields(t *testing.T) {
	d, _ := Decode([]byte(`{"event":{"name":"n","properties":null}}`))
	if string(d.Payload.Event.Properties) != "null" || d.Payload.Event.Groups != nil {
		t.Fatalf("properties = %q, groups = %q", d.Payload.Event.Properties, d.Payload.Event.Groups)
	}
	if _, ok := d.Payload.HeaderMap(); ok {
		t.Fatal("a payload without headers has no header map")
	}
}

func TestDecodeValuesThatAreNotObjectsCarryAShapeError(t *testing.T) {
	for _, text := range []string{"null", "5", `"x"`, "[1]", " true "} {
		d, err := Decode([]byte(text))
		if err != nil || d.ShapeErr != ErrNotObject {
			t.Errorf("%s: err = %v, shape = %v", text, err, d.ShapeErr)
		}
	}
}

func TestDecodeAFieldOfTheWrongTypeCarriesAShapeError(t *testing.T) {
	for _, text := range []string{`{"projectId":5}`, `{"event":"x"}`, `{"uaInfo":{"isServer":"yes"}}`, `{"geo":{"longitude":"x"}}`} {
		d, err := Decode([]byte(text))
		if err != nil || d.ShapeErr == nil {
			t.Errorf("%s: err = %v, shape = %v", text, err, d.ShapeErr)
		}
	}
}

func TestDecodeSyntaxErrorsAreParseErrors(t *testing.T) {
	if _, err := Decode([]byte(`{"bad":`)); err == nil {
		t.Fatal("expected a syntax error")
	}
}
