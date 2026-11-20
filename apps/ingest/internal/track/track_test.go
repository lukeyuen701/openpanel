package track

import (
	"encoding/json"
	"os"
	"testing"

	"openpanel/ingest/internal/geo"
	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/reqheaders"
)

func readGolden(t *testing.T, name string, into any) {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if err := json.Unmarshal(data, into); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
}

func rawValue(t *testing.T, raw json.RawMessage) *jsjson.Value {
	t.Helper()
	if string(raw) == "\"__undefined__\"" {
		return nil
	}
	v, err := jsjson.Parse(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}
	return v
}

func TestTimestampMatchesTypeScript(t *testing.T) {
	var cases []struct {
		Now   int64
		Input json.RawMessage
		Out   struct {
			Timestamp              int64
			IsTimestampFromThePast bool
		}
	}
	readGolden(t, "timestamps.json", &cases)
	modelled := 0
	for _, c := range cases {
		got, ok := ResolveTimestamp(c.Now, rawValue(t, c.Input))
		if !ok {
			continue
		}
		modelled++
		if got.Ms != c.Out.Timestamp || got.FromPast != c.Out.IsTimestampFromThePast {
			t.Errorf("%s: got %+v want %+v", c.Input, got, c.Out)
		}
	}
	t.Logf("%d of %d inputs handled natively", modelled, len(cases))
}

func TestSchemaMatchesTypeScript(t *testing.T) {
	var cases []struct {
		Text    string
		Success bool
		Data    *string
	}
	readGolden(t, "schema.json", &cases)
	for _, c := range cases {
		body, err := jsjson.Parse([]byte(c.Text))
		if err != nil {
			t.Fatal(err)
		}
		got, ok := ParseTrackBody(body)
		if !ok {
			continue // forwarded: zod answers
		}
		if !c.Success {
			t.Errorf("%s: accepted, zod refuses", c.Text)
			continue
		}
		if out := string(jsjson.Stringify(got)); out != *c.Data {
			t.Errorf("%s:\n got %s\nwant %s", c.Text, out, *c.Data)
		}
	}
	accepted := 0
	for _, c := range cases {
		body, _ := jsjson.Parse([]byte(c.Text))
		if _, ok := ParseTrackBody(body); ok {
			accepted++
		}
	}
	t.Logf("%d of %d bodies handled natively", accepted, len(cases))
}

func TestBlockedNamesMatchTypeScript(t *testing.T) {
	var cases []struct {
		Name string
		Out  bool
	}
	readGolden(t, "blocked.json", &cases)
	for _, c := range cases {
		if got := IsBlockedEventName(c.Name); got != c.Out {
			t.Errorf("%q: got %v want %v", c.Name, got, c.Out)
		}
	}
}

func TestSuspicionMatchesTypeScript(t *testing.T) {
	var cases []struct {
		ASNInfo struct {
			Asn       *uint32
			IsHosting bool
		} `json:"asnInfo"`
		Headers          map[string]string
		Props            json.RawMessage
		ClientSecretAuth bool
		IsServer         bool
		Out              string
	}
	readGolden(t, "suspicion.json", &cases)
	for _, c := range cases {
		var props *jsjson.Obj
		if string(c.Props) != "null" {
			props = rawValue(t, c.Props).Obj
		}
		out := ApplyBotSuspicion(props, geo.ASN{Number: c.ASNInfo.Asn, IsHosting: c.ASNInfo.IsHosting}, reqheaders.FromMap(c.Headers), c.ClientSecretAuth, c.IsServer)
		got := "__undefined__"
		if out != nil {
			got = string(jsjson.Stringify(jsjson.NewObject(out)))
		}
		if got != c.Out {
			t.Errorf("%+v: got %s want %s", c, got, c.Out)
		}
	}
}
