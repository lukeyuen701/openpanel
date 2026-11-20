package useragent

import (
	"encoding/json"
	"os"
	"testing"

	"openpanel/ingest/internal/jsjson"
)

type goldenCase struct {
	UA        string          `json:"ua"`
	Overrides json.RawMessage `json:"overrides"`
	Out       json.RawMessage `json:"out"`
}

// goldenPath is the committed sample; INGEST_GOLDEN_UA points at the full
// corpus parity/golden-ua.ts writes.
func goldenPath() string {
	if path := os.Getenv("INGEST_GOLDEN_UA"); path != "" {
		return path
	}
	return "testdata/ua-sample.json"
}

func TestParseMatchesTypeScript(t *testing.T) {
	data, err := os.ReadFile(goldenPath())
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	parser, err := New()
	if err != nil {
		t.Fatalf("new parser: %v", err)
	}
	failures := 0
	for _, c := range cases {
		var overrides Overrides
		if string(c.Overrides) != "null" {
			value, err := jsjson.Parse(c.Overrides)
			if err != nil {
				t.Fatalf("overrides: %v", err)
			}
			overrides = PropertyOverrides{Properties: value.Obj}
		}
		want, err := jsjson.Parse(c.Out)
		if err != nil {
			t.Fatalf("out: %v", err)
		}
		got := string(jsjson.Stringify(parser.Parse(c.UA, overrides).JSON()))
		if got != string(jsjson.Stringify(want)) {
			failures++
			if failures <= 25 {
				t.Errorf("ua %q\n got %s\nwant %s", c.UA, got, jsjson.Stringify(want))
			}
		}
	}
	if failures > 0 {
		t.Errorf("%d of %d cases differ", failures, len(cases))
	}
}
