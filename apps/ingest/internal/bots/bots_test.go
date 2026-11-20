package bots

import (
	"encoding/json"
	"os"
	"testing"
)

func goldenPath() string {
	if path := os.Getenv("INGEST_GOLDEN_UA"); path != "" {
		return path
	}
	return "../useragent/testdata/ua-sample.json"
}

func TestDetectMatchesTypeScript(t *testing.T) {
	data, err := os.ReadFile(goldenPath())
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var cases []struct {
		UA  string `json:"ua"`
		Bot *Match `json:"bot"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	detector, err := New()
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	failures := 0
	for _, c := range cases {
		got := detector.Detect(c.UA)
		same := (got == nil && c.Bot == nil) || (got != nil && c.Bot != nil && *got == *c.Bot)
		if !same {
			failures++
			if failures <= 20 {
				t.Errorf("ua %q: got %+v want %+v", c.UA, got, c.Bot)
			}
		}
	}
	if failures > 0 {
		t.Errorf("%d of %d differ", failures, len(cases))
	}
}
