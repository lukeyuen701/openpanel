package jsregex

import (
	"encoding/json"
	"os"
	"testing"
)

// TestEnginesAgree runs every table pattern over a UA corpus under both
// engines. INGEST_REGEX_CORPUS names a JSON array of patterns and inputs:
// {"patterns":[{"re","flags"}],"inputs":[...]}.
func TestEnginesAgree(t *testing.T) {
	path := os.Getenv("INGEST_REGEX_CORPUS")
	if path == "" {
		t.Skip("INGEST_REGEX_CORPUS not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Patterns []struct{ Re, Flags string } `json:"patterns"`
		Inputs   []string                     `json:"inputs"`
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	fastCount := 0
	for _, p := range corpus.Patterns {
		re, err := Compile(p.Re, p.Flags)
		if err != nil {
			t.Fatal(err)
		}
		if re.fast == nil {
			continue
		}
		fastCount++
		for _, input := range corpus.Inputs {
			if !printableASCII(input) {
				continue
			}
			fast := re.fastExec(input)
			slowRe := &Regexp{source: re.source, global: re.global, re: re.re}
			slow := slowRe.Exec(input)
			if !sameGroups(fast, slow) {
				t.Errorf("/%s/%s on %q: fast %v slow %v", p.Re, p.Flags, input, deref(fast), deref(slow))
			}
		}
	}
	t.Logf("%d of %d patterns have a fast form", fastCount, len(corpus.Patterns))
}

func sameGroups(a, b []*string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if (a[i] == nil) != (b[i] == nil) {
			return false
		}
		if a[i] != nil && *a[i] != *b[i] {
			return false
		}
	}
	return true
}

func deref(groups []*string) []string {
	out := []string{}
	for _, g := range groups {
		if g == nil {
			out = append(out, "<undefined>")
			continue
		}
		out = append(out, *g)
	}
	return out
}
