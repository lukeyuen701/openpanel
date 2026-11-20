package clientauth

import (
	"encoding/json"
	"os"
	"testing"
)

func TestOriginMatchesTypeScript(t *testing.T) {
	data, err := os.ReadFile("testdata/origins.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Cors   []string
		Origin string
		Out    bool
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	skipped := 0
	for _, c := range cases {
		hasOrigin := c.Origin != "__undefined__"
		origin := c.Origin
		if !hasOrigin {
			origin = ""
		}
		host, modelled := "", true
		if hasOrigin && origin != "" {
			host, modelled = OriginHost(origin)
		}
		if !modelled {
			skipped++
			continue
		}
		if got := IsOriginAllowed(c.Cors, origin, hasOrigin, host); got != c.Out {
			t.Errorf("cors %v origin %q: got %v want %v", c.Cors, c.Origin, got, c.Out)
		}
	}
	t.Logf("%d of %d cases forwarded", skipped, len(cases))
}
