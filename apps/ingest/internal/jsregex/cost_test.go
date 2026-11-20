package jsregex

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
)

func TestPatternCost(t *testing.T) {
	path := os.Getenv("INGEST_REGEX_CORPUS")
	uasPath := os.Getenv("INGEST_BENCH_UAS")
	if path == "" || uasPath == "" {
		t.Skip()
	}
	data, _ := os.ReadFile(path)
	var corpus struct {
		Patterns []struct{ Re, Flags string } `json:"patterns"`
	}
	json.Unmarshal(data, &corpus)
	data, _ = os.ReadFile(uasPath)
	var uas []string
	json.Unmarshal(data, &uas)
	type cost struct {
		src  string
		mode string
		d    time.Duration
	}
	var costs []cost
	var total time.Duration
	for _, p := range corpus.Patterns {
		re, _ := Compile(p.Re, p.Flags)
		start := time.Now()
		for _, u := range uas {
			re.Exec(u)
		}
		d := time.Since(start)
		total += d
		mode := "slow"
		if re.fast != nil {
			mode = "fast"
		} else if re.prefilter != nil {
			mode = "prefilter"
		}
		costs = append(costs, cost{p.Re, mode, d})
	}
	sort.Slice(costs, func(i, j int) bool { return costs[i].d > costs[j].d })
	fmt.Printf("total %v per ua %v\n", total, total/time.Duration(len(uas)))
	for _, c := range costs[:20] {
		s := c.src
		if len(s) > 110 {
			s = s[:110]
		}
		fmt.Printf("%8v %-9s %s\n", c.d/time.Duration(len(uas)), c.mode, s)
	}
}
