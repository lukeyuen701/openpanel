// Package bots is the ingest bot check: the device-detector list that
// scripts/gen-tables.ts exports from packages/core's generated bots.ts.
package bots

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"

	"openpanel/ingest/internal/jsregex"
)

//go:embed data/bots.json
var botsJSON []byte

// cacheSize bounds the per-process verdict cache. The Bun API also shares
// verdicts through Redis; the check is a pure function of the user agent, so
// an in-process cache answers the same thing without the round trip.
const cacheSize = 10_000

// Match is the bot a user agent belongs to.
type Match struct {
	Name string
	Type string
}

type entry struct {
	includes string
	regexp   *jsregex.Regexp
	match    Match
}

// Detector holds the compiled list.
type Detector struct {
	includes []entry
	regexes  []entry
	cache    *lru.Cache[string, *Match]
}

// New compiles the embedded list.
func New() (*Detector, error) {
	var raw []struct {
		Includes string `json:"includes"`
		Regex    string `json:"regex"`
		Name     string `json:"name"`
		Category string `json:"category"`
	}
	if err := json.Unmarshal(botsJSON, &raw); err != nil {
		return nil, fmt.Errorf("bots: decode list: %w", err)
	}
	cache, err := lru.New[string, *Match](cacheSize)
	if err != nil {
		return nil, err
	}
	d := &Detector{cache: cache}
	for _, bot := range raw {
		match := Match{Name: bot.Name, Type: bot.Category}
		if bot.Regex != "" {
			re, err := jsregex.Compile(bot.Regex, "")
			if err != nil {
				return nil, err
			}
			d.regexes = append(d.regexes, entry{regexp: re, match: match})
			continue
		}
		d.includes = append(d.includes, entry{includes: bot.Includes, match: match})
	}
	return d, nil
}

// Detect is detectBot: plain substrings first, then the regexes, first hit
// wins. nil means not a bot.
func (d *Detector) Detect(ua string) *Match {
	if cached, ok := d.cache.Get(ua); ok {
		return cached
	}
	match := d.detect(ua)
	d.cache.Add(ua, match)
	return match
}

func (d *Detector) detect(ua string) *Match {
	for i := range d.includes {
		if strings.Contains(ua, d.includes[i].includes) {
			return &d.includes[i].match
		}
	}
	for i := range d.regexes {
		if d.regexes[i].regexp.Test(ua) {
			return &d.regexes[i].match
		}
	}
	return nil
}
