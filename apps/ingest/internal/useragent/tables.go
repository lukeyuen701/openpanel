package useragent

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"openpanel/ingest/internal/jsregex"
)

// regexesJSON is ua-parser-js's default regex map, exported by
// scripts/gen-tables.ts from the copy @openpanel/shared resolves.
//
//go:embed data/regexes.json
var regexesJSON []byte

// encoded mirrors what gen-tables.ts writes for one value.
type encoded struct {
	Str   *string              `json:"str"`
	Undef bool                 `json:"undef"`
	Re    *string              `json:"re"`
	Flags string               `json:"flags"`
	Fn    string               `json:"fn"`
	List  []encoded            `json:"list"`
	Map   [][2]json.RawMessage `json:"map"`
}

type tableFile struct {
	Version string `json:"version"`
	Items   map[string][]struct {
		Regexes []struct {
			Re    string `json:"re"`
			Flags string `json:"flags"`
		} `json:"regexes"`
		Props []encoded `json:"props"`
	} `json:"items"`
}

// argKind tells what a property descriptor slot holds.
type argKind uint8

const (
	argUndefined argKind = iota
	argString
	argRegexp
	argFunction
	argMap
	argList
)

// mapEntry is one key of a string map, in for..in order.
type mapEntry struct {
	key    string
	values []*string // one entry for a scalar, several for an array; nil = undefined
	isList bool
	regexp *jsregex.Regexp // strTest's { test: /re/ }
}

type arg struct {
	kind    argKind
	str     string
	regexp  *jsregex.Regexp
	fn      string
	entries []mapEntry
}

// property is one element of a props array: a bare name, or a descriptor
// list whose first slot is the name.
type property struct {
	name string
	args []arg // empty for a bare name
}

type regexGroup struct {
	regexes []*jsregex.Regexp
	props   []property
}

type tables struct {
	version string
	browser []regexGroup
	os      []regexGroup
	device  []regexGroup
}

func loadTables() (*tables, error) {
	var file tableFile
	if err := json.Unmarshal(regexesJSON, &file); err != nil {
		return nil, fmt.Errorf("useragent: decode regexes.json: %w", err)
	}
	t := &tables{version: file.Version}
	for name, target := range map[string]*[]regexGroup{"browser": &t.browser, "os": &t.os, "device": &t.device} {
		for _, raw := range file.Items[name] {
			group := regexGroup{}
			for _, r := range raw.Regexes {
				re, err := jsregex.Compile(r.Re, r.Flags)
				if err != nil {
					return nil, err
				}
				group.regexes = append(group.regexes, re)
			}
			for _, p := range raw.Props {
				prop, err := decodeProperty(p)
				if err != nil {
					return nil, err
				}
				group.props = append(group.props, prop)
			}
			*target = append(*target, group)
		}
	}
	return t, nil
}

func decodeProperty(p encoded) (property, error) {
	if p.Str != nil {
		return property{name: *p.Str}, nil
	}
	if len(p.List) == 0 || p.List[0].Str == nil {
		return property{}, fmt.Errorf("useragent: malformed property descriptor")
	}
	prop := property{name: *p.List[0].Str}
	for _, slot := range p.List[1:] {
		decoded, err := decodeArg(slot)
		if err != nil {
			return property{}, err
		}
		prop.args = append(prop.args, decoded)
	}
	return prop, nil
}

func decodeArg(slot encoded) (arg, error) {
	switch {
	case slot.Undef:
		return arg{kind: argUndefined}, nil
	case slot.Str != nil:
		return arg{kind: argString, str: *slot.Str}, nil
	case slot.Re != nil:
		re, err := jsregex.Compile(*slot.Re, slot.Flags)
		if err != nil {
			return arg{}, err
		}
		return arg{kind: argRegexp, regexp: re}, nil
	case slot.Fn != "":
		return arg{kind: argFunction, fn: slot.Fn}, nil
	case slot.Map != nil:
		entries, err := decodeMap(slot.Map)
		if err != nil {
			return arg{}, err
		}
		return arg{kind: argMap, entries: entries}, nil
	case slot.List != nil:
		return arg{kind: argList}, nil
	}
	return arg{}, fmt.Errorf("useragent: unknown descriptor slot")
}

func decodeMap(raw [][2]json.RawMessage) ([]mapEntry, error) {
	entries := make([]mapEntry, 0, len(raw))
	for _, pair := range raw {
		var key string
		if err := json.Unmarshal(pair[0], &key); err != nil {
			return nil, fmt.Errorf("useragent: map key: %w", err)
		}
		var value encoded
		if err := json.Unmarshal(pair[1], &value); err != nil {
			return nil, fmt.Errorf("useragent: map value: %w", err)
		}
		entry := mapEntry{key: key}
		switch {
		case value.Undef:
			entry.values = []*string{nil}
		case value.Str != nil:
			entry.values = []*string{value.Str}
		case value.Re != nil:
			re, err := jsregex.Compile(*value.Re, value.Flags)
			if err != nil {
				return nil, err
			}
			entry.regexp = re
		case value.List != nil:
			entry.isList = true
			for _, item := range value.List {
				if item.Str == nil {
					entry.values = append(entry.values, nil)
					continue
				}
				entry.values = append(entry.values, item.Str)
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
