package useragent

import (
	"strings"

	"openpanel/ingest/internal/jsregex"
)

// fields is one ua-parser-js item (browser, os or device): property name to
// value, a nil value being JavaScript's undefined.
type fields map[string]*string

func (f fields) get(name string) *string { return f[name] }

func str(s string) *string { return &s }

func truthy(s *string) bool { return s != nil && *s != "" }

// mapRegexes is ua-parser-js's rgxMapper: the first regex of the first group
// that matches assigns that group's properties from its captures.
func mapRegexes(ua string, groups []regexGroup, out fields) {
	for _, group := range groups {
		for _, re := range group.regexes {
			matches := re.Exec(ua)
			if matches == nil {
				continue
			}
			for index, prop := range group.props {
				var match *string
				if index+1 < len(matches) {
					match = matches[index+1]
				}
				out[prop.name] = resolve(prop, match)
			}
			return
		}
	}
}

func resolve(prop property, match *string) *string {
	args := prop.args
	switch {
	case len(args) == 0:
		if truthy(match) {
			return match
		}
		return nil
	case len(args) == 1:
		if args[0].kind == argFunction {
			return callHelper(args[0].fn, match, nil)
		}
		if args[0].kind == argString {
			return str(args[0].str)
		}
		return nil
	case args[0].kind == argFunction:
		if !truthy(match) {
			return nil
		}
		return callHelper(args[0].fn, match, &args[1])
	default:
		if !truthy(match) {
			return nil
		}
		replaced := args[0].regexp.Replace(*match, args[1].str)
		switch len(args) {
		case 2:
			return &replaced
		case 3:
			return callHelper(args[2].fn, &replaced, nil)
		default:
			return callHelper(args[2].fn, &replaced, &args[3])
		}
	}
}

func callHelper(name string, value *string, extra *arg) *string {
	switch name {
	case "lowerize":
		if value == nil {
			return nil
		}
		return str(strings.ToLower(*value))
	case "trim":
		return str(trimLeft(stringOf(value)))
	case "strMapper":
		return strMapper(value, extra.entries)
	case "strTest":
		return strTest(value, extra.entries)
	}
	panic("useragent: unknown helper " + name)
}

// stringOf is String(value): undefined becomes "undefined".
func stringOf(value *string) string {
	if value == nil {
		return "undefined"
	}
	return *value
}

// has is ua-parser-js's case-insensitive equality between a map value and
// the captured string.
func has(candidate *string, value *string) bool {
	if candidate == nil || value == nil {
		return false
	}
	return strings.ToLower(*candidate) == strings.ToLower(*value)
}

func strMapper(value *string, entries []mapEntry) *string {
	for _, entry := range entries {
		for _, candidate := range entry.values {
			if has(candidate, value) {
				if entry.key == "?" {
					return nil
				}
				return str(entry.key)
			}
		}
	}
	for _, entry := range entries {
		if entry.key == "*" {
			if len(entry.values) == 0 {
				return nil
			}
			return entry.values[0]
		}
	}
	return value
}

func strTest(value *string, entries []mapEntry) *string {
	var test *jsregex.Regexp
	var ifTrue, ifFalse *string
	for _, entry := range entries {
		switch entry.key {
		case "test":
			test = entry.regexp
		case "ifTrue":
			ifTrue = first(entry.values)
		case "ifFalse":
			ifFalse = first(entry.values)
		}
	}
	if test != nil && test.Test(stringOf(value)) {
		return ifTrue
	}
	return ifFalse
}

func first(values []*string) *string {
	if len(values) == 0 {
		return nil
	}
	return values[0]
}

// isJSSpace is the set JavaScript's \s matches.
func isJSSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xA0, 0x1680, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF:
		return true
	}
	return r >= 0x2000 && r <= 0x200A
}

func trimLeft(s string) string {
	return strings.TrimLeftFunc(s, isJSSpace)
}

// trimJS is String.prototype.trim.
func trimJS(s string) string {
	return strings.TrimFunc(s, isJSSpace)
}
