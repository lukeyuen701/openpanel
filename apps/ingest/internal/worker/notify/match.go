// Package notify is the event consumer's notification rules: which rules an
// event matches, the title templates, and the delivery of a match (a
// notifications row for the dashboard, a sendNotification job for all of them).
// Everything works on the event payload as a jsjson tree, because the rules'
// paths, the stored payload and the job data all depend on JavaScript's
// property order and value semantics.
package notify

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"

	"openpanel/ingest/internal/jsjson"
)

const compiledPatternCacheSize = 1000

// ErrTypeError marks the places where the TypeScript code throws a TypeError
// on a value of an unexpected shape (a number where `.trim()` is called, a
// filter without `value`). A project filter fails the message on it; the rule
// check stops at it.
var ErrTypeError = errors.New("TypeError")

func typeError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrTypeError, fmt.Sprintf(format, args...))
}

// Matcher evaluates chart-event definitions (a name and property filters)
// against an event payload: the rules' events and the project's event filters.
type Matcher struct {
	logger   *slog.Logger
	patterns *lru.Cache[string, *regexp.Regexp]
}

// NewMatcher returns a Matcher; a nil logger discards.
func NewMatcher(logger *slog.Logger) *Matcher {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	patterns, _ := lru.New[string, *regexp.Regexp](compiledPatternCacheSize)
	return &Matcher{logger: logger, patterns: patterns}
}

// MatchEvent is matchEvent: the name equals the definition's (or it is "*"),
// and every property filter holds. An error is a TypeError the TypeScript
// would have thrown.
func (m *Matcher) MatchEvent(payload, event *jsjson.Value) (bool, error) {
	if !event.IsObject() {
		return false, typeError("chart event is not an object")
	}
	name := event.Get("name")
	if !strictEquals(payload.Get("name"), name) && !(name != nil && name.Kind == jsjson.String && name.Str == "*") {
		return false, nil
	}
	filters := event.Get("filters")
	if filters == nil || filters.Kind == jsjson.Null {
		return false, typeError("Cannot read properties of undefined (reading 'length')")
	}
	if filters.Kind != jsjson.Array {
		return false, typeError("chart event filters is not an array")
	}
	if len(filters.Arr) == 0 {
		return true, nil
	}
	return m.MatchEventFilters(payload, filters.Arr)
}

// MatchEventFilters is matchEventFilters (Array.every, so the first false
// ends the evaluation and nothing after it is looked at).
func (m *Matcher) MatchEventFilters(payload *jsjson.Value, filters []*jsjson.Value) (bool, error) {
	for _, filter := range filters {
		ok, err := m.matchFilter(payload, filter)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

func (m *Matcher) matchFilter(payload, filter *jsjson.Value) (bool, error) {
	if !filter.IsObject() {
		return false, typeError("filter is not an object")
	}
	values, text, err := filterValues(filter.Get("value"))
	if err != nil {
		return false, err
	}
	if len(values) == 0 && text == "" {
		return true, nil
	}
	nameValue := filter.Get("name")
	if nameValue != nil && nameValue.Kind == jsjson.String && nameValue.Str == "has_profile" {
		if text == "" && includesString(values, "true") || text != "" && strings.Contains(text, "true") {
			return !strictEquals(payload.Get("profileId"), payload.Get("deviceId")), nil
		}
		return strictEquals(payload.Get("profileId"), payload.Get("deviceId")), nil
	}
	if nameValue == nil || nameValue.Kind != jsjson.String {
		return false, typeError("name.startsWith is not a function")
	}
	path := []string{nameValue.Str}
	if strings.HasPrefix(nameValue.Str, "properties.") {
		path = strings.Split(nameValue.Str, ".")
	}
	raw := pathOr(payload, path)
	if raw.isDate || raw.isBuiltin || raw.value.Kind != jsjson.String {
		return false, typeError("propertyValue.trim is not a function")
	}
	value := jsTrim(raw.value.Str)
	operator := ""
	if op := filter.Get("operator"); op != nil && op.Kind == jsjson.String {
		operator = op.Str
	}
	if text != "" {
		return applyToText(operator, value, text)
	}
	return m.apply(operator, value, values), nil
}

// applyToText is a filter whose `value` is a string rather than an array:
// includes is a substring test and the operators built on some() throw.
func applyToText(operator, value, text string) (bool, error) {
	switch operator {
	case "is":
		return strings.Contains(text, value), nil
	case "isNot":
		return !strings.Contains(text, value), nil
	case "isNull":
		return value == "", nil
	case "isNotNull":
		return value != "", nil
	case "contains", "doesNotContain", "startsWith", "endsWith", "regex", "gt", "lt", "gte", "lte":
		return false, typeError("value.some is not a function")
	}
	return false, nil
}

func (m *Matcher) apply(operator, value string, values []*jsjson.Value) bool {
	switch operator {
	case "is":
		return includesString(values, value)
	case "isNot":
		return !includesString(values, value)
	case "contains":
		return anyString(values, func(x string) bool { return strings.Contains(value, x) })
	case "doesNotContain":
		return !anyString(values, func(x string) bool { return strings.Contains(value, x) })
	case "startsWith":
		return anyString(values, func(x string) bool { return strings.HasPrefix(value, x) })
	case "endsWith":
		return anyString(values, func(x string) bool { return strings.HasSuffix(value, x) })
	case "regex":
		return anyString(values, func(x string) bool {
			return m.TestFilterPattern(strings.Trim(x, "/"), value)
		})
	case "isNull":
		return value == ""
	case "isNotNull":
		return value != ""
	case "gt", "lt", "gte", "lte":
		number := jsNumberFromString(value)
		for _, x := range values {
			if compareNumbers(operator, number, jsNumber(x)) {
				return true
			}
		}
	}
	return false
}

func compareNumbers(operator string, a, b float64) bool {
	switch operator {
	case "gt":
		return a > b
	case "lt":
		return a < b
	case "gte":
		return a >= b
	}
	return a <= b
}

// filterValues is the filter's `value`. The TypeScript reads `value.length`
// first, so a missing value throws; a string has a length too and goes on to
// the operators (text is its content).
func filterValues(value *jsjson.Value) (values []*jsjson.Value, text string, err error) {
	switch {
	case value == nil || value.Kind == jsjson.Null:
		return nil, "", typeError("Cannot read properties of undefined (reading 'length')")
	case value.Kind == jsjson.Array:
		return value.Arr, "", nil
	case value.Kind == jsjson.String:
		return nil, value.Str, nil
	}
	return nil, "", typeError("filter value is not an array")
}

// includesString is Array.includes(string): only a string element can equal one.
func includesString(values []*jsjson.Value, s string) bool {
	for _, v := range values {
		if v.Kind == jsjson.String && v.Str == s {
			return true
		}
	}
	return false
}

// anyString is values.some(x => test(String(x))).
func anyString(values []*jsjson.Value, test func(string) bool) bool {
	for _, v := range values {
		if test(jsjson.JSString(v)) {
			return true
		}
	}
	return false
}

// TestFilterPattern is testFilterPattern: an unanchored search, where a
// pattern that does not compile never matches and is logged once.
func (m *Matcher) TestFilterPattern(pattern, input string) bool {
	re, seen := m.patterns.Get(pattern)
	if !seen {
		var err error
		re, err = regexp.Compile(pattern)
		if err != nil {
			re = nil
			m.logger.Warn("Filter regex is not valid RE2, treating it as non-matching", "pattern", pattern, "error", err)
		}
		m.patterns.Add(pattern, re)
	}
	return re != nil && re.MatchString(input)
}
