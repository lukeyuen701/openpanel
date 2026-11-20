// Package reqheaders is the request header record the ingest path reads: a
// lowercase name to the value a fetch Headers object would return for it.
package reqheaders

import (
	"net/http"
	"strings"
)

// Headers is immutable once built.
type Headers struct {
	values map[string]string
}

// FromHTTP flattens an http.Header. Repeated headers are joined with ", ",
// as Headers.get does.
func FromHTTP(h http.Header) Headers {
	values := make(map[string]string, len(h))
	for name, list := range h {
		values[strings.ToLower(name)] = strings.Join(list, ", ")
	}
	return Headers{values: values}
}

// FromMap builds Headers from lowercase names, for tests.
func FromMap(m map[string]string) Headers {
	values := make(map[string]string, len(m))
	for name, value := range m {
		values[strings.ToLower(name)] = value
	}
	return Headers{values: values}
}

// Get returns the value and whether the header was sent at all.
func (h Headers) Get(name string) (string, bool) {
	value, ok := h.values[name]
	return value, ok
}

// Value is the value, "" when absent.
func (h Headers) Value(name string) string {
	return h.values[name]
}

// Names lists the header names present.
func (h Headers) Names() []string {
	names := make([]string, 0, len(h.values))
	for name := range h.values {
		names = append(names, name)
	}
	return names
}
