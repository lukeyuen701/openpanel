package jsport

import (
	_ "embed"
	"strings"
	"sync"

	"openpanel/ingest/internal/jsjson"
)

//go:embed data/referrers.json
var referrersJSON string

// Referrer is the {name, type, url} object the referrer helpers return. A
// name or type that is undefined in JavaScript (a hit on an inherited
// Object.prototype member) is "", which every consumer treats the same.
type Referrer struct {
	Name string
	Type string
	URL  string
}

type referrerEntry struct {
	host, kind, name string
}

type referrerTable struct {
	byHost      map[string]referrerEntry
	byLowerName map[string]referrerEntry
	// inherited holds what referrers[key] finds on Object.prototype for a key the table lacks.
	inherited map[string]referrerEntry
}

var (
	loadTable sync.Once
	table     *referrerTable
)

func referrers() *referrerTable {
	loadTable.Do(func() {
		parsed, err := ParseJSON(referrersJSON)
		if err != nil {
			panic("jsport: embedded referrers.json: " + err.Error())
		}
		table = &referrerTable{
			byHost:      map[string]referrerEntry{},
			byLowerName: map[string]referrerEntry{},
			inherited:   map[string]referrerEntry{},
		}
		for _, row := range parsed.Get("entries").Arr {
			entry := referrerEntry{host: row.Arr[0].Str, kind: row.Arr[1].Str, name: row.Arr[2].Str}
			table.byHost[entry.host] = entry
			lowerName := ToLower(entry.name)
			if _, seen := table.byLowerName[lowerName]; !seen {
				table.byLowerName[lowerName] = entry
			}
		}
		for _, row := range parsed.Get("prototype").Arr {
			entry := referrerEntry{host: row.Arr[0].Str}
			if row.Arr[1].Kind == jsjson.String {
				entry.name = row.Arr[1].Str
			}
			if row.Arr[2].Kind == jsjson.String {
				entry.kind = row.Arr[2].Str
			}
			table.inherited[entry.host] = entry
		}
	})
	return table
}

// lookup is referrers[key]: an own entry, else an inherited Object.prototype member.
func (t *referrerTable) lookup(key string) (referrerEntry, bool) {
	if entry, ok := t.byHost[key]; ok {
		return entry, true
	}
	entry, ok := t.inherited[key]
	return entry, ok
}

// IsSameDomain is isSameDomain(a, b): both truthy and both absolute URLs with the same hostname.
func IsSameDomain(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	hostA, okA := hostname(a)
	hostB, okB := hostname(b)
	return okA && okB && hostA == hostB
}

// IsSameDomainValue is IsSameDomain for untyped arguments; new URL(x)
// converts its argument with String(), so an array of one URL string works.
func IsSameDomainValue(a, b *jsjson.Value) bool {
	if !truthy(a) || !truthy(b) {
		return false
	}
	return IsSameDomain(jsjson.JSString(a), jsjson.JSString(b))
}

// ParseReferrer is parseReferrer(url).
func ParseReferrer(url string) Referrer {
	host, _ := hostname(url)
	t := referrers()
	match, found := t.lookup(host)
	if !found {
		match, found = t.lookup(strings.Replace(host, "www.", "", 1))
	}
	result := Referrer{URL: StripTrailingSlash(url)}
	if found {
		result.Name, result.Type = match.name, match.kind
	}
	return result
}

// ParseReferrerValue is ParseReferrer for an untyped argument: null and
// undefined are the empty URL, any other non-string throws.
func ParseReferrerValue(v *jsjson.Value) (Referrer, error) {
	if v == nil || v.Kind == jsjson.Null {
		return ParseReferrer(""), nil
	}
	if v.Kind != jsjson.String {
		return Referrer{}, ErrTypeError
	}
	return ParseReferrer(v.Str), nil
}

var referrerQueryKeys = [...]string{"utm_source", "ref", "utm_referrer"}

// GetReferrerWithQuery is getReferrerWithQuery(query): the first defined of
// utm_source, ref and utm_referrer (an empty one shadows the later keys),
// lower-cased, looked up by host, by host plus ".com" and by name. A nil
// result is null. A non-string value throws.
func GetReferrerWithQuery(query *jsjson.Obj) (*Referrer, error) {
	if query == nil {
		return nil, nil
	}
	var source string
	for _, key := range referrerQueryKeys {
		value := query.Get(key)
		if value == nil || value.Kind == jsjson.Null {
			continue
		}
		if value.Kind != jsjson.String {
			return nil, ErrTypeError
		}
		source = value.Str
		break
	}
	source = ToLower(source)
	if source == "" {
		return nil, nil
	}
	t := referrers()
	match, found := t.lookup(source)
	if !found {
		match, found = t.lookup(source + ".com")
	}
	if !found {
		match, found = t.byLowerName[source]
	}
	if found {
		return &Referrer{Name: match.name, Type: match.kind}, nil
	}
	return &Referrer{Name: source}, nil
}
