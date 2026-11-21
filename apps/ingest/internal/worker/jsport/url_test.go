package jsport

import (
	"testing"

	"openpanel/ingest/internal/jsjson"
)

func queryText(query *jsjson.Obj) string {
	if query == nil {
		return "undefined"
	}
	text, _ := Stringify(jsjson.NewObject(query))
	return text
}

// goldenQueryText renders the expected query member the same way.
func goldenQueryText(v *jsjson.Value) string {
	if v == nil {
		return "undefined"
	}
	text, _ := Stringify(v)
	return text
}

func TestParsePathGolden(t *testing.T) {
	cases := golden(t, "url.json").Get("cases").Arr
	f := newFailures(t)
	for _, c := range cases {
		in := text(c.Get("in"))
		want := c.Get("out")
		got := ParsePath(in)
		f.check(got.Path == text(want.Get("path")) && got.Origin == text(want.Get("origin")) && got.Hash == text(want.Get("hash")) && queryText(got.Query) == goldenQueryText(want.Get("query")),
			"parsePath(%q)\n got path=%q origin=%q hash=%q query=%s\nwant path=%q origin=%q hash=%q query=%s", in,
			got.Path, got.Origin, got.Hash, queryText(got.Query),
			text(want.Get("path")), text(want.Get("origin")), text(want.Get("hash")), goldenQueryText(want.Get("query")))
	}
	f.report()
}

func TestIsSameDomainGolden(t *testing.T) {
	file := golden(t, "samedomain.json")
	f := newFailures(t)
	for _, c := range file.Get("cases").Arr {
		a, b := text(c.Get("a")), text(c.Get("b"))
		f.check(IsSameDomain(a, b) == boolean(c.Get("out")), "isSameDomain(%q, %q) = %v", a, b, !boolean(c.Get("out")))
	}
	for _, c := range file.Get("valueCases").Arr {
		a, err := ParseJSON(text(c.Get("a")))
		if err != nil {
			t.Fatal(err)
		}
		b, err := ParseJSON(text(c.Get("b")))
		if err != nil {
			t.Fatal(err)
		}
		f.check(IsSameDomainValue(a, b) == boolean(c.Get("out")), "isSameDomain(%s, %s) = %v", text(c.Get("a")), text(c.Get("b")), !boolean(c.Get("out")))
	}
	f.report()
}

func referrerMatches(got Referrer, want *jsjson.Value) bool {
	return got.Name == text(want.Get("name")) && got.Type == text(want.Get("type")) && got.URL == text(want.Get("url"))
}

func TestReferrerGolden(t *testing.T) {
	file := golden(t, "referrer.json")
	t.Run("parseReferrer", func(t *testing.T) {
		f := newFailures(t)
		for _, c := range file.Get("cases").Arr {
			in := text(c.Get("in"))
			got := ParseReferrer(in)
			f.check(referrerMatches(got, c.Get("out")), "parseReferrer(%q) = %+v, want %v", in, got, c.Get("out"))
		}
		f.report()
	})
	t.Run("getReferrerWithQuery", func(t *testing.T) {
		f := newFailures(t)
		for _, c := range file.Get("queryCases").Arr {
			var query *jsjson.Obj
			if c.Get("in").IsObject() {
				query = c.Get("in").Obj
			}
			got, err := GetReferrerWithQuery(query)
			want := c.Get("out")
			if want.Kind == jsjson.Null {
				f.check(err == nil && got == nil, "getReferrerWithQuery(%s) = %+v, want null", queryText(query), got)
				continue
			}
			f.check(err == nil && got != nil && referrerMatches(*got, want), "getReferrerWithQuery(%s) = %+v, want %v", queryText(query), got, want)
		}
		f.report()
	})
	t.Run("prototype keys", func(t *testing.T) {
		f := newFailures(t)
		for _, c := range file.Get("protoKeys").Arr {
			key := text(c.Get("key"))
			entry, found := referrers().lookup(key)
			f.check(found && !boolean(c.Get("own")) && entry.name == text(c.Get("name")) && entry.kind == text(c.Get("type")), "referrers[%q] = %+v", key, entry)
		}
		f.report()
	})
	t.Run("untyped arguments", func(t *testing.T) {
		f := newFailures(t)
		for _, c := range file.Get("valueCases").Arr {
			arg, err := ParseJSON(text(c.Get("in")))
			if err != nil {
				t.Fatal(err)
			}
			path, pathErr := ParsePathValue(arg)
			wantPath := c.Get("parsePath")
			if boolean(wantPath.Get("throws")) {
				f.check(pathErr != nil, "parsePath(%s) should throw", text(c.Get("in")))
			} else {
				want := wantPath.Get("v")
				f.check(pathErr == nil && path.Path == text(want.Get("path")) && path.Origin == text(want.Get("origin")) && path.Hash == text(want.Get("hash")), "parsePath(%s) = %+v", text(c.Get("in")), path)
			}
			referrer, referrerErr := ParseReferrerValue(arg)
			wantReferrer := c.Get("parseReferrer")
			if boolean(wantReferrer.Get("throws")) {
				f.check(referrerErr != nil, "parseReferrer(%s) should throw", text(c.Get("in")))
			} else {
				f.check(referrerErr == nil && referrerMatches(referrer, wantReferrer.Get("v")), "parseReferrer(%s) = %+v", text(c.Get("in")), referrer)
			}
		}
		f.report()
	})
}
