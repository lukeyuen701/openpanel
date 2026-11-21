package jsport

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"openpanel/ingest/internal/jsjson"
)

func mustParse(t *testing.T, text string) *jsjson.Value {
	t.Helper()
	value, err := ParseJSON(text)
	if err != nil {
		t.Fatalf("ParseJSON(%q): %v", text, err)
	}
	return value
}

func dots(t *testing.T, text string) string {
	t.Helper()
	return stringifyObject(mustDots(t, text))
}

func mustDots(t *testing.T, text string) *jsjson.Obj {
	t.Helper()
	result, err := ToDots(mustParse(t, text).Obj)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func stringifyObject(obj *jsjson.Obj) string {
	got, _ := Stringify(jsjson.NewObject(obj))
	return got
}

func wideObject(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("\"k%d\":%d", i, i)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// The cases of packages/core/src/shared/object.test.ts (toDots).
func TestToDotsPortedCases(t *testing.T) {
	buffer := make([]string, 0)
	for _, b := range []byte(`{"signedPayload":"abc"}`) {
		buffer = append(buffer, strconv.Itoa(int(b)))
	}
	cases := []struct {
		name, in, want string
	}{
		{
			"nested objects, arrays and embedded JSON",
			`{"a":1,"b":2,"array":["1","2","3"],"arrayWithObjects":[{"a":1},{"b":2},{"c":3}],"objectWithArrays":{"a":[1,2,3]},"null":null,"empty":"","jsonString":"{\"a\": 1, \"b\": 2}"}`,
			`{"a":"1","b":"2","array.0":"1","array.1":"2","array.2":"3","arrayWithObjects.0.a":"1","arrayWithObjects.1.b":"2","arrayWithObjects.2.c":"3","objectWithArrays.a.0":"1","objectWithArrays.a.1":"2","objectWithArrays.a.2":"3","jsonString.a":"1","jsonString.b":"2"}`,
		},
		{
			"malformed JSON strings are dropped",
			`{"validJson":"{\"key\":\"value\"}","malformedJson":"{\"key\":\"unterminated string","startsWithBrace":"{not json at all","startsWithBracket":"[also not json","regularString":"normal string"}`,
			`{"validJson.key":"value","regularString":"normal string"}`,
		},
		{
			"a serialised Node Buffer is decoded",
			`{"requestBody":{"type":"Buffer","data":[` + strings.Join(buffer, ",") + `]}}`,
			`{"requestBody.signedPayload":"abc"}`,
		},
		{"lone surrogates are stripped, pairs kept", `{"s":"a\ud800b\ud83d\udc4b"}`, "{\"s\":\"ab\U0001F44B\"}"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := dots(t, c.in); got != c.want {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
		})
	}

	t.Run("every root property up to the total cap", func(t *testing.T) {
		if got := mustDots(t, wideObject(ToDotsMaxKeysPerObject*2)).Len(); got != ToDotsMaxKeysPerObject*2 {
			t.Errorf("got %d keys", got)
		}
	})
	t.Run("a wide root is capped at the total cap", func(t *testing.T) {
		result := mustDots(t, wideObject(ToDotsMaxKeys*2))
		if result.Len() != ToDotsMaxKeys || !result.Has("k0") || result.Has("k"+strconv.Itoa(ToDotsMaxKeys)) {
			t.Errorf("got %d keys", result.Len())
		}
	})
	t.Run("nested objects and arrays are capped per object", func(t *testing.T) {
		longArray := make([]string, ToDotsMaxKeysPerObject+10)
		for i := range longArray {
			longArray[i] = strconv.Itoa(i)
		}
		input := `{"longArray":[` + strings.Join(longArray, ",") + `],"child":` + wideObject(ToDotsMaxKeysPerObject+10) + `,"short":[1,2]}`
		result := mustDots(t, input)
		counts := map[string]int{}
		for _, key := range result.Keys() {
			counts[key[:strings.IndexByte(key, '.')]]++
		}
		if counts["longArray"] != ToDotsMaxKeysPerObject || counts["child"] != ToDotsMaxKeysPerObject || result.Get("short.1").Str != "2" {
			t.Errorf("counts %v", counts)
		}
	})
	t.Run("deeply nested input is capped in total", func(t *testing.T) {
		level := func(child string) string {
			parts := make([]string, ToDotsMaxKeysPerObject)
			for i := range parts {
				parts[i] = fmt.Sprintf("\"k%d\":%s", i, child)
			}
			return "{" + strings.Join(parts, ",") + "}"
		}
		if got := mustDots(t, level(level(level("1")))).Len(); got != ToDotsMaxKeys {
			t.Errorf("got %d keys", got)
		}
	})
	t.Run("__proto__ at the root is counted but not stored", func(t *testing.T) {
		if got := dots(t, `{"__proto__":"x","a":1}`); got != `{"a":"1"}` {
			t.Errorf("got %s", got)
		}
	})
}

// The cases of packages/core/src/shared/parse-referrer.test.ts.
func TestReferrerPortedCases(t *testing.T) {
	parse := []struct {
		url  string
		want Referrer
	}{
		{"", Referrer{}},
		{"https://google.com/search?q=test", Referrer{"Google", "search", "https://google.com/search?q=test"}},
		{"https://www.twitter.com/user", Referrer{"Twitter", "social", "https://www.twitter.com/user"}},
		{"https://twitter.com/user", Referrer{"Twitter", "social", "https://twitter.com/user"}},
		{"https://unknown-site.com", Referrer{"", "", "https://unknown-site.com"}},
		{"not-a-url", Referrer{"", "", "not-a-url"}},
	}
	for _, c := range parse {
		if got := ParseReferrer(c.url); got != c.want {
			t.Errorf("ParseReferrer(%q) = %+v, want %+v", c.url, got, c.want)
		}
	}

	query := func(pairs ...string) *jsjson.Obj {
		obj := jsjson.NewObj()
		for i := 0; i < len(pairs); i += 2 {
			obj.Set(pairs[i], jsjson.NewString(pairs[i+1]))
		}
		return obj
	}
	withQuery := []struct {
		name  string
		query *jsjson.Obj
		want  *Referrer
	}{
		{"undefined", nil, nil},
		{"empty", query(), nil},
		{"utm_source", query("utm_source", "google"), &Referrer{"Google", "search", ""}},
		{"ref", query("ref", "facebook"), &Referrer{"Facebook", "social", ""}},
		{"utm_referrer", query("utm_referrer", "twitter"), &Referrer{"Twitter", "social", ""}},
		{"case-insensitive", query("utm_source", "GoOgLe"), &Referrer{"Google", "search", ""}},
		{"unknown source", query("utm_source", "unknown-source"), &Referrer{"unknown-source", "", ""}},
		{"utm_source wins", query("utm_source", "google", "ref", "facebook", "utm_referrer", "twitter"), &Referrer{"Google", "search", ""}},
		{"an empty utm_source shadows ref", query("utm_source", "", "ref", "twitter"), nil},
		{"constructor reaches Object.prototype", query("utm_source", "constructor"), &Referrer{"Object", "", ""}},
		{"__proto__ finds an object without a name", query("utm_source", "__proto__"), &Referrer{"", "", ""}},
	}
	for _, c := range withQuery {
		got, err := GetReferrerWithQuery(c.query)
		if err != nil || (got == nil) != (c.want == nil) || (got != nil && *got != *c.want) {
			t.Errorf("%s: got %+v (%v), want %+v", c.name, got, err, c.want)
		}
	}
	numeric := jsjson.NewObj()
	numeric.Set("utm_source", jsjson.NewNumber(5))
	if _, err := GetReferrerWithQuery(numeric); err != ErrTypeError {
		t.Errorf("a numeric utm_source must throw, got %v", err)
	}
}

func TestReplaceAllDollarPatterns(t *testing.T) {
	cases := []struct{ s, search, rep, want string }{
		{"x $EVENT_NAME y", "$EVENT_NAME", "a$&b", "x a$EVENT_NAMEb y"},
		{"x $EVENT_NAME y", "$EVENT_NAME", "$$", "x $ y"},
		{"x $EVENT_NAME y", "$EVENT_NAME", "[$`]", "x [x ] y"},
		{"x $EVENT_NAME y", "$EVENT_NAME", "[$']", "x [ y] y"},
		{"x $EVENT_NAME y", "$EVENT_NAME", "$1$<n>$0", "x $1$<n>$0 y"},
		{"aaa", "a", "$&$&", "aaaaaa"},
		{"abc", "", "-", "-a-b-c-"},
	}
	for _, c := range cases {
		if got := ReplaceAll(c.s, c.search, c.rep); got != c.want {
			t.Errorf("ReplaceAll(%q, %q, %q) = %q, want %q", c.s, c.search, c.rep, got, c.want)
		}
	}
}

func TestLoneSurrogatesSurviveJSON(t *testing.T) {
	const text = `{"name":"a\ud800b","k\udc00":["\ud83d"]}`
	value := mustParse(t, text)
	if got, _ := Stringify(value); got != text {
		t.Errorf("round trip changed the bytes: %s", got)
	}
	if got := Len16(value.Get("name").Str); got != 3 {
		t.Errorf("length = %d, want 3", got)
	}
	// A raw high surrogate followed by an escaped low one is one character.
	joined := mustParse(t, "\"\xed\xa0\xbd\\ude00\"")
	if joined.Str != "\U0001F600" {
		t.Errorf("joined = %+q", joined.Str)
	}
}

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

func TestFormatNowUsesTheClock(t *testing.T) {
	clock := fixedClock{time.Date(2026, 3, 4, 5, 6, 7, 89_000_000, time.FixedZone("x", 3600))}
	if got := FormatNow(clock); got != "2026-03-04 04:06:07.089" {
		t.Errorf("got %q", got)
	}
}

func TestParseJSONBytesDecodesLikeBuffer(t *testing.T) {
	if _, err := ParseJSONBytes([]byte("\xef\xbb\xbf{}")); err == nil {
		t.Error("a byte-order mark must fail the parse, as JSON.parse(buffer.toString()) does")
	}
	value, err := ParseJSONBytes([]byte("\"a\xe2\x82b\""))
	if err != nil || value.Str != "a\uFFFDb" {
		t.Errorf("got %+v, %v", value, err)
	}
}

func TestNestingLimitsMatchTheEngine(t *testing.T) {
	nested := func(depth int) string {
		return strings.Repeat(`{"a":`, depth) + `"x"` + strings.Repeat("}", depth)
	}
	if _, err := ToDots(mustParse(t, nested(ToDotsMaxDepth)).Obj); err != nil {
		t.Errorf("%d nested objects must flatten: %v", ToDotsMaxDepth, err)
	}
	if _, err := ToDots(mustParse(t, nested(ToDotsMaxDepth+1)).Obj); err != ErrTooDeep {
		t.Errorf("%d nested objects must overflow, got %v", ToDotsMaxDepth+1, err)
	}
	deep := mustParse(t, strings.Repeat("[", maxStringifyDepth+1)+strings.Repeat("]", maxStringifyDepth+1))
	if _, err := Stringify(deep); err != ErrTooDeep {
		t.Errorf("stringify of %d levels must overflow, got %v", maxStringifyDepth+1, err)
	}
	if _, err := Stringify(nil); err != ErrUndefined {
		t.Errorf("stringify(undefined) = %v", err)
	}
}

// A Kafka message is at most about a megabyte; the deepest document that
// fits must neither crash nor take noticeable time.
func TestLargestNestedMessageIsSafe(t *testing.T) {
	const messageBytes = 1 << 20
	value, err := ParseJSON(strings.Repeat("[", messageBytes/2) + strings.Repeat("]", messageBytes/2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Stringify(value); err != ErrTooDeep {
		t.Errorf("stringify: %v", err)
	}
	parsed, err := ParseJSON(strings.Repeat(`{"a":`, messageBytes/6) + "1" + strings.Repeat("}", messageBytes/6))
	if err != nil {
		t.Fatal(err)
	}
	wrapper := jsjson.NewObj()
	wrapper.Set("p", parsed)
	if _, err := ToDots(wrapper); err != ErrTooDeep {
		t.Errorf("toDots: %v", err)
	}
}
