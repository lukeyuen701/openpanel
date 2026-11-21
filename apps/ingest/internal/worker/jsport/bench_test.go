package jsport

import (
	"strings"
	"testing"
)

func BenchmarkParsePath(b *testing.B) {
	for b.Loop() {
		ParsePath("https://www.example.com/pricing/plans?utm_source=Google&utm_medium=cpc&ref=x#top")
	}
}

func BenchmarkParsePathRelative(b *testing.B) {
	for b.Loop() {
		ParsePath("/docs/getting-started?tab=install")
	}
}

func BenchmarkToDots(b *testing.B) {
	input, err := ParseJSON(`{"plan":"pro","seats":5,"flags":{"beta":true,"tags":["a","b","c"]},"meta":"{\"k\":1}","empty":""}`)
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		ToDots(input.Obj)
	}
}

func BenchmarkParseReferrer(b *testing.B) {
	for b.Loop() {
		ParseReferrer("https://www.google.com/search?q=openpanel")
	}
}

func TestDeeplyNestedJSONParses(t *testing.T) {
	const depth = 100_000
	if _, err := ParseJSON(strings.Repeat("[", depth) + strings.Repeat("]", depth)); err != nil {
		t.Fatal(err)
	}
}
