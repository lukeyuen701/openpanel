package jsjson

import (
	"encoding/json"
	"os"
	"testing"
)

func TestRoundTripMatchesJSONStringify(t *testing.T) {
	data, err := os.ReadFile("testdata/jsjson.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Text, Out string }
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		v, err := Parse([]byte(c.Text))
		if err != nil {
			t.Errorf("parse %q: %v", c.Text, err)
			continue
		}
		if got := string(Stringify(v)); got != c.Out {
			t.Errorf("%q:\n got %s\nwant %s", c.Text, got, c.Out)
		}
	}
}

func TestStableHashMatchesFastJSONStableHash(t *testing.T) {
	data, err := os.ReadFile("testdata/stablehash.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Text, MD5 string }
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		v, err := Parse([]byte(c.Text))
		if err != nil {
			t.Fatal(err)
		}
		if got := StableMD5(v); got != c.MD5 {
			t.Errorf("%q: got %s want %s (%s)", c.Text, got, c.MD5, StableStringify(v))
		}
	}
}

func TestLoneSurrogateIsUnsupported(t *testing.T) {
	for _, text := range []string{`"\ud800"`, `"\udc00x"`, `"\ud800\u0041"`} {
		if _, err := Parse([]byte(text)); err == nil {
			t.Errorf("%s: expected ErrUnsupported", text)
		}
	}
}
