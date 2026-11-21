package jsport

import (
	"bytes"
	"compress/gzip"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"encoding/hex"

	"openpanel/ingest/internal/jsjson"
)

const maxReportedFailures = 15

// goldenDirEnv points the tests at goldens generated elsewhere, for example
// with another GOLDEN_SEED: GOLDEN_SEED=7 bun parity/golden-jsport.ts /tmp/g.
const goldenDirEnv = "JSPORT_GOLDEN_DIR"

// golden loads a file written by parity/golden-jsport.ts (name.gz). They are ASCII JSON,
// read with this package's parser so lone surrogates survive.
func golden(t *testing.T, name string) *jsjson.Value {
	t.Helper()
	dir := os.Getenv(goldenDirEnv)
	if dir == "" {
		dir = "testdata"
	}
	compressed, err := os.ReadFile(filepath.Join(dir, name+".gz"))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	value, err := ParseJSON(string(data))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return value
}

// failures stops a table test from printing thousands of lines.
type failures struct {
	t      *testing.T
	shown  int
	failed int
	total  int
}

func newFailures(t *testing.T) *failures { return &failures{t: t} }

func (f *failures) check(ok bool, format string, args ...any) {
	f.total++
	if ok {
		return
	}
	f.failed++
	if f.shown < maxReportedFailures {
		f.shown++
		f.t.Errorf(format, args...)
	}
}

func (f *failures) report() {
	f.t.Logf("%d/%d golden cases pass", f.total-f.failed, f.total)
	if f.failed > 0 {
		f.t.Errorf("%d of %d golden cases fail", f.failed, f.total)
	}
}

func text(v *jsjson.Value) string {
	if v == nil || v.Kind != jsjson.String {
		return ""
	}
	return v.Str
}

func boolean(v *jsjson.Value) bool { return v != nil && v.Kind == jsjson.Bool && v.Bool }

func TestToDotsGolden(t *testing.T) {
	cases := golden(t, "todots.json").Get("cases").Arr
	f := newFailures(t)
	for _, c := range cases {
		input, err := ParseJSON(text(c.Get("in")))
		if err != nil {
			t.Fatalf("golden input does not parse: %v", err)
		}
		dotted, err := ToDots(input.Obj)
		if err != nil {
			f.check(false, "toDots(%.200q): %v", text(c.Get("in")), err)
			continue
		}
		got, _ := Stringify(jsjson.NewObject(dotted))
		f.check(got == text(c.Get("out")), "toDots(%.200q)\n got %.300q\nwant %.300q", text(c.Get("in")), got, text(c.Get("out")))
	}
	f.report()
}

func TestJSONGolden(t *testing.T) {
	cases := golden(t, "json.json").Get("cases").Arr
	f := newFailures(t)
	for _, c := range cases {
		in := text(c.Get("in"))
		value, err := ParseJSON(in)
		if !boolean(c.Get("ok")) {
			f.check(err != nil, "JSON.parse(%.100q) should fail", in)
			continue
		}
		if err != nil {
			f.check(false, "JSON.parse(%.100q): %v", in, err)
			continue
		}
		got, _ := Stringify(value)
		f.check(got == text(c.Get("out")), "JSON round trip of %.100q\n got %.200q\nwant %.200q", in, got, text(c.Get("out")))
	}
	f.report()
}

func TestStringsGolden(t *testing.T) {
	file := golden(t, "strings.json")
	groups := file.Get("cases")
	run := func(name string, fn func(c *jsjson.Value, f *failures)) {
		t.Run(name, func(t *testing.T) {
			f := newFailures(t)
			for _, c := range groups.Get(name).Arr {
				fn(c, f)
			}
			f.report()
		})
	}
	run("trim", func(c *jsjson.Value, f *failures) {
		got := Trim(text(c.Get("in")))
		f.check(got == text(c.Get("out")), "trim(%q) = %q, want %q", text(c.Get("in")), got, text(c.Get("out")))
	})
	run("lower", func(c *jsjson.Value, f *failures) {
		got := ToLower(text(c.Get("in")))
		f.check(got == text(c.Get("out")), "toLowerCase(%q) = %q, want %q", text(c.Get("in")), got, text(c.Get("out")))
	})
	run("len16", func(c *jsjson.Value, f *failures) {
		got := Len16(text(c.Get("in")))
		f.check(float64(got) == c.Get("out").Num, "len(%q) = %d, want %v", text(c.Get("in")), got, c.Get("out").Num)
	})
	run("wellFormed", func(c *jsjson.Value, f *failures) {
		got := IsWellFormed(text(c.Get("in")))
		f.check(got == boolean(c.Get("out")), "isWellFormed(%q) = %v", text(c.Get("in")), got)
	})
	run("removeLone", func(c *jsjson.Value, f *failures) {
		got := RemoveLoneSurrogates(text(c.Get("in")))
		f.check(got == text(c.Get("out")), "removeLone(%q) = %q, want %q", text(c.Get("in")), got, text(c.Get("out")))
	})
	run("stripTrailing", func(c *jsjson.Value, f *failures) {
		got := StripTrailingSlash(text(c.Get("in")))
		f.check(got == text(c.Get("out")), "stripTrailingSlash(%q) = %q, want %q", text(c.Get("in")), got, text(c.Get("out")))
	})
	run("stripBoth", func(c *jsjson.Value, f *failures) {
		got := StripLeadingAndTrailingSlashes(text(c.Get("in")))
		f.check(got == text(c.Get("out")), "stripBoth(%q) = %q, want %q", text(c.Get("in")), got, text(c.Get("out")))
	})
	run("replaceAll", func(c *jsjson.Value, f *failures) {
		got := ReplaceAll(text(c.Get("s")), text(c.Get("search")), text(c.Get("rep")))
		f.check(got == text(c.Get("out")), "replaceAll(%q, %q, %q) = %q, want %q", text(c.Get("s")), text(c.Get("search")), text(c.Get("rep")), got, text(c.Get("out")))
	})
	run("decode", func(c *jsjson.Value, f *failures) {
		raw, err := hex.DecodeString(text(c.Get("hex")))
		if err != nil {
			t.Fatal(err)
		}
		f.check(DecodeUTF8(raw) == text(c.Get("out")), "Buffer.toString(%x) = %q, want %q", raw, DecodeUTF8(raw), text(c.Get("out")))
		f.check(DecodeUTF8StripBOM(raw) == text(c.Get("noBom")), "TextDecoder(%x) = %q, want %q", raw, DecodeUTF8StripBOM(raw), text(c.Get("noBom")))
	})

	t.Run("trim every code point", func(t *testing.T) {
		whitespace := map[rune]bool{}
		for _, cp := range file.Get("trimmedCodePoints").Arr {
			whitespace[rune(cp.Num)] = true
		}
		f := newFailures(t)
		for cp := rune(0); cp <= 0x10FFFF; cp++ {
			f.check(IsJSWhitespace(cp) == whitespace[cp], "IsJSWhitespace(%U) = %v", cp, IsJSWhitespace(cp))
		}
		f.report()
	})
	t.Run("final sigma in every context", func(t *testing.T) {
		const sigma = "\u03A3"
		const finalSigma = "\u03C2"
		contexts := file.Get("sigmaContexts")
		sets := map[string]map[rune]bool{}
		for name, list := range map[string]*jsjson.Value{
			"ignorableBefore": contexts.Get("ignorableBefore"), "followedBy": contexts.Get("followedBy"),
			"precededBy": contexts.Get("precededBy"), "ignorableAfter": contexts.Get("ignorableAfter"),
		} {
			sets[name] = map[rune]bool{}
			for _, span := range list.Arr {
				for cp := rune(span.Arr[0].Num); cp <= rune(span.Arr[1].Num); cp++ {
					sets[name][cp] = true
				}
			}
		}
		f := newFailures(t)
		for cp := rune(0); cp <= 0x10FFFF; cp++ {
			if isSurrogate(cp) {
				continue
			}
			if isAppleCaseIgnorable(cp) {
				continue
			}
			c := string(cp)
			f.check(strings.HasSuffix(ToLower("A"+c+sigma), finalSigma) == sets["ignorableBefore"][cp], "final sigma after A%s mismatch for %U", c, cp)
			f.check(strings.Contains(ToLower("A"+sigma+c), finalSigma) == sets["followedBy"][cp], "final sigma before %s mismatch for %U", c, cp)
			f.check(strings.HasSuffix(ToLower(c+sigma), finalSigma) == sets["precededBy"][cp], "final sigma after %s mismatch for %U", c, cp)
			f.check(strings.Contains(ToLower("A"+sigma+c+"b"), finalSigma) == sets["ignorableAfter"][cp], "final sigma before %sb mismatch for %U", c, cp)
		}
		f.report()
	})
	t.Run("lower every code point", func(t *testing.T) {
		lowered := map[rune]string{}
		for _, pair := range file.Get("loweredCodePoints").Arr {
			lowered[rune(pair.Arr[0].Num)] = pair.Arr[1].Str
		}
		f := newFailures(t)
		for cp := rune(0); cp <= 0x10FFFF; cp++ {
			if isSurrogate(cp) {
				continue
			}
			want, changed := lowered[cp]
			if !changed {
				want = string(cp)
			}
			got := ToLower(string(cp))
			f.check(got == want, "toLowerCase(%U) = %+q, want %+q", cp, got, want)
		}
		f.report()
	})
}

// isAppleCaseIgnorable marks U+F800-U+F8FF, private-use code points some of which the
// macOS ICU behind the Bun that wrote the goldens treats as case-ignorable;
// no other ICU does, so they are not part of the contract.
func isAppleCaseIgnorable(cp rune) bool { return cp >= 0xF800 && cp <= 0xF8FF }

func TestNumbersGolden(t *testing.T) {
	file := golden(t, "numbers.json")
	same := func(got float64, want string, negativeZero bool) bool {
		if math.Signbit(got) && got == 0 {
			return negativeZero
		}
		return NumberToString(got) == want && !negativeZero
	}
	t.Run("parseFloat", func(t *testing.T) {
		f := newFailures(t)
		for _, c := range file.Get("parseFloat").Arr {
			got := ParseFloat(text(c.Get("in")))
			f.check(same(got, text(c.Get("out")), boolean(c.Get("neg0"))), "parseFloat(%q) = %v, want %s", text(c.Get("in")), got, text(c.Get("out")))
		}
		f.report()
	})
	t.Run("Number", func(t *testing.T) {
		f := newFailures(t)
		for _, c := range file.Get("number").Arr {
			got := ToNumber(text(c.Get("in")))
			f.check(same(got, text(c.Get("out")), boolean(c.Get("neg0"))), "Number(%q) = %v, want %s", text(c.Get("in")), got, text(c.Get("out")))
		}
		f.report()
	})
	t.Run("toString", func(t *testing.T) {
		f := newFailures(t)
		for _, c := range file.Get("toString").Arr {
			bits := uint64(c.Get("bits").Arr[0].Num)<<32 | uint64(c.Get("bits").Arr[1].Num)
			x := math.Float64frombits(bits)
			f.check(NumberToString(x) == text(c.Get("out")), "String(%v) = %s, want %s", strconv.FormatUint(bits, 16), NumberToString(x), text(c.Get("out")))
		}
		f.report()
	})
}

// TestDateFormsAreSupported keeps the parser from passing by refusing everything.
func TestDateFormsAreSupported(t *testing.T) {
	supported := map[string]int64{
		"2024-01-01":                  1704067200000,
		"2024-01":                     1704067200000,
		"2024":                        1704067200000,
		"2024-01-01T00:00:00Z":        1704067200000,
		"2024-01-01T00:00:00.000Z":    1704067200000,
		"2024-01-01T10:20:30.123Z":    1704104430123,
		"2024-01-01T10:20:30.123456Z": 1704104430123,
		"2024-01-01T10:20:30+05:30":   1704084630000,
		"2024-01-01T10:20:30-08:00":   1704133230000,
		"2024-01-01T10:20:30.5+0530":  1704084630500,
		"2024-01-01T10:20:30":         1704104430000,
		"2024-01-01 10:20:30":         1704104430000,
		"2024-02-30":                  1709251200000,
		"+002024-01-01T00:00:00Z":     1704067200000,
		"-000001-01-01T00:00:00Z":     -62198755200000,
		"0099-12-31T00:00:00Z":        -59011545600000,
		"1970-01-01T00:00:00.000Z":    0,
		"2024-01-01T24:00:00Z":        1704153600000,
		"9999-12-31T23:59:59.999Z":    253402300799999,
	}
	for in, want := range supported {
		date, err := ParseDateString(in)
		ms, valid := date.Millis()
		if err != nil || !valid || ms != want {
			t.Errorf("ParseDateString(%q) = %d valid=%v err=%v, want %d", in, ms, valid, err, want)
		}
	}
	if date, err := ParseDateString("+275760-09-13T00:00:00.000Z"); err != nil || date.JSON() != "\"+275760-09-13T00:00:00.000Z\"" {
		t.Errorf("the last representable instant must parse, got %v %v", date, err)
	}
	if date, err := ParseDateString("+275760-09-13T00:00:00.001Z"); err != nil || date.Valid() {
		t.Errorf("a time beyond the Date range is an Invalid Date, got %v %v", date, err)
	}
	for _, in := range []string{"Jan 1 2024", "2024-13-01", "garbage"} {
		if _, err := ParseDateString(in); err != ErrUnsupportedDate {
			t.Errorf("ParseDateString(%q) = %v, want ErrUnsupportedDate", in, err)
		}
	}
}

func TestParseClickhouseDateTimeGolden(t *testing.T) {
	f := newFailures(t)
	for _, c := range golden(t, "dates.json").Get("parseCases").Arr {
		in := text(c.Get("in"))
		ms, valid := ParseClickhouseDateTime(in).Millis()
		wantValid := c.Get("ms").Kind == jsjson.Number
		f.check(valid == wantValid && (!valid || float64(ms) == c.Get("ms").Num), "fromFormat(%q) = %d valid=%v, want %v", in, ms, valid, c.Get("ms"))
	}
	f.report()
}

func TestDatesGolden(t *testing.T) {
	cases := golden(t, "dates.json").Get("cases").Arr
	f := newFailures(t)
	unsupported, supported := 0, 0
	for _, c := range cases {
		in := text(c.Get("in"))
		value, err := ParseJSON(in)
		if err != nil {
			t.Fatal(err)
		}
		date, err := ParseDate(value)
		if err != nil {
			unsupported++
			f.check(value.Kind == jsjson.String, "number %s unsupported", in)
			continue
		}
		supported++
		ms, valid := date.Millis()
		wantValid := c.Get("ms").Kind == jsjson.Number
		f.check(valid == wantValid && (!valid || float64(ms) == c.Get("ms").Num), "new Date(%s) = %d valid=%v, want %v", in, ms, valid, c.Get("ms"))
		iso, isoErr := date.ISOString()
		f.check((isoErr == nil) == (c.Get("iso").Kind == jsjson.String) && iso == text(c.Get("iso")), "toISOString(%s) = %q, want %v", in, iso, c.Get("iso"))
		f.check(date.JSON() == text(c.Get("json")), "JSON.stringify(%s) = %s", in, date.JSON())
		f.check(date.LuxonFormat() == text(c.Get("luxon")), "luxon(%s) = %q, want %q", in, date.LuxonFormat(), text(c.Get("luxon")))
		chDate, chErr := date.ClickhouseDate()
		f.check((chErr == nil) == (c.Get("ch").Kind == jsjson.String) && chDate == text(c.Get("ch")), "formatClickhouseDate(%s) = %q, want %v", in, chDate, c.Get("ch"))
	}
	t.Logf("%d supported, %d legacy date strings left to the Bun path", supported, unsupported)
	f.report()
}
