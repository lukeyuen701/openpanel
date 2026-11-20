package jsjson

import (
	"math"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Stringify writes v exactly as JSON.stringify(v) does.
func Stringify(v *Value) []byte {
	var b strings.Builder
	writeValue(&b, v)
	return []byte(b.String())
}

// StringifyString is JSON.stringify for one string.
func StringifyString(s string) string {
	var b strings.Builder
	writeString(&b, s)
	return b.String()
}

func writeValue(b *strings.Builder, v *Value) {
	switch v.Kind {
	case Null:
		b.WriteString("null")
	case Bool:
		if v.Bool {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case Number:
		if math.IsNaN(v.Num) || math.IsInf(v.Num, 0) {
			b.WriteString("null")
			return
		}
		b.WriteString(FormatNumber(v.Num))
	case String:
		writeString(b, v.Str)
	case Array:
		b.WriteByte('[')
		for i, item := range v.Arr {
			if i > 0 {
				b.WriteByte(',')
			}
			writeValue(b, item)
		}
		b.WriteByte(']')
	case Object:
		b.WriteByte('{')
		for i, key := range v.Obj.Keys() {
			if i > 0 {
				b.WriteByte(',')
			}
			writeString(b, key)
			b.WriteByte(':')
			writeValue(b, v.Obj.Get(key))
		}
		b.WriteByte('}')
	}
}

const hexDigits = "0123456789abcdef"

func writeString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString("\\\"")
		case '\\':
			b.WriteString("\\\\")
		case '\b':
			b.WriteString("\\b")
		case '\f':
			b.WriteString("\\f")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		default:
			if r < 0x20 {
				b.WriteString("\\u00")
				b.WriteByte(hexDigits[r>>4])
				b.WriteByte(hexDigits[r&0xF])
				continue
			}
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}

// numberToString is the template-literal form of any number, Infinity and
// NaN included.
func numberToString(x float64) string {
	switch {
	case math.IsNaN(x):
		return "NaN"
	case math.IsInf(x, 1):
		return "Infinity"
	case math.IsInf(x, -1):
		return "-Infinity"
	}
	return FormatNumber(x)
}

// FormatNumber is ECMAScript's Number::toString(x) for a finite x: the
// shortest round-trip digits, positional from 1e-7 up to 1e21 and
// exponential outside it.
func FormatNumber(x float64) string {
	if x == 0 {
		return "0"
	}
	if x < 0 {
		return "-" + FormatNumber(-x)
	}
	// "d.ddddde±XX": the shortest digits and the decimal exponent.
	e := strconv.FormatFloat(x, 'e', -1, 64)
	mantissa, exponent, _ := strings.Cut(e, "e")
	digits := strings.Replace(mantissa, ".", "", 1)
	exp, _ := strconv.Atoi(exponent)
	k := len(digits)
	n := exp + 1

	switch {
	case k <= n && n <= 21:
		return digits + strings.Repeat("0", n-k)
	case 0 < n && n <= 21:
		return digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		return "0." + strings.Repeat("0", -n) + digits
	}
	sign := "+"
	if n-1 < 0 {
		sign = "-"
	}
	abs := n - 1
	if abs < 0 {
		abs = -abs
	}
	if k == 1 {
		return digits + "e" + sign + strconv.Itoa(abs)
	}
	return digits[:1] + "." + digits[1:] + "e" + sign + strconv.Itoa(abs)
}

// CompareUTF16 orders strings the way Array.prototype.sort does by default:
// by UTF-16 code units, not by code points or bytes.
func CompareUTF16(a, b string) int {
	for a != "" && b != "" {
		ra, sa := utf8.DecodeRuneInString(a)
		rb, sb := utf8.DecodeRuneInString(b)
		if ra != rb {
			ua := firstUnit(ra)
			ub := firstUnit(rb)
			if ua != ub {
				if ua < ub {
					return -1
				}
				return 1
			}
			// Same high surrogate: the low surrogates decide.
			_, la := utf16.EncodeRune(ra)
			_, lb := utf16.EncodeRune(rb)
			if la < lb {
				return -1
			}
			return 1
		}
		a = a[sa:]
		b = b[sb:]
	}
	switch {
	case a == "" && b == "":
		return 0
	case a == "":
		return -1
	default:
		return 1
	}
}

func firstUnit(r rune) rune {
	if r >= 0x10000 {
		high, _ := utf16.EncodeRune(r)
		return high
	}
	return r
}
