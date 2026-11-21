package jsport

import (
	"math"
	"strconv"
	"strings"

	"openpanel/ingest/internal/jsjson"
)

const infinityWord = "Infinity"

// NumberToString is String(x): ECMAScript Number::toString, with NaN and
// the infinities spelled out.
func NumberToString(x float64) string { return jsjson.JSString(jsjson.NewNumber(x)) }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// ParseFloat is parseFloat(s): the longest decimal prefix after leading
// whitespace, NaN when there is none. Hex, binary and octal are not read.
func ParseFloat(s string) float64 {
	s = TrimStart(s)
	end := decimalPrefixLength(s)
	if end == 0 {
		return math.NaN()
	}
	return parseDecimal(s[:end])
}

// decimalPrefixLength is the length of the StrDecimalLiteral that starts s,
// 0 when none does.
func decimalPrefixLength(s string) int {
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	if strings.HasPrefix(s[i:], infinityWord) {
		return i + len(infinityWord)
	}
	digitsStart := i
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	integerDigits := i - digitsStart
	fractionDigits := 0
	if i < len(s) && s[i] == '.' {
		j := i + 1
		for j < len(s) && isDigit(s[j]) {
			j++
		}
		fractionDigits = j - i - 1
		if integerDigits > 0 || fractionDigits > 0 {
			i = j
		}
	}
	if integerDigits == 0 && fractionDigits == 0 {
		return 0
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		exponentStart := j
		for j < len(s) && isDigit(s[j]) {
			j++
		}
		if j > exponentStart {
			i = j
		}
	}
	return i
}

func parseDecimal(literal string) float64 {
	negative := strings.HasPrefix(literal, "-")
	if strings.HasSuffix(literal, infinityWord) {
		if negative {
			return math.Inf(-1)
		}
		return math.Inf(1)
	}
	n, err := strconv.ParseFloat(literal, 64)
	if err != nil && !isRangeError(err) {
		return math.NaN()
	}
	return n
}

func isRangeError(err error) bool {
	numErr, ok := err.(*strconv.NumError)
	return ok && numErr.Err == strconv.ErrRange
}

// ToNumber is Number(s) for a string: whitespace is trimmed, the empty string
// is 0, 0x/0o/0b literals and Infinity are read, and anything with trailing
// text is NaN.
func ToNumber(s string) float64 {
	s = Trim(s)
	if s == "" {
		return 0
	}
	if len(s) > 2 && s[0] == '0' {
		if base := radixOfPrefix(s[1]); base != 0 {
			return parseRadix(s[2:], base)
		}
	}
	end := decimalPrefixLength(s)
	if end != len(s) {
		return math.NaN()
	}
	return parseDecimal(s)
}

func radixOfPrefix(marker byte) int {
	switch marker {
	case 'x', 'X':
		return 16
	case 'o', 'O':
		return 8
	case 'b', 'B':
		return 2
	}
	return 0
}

// parseRadix reads the digits of a 0x/0o/0b literal the way JavaScriptCore
// does: summed from the last digit with a floating-point place value, so a
// literal beyond 2^53 can differ from the correctly rounded value by an ulp.
func parseRadix(digits string, base int) float64 {
	if digits == "" {
		return math.NaN()
	}
	number, placeValue := 0.0, 1.0
	for i := len(digits) - 1; i >= 0; i-- {
		digit := digitValue(digits[i])
		if digit < 0 || digit >= base {
			return math.NaN()
		}
		if math.IsInf(placeValue, 1) {
			if digit != 0 {
				number = math.Inf(1)
			}
		} else {
			number += float64(digit) * placeValue
		}
		placeValue *= float64(base)
	}
	return number
}

func digitValue(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'z':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'Z':
		return int(c-'A') + 10
	}
	return -1
}

// ToNumberValue is Number(v) for a parsed JSON value: null is 0, booleans 0 or
// 1, an array its comma-joined string (so [5] is 5 and [1,2] is NaN), an
// object NaN, and nil (undefined) NaN.
func ToNumberValue(v *jsjson.Value) float64 {
	if v == nil {
		return math.NaN()
	}
	switch v.Kind {
	case jsjson.Null:
		return 0
	case jsjson.Bool:
		if v.Bool {
			return 1
		}
		return 0
	case jsjson.Number:
		return v.Num
	case jsjson.String:
		return ToNumber(v.Str)
	case jsjson.Array:
		return ToNumber(jsjson.JSString(v))
	}
	return math.NaN()
}

const byteModulus = 256

// ToUint8 is the conversion Uint8Array.from applies to each element: NaN and
// infinities are 0, the rest truncates toward zero and wraps modulo 256.
func ToUint8(x float64) byte {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0
	}
	wrapped := math.Mod(math.Trunc(x), byteModulus)
	if wrapped < 0 {
		wrapped += byteModulus
	}
	return byte(wrapped)
}
