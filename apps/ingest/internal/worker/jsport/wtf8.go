package jsport

import (
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	highSurrogateFirst = 0xD800
	highSurrogateLast  = 0xDBFF
	lowSurrogateFirst  = 0xDC00
	lowSurrogateLast   = 0xDFFF
	replacementRune    = 0xFFFD
	surrogateLead      = 0xED
	// The second byte of a surrogate's three-byte form is 0xA0-0xBF; 0xA0-0xAF
	// is a high surrogate, 0xB0-0xBF a low one.
	surrogateSecondMin = 0xA0
	lowSurrogateSecond = 0xB0
	continuationMask   = 0xC0
	continuationBits   = 0x80
)

func isSurrogate(r rune) bool { return r >= highSurrogateFirst && r <= lowSurrogateLast }

// decodeRune reads one code point of a WTF-8 string; an unpaired surrogate is
// returned as itself with size 3.
func decodeRune(s string) (rune, int) {
	if len(s) >= 3 && s[0] == surrogateLead && s[1] >= surrogateSecondMin && s[1]&continuationMask == continuationBits && s[2]&continuationMask == continuationBits {
		return rune(0xD000) | rune(s[1]&0x3F)<<6 | rune(s[2]&0x3F), 3
	}
	return utf8.DecodeRuneInString(s)
}

// decodeLastRune is decodeRune for the end of s.
func decodeLastRune(s string) (rune, int) {
	if len(s) >= 3 {
		tail := s[len(s)-3:]
		if tail[0] == surrogateLead && tail[1] >= surrogateSecondMin && tail[1]&continuationMask == continuationBits && tail[2]&continuationMask == continuationBits {
			return decodeRune(tail)
		}
	}
	return utf8.DecodeLastRuneInString(s)
}

// appendRune appends r, writing a surrogate in its three-byte form.
func appendRune(buf []byte, r rune) []byte {
	if isSurrogate(r) {
		return append(buf, surrogateLead, byte(0x80|(r>>6)&0x3F), byte(0x80|r&0x3F))
	}
	return utf8.AppendRune(buf, r)
}

// endsWithHighSurrogate and startsWithLowSurrogate detect the seam where two
// halves would have to be re-joined.
func endsWithHighSurrogate(b []byte) bool {
	n := len(b)
	return n >= 3 && b[n-3] == surrogateLead && b[n-2] >= surrogateSecondMin && b[n-2] < lowSurrogateSecond
}

func startsWithLowSurrogate(s string) bool {
	return len(s) >= 3 && s[0] == surrogateLead && s[1] >= lowSurrogateSecond && s[1] <= 0xBF
}

// appendJoined appends chunk to buf as JavaScript's string concatenation
// would: a high surrogate at the end of buf and a low one at the start of
// chunk become one character.
func appendJoined(buf []byte, chunk string) []byte {
	if len(chunk) > 0 && startsWithLowSurrogate(chunk) && endsWithHighSurrogate(buf) {
		high, _ := decodeRune(string(buf[len(buf)-3:]))
		low, size := decodeRune(chunk)
		buf = buf[:len(buf)-3]
		buf = utf8.AppendRune(buf, utf16.DecodeRune(high, low))
		return append(buf, chunk[size:]...)
	}
	return append(buf, chunk...)
}

// Join is a + b in JavaScript.
func Join(a, b string) string {
	if !startsWithLowSurrogate(b) || !endsWithHighSurrogate([]byte(a[max(0, len(a)-3):])) {
		return a + b
	}
	return string(appendJoined([]byte(a), b))
}

// appendUnit appends one UTF-16 code unit, pairing it with a preceding lone
// high surrogate.
func appendUnit(buf []byte, unit rune) []byte {
	if unit >= lowSurrogateFirst && unit <= lowSurrogateLast && endsWithHighSurrogate(buf) {
		high, _ := decodeRune(string(buf[len(buf)-3:]))
		buf = buf[:len(buf)-3]
		return utf8.AppendRune(buf, utf16.DecodeRune(high, unit))
	}
	return appendRune(buf, unit)
}

// Len16 is s.length: UTF-16 code units.
func Len16(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		switch b := s[i]; {
		case b&continuationMask == continuationBits:
		case b >= 0xF0:
			n += 2
		default:
			n++
		}
	}
	return n
}

// ToUTF16 returns the code units of s.
func ToUTF16(s string) []uint16 {
	units := make([]uint16, 0, len(s))
	for i := 0; i < len(s); {
		r, size := decodeRune(s[i:])
		i += size
		if r >= 0x10000 {
			high, low := utf16.EncodeRune(r)
			units = append(units, uint16(high), uint16(low))
			continue
		}
		units = append(units, uint16(r))
	}
	return units
}

// FromUTF16 builds the WTF-8 string of code units.
func FromUTF16(units []uint16) string {
	buf := make([]byte, 0, len(units))
	for _, unit := range units {
		buf = appendUnit(buf, rune(unit))
	}
	return string(buf)
}

// IsWellFormed is s.isWellFormed(): no unpaired surrogate.
func IsWellFormed(s string) bool {
	for i := 0; i+2 < len(s); i++ {
		if s[i] == surrogateLead && s[i+1] >= surrogateSecondMin {
			return false
		}
	}
	return true
}

// RemoveLoneSurrogates deletes unpaired surrogates (a no-op on well-formed text).
func RemoveLoneSurrogates(s string) string {
	if IsWellFormed(s) {
		return s
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); {
		r, size := decodeRune(s[i:])
		if !isSurrogate(r) {
			out = append(out, s[i:i+size]...)
		}
		i += size
	}
	return string(out)
}

// IsJSWhitespace reports ECMAScript WhiteSpace and LineTerminator, the set
// String.prototype.trim removes; it is not unicode.IsSpace (U+FEFF is in, U+0085 is out).
func IsJSWhitespace(r rune) bool {
	switch r {
	case 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0xA0, 0x1680, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF:
		return true
	}
	return r >= 0x2000 && r <= 0x200A
}

// Trim is s.trim().
func Trim(s string) string { return strings.TrimFunc(s, IsJSWhitespace) }

// TrimStart is s.trimStart().
func TrimStart(s string) string { return strings.TrimLeftFunc(s, IsJSWhitespace) }

// ToLower is s.toLowerCase(): full Unicode lower-casing, including the two
// context rules Go's strings.ToLower lacks (U+0130 becomes "i" plus a
// combining dot, and a sigma at the end of a word becomes the final form).
func ToLower(s string) string {
	if isASCII(s) {
		return strings.ToLower(s)
	}
	out := make([]byte, 0, len(s)+len(s)/4)
	for i := 0; i < len(s); {
		r, size := decodeRune(s[i:])
		switch {
		case isSurrogate(r):
			out = append(out, s[i:i+size]...)
		case r == 0x130:
			out = append(out, 'i', 0xCC, 0x87)
		case r == 0x3A3 && isFinalSigma(s, i, size):
			out = utf8.AppendRune(out, 0x3C2)
		default:
			out = utf8.AppendRune(out, unicode.ToLower(r))
		}
		i += size
	}
	return string(out)
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

var caseIgnorableExtra = map[rune]bool{
	0x27: true, 0x2E: true, 0x3A: true, 0xB7: true, 0x387: true, 0x55F: true, 0x5F4: true, 0x2018: true, 0x2019: true,
	0x2024: true, 0x2027: true, 0xFE13: true, 0xFE52: true, 0xFE55: true, 0xFF07: true, 0xFF0E: true, 0xFF1A: true,
}

func isCased(r rune) bool {
	return unicode.IsUpper(r) || unicode.IsLower(r) || unicode.IsTitle(r) ||
		unicode.Is(unicode.Other_Lowercase, r) || unicode.Is(unicode.Other_Uppercase, r)
}

func isCaseIgnorable(r rune) bool {
	return caseIgnorableExtra[r] || unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf, unicode.Lm, unicode.Sk)
}

// isFinalSigma implements the Final_Sigma condition of Unicode's SpecialCasing.
func isFinalSigma(s string, at, size int) bool {
	before := s[:at]
	precededByCased := false
	for len(before) > 0 {
		r, n := decodeLastRune(before)
		before = before[:len(before)-n]
		if isCaseIgnorable(r) {
			continue
		}
		precededByCased = isCased(r)
		break
	}
	if !precededByCased {
		return false
	}
	after := s[at+size:]
	for len(after) > 0 {
		r, n := decodeRune(after)
		after = after[n:]
		if isCaseIgnorable(r) {
			continue
		}
		return !isCased(r)
	}
	return true
}

// StripTrailingSlash is url.replace(/\/+$/, ”).
func StripTrailingSlash(s string) string { return strings.TrimRight(s, "/") }

// StripLeadingAndTrailingSlashes is url.replace(/^[/]+|[/]+$/g, ”).
func StripLeadingAndTrailingSlashes(s string) string { return strings.Trim(s, "/") }

// DecodeUTF8 is Buffer#toString(): invalid bytes become U+FFFD one per
// maximal invalid subpart (the WHATWG decoder), and a byte-order mark is kept.
func DecodeUTF8(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	out := make([]byte, 0, len(b)+8)
	for i := 0; i < len(b); {
		if b[i] < utf8.RuneSelf {
			out = append(out, b[i])
			i++
			continue
		}
		r, size := utf8.DecodeRune(b[i:])
		if r != utf8.RuneError || size > 1 {
			out = append(out, b[i:i+size]...)
			i += size
			continue
		}
		out = utf8.AppendRune(out, replacementRune)
		i += invalidPrefixLength(b[i:])
	}
	return string(out)
}

// DecodeUTF8StripBOM is TextDecoder#decode(): DecodeUTF8 without a leading byte-order mark.
func DecodeUTF8StripBOM(b []byte) string {
	if len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		b = b[3:]
	}
	return DecodeUTF8(b)
}

// invalidPrefixLength is the length of the maximal subpart of an ill-formed
// sequence at the start of b (Unicode's best practice, which the WHATWG
// decoder follows): the bytes of a valid prefix of some character, at least one.
func invalidPrefixLength(b []byte) int {
	first := b[0]
	var need int
	lower, upper := byte(0x80), byte(0xBF)
	switch {
	case first >= 0xC2 && first <= 0xDF:
		need = 1
	case first >= 0xE0 && first <= 0xEF:
		need = 2
		if first == 0xE0 {
			lower = 0xA0
		}
		if first == 0xED {
			upper = 0x9F
		}
	case first >= 0xF0 && first <= 0xF4:
		need = 3
		if first == 0xF0 {
			lower = 0x90
		}
		if first == 0xF4 {
			upper = 0x8F
		}
	default:
		return 1
	}
	n := 1
	for ; n <= need && n < len(b); n++ {
		if b[n] < lower || b[n] > upper {
			return n
		}
		lower, upper = 0x80, 0xBF
	}
	return n
}
