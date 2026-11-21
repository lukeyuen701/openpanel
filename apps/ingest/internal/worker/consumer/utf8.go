package consumer

import (
	"strings"
	"unicode/utf8"
)

const replacement = "\xef\xbf\xbd"

// decodeText is Buffer#toString(): UTF-8 where every maximal invalid
// subpart becomes one U+FFFD (the WHATWG decoder), which Go's own
// conversion does per byte.
func decodeText(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	var out strings.Builder
	out.Grow(len(b))
	for i := 0; i < len(b); {
		c := b[i]
		if c < utf8.RuneSelf {
			out.WriteByte(c)
			i++
			continue
		}
		follow, lower, upper, ok := continuation(c)
		if !ok {
			out.WriteString(replacement)
			i++
			continue
		}
		j := i + 1
		complete := true
		for ; follow > 0; follow-- {
			if j >= len(b) || b[j] < lower || b[j] > upper {
				complete = false
				break
			}
			lower, upper = 0x80, 0xBF
			j++
		}
		if complete {
			out.Write(b[i:j])
		} else {
			out.WriteString(replacement)
		}
		i = j
	}
	return out.String()
}

// continuation gives, for a lead byte, how many continuation bytes follow
// and the range the first of them may take.
func continuation(lead byte) (follow int, lower, upper byte, ok bool) {
	lower, upper = 0x80, 0xBF
	switch {
	case lead >= 0xC2 && lead <= 0xDF:
		return 1, lower, upper, true
	case lead == 0xE0:
		return 2, 0xA0, upper, true
	case lead == 0xED:
		return 2, lower, 0x9F, true
	case lead >= 0xE1 && lead <= 0xEF:
		return 2, lower, upper, true
	case lead == 0xF0:
		return 3, 0x90, upper, true
	case lead >= 0xF1 && lead <= 0xF3:
		return 3, lower, upper, true
	case lead == 0xF4:
		return 3, lower, 0x8F, true
	}
	return 0, 0, 0, false
}
