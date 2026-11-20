package track

import "strings"

// isJSSpace is the set String.prototype.trim removes.
func isJSSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xA0, 0x1680, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF:
		return true
	}
	return r >= 0x2000 && r <= 0x200A
}

// trimJS is String.prototype.trim.
func trimJS(s string) string { return strings.TrimFunc(s, isJSSpace) }
