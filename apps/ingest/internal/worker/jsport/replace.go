package jsport

import "strings"

// ReplaceAll is s.replaceAll(search, replacement) with a string pattern. The
// replacement is not literal: $$, $&, $` and $' are expanded as String.prototype.replace
// does; $1 and $<name> stay as written because a string pattern has no groups.
func ReplaceAll(s, search, replacement string) string {
	if search == "" || !IsWellFormed(search) {
		return replaceAllUnits(s, search, replacement)
	}
	var out []byte
	position := 0
	for {
		found := strings.Index(s[position:], search)
		if found < 0 {
			break
		}
		start := position + found
		end := start + len(search)
		out = appendJoined(out, s[position:start])
		out = appendSubstitution(out, replacement, s, start, end)
		position = end
	}
	if position == 0 {
		return s
	}
	return string(appendJoined(out, s[position:]))
}

func appendSubstitution(out []byte, replacement, s string, start, end int) []byte {
	runStart := 0
	for i := 0; i+1 < len(replacement); i++ {
		if replacement[i] != '$' {
			continue
		}
		var piece string
		switch replacement[i+1] {
		case '$':
			piece = "$"
		case '&':
			piece = s[start:end]
		case '`':
			piece = s[:start]
		case '\'':
			piece = s[end:]
		default:
			continue
		}
		out = appendJoined(out, replacement[runStart:i])
		out = appendJoined(out, piece)
		i++
		runStart = i + 1
	}
	return appendJoined(out, replacement[runStart:])
}

// replaceAllUnits works on UTF-16 code units for the two patterns whose
// matches need not fall on character boundaries: the empty string (between
// every unit) and one holding a lone surrogate.
func replaceAllUnits(s, search, replacement string) string {
	subject := ToUTF16(s)
	pattern := ToUTF16(search)
	template := ToUTF16(replacement)
	advance := max(1, len(pattern))
	var out []uint16
	position := 0
	for start := indexUnits(subject, pattern, 0); start >= 0; start = indexUnits(subject, pattern, position) {
		end := start + len(pattern)
		out = append(out, subject[position:start]...)
		out = appendSubstitutionUnits(out, template, subject, start, end)
		position = end
		if len(pattern) == 0 {
			if start >= len(subject) {
				return FromUTF16(out)
			}
			out = append(out, subject[start])
			position = start + advance
		}
	}
	out = append(out, subject[position:]...)
	return FromUTF16(out)
}

func indexUnits(subject, pattern []uint16, from int) int {
	for i := from; i+len(pattern) <= len(subject); i++ {
		match := true
		for j, unit := range pattern {
			if subject[i+j] != unit {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func appendSubstitutionUnits(out, template, subject []uint16, start, end int) []uint16 {
	for i := 0; i < len(template); i++ {
		if template[i] != '$' || i+1 == len(template) {
			out = append(out, template[i])
			continue
		}
		switch template[i+1] {
		case '$':
			out = append(out, '$')
		case '&':
			out = append(out, subject[start:end]...)
		case '`':
			out = append(out, subject[:start]...)
		case '\'':
			out = append(out, subject[end:]...)
		default:
			out = append(out, '$')
			continue
		}
		i++
	}
	return out
}
