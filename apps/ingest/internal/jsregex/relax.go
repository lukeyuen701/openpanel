package jsregex

import "strings"

// relax rewrites a JavaScript pattern into one that matches a superset of
// its matches and that Go's engine may accept: every lookaround group is
// dropped (removing a constraint can only widen the match) and every
// backreference becomes "anything". It reports false for syntax it does not
// understand, in which case no prefilter is used.
func relax(source string) (string, bool) {
	var b strings.Builder
	changed := false
	for i := 0; i < len(source); i++ {
		c := source[i]
		switch {
		case c == '\\':
			if i+1 >= len(source) {
				return "", false
			}
			next := source[i+1]
			if next >= '1' && next <= '9' {
				b.WriteString("(?:.*)")
				changed = true
				i++
				for i+1 < len(source) && source[i+1] >= '0' && source[i+1] <= '9' {
					i++
				}
				continue
			}
			b.WriteByte(c)
			b.WriteByte(next)
			i++
		case c == '[':
			end := classEnd(source, i)
			if end < 0 {
				return "", false
			}
			b.WriteString(source[i : end+1])
			i = end
		case c == '(' && isLookaround(source[i:]):
			end := groupEnd(source, i)
			if end < 0 {
				return "", false
			}
			changed = true
			i = skipQuantifier(source, end)
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), changed
}

// skipQuantifier returns the index of the last character of a quantifier
// following position end (or end itself when there is none). A quantified
// lookaround is still zero-width, so it goes with the group.
func skipQuantifier(source string, end int) int {
	i := end + 1
	if i >= len(source) {
		return end
	}
	switch source[i] {
	case '*', '+', '?':
	case '{':
		close := strings.IndexByte(source[i:], '}')
		if close < 0 {
			return end
		}
		i += close
	default:
		return end
	}
	if i+1 < len(source) && source[i+1] == '?' {
		i++
	}
	return i
}

func isLookaround(s string) bool {
	return strings.HasPrefix(s, "(?=") || strings.HasPrefix(s, "(?!") ||
		strings.HasPrefix(s, "(?<=") || strings.HasPrefix(s, "(?<!")
}

// classEnd returns the index of the ] closing the class opened at start.
func classEnd(source string, start int) int {
	i := start + 1
	if i < len(source) && source[i] == '^' {
		i++
	}
	// A ] first in a JavaScript class closes an empty class, unlike POSIX.
	for ; i < len(source); i++ {
		switch source[i] {
		case '\\':
			i++
		case ']':
			return i
		}
	}
	return -1
}

// groupEnd returns the index of the ) closing the group opened at start.
func groupEnd(source string, start int) int {
	depth := 0
	for i := start; i < len(source); i++ {
		switch source[i] {
		case '\\':
			i++
		case '[':
			end := classEnd(source, i)
			if end < 0 {
				return -1
			}
			i = end
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}
