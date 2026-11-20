// Package jsregex runs JavaScript regular expressions: the bot list and
// ua-parser-js's tables are written for V8/JavaScriptCore, with lookbehind,
// lookahead and backreferences that Go's regexp cannot express.
package jsregex

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf16"

	"github.com/dlclark/regexp2"
)

// Regexp is one compiled JavaScript pattern with its flags.
type Regexp struct {
	source string
	global bool
	re     *regexp2.Regexp
	// fast is the same pattern under Go's linear-time engine, when it accepts
	// the syntax. It is only consulted for printable-ASCII input, where the
	// two engines cannot disagree: case folding, \s, . and \b differ only
	// outside that range. fast_test.go holds both engines to the same answers.
	fast *regexp.Regexp
	// prefilter is a pattern that matches everything this one matches (its
	// lookarounds and backreferences relaxed), for patterns with no fast
	// form: when it misses on printable ASCII, the slow engine is skipped.
	prefilter *regexp.Regexp
	// literals is a substring check every match passes, from the fast or
	// relaxed form; it rules most patterns out before any engine runs.
	literals *literalFilter
}

// MustCompile compiles a pattern given the way JavaScript writes it: source
// and flags (only "i" and "g" occur in the tables this package serves).
func MustCompile(source, flags string) *Regexp {
	re, err := Compile(source, flags)
	if err != nil {
		panic(err)
	}
	return re
}

// Compile is MustCompile returning the error.
func Compile(source, flags string) (*Regexp, error) {
	options := regexp2.RegexOptions(regexp2.ECMAScript)
	global := false
	for _, flag := range flags {
		switch flag {
		case 'i':
			options |= regexp2.IgnoreCase
		case 'g':
			global = true
		default:
			return nil, fmt.Errorf("jsregex: unsupported flag %q in /%s/%s", flag, source, flags)
		}
	}
	re, err := regexp2.Compile(source, options)
	if err != nil {
		return nil, fmt.Errorf("jsregex: compile /%s/%s: %w", source, flags, err)
	}
	out := &Regexp{source: source, global: global, re: re}
	if !disableFast {
		prefix := ""
		if options&regexp2.IgnoreCase != 0 {
			prefix = "(?i)"
		}
		if fast, err := regexp.Compile(prefix + source); err == nil {
			out.fast = fast
			out.literals = buildLiteralFilter(prefix + source)
		} else if superset, ok := relax(source); ok {
			if prefilter, err := regexp.Compile(prefix + superset); err == nil {
				out.prefilter = prefilter
				out.literals = buildLiteralFilter(prefix + superset)
			}
		}
	}
	return out, nil
}

// ruledOut reports that the literal filter proves s cannot match.
func (r *Regexp) ruledOut(s string) bool {
	if r.literals == nil || !printableASCII(s) {
		return false
	}
	return !r.literals.mayMatch(s, lowerASCII(s))
}

// lowerASCII lowercases ASCII letters. Inputs repeat across hundreds of
// patterns, so the last conversion is kept.
func lowerASCII(s string) string {
	if cached := lastLower.Load(); cached != nil && cached[0] == s {
		return cached[1]
	}
	lower := strings.ToLower(s)
	lastLower.Store(&[2]string{s, lower})
	return lower
}

var lastLower atomic.Pointer[[2]string]

// disableFast is flipped by tests that compare the engines.
var disableFast = false

// printableASCII reports whether every byte is in 0x20..0x7E.
func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7E {
			return false
		}
	}
	return true
}

// Source is the pattern as written.
func (r *Regexp) Source() string { return r.source }

// Test is RegExp.prototype.test for a non-global pattern.
func (r *Regexp) Test(s string) bool {
	if r.ruledOut(s) {
		return false
	}
	if r.fast != nil && printableASCII(s) {
		return r.fast.MatchString(s)
	}
	if r.prefilter != nil && printableASCII(s) && !r.prefilter.MatchString(s) {
		return false
	}
	ok, err := r.re.MatchString(s)
	return err == nil && ok
}

// Exec is RegExp.prototype.exec for a non-global pattern: nil when nothing
// matched, otherwise the whole match followed by each group, with nil for a
// group that did not participate (undefined in JavaScript).
func (r *Regexp) Exec(s string) []*string {
	if r.ruledOut(s) {
		return nil
	}
	if r.fast != nil && printableASCII(s) {
		return r.fastExec(s)
	}
	if r.prefilter != nil && printableASCII(s) && !r.prefilter.MatchString(s) {
		return nil
	}
	m, err := r.re.FindStringMatch(s)
	if err != nil || m == nil {
		return nil
	}
	return groupsOf(m)
}

func (r *Regexp) fastExec(s string) []*string {
	loc := r.fast.FindStringSubmatchIndex(s)
	if loc == nil {
		return nil
	}
	out := make([]*string, len(loc)/2)
	for i := range out {
		if loc[2*i] < 0 {
			continue
		}
		text := s[loc[2*i]:loc[2*i+1]]
		out[i] = &text
	}
	return out
}

func groupsOf(m *regexp2.Match) []*string {
	groups := m.Groups()
	out := make([]*string, len(groups))
	for i := range groups {
		g := &groups[i]
		if len(g.Captures) == 0 {
			continue
		}
		text := g.String()
		out[i] = &text
	}
	return out
}

// Replace is String.prototype.replace(regex, replacement): the first match, or
// every match for a /g pattern, with $&, $1..$99 and $$ expanded.
func (r *Regexp) Replace(s, replacement string) string {
	var b strings.Builder
	last := 0
	m, err := r.re.FindStringMatch(s)
	for err == nil && m != nil {
		start := runeOffsetToByte(s, m.Index)
		end := runeOffsetToByte(s, m.Index+m.Length)
		b.WriteString(s[last:start])
		expand(&b, replacement, groupsOf(m))
		last = end
		if !r.global {
			break
		}
		m, err = r.re.FindNextMatch(m)
	}
	b.WriteString(s[last:])
	return b.String()
}

// runeOffsetToByte converts regexp2's rune index into a byte index.
func runeOffsetToByte(s string, runeIndex int) int {
	if runeIndex <= 0 {
		return 0
	}
	count := 0
	for i := range s {
		if count == runeIndex {
			return i
		}
		count++
	}
	return len(s)
}

func expand(b *strings.Builder, replacement string, groups []*string) {
	for i := 0; i < len(replacement); i++ {
		c := replacement[i]
		if c != '$' || i+1 >= len(replacement) {
			b.WriteByte(c)
			continue
		}
		next := replacement[i+1]
		switch {
		case next == '$':
			b.WriteByte('$')
			i++
		case next == '&':
			if groups[0] != nil {
				b.WriteString(*groups[0])
			}
			i++
		case next >= '0' && next <= '9':
			// Two digits when that names a group, else one, as ECMAScript's
			// GetSubstitution reads them.
			consumed := 1
			index, _ := strconv.Atoi(string(next))
			if i+2 < len(replacement) && replacement[i+2] >= '0' && replacement[i+2] <= '9' {
				twoDigit, _ := strconv.Atoi(replacement[i+1 : i+3])
				if twoDigit >= 1 && twoDigit < len(groups) {
					index = twoDigit
					consumed = 2
				}
			}
			if index < 1 || index >= len(groups) {
				b.WriteByte('$')
				continue
			}
			if groups[index] != nil {
				b.WriteString(*groups[index])
			}
			i += consumed
		default:
			b.WriteByte('$')
		}
	}
}

// SliceUTF16 is String.prototype.slice(0, n): the first n UTF-16 code units.
// A surrogate pair the cut would split is dropped whole.
func SliceUTF16(s string, n int) string {
	units := 0
	for i, r := range s {
		width := 1
		if r >= 0x10000 {
			width = 2
		}
		if units+width > n {
			return s[:i]
		}
		units += width
	}
	return s
}

// LenUTF16 is String.prototype.length.
func LenUTF16(s string) int {
	n := 0
	for _, r := range s {
		n += len(utf16.Encode([]rune{r}))
	}
	return n
}
