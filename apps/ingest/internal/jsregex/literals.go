package jsregex

import (
	"regexp/syntax"
	"strings"
	"unicode/utf8"
)

// literalFilter is a necessary condition for a match: any match contains at
// least one of the literals (compared lowercased when fold is set). It lets a
// pattern be skipped with substring searches instead of running an engine.
type literalFilter struct {
	literals []string
	fold     bool
}

// mayMatch reports whether s contains one of the literals; lower is s
// lowercased, computed by the caller once per input.
func (f *literalFilter) mayMatch(s, lower string) bool {
	haystack := s
	if f.fold {
		haystack = lower
	}
	for _, literal := range f.literals {
		if strings.Contains(haystack, literal) {
			return true
		}
	}
	return false
}

const minUsefulLiteral = 2

// buildLiteralFilter derives the filter from Go-syntax source; nil when no
// useful literal set exists.
func buildLiteralFilter(goSource string) *literalFilter {
	re, err := syntax.Parse(goSource, syntax.Perl)
	if err != nil {
		return nil
	}
	set, ok := necessaryLiterals(re.Simplify())
	if !ok || len(set.literals) == 0 {
		return nil
	}
	for _, literal := range set.literals {
		if len(literal) < minUsefulLiteral {
			return nil
		}
	}
	return &literalFilter{literals: dedupe(set.literals), fold: set.fold}
}

type literalSet struct {
	literals []string
	fold     bool
}

func (s literalSet) shortest() int {
	shortest := -1
	for _, literal := range s.literals {
		if shortest < 0 || len(literal) < shortest {
			shortest = len(literal)
		}
	}
	return shortest
}

func necessaryLiterals(re *syntax.Regexp) (literalSet, bool) {
	switch re.Op {
	case syntax.OpLiteral:
		text := string(re.Rune)
		if !isPrintableASCIIString(text) {
			return literalSet{}, false
		}
		fold := re.Flags&syntax.FoldCase != 0
		if fold {
			text = strings.ToLower(text)
		}
		return literalSet{literals: []string{text}, fold: fold}, true
	case syntax.OpCapture:
		return necessaryLiterals(re.Sub[0])
	case syntax.OpPlus:
		return necessaryLiterals(re.Sub[0])
	case syntax.OpRepeat:
		if re.Min >= 1 {
			return necessaryLiterals(re.Sub[0])
		}
		return literalSet{}, false
	case syntax.OpConcat:
		var best literalSet
		found := false
		for _, sub := range re.Sub {
			set, ok := necessaryLiterals(sub)
			if !ok {
				continue
			}
			if !found || set.shortest() > best.shortest() {
				best = set
				found = true
			}
		}
		return best, found
	case syntax.OpAlternate:
		union := literalSet{}
		for i, sub := range re.Sub {
			set, ok := necessaryLiterals(sub)
			if !ok {
				return literalSet{}, false
			}
			if i == 0 {
				union.fold = set.fold
			} else if set.fold != union.fold {
				// Mixed case sensitivity: compare everything lowercased,
				// which can only widen the match.
				union.fold = true
			}
			union.literals = append(union.literals, set.literals...)
		}
		if union.fold {
			for i := range union.literals {
				union.literals[i] = strings.ToLower(union.literals[i])
			}
		}
		return union, true
	}
	return literalSet{}, false
}

func isPrintableASCIIString(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	return printableASCII(s)
}

func dedupe(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := values[:0]
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
