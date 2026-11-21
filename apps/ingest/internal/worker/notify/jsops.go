package notify

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"

	"openpanel/ingest/internal/jsjson"
)

// isJSWhitespace is ECMAScript WhiteSpace plus LineTerminator, the set
// String.prototype.trim and the regexp class \s use. unicode.IsSpace differs
// (it has U+0085 and lacks U+FEFF).
func isJSWhitespace(r rune) bool {
	switch {
	case r >= 0x09 && r <= 0x0d, r == 0x20, r == 0xa0, r == 0x1680,
		r >= 0x2000 && r <= 0x200a, r == 0x2028, r == 0x2029,
		r == 0x202f, r == 0x205f, r == 0x3000, r == 0xfeff:
		return true
	}
	return false
}

func jsTrim(s string) string { return strings.TrimFunc(s, isJSWhitespace) }

var decimalLiteral = regexp.MustCompile(`^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

// jsNumberFromString is Number(string): StringToNumber with the whitespace
// trim, empty as 0, the 0x/0o/0b literals (unsigned only) and Infinity.
func jsNumberFromString(s string) float64 {
	s = jsTrim(s)
	if s == "" {
		return 0
	}
	if len(s) > 2 && s[0] == '0' {
		base := 0
		switch s[1] {
		case 'x', 'X':
			base = 16
		case 'o', 'O':
			base = 8
		case 'b', 'B':
			base = 2
		}
		if base != 0 {
			return parseRadix(s[2:], base)
		}
	}
	switch s {
	case "Infinity", "+Infinity":
		return math.Inf(1)
	case "-Infinity":
		return math.Inf(-1)
	}
	if !decimalLiteral.MatchString(s) {
		return math.NaN()
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil && !math.IsInf(f, 0) {
		return math.NaN()
	}
	return f
}

func parseRadix(digits string, base int) float64 {
	result := 0.0
	for _, c := range digits {
		d := strings.IndexRune("0123456789abcdef", c|0x20)
		if c < '0' || d < 0 || d >= base {
			return math.NaN()
		}
		result = result*float64(base) + float64(d)
	}
	return result
}

// jsNumber is Number(value) for the JSON value kinds.
func jsNumber(v *jsjson.Value) float64 {
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
	}
	return jsNumberFromString(jsjson.JSString(v))
}

// isTruthy is JavaScript truthiness of a JSON value; nil is undefined.
func isTruthy(v *jsjson.Value) bool {
	if v == nil {
		return false
	}
	switch v.Kind {
	case jsjson.Null:
		return false
	case jsjson.Bool:
		return v.Bool
	case jsjson.Number:
		return v.Num != 0 && !math.IsNaN(v.Num)
	case jsjson.String:
		return v.Str != ""
	}
	return true
}

// strictEquals is ===. Arrays and objects compare by identity.
func strictEquals(a, b *jsjson.Value) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case jsjson.Null:
		return true
	case jsjson.Bool:
		return a.Bool == b.Bool
	case jsjson.Number:
		return a.Num == b.Num
	case jsjson.String:
		return a.Str == b.Str
	}
	return a == b
}

// isDatePath names the payload members that are Date objects in the
// TypeScript payload (createdAt, and the profile's two dates): the tree
// carries them as ISO strings, so a path alone says a value is a Date.
func isDatePath(path []string) bool {
	switch len(path) {
	case 1:
		return path[0] == "createdAt"
	case 2:
		return path[0] == "profile" && (path[1] == "createdAt" || path[1] == "lastSeenAt")
	}
	return false
}

// found is what a path lookup reached.
type found struct {
	// value is nil for undefined.
	value *jsjson.Value
	// isDate marks a Date, even an invalid one that serialises as null.
	isDate bool
	// isBuiltin marks a name that resolves through the prototype chain to a
	// function (constructor, toString, trim, ...) or Object.prototype.
	isBuiltin bool
}

// lookup is ramda's path over the payload: nil is undefined. A step past a
// Date or a builtin is not modelled and reads as undefined.
func lookup(payload *jsjson.Value, path []string) found {
	current := payload
	for i, key := range path {
		if current == nil || current.Kind == jsjson.Null {
			return found{}
		}
		if i > 0 && isDatePath(path[:i]) {
			return found{}
		}
		next, builtin := member(current, key)
		if builtin {
			return found{isBuiltin: i == len(path)-1}
		}
		current = next
	}
	if current != nil && isDatePath(path) {
		return found{value: current, isDate: true}
	}
	return found{value: current}
}

// member is value[key]: own properties, array indexes and length, then the
// names the prototype chain supplies.
func member(v *jsjson.Value, key string) (value *jsjson.Value, builtin bool) {
	switch v.Kind {
	case jsjson.Object:
		if v.Obj.Has(key) {
			return v.Obj.Get(key), false
		}
		return nil, inPrototype(objectPrototype, key)
	case jsjson.Array:
		if key == "length" {
			return jsjson.NewNumber(float64(len(v.Arr))), false
		}
		if i, ok := canonicalIndex(key); ok && i < len(v.Arr) {
			return v.Arr[i], false
		}
		return nil, inPrototype(arrayPrototype, key)
	case jsjson.String:
		units := utf16.Encode([]rune(v.Str))
		if key == "length" {
			return jsjson.NewNumber(float64(len(units))), false
		}
		if i, ok := canonicalIndex(key); ok && i < len(units) {
			return jsjson.NewString(string(utf16.Decode(units[i : i+1]))), false
		}
		return nil, inPrototype(stringPrototype, key)
	case jsjson.Number:
		return nil, inPrototype(numberPrototype, key)
	case jsjson.Bool:
		return nil, inPrototype(booleanPrototype, key)
	}
	return nil, false
}

// Own property names of the prototypes, from the JavaScript engine; a name
// not set on the value itself resolves to them. Array, string, number and
// boolean values also inherit Object.prototype's.
var (
	objectPrototype  = nameSet("__defineGetter__ __defineSetter__ __lookupGetter__ __lookupSetter__ __proto__ constructor hasOwnProperty isPrototypeOf propertyIsEnumerable toLocaleString toString valueOf")
	arrayPrototype   = nameSet("at concat copyWithin entries every fill filter find findIndex findLast findLastIndex flat flatMap forEach includes indexOf join keys lastIndexOf map pop push reduce reduceRight reverse shift slice some sort splice toReversed toSorted toSpliced unshift values with")
	stringPrototype  = nameSet("anchor at big blink bold charAt charCodeAt codePointAt concat endsWith fixed fontcolor fontsize includes indexOf isWellFormed italics lastIndexOf link localeCompare match matchAll normalize padEnd padStart repeat replace replaceAll search slice small split startsWith strike sub substr substring sup toLocaleLowerCase toLocaleUpperCase toLowerCase toUpperCase toWellFormed trim trimEnd trimLeft trimRight trimStart")
	numberPrototype  = nameSet("toExponential toFixed toPrecision")
	booleanPrototype = nameSet("")
)

func nameSet(names string) map[string]bool {
	set := map[string]bool{}
	for _, name := range strings.Fields(names) {
		set[name] = true
	}
	return set
}

func inPrototype(own map[string]bool, key string) bool {
	return own[key] || objectPrototype[key]
}

func canonicalIndex(key string) (int, bool) {
	if key == "" || (len(key) > 1 && key[0] == '0') || len(key) > 9 {
		return 0, false
	}
	n := 0
	for _, c := range key {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// pathOr is pathOr(”, path, payload): null and undefined fall back to ”.
func pathOr(payload *jsjson.Value, path []string) found {
	result := lookup(payload, path)
	if result.isBuiltin || result.isDate {
		return result
	}
	if result.value == nil || result.value.Kind == jsjson.Null {
		return found{value: jsjson.NewString("")}
	}
	return result
}
