package events

import (
	"math"
	"strconv"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

func str(s string) *jsjson.Value { return jsjson.NewString(s) }

func num(n float64) *jsjson.Value { return jsjson.NewNumber(n) }

func optString(p *string) *jsjson.Value {
	if p == nil {
		return nil
	}
	return str(*p)
}

func optNumber(p *float64) *jsjson.Value {
	if p == nil {
		return nil
	}
	return num(*p)
}

// isNullish is value == null: undefined (nil) or null.
func isNullish(v *jsjson.Value) bool { return v == nil || v.Kind == jsjson.Null }

// isTruthy is JavaScript's ToBoolean.
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
	default:
		return true
	}
}

// orDefault is a ?? b.
func orDefault(v, fallback *jsjson.Value) *jsjson.Value {
	if isNullish(v) {
		return fallback
	}
	return v
}

// orElse is a || b.
func orElse(v, fallback *jsjson.Value) *jsjson.Value {
	if isTruthy(v) {
		return v
	}
	return fallback
}

// undefinedIfNull is v ?? undefined.
func undefinedIfNull(v *jsjson.Value) *jsjson.Value {
	if v != nil && v.Kind == jsjson.Null {
		return nil
	}
	return v
}

// isEmptyValue is ramda's isEmpty for the values JSON holds: "", [] and {}.
func isEmptyValue(v *jsjson.Value) bool {
	if v == nil {
		return false
	}
	switch v.Kind {
	case jsjson.String:
		return v.Str == ""
	case jsjson.Array:
		return len(v.Arr) == 0
	case jsjson.Object:
		return v.Obj.Len() == 0
	default:
		return false
	}
}

// stringOf is String(v).
func stringOf(v *jsjson.Value) string { return jsjson.JSString(v) }

// get reads a property of o; nil is undefined.
func get(o *jsjson.Obj, key string) *jsjson.Value {
	if o == nil {
		return nil
	}
	return o.Get(key)
}

// shallowCopy is {...o}.
func shallowCopy(o *jsjson.Obj) *jsjson.Obj {
	out := jsjson.NewObj()
	for _, key := range o.Keys() {
		out.Set(key, o.Get(key))
	}
	return out
}

// spreadInto is dst = {...dst, ...src} for a parsed JSON value: an object
// contributes its entries, an array its indexes, a string its UTF-16 units,
// anything else nothing.
func spreadInto(dst *jsjson.Obj, src *jsjson.Value) {
	if src == nil {
		return
	}
	switch src.Kind {
	case jsjson.Object:
		for _, key := range src.Obj.Keys() {
			dst.Set(key, src.Obj.Get(key))
		}
	case jsjson.Array:
		for i, item := range src.Arr {
			dst.Set(strconv.Itoa(i), item)
		}
	case jsjson.String:
		for i, unit := range jsport.ToUTF16(src.Str) {
			dst.Set(strconv.Itoa(i), str(jsport.FromUTF16([]uint16{unit})))
		}
	}
}

// asObject is the value as an object for property reads: a truthy non-object
// (a number, an array) has no properties, so it reads like {}.
func asObject(v *jsjson.Value) *jsjson.Obj {
	if v != nil && v.Kind == jsjson.Object {
		return v.Obj
	}
	return jsjson.NewObj()
}

// numberOf is the number a JavaScript arithmetic operator reads from a value:
// NaN for anything that is not a number.
func numberOf(v *jsjson.Value) float64 {
	if v != nil && v.Kind == jsjson.Number {
		return v.Num
	}
	return math.NaN()
}

// sameValueKey identifies a value for Set membership (SameValueZero).
func sameValueKey(v *jsjson.Value) string {
	if v == nil {
		return "u"
	}
	switch v.Kind {
	case jsjson.String:
		return "s" + v.Str
	case jsjson.Number:
		return "n" + jsjson.FormatNumber(v.Num)
	case jsjson.Bool:
		if v.Bool {
			return "bt"
		}
		return "bf"
	case jsjson.Null:
		return "l"
	default:
		text, _ := jsport.Stringify(v)
		return "o" + text
	}
}

// strictEqual is a === b for the values the payload holds: primitives by
// value, objects never equal unless they are the same one.
func strictEqual(a, b *jsjson.Value) bool {
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
	default:
		return a == b
	}
}

// stringify is JSON.stringify(v) as text; an undefined value has none.
func stringify(v *jsjson.Value) (string, error) { return jsport.Stringify(v) }
