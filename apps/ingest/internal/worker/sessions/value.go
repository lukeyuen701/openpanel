package sessions

import (
	"math"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

func str(s string) *jsjson.Value                       { return jsjson.NewString(s) }
func num(n float64) *jsjson.Value                      { return jsjson.NewNumber(n) }
func member(v *jsjson.Value, key string) *jsjson.Value { return v.Get(key) }

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
	}
	return true
}

func isNullish(v *jsjson.Value) bool { return v == nil || v.Kind == jsjson.Null }

// orElse is a || b.
func orElse(v, fallback *jsjson.Value) *jsjson.Value {
	if isTruthy(v) {
		return v
	}
	return fallback
}

// orDefault is a ?? b.
func orDefault(v, fallback *jsjson.Value) *jsjson.Value {
	if isNullish(v) {
		return fallback
	}
	return v
}

// undefinedIfNull is v ?? undefined.
func undefinedIfNull(v *jsjson.Value) *jsjson.Value {
	if v != nil && v.Kind == jsjson.Null {
		return nil
	}
	return v
}

// strictEqual is a === b for JSON primitives; objects are never equal.
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
	}
	return a == b
}

func isString(v *jsjson.Value) bool { return v != nil && v.Kind == jsjson.String }

func stringOr(v *jsjson.Value) string {
	if isString(v) {
		return v.Str
	}
	return jsjson.JSString(v)
}

// numberOr is a numeric member as a JavaScript addition reads it: a number, or 0
// for a missing one (the guarded callers never see anything else).
func numberOf(v *jsjson.Value) float64 {
	if v != nil && v.Kind == jsjson.Number {
		return v.Num
	}
	return math.NaN()
}

// shallowCopy is {...o}.
func shallowCopy(o *jsjson.Obj) *jsjson.Obj {
	out := jsjson.NewObj()
	for _, key := range o.Keys() {
		out.Set(key, o.Get(key))
	}
	return out
}

// spreadObject is dst = {...dst, ...src} for an object value.
func spreadObject(dst *jsjson.Obj, src *jsjson.Value) {
	if src == nil || src.Kind != jsjson.Object {
		return
	}
	for _, key := range src.Obj.Keys() {
		dst.Set(key, src.Obj.Get(key))
	}
}

// clickhouseDate is convertClickhouseDateToJs: new Date(date.replace(' ', 'T') + 'Z').
func clickhouseDate(text string) jsport.Date {
	replaced := []byte(text)
	for i, b := range replaced {
		if b == ' ' {
			replaced[i] = 'T'
			break
		}
	}
	date, err := jsport.ParseDate(str(string(replaced) + "Z"))
	if err != nil {
		return jsport.DateFromMillis(math.NaN())
	}
	return date
}

// dateValue is a Date in a JSON tree: its ISO string, null when invalid.
func dateValue(date jsport.Date) *jsjson.Value {
	text, err := date.ISOString()
	if err != nil {
		return jsjson.NewNull()
	}
	return str(text)
}

func dateMillis(date jsport.Date) float64 {
	ms, ok := date.Millis()
	if !ok {
		return math.NaN()
	}
	return float64(ms)
}
