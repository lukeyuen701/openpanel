// Package jsjson holds JSON the way a JavaScript engine does: objects keep
// JavaScript's property order, numbers are float64, and Stringify writes what
// JSON.stringify writes. The Kafka payload is read by a Bun consumer and
// compared against what the Bun API produced, so "equivalent JSON" is not
// enough where order or number formatting reaches storage.
package jsjson

import (
	"strconv"
	"strings"
)

// Kind is the JSON type of a Value.
type Kind uint8

const (
	Null Kind = iota
	Bool
	Number
	String
	Array
	Object
)

// Value is one parsed JSON value. Exactly one payload field is meaningful,
// chosen by Kind.
type Value struct {
	Kind Kind
	Bool bool
	Num  float64
	Str  string
	Arr  []*Value
	Obj  *Obj
}

// NewString, NewNumber, NewBool and NewNull build leaf values.
func NewString(s string) *Value  { return &Value{Kind: String, Str: s} }
func NewNumber(n float64) *Value { return &Value{Kind: Number, Num: n} }
func NewBool(b bool) *Value      { return &Value{Kind: Bool, Bool: b} }
func NewNull() *Value            { return &Value{Kind: Null} }

// NewObject wraps an object.
func NewObject(o *Obj) *Value { return &Value{Kind: Object, Obj: o} }

// JSString is String(v): nil is "undefined", objects "[object Object]",
// arrays their elements joined with commas.
func JSString(v *Value) string {
	if v == nil {
		return "undefined"
	}
	switch v.Kind {
	case Null:
		return "null"
	case Bool:
		if v.Bool {
			return "true"
		}
		return "false"
	case Number:
		return numberToString(v.Num)
	case String:
		return v.Str
	case Array:
		parts := make([]string, len(v.Arr))
		for i, item := range v.Arr {
			if item.Kind == Null {
				continue
			}
			parts[i] = JSString(item)
		}
		return strings.Join(parts, ",")
	}
	return "[object Object]"
}

// IsObject reports whether v is a JSON object (not null, not an array).
func (v *Value) IsObject() bool { return v != nil && v.Kind == Object }

// Get reads a property of an object value; nil when v is not an object or
// the key is absent.
func (v *Value) Get(key string) *Value {
	if !v.IsObject() {
		return nil
	}
	return v.Obj.Get(key)
}

// Path walks nested object properties, like ramda's path.
func (v *Value) Path(keys ...string) *Value {
	current := v
	for _, key := range keys {
		current = current.Get(key)
		if current == nil {
			return nil
		}
	}
	return current
}

// Clone deep-copies v.
func (v *Value) Clone() *Value {
	if v == nil {
		return nil
	}
	out := *v
	switch v.Kind {
	case Array:
		out.Arr = make([]*Value, len(v.Arr))
		for i, item := range v.Arr {
			out.Arr[i] = item.Clone()
		}
	case Object:
		out.Obj = v.Obj.Clone()
	}
	return &out
}

// Obj is a JavaScript-ordered object: array-index keys first in ascending
// numeric order, then every other key in insertion order. A key set twice
// keeps its first position and its last value, as JSON.parse does.
type Obj struct {
	indexKeys []string
	nameKeys  []string
	values    map[string]*Value
}

// NewObj returns an empty object.
func NewObj() *Obj {
	return &Obj{values: map[string]*Value{}}
}

// Len is the number of properties.
func (o *Obj) Len() int { return len(o.values) }

// Get returns the property or nil.
func (o *Obj) Get(key string) *Value { return o.values[key] }

// Has reports whether the property exists.
func (o *Obj) Has(key string) bool {
	_, ok := o.values[key]
	return ok
}

// Set adds or replaces a property.
func (o *Obj) Set(key string, value *Value) {
	if _, exists := o.values[key]; exists {
		o.values[key] = value
		return
	}
	o.values[key] = value
	index, isIndex := arrayIndex(key)
	if !isIndex {
		o.nameKeys = append(o.nameKeys, key)
		return
	}
	position := len(o.indexKeys)
	for i, existing := range o.indexKeys {
		existingIndex, _ := arrayIndex(existing)
		if index < existingIndex {
			position = i
			break
		}
	}
	o.indexKeys = append(o.indexKeys, "")
	copy(o.indexKeys[position+1:], o.indexKeys[position:])
	o.indexKeys[position] = key
}

// Delete removes a property if present.
func (o *Obj) Delete(key string) {
	if _, exists := o.values[key]; !exists {
		return
	}
	delete(o.values, key)
	o.indexKeys = removeKey(o.indexKeys, key)
	o.nameKeys = removeKey(o.nameKeys, key)
}

// Keys returns the property names in JavaScript enumeration order.
func (o *Obj) Keys() []string {
	keys := make([]string, 0, len(o.indexKeys)+len(o.nameKeys))
	keys = append(keys, o.indexKeys...)
	return append(keys, o.nameKeys...)
}

// Clone deep-copies the object.
func (o *Obj) Clone() *Obj {
	out := &Obj{
		indexKeys: append([]string(nil), o.indexKeys...),
		nameKeys:  append([]string(nil), o.nameKeys...),
		values:    make(map[string]*Value, len(o.values)),
	}
	for key, value := range o.values {
		out.values[key] = value.Clone()
	}
	return out
}

func removeKey(keys []string, key string) []string {
	for i, existing := range keys {
		if existing == key {
			return append(keys[:i], keys[i+1:]...)
		}
	}
	return keys
}

// maxArrayIndex is 2^32 - 2, the largest array index ECMAScript recognises.
const maxArrayIndex = 4294967294

// arrayIndex reports whether key is a canonical array index ("0", "17", but
// not "017" or "-1"), which JavaScript enumerates before other keys.
func arrayIndex(key string) (uint64, bool) {
	if key == "" || len(key) > 10 {
		return 0, false
	}
	if len(key) > 1 && key[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < '0' || key[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(key, 10, 64)
	if err != nil || n > maxArrayIndex {
		return 0, false
	}
	return n, true
}
