package jsport

import (
	"strconv"

	"openpanel/ingest/internal/jsjson"
)

// Limits of toDots: one event's properties become one ClickHouse Map, so the
// flattener is bounded. The per-object cap does not apply to the root.
const (
	ToDotsMaxKeys          = 1000
	ToDotsMaxKeysPerObject = 50
)

const (
	bufferTypeValue = "Buffer"
	prototypeKey    = "__proto__"
)

type stringKind uint8

const (
	kindText stringKind = iota
	kindJSON
	kindMalformed
)

// ToDotsMaxDepth is the number of nested objects and arrays after which the
// TypeScript flattener's recursion overflows Bun's stack and throws (measured
// on Bun 1.4.4: 11,399 levels below the root). Which depth it is exactly
// depends on the engine's frame sizes; deeper input fails in both worlds.
const ToDotsMaxDepth = 11_400

// ToDots is toDots(obj): nested properties become dotted string keys. The
// result keeps JavaScript's property order (array-index keys first). Input
// nested beyond ToDotsMaxDepth gives ErrTooDeep, where toDots throws a RangeError.
func ToDots(obj *jsjson.Obj) (*jsjson.Obj, error) {
	flattener := &dotFlattener{out: jsjson.NewObj()}
	flattener.visitEntries(jsjson.NewObject(obj), "", ToDotsMaxKeys, 1)
	if flattener.tooDeep {
		return nil, ErrTooDeep
	}
	return flattener.out, nil
}

type dotFlattener struct {
	out     *jsjson.Obj
	count   int
	tooDeep bool
}

func (f *dotFlattener) visit(value *jsjson.Value, key string, depth int) {
	if f.count >= ToDotsMaxKeys || f.tooDeep {
		return
	}
	if value == nil || value.Kind == jsjson.Null || (value.Kind == jsjson.String && value.Str == "") {
		return
	}
	switch value.Kind {
	case jsjson.String:
		f.visitString(value.Str, key, depth)
	case jsjson.Array:
		f.visitEntries(value, key+".", ToDotsMaxKeysPerObject, depth+1)
	case jsjson.Object:
		if data, ok := serializedBuffer(value); ok {
			f.visit(jsjson.NewString(decodeSerializedBuffer(data)), key, depth)
			return
		}
		f.visitEntries(value, key+".", ToDotsMaxKeysPerObject, depth+1)
	default:
		f.set(key, jsjson.JSString(value))
	}
}

func (f *dotFlattener) visitString(text, key string, depth int) {
	switch classifyString(text) {
	case kindMalformed:
	case kindJSON:
		parsed, err := ParseJSON(text)
		if err != nil {
			return
		}
		f.visit(parsed, key, depth)
	default:
		f.set(key, Trim(RemoveLoneSurrogates(text)))
	}
}

// set is out[key] = value followed by count += 1. Assigning a string to
// "__proto__" on a plain object invokes the prototype setter and stores
// nothing, but the key is still counted.
func (f *dotFlattener) set(key, value string) {
	if key != prototypeKey {
		f.out.Set(key, jsjson.NewString(value))
	}
	f.count++
}

func (f *dotFlattener) visitEntries(value *jsjson.Value, prefix string, maxEntries, depth int) {
	if depth > ToDotsMaxDepth {
		f.tooDeep = true
		return
	}
	if value.Kind == jsjson.Array {
		end := min(len(value.Arr), maxEntries)
		for index := 0; index < end && f.count < ToDotsMaxKeys; index++ {
			f.visit(value.Arr[index], prefix+strconv.Itoa(index), depth)
		}
		return
	}
	seen := 0
	for _, childKey := range value.Obj.Keys() {
		if seen >= maxEntries || f.count >= ToDotsMaxKeys {
			return
		}
		seen++
		f.visit(value.Obj.Get(childKey), prefix+childKey, depth)
	}
}

// classifyString reports whether a string looks like a whole object/array
// literal, only starts like one, or is plain text.
func classifyString(text string) stringKind {
	var closing byte
	switch text[0] {
	case '{':
		closing = '}'
	case '[':
		closing = ']'
	default:
		return kindText
	}
	if text[len(text)-1] == closing {
		return kindJSON
	}
	return kindMalformed
}

// serializedBuffer recognises what JSON.stringify(Buffer) writes in the Node
// SDK: an object with type "Buffer" and an array data.
func serializedBuffer(value *jsjson.Value) ([]*jsjson.Value, bool) {
	typeValue := value.Get("type")
	data := value.Get("data")
	if typeValue == nil || typeValue.Kind != jsjson.String || typeValue.Str != bufferTypeValue {
		return nil, false
	}
	if data == nil || data.Kind != jsjson.Array {
		return nil, false
	}
	return data.Arr, true
}

// decodeSerializedBuffer is new TextDecoder().decode(Uint8Array.from(data)).
func decodeSerializedBuffer(data []*jsjson.Value) string {
	bytes := make([]byte, len(data))
	for i, element := range data {
		bytes[i] = ToUint8(ToNumberValue(element))
	}
	return DecodeUTF8StripBOM(bytes)
}
