package jsjson

import (
	"crypto/md5"
	"encoding/hex"
	"sort"
	"strings"
)

// StableStringify is fast-json-stable-hash's stringify: object keys sorted the
// way Array.prototype.sort orders them, strings through JSON.stringify,
// numbers through Number::toString.
func StableStringify(v *Value) string {
	var b strings.Builder
	writeStable(&b, v)
	return b.String()
}

// StableMD5 is fastJsonStableHash.hash(v, 'md5').
func StableMD5(v *Value) string {
	sum := md5.Sum([]byte(StableStringify(v)))
	return hex.EncodeToString(sum[:])
}

func writeStable(b *strings.Builder, v *Value) {
	switch v.Kind {
	case String:
		writeString(b, v.Str)
	case Array:
		b.WriteByte('[')
		for i, item := range v.Arr {
			if i > 0 {
				b.WriteByte(',')
			}
			writeStable(b, item)
		}
		b.WriteByte(']')
	case Object:
		keys := v.Obj.Keys()
		sort.SliceStable(keys, func(i, j int) bool { return CompareUTF16(keys[i], keys[j]) < 0 })
		b.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeString(b, key)
			b.WriteByte(':')
			writeStable(b, v.Obj.Get(key))
		}
		b.WriteByte('}')
	case Number:
		b.WriteString(numberToString(v.Num))
	case Bool:
		if v.Bool {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case Null:
		b.WriteString("null")
	}
}
