package bullmq

import (
	"fmt"
	"math"
)

// Wire-format bytes of the MessagePack subset msgpackr (BullMQ's encoder)
// produces for job arguments.
const (
	mpNil       = 0xc0
	mpFalse     = 0xc2
	mpTrue      = 0xc3
	mpUint8     = 0xcc
	mpUint16    = 0xcd
	mpUint32    = 0xce
	mpFloat64   = 0xcb
	mpStr8      = 0xd9
	mpStr16     = 0xda
	mpStr32     = 0xdb
	mpArray16   = 0xdc
	mpArray32   = 0xdd
	mpMap16     = 0xde
	mpMap32     = 0xdf
	mpFixStr    = 0xa0
	mpFixArray  = 0x90
	fixStrMax   = 31
	fixArrayMax = 15
	fixIntMax   = 127
)

// member is one key of a packed map. Maps are ordered because BullMQ's Lua
// re-encodes them with cjson, whose output order follows the packed order.
type member struct {
	key   string
	value any
}

type packedMap []member

// pack encodes nil, bool, int, int64, float64, string, []any and packedMap
// the way msgpackr does: the smallest unsigned width for non-negative
// integers up to 2^32-1, float64 for everything else numeric, maps always
// with a 16-bit length.
func pack(value any) []byte {
	return appendPacked(nil, value)
}

func appendPacked(out []byte, value any) []byte {
	switch v := value.(type) {
	case nil:
		return append(out, mpNil)
	case bool:
		if v {
			return append(out, mpTrue)
		}
		return append(out, mpFalse)
	case int:
		return appendNumber(out, float64(v))
	case int64:
		return appendNumber(out, float64(v))
	case float64:
		return appendNumber(out, v)
	case string:
		return appendString(out, v)
	case []any:
		out = appendHeader(out, len(v), mpFixArray, fixArrayMax, mpArray16, mpArray32)
		for _, item := range v {
			out = appendPacked(out, item)
		}
		return out
	case packedMap:
		out = appendHeader(out, len(v), 0, -1, mpMap16, mpMap32)
		for _, m := range v {
			out = appendString(out, m.key)
			out = appendPacked(out, m.value)
		}
		return out
	default:
		panic(fmt.Sprintf("bullmq: cannot pack %T", value))
	}
}

func appendNumber(out []byte, v float64) []byte {
	isUint32 := v >= 0 && v <= math.MaxUint32 && v == math.Trunc(v)
	if !isUint32 {
		bits := math.Float64bits(v)
		return append(out, mpFloat64,
			byte(bits>>56), byte(bits>>48), byte(bits>>40), byte(bits>>32),
			byte(bits>>24), byte(bits>>16), byte(bits>>8), byte(bits))
	}
	n := uint32(v)
	switch {
	case n <= fixIntMax:
		return append(out, byte(n))
	case n <= math.MaxUint8:
		return append(out, mpUint8, byte(n))
	case n <= math.MaxUint16:
		return append(out, mpUint16, byte(n>>8), byte(n))
	default:
		return append(out, mpUint32, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}

func appendString(out []byte, s string) []byte {
	n := len(s)
	switch {
	case n <= fixStrMax:
		out = append(out, mpFixStr|byte(n))
	case n <= math.MaxUint8:
		out = append(out, mpStr8, byte(n))
	case n <= math.MaxUint16:
		out = append(out, mpStr16, byte(n>>8), byte(n))
	default:
		out = append(out, mpStr32, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return append(out, s...)
}

// appendHeader writes a collection length: inline when it fits the fixed
// form (fixMax < 0 disables it), else 16 or 32 bits.
func appendHeader(out []byte, n int, fixBase byte, fixMax int, tag16, tag32 byte) []byte {
	switch {
	case n <= fixMax:
		return append(out, fixBase|byte(n))
	case n <= math.MaxUint16:
		return append(out, tag16, byte(n>>8), byte(n))
	default:
		return append(out, tag32, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}
