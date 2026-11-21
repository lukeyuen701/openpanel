package jsport

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"openpanel/ingest/internal/jsjson"
)

var (
	// ErrSyntax is JSON.parse's SyntaxError.
	ErrSyntax = errors.New("jsport: JSON syntax error")
	// ErrTooDeep stands for the RangeError JavaScript raises when recursion
	// exhausts the stack. JSON.parse itself has no such limit in Bun, so
	// ParseJSON refuses only nesting no Kafka message can reach.
	ErrTooDeep = errors.New("jsport: nesting too deep")
	// ErrUndefined is JSON.stringify(undefined): no text at all.
	ErrUndefined = errors.New("jsport: undefined has no JSON text")
)

const (
	// maxParseDepth only guards the Go stack; Bun parsed 3,000,000 levels.
	maxParseDepth = 1_000_000
	// maxStringifyDepth is where Bun's JSON.stringify threw a RangeError (measured: 40,000 levels).
	maxStringifyDepth = 40_000
)

const hexDigits = "0123456789abcdef"

// ParseJSON is JSON.parse(text) for a WTF-8 string: the grammar of
// jsjson.Parse, but a lone surrogate survives (written as an escape or as raw
// WTF-8) instead of being refused, and nesting has no limit of its own.
func ParseJSON(text string) (*jsjson.Value, error) {
	p := parser{data: text}
	p.skipSpace()
	value, err := p.value(0)
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos != len(p.data) {
		return nil, p.fail("trailing data")
	}
	return value, nil
}

// ParseJSONBytes is JSON.parse(buffer.toString()): the bytes are decoded as
// Buffer does (invalid sequences become U+FFFD, a byte-order mark is kept and
// so fails the parse).
func ParseJSONBytes(data []byte) (*jsjson.Value, error) {
	return ParseJSON(DecodeUTF8(data))
}

type parser struct {
	data string
	pos  int
}

func (p *parser) fail(reason string) error {
	return fmt.Errorf("%w: %s at offset %d", ErrSyntax, reason, p.pos)
}

func (p *parser) skipSpace() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *parser) value(depth int) (*jsjson.Value, error) {
	if depth > maxParseDepth {
		return nil, ErrTooDeep
	}
	if p.pos >= len(p.data) {
		return nil, p.fail("unexpected end")
	}
	switch c := p.data[p.pos]; {
	case c == '{':
		return p.object(depth)
	case c == '[':
		return p.array(depth)
	case c == '"':
		s, err := p.string()
		if err != nil {
			return nil, err
		}
		return jsjson.NewString(s), nil
	case c == 't':
		return p.literal("true", jsjson.NewBool(true))
	case c == 'f':
		return p.literal("false", jsjson.NewBool(false))
	case c == 'n':
		return p.literal("null", jsjson.NewNull())
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number()
	default:
		return nil, p.fail("unexpected character")
	}
}

func (p *parser) literal(word string, value *jsjson.Value) (*jsjson.Value, error) {
	if !strings.HasPrefix(p.data[p.pos:], word) {
		return nil, p.fail("invalid literal")
	}
	p.pos += len(word)
	return value, nil
}

func (p *parser) object(depth int) (*jsjson.Value, error) {
	p.pos++
	obj := jsjson.NewObj()
	p.skipSpace()
	if p.pos < len(p.data) && p.data[p.pos] == '}' {
		p.pos++
		return jsjson.NewObject(obj), nil
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.data) || p.data[p.pos] != '"' {
			return nil, p.fail("expected property name")
		}
		key, err := p.string()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if p.pos >= len(p.data) || p.data[p.pos] != ':' {
			return nil, p.fail("expected colon")
		}
		p.pos++
		p.skipSpace()
		item, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		obj.Set(key, item)
		p.skipSpace()
		if p.pos >= len(p.data) {
			return nil, p.fail("unterminated object")
		}
		switch p.data[p.pos] {
		case ',':
			p.pos++
		case '}':
			p.pos++
			return jsjson.NewObject(obj), nil
		default:
			return nil, p.fail("expected comma or end of object")
		}
	}
}

func (p *parser) array(depth int) (*jsjson.Value, error) {
	p.pos++
	items := []*jsjson.Value{}
	p.skipSpace()
	if p.pos < len(p.data) && p.data[p.pos] == ']' {
		p.pos++
		return &jsjson.Value{Kind: jsjson.Array, Arr: items}, nil
	}
	for {
		p.skipSpace()
		item, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		p.skipSpace()
		if p.pos >= len(p.data) {
			return nil, p.fail("unterminated array")
		}
		switch p.data[p.pos] {
		case ',':
			p.pos++
		case ']':
			p.pos++
			return &jsjson.Value{Kind: jsjson.Array, Arr: items}, nil
		default:
			return nil, p.fail("expected comma or end of array")
		}
	}
}

func (p *parser) number() (*jsjson.Value, error) {
	start := p.pos
	if p.data[p.pos] == '-' {
		p.pos++
	}
	if p.pos >= len(p.data) {
		return nil, p.fail("invalid number")
	}
	if p.data[p.pos] == '0' {
		p.pos++
	} else if p.data[p.pos] >= '1' && p.data[p.pos] <= '9' {
		p.digits()
	} else {
		return nil, p.fail("invalid number")
	}
	if p.pos < len(p.data) && p.data[p.pos] == '.' {
		p.pos++
		if !p.digits() {
			return nil, p.fail("invalid fraction")
		}
	}
	if p.pos < len(p.data) && (p.data[p.pos] == 'e' || p.data[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.data) && (p.data[p.pos] == '+' || p.data[p.pos] == '-') {
			p.pos++
		}
		if !p.digits() {
			return nil, p.fail("invalid exponent")
		}
	}
	n, err := strconv.ParseFloat(p.data[start:p.pos], 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return nil, p.fail("invalid number")
	}
	return jsjson.NewNumber(n), nil
}

func (p *parser) digits() bool {
	start := p.pos
	for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
		p.pos++
	}
	return p.pos > start
}

func (p *parser) string() (string, error) {
	p.pos++
	start := p.pos
	for p.pos < len(p.data) {
		c := p.data[p.pos]
		if c == '"' {
			s := p.data[start:p.pos]
			p.pos++
			return s, nil
		}
		if c == '\\' {
			break
		}
		if c < 0x20 {
			return "", p.fail("control character in string")
		}
		p.pos++
	}
	buf := append([]byte(nil), p.data[start:p.pos]...)
	for p.pos < len(p.data) {
		c := p.data[p.pos]
		switch {
		case c == '"':
			p.pos++
			return string(buf), nil
		case c < 0x20:
			return "", p.fail("control character in string")
		case c != '\\':
			end := p.pos + 1
			for end < len(p.data) && p.data[end] != '"' && p.data[end] != '\\' && p.data[end] >= 0x20 {
				end++
			}
			buf = appendJoined(buf, p.data[p.pos:end])
			p.pos = end
			continue
		}
		p.pos++
		if p.pos >= len(p.data) {
			return "", p.fail("unterminated escape")
		}
		escape := p.data[p.pos]
		p.pos++
		switch escape {
		case '"', '\\', '/':
			buf = append(buf, escape)
		case 'b':
			buf = append(buf, '\b')
		case 'f':
			buf = append(buf, '\f')
		case 'n':
			buf = append(buf, '\n')
		case 'r':
			buf = append(buf, '\r')
		case 't':
			buf = append(buf, '\t')
		case 'u':
			if p.pos+4 > len(p.data) {
				return "", p.fail("short unicode escape")
			}
			unit, err := strconv.ParseUint(p.data[p.pos:p.pos+4], 16, 32)
			if err != nil {
				return "", p.fail("invalid unicode escape")
			}
			p.pos += 4
			buf = appendUnit(buf, rune(unit))
		default:
			return "", p.fail("invalid escape")
		}
	}
	return "", p.fail("unterminated string")
}

// Stringify is JSON.stringify(v). A nil value is undefined: ErrUndefined at
// the top level, the member is omitted in an object and written as null in an
// array. Nesting beyond what the JavaScript engine survives gives ErrTooDeep.
func Stringify(v *jsjson.Value) (string, error) {
	if v == nil {
		return "", ErrUndefined
	}
	var b strings.Builder
	if err := writeValue(&b, v, 0); err != nil {
		return "", err
	}
	return b.String(), nil
}

func writeValue(b *strings.Builder, v *jsjson.Value, depth int) error {
	switch v.Kind {
	case jsjson.Null:
		b.WriteString("null")
	case jsjson.Bool:
		b.WriteString(strconv.FormatBool(v.Bool))
	case jsjson.Number:
		if math.IsNaN(v.Num) || math.IsInf(v.Num, 0) {
			b.WriteString("null")
			return nil
		}
		b.WriteString(jsjson.FormatNumber(v.Num))
	case jsjson.String:
		writeString(b, v.Str)
	case jsjson.Array:
		if depth >= maxStringifyDepth {
			return ErrTooDeep
		}
		b.WriteByte('[')
		for i, item := range v.Arr {
			if i > 0 {
				b.WriteByte(',')
			}
			if item == nil {
				b.WriteString("null")
				continue
			}
			if err := writeValue(b, item, depth+1); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case jsjson.Object:
		if depth >= maxStringifyDepth {
			return ErrTooDeep
		}
		b.WriteByte('{')
		first := true
		for _, key := range v.Obj.Keys() {
			member := v.Obj.Get(key)
			if member == nil {
				continue
			}
			if !first {
				b.WriteByte(',')
			}
			first = false
			writeString(b, key)
			b.WriteByte(':')
			if err := writeValue(b, member, depth+1); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	}
	return nil
}

// StringifyString is JSON.stringify for one string.
func StringifyString(s string) string {
	var b strings.Builder
	writeString(&b, s)
	return b.String()
}

func writeString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for i := 0; i < len(s); {
		r, size := decodeRune(s[i:])
		i += size
		switch {
		case r == '"':
			b.WriteString("\\\"")
		case r == '\\':
			b.WriteString("\\\\")
		case r == '\b':
			b.WriteString("\\b")
		case r == '\f':
			b.WriteString("\\f")
		case r == '\n':
			b.WriteString("\\n")
		case r == '\r':
			b.WriteString("\\r")
		case r == '\t':
			b.WriteString("\\t")
		case r < 0x20 || isSurrogate(r):
			b.WriteString("\\u")
			b.WriteByte(hexDigits[r>>12&0xF])
			b.WriteByte(hexDigits[r>>8&0xF])
			b.WriteByte(hexDigits[r>>4&0xF])
			b.WriteByte(hexDigits[r&0xF])
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}
