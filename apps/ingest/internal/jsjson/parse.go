package jsjson

import (
	"errors"
	"fmt"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// ErrUnsupported marks input JSON.parse accepts but this package does not
// model: a lone surrogate escape, or nesting deeper than maxDepth. Callers hand
// such a request to the Bun API instead of guessing.
var ErrUnsupported = errors.New("jsjson: input outside the modelled subset")

const maxDepth = 256

// Parse reads one JSON text with JSON.parse's grammar.
func Parse(data []byte) (*Value, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("%w: invalid utf-8", ErrUnsupported)
	}
	p := parser{data: data}
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

type parser struct {
	data []byte
	pos  int
}

func (p *parser) fail(reason string) error {
	return fmt.Errorf("jsjson: %s at offset %d", reason, p.pos)
}

// skipSpace skips the four whitespace characters JSON allows.
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

func (p *parser) value(depth int) (*Value, error) {
	if depth > maxDepth {
		return nil, fmt.Errorf("%w: nesting deeper than %d", ErrUnsupported, maxDepth)
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
		return NewString(s), nil
	case c == 't':
		return p.literal("true", NewBool(true))
	case c == 'f':
		return p.literal("false", NewBool(false))
	case c == 'n':
		return p.literal("null", NewNull())
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number()
	default:
		return nil, p.fail("unexpected character")
	}
}

func (p *parser) literal(word string, value *Value) (*Value, error) {
	if len(p.data)-p.pos < len(word) || string(p.data[p.pos:p.pos+len(word)]) != word {
		return nil, p.fail("invalid literal")
	}
	p.pos += len(word)
	return value, nil
}

func (p *parser) object(depth int) (*Value, error) {
	p.pos++
	obj := NewObj()
	p.skipSpace()
	if p.pos < len(p.data) && p.data[p.pos] == '}' {
		p.pos++
		return NewObject(obj), nil
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
			return NewObject(obj), nil
		default:
			return nil, p.fail("expected comma or end of object")
		}
	}
}

func (p *parser) array(depth int) (*Value, error) {
	p.pos++
	items := []*Value{}
	p.skipSpace()
	if p.pos < len(p.data) && p.data[p.pos] == ']' {
		p.pos++
		return &Value{Kind: Array, Arr: items}, nil
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
			return &Value{Kind: Array, Arr: items}, nil
		default:
			return nil, p.fail("expected comma or end of array")
		}
	}
}

func (p *parser) number() (*Value, error) {
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
	n, err := strconv.ParseFloat(string(p.data[start:p.pos]), 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return nil, p.fail("invalid number")
	}
	return NewNumber(n), nil
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
	// Fast path: no escapes.
	for p.pos < len(p.data) {
		c := p.data[p.pos]
		if c == '"' {
			s := string(p.data[start:p.pos])
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
			buf = append(buf, c)
			p.pos++
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
			r, err := p.unicodeEscape()
			if err != nil {
				return "", err
			}
			buf = utf8.AppendRune(buf, r)
		default:
			return "", p.fail("invalid escape")
		}
	}
	return "", p.fail("unterminated string")
}

func (p *parser) hex4() (rune, error) {
	if len(p.data)-p.pos < 4 {
		return 0, p.fail("short unicode escape")
	}
	n, err := strconv.ParseUint(string(p.data[p.pos:p.pos+4]), 16, 32)
	if err != nil {
		return 0, p.fail("invalid unicode escape")
	}
	p.pos += 4
	return rune(n), nil
}

func (p *parser) unicodeEscape() (rune, error) {
	r, err := p.hex4()
	if err != nil {
		return 0, err
	}
	if !utf16.IsSurrogate(r) {
		return r, nil
	}
	isHigh := r < 0xDC00
	hasLow := len(p.data)-p.pos >= 6 && p.data[p.pos] == '\\' && p.data[p.pos+1] == 'u'
	if !isHigh || !hasLow {
		return 0, fmt.Errorf("%w: lone surrogate", ErrUnsupported)
	}
	saved := p.pos
	p.pos += 2
	low, err := p.hex4()
	if err != nil {
		return 0, err
	}
	if low < 0xDC00 || low > 0xDFFF {
		p.pos = saved
		return 0, fmt.Errorf("%w: lone surrogate", ErrUnsupported)
	}
	return utf16.DecodeRune(r, low), nil
}
