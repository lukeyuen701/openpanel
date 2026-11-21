package notify

import (
	"math"
	"testing"
)

func TestJSNumberFromString(t *testing.T) {
	tests := []struct {
		in   string
		want float64
	}{
		{"", 0}, {"  ", 0}, {"12", 12}, {" 12\n", 12}, {"-5", -5}, {"+5", 5}, {".5", 0.5}, {"5.", 5},
		{"1e3", 1000}, {"1E-2", 0.01}, {"0x10", 16}, {"0X1f", 31}, {"0o17", 15}, {"0b101", 5},
		{"Infinity", math.Inf(1)}, {"-Infinity", math.Inf(-1)}, {"+Infinity", math.Inf(1)},
		{"\u00a012\ufeff", 12}, {"1e999", math.Inf(1)},
	}
	for _, tt := range tests {
		if got := jsNumberFromString(tt.in); got != tt.want {
			t.Errorf("Number(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
	for _, in := range []string{"abc", "1,5", "0x", "0xg", "-0x10", "1_000", "infinity", "1e", "--1", "0b2", "12px", "."} {
		if got := jsNumberFromString(in); !math.IsNaN(got) {
			t.Errorf("Number(%q) = %v, want NaN", in, got)
		}
	}
}

func TestJSTrimIsNotGoSpace(t *testing.T) {
	if got := jsTrim("\ufeff x \u00a0"); got != "x" {
		t.Errorf("BOM and NBSP are trimmed: %q", got)
	}
	if got := jsTrim("\u0085x"); got != "\u0085x" {
		t.Errorf("U+0085 is not JavaScript whitespace: %q", got)
	}
}
