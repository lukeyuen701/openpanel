package server

import (
	"context"
	"crypto/rand"
	"log/slog"
)

const requestIDLength = 21

const requestIDAlphabet = "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict"

type requestIDKey struct{}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

func newRequestID() string {
	buf := make([]byte, requestIDLength)
	_, _ = rand.Read(buf)
	for i := range buf {
		buf[i] = requestIDAlphabet[buf[i]&63]
	}
	return string(buf)
}

// LogHandler adds the request id to every record logged with a request's
// context, under the field name the API's logs use.
type LogHandler struct{ slog.Handler }

// Handle implements slog.Handler.
func (h LogHandler) Handle(ctx context.Context, record slog.Record) error {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		record.AddAttrs(slog.String("requestId", id))
	}
	return h.Handler.Handle(ctx, record)
}

// WithAttrs implements slog.Handler.
func (h LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return LogHandler{h.Handler.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler.
func (h LogHandler) WithGroup(name string) slog.Handler { return LogHandler{h.Handler.WithGroup(name)} }
