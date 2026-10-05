package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"strings"
)

// NewLogger returns a structured logger. JSON output is for machines
// (ingest into a log pipeline); text is for local development.
func NewLogger(w io.Writer, level string, json bool) *slog.Logger {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lv}
	if json {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}

// Discard returns a logger that drops everything.
func Discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type loggerKey struct{}

// WithLogger stores a request-scoped logger in ctx.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// L returns the request-scoped logger, or a discarding logger if none is set.
func L(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return l
	}
	return Discard()
}

// NewRequestID returns a random 128-bit hex ID.
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("observability: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// ValidRequestID accepts a caller-supplied request ID only if it is short and
// uses a safe alphabet, so it can never forge log lines or headers.
func ValidRequestID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

// ParseTraceparent extracts the trace ID from a W3C traceparent header
// ("00-<32 hex trace id>-<16 hex span id>-<2 hex flags>"). Logging the
// caller's trace ID lets logs be joined to traces now; full OpenTelemetry
// span propagation is a later step.
func ParseTraceparent(h string) (traceID string, ok bool) {
	parts := strings.Split(h, "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return "", false
	}
	if parts[1] == strings.Repeat("0", 32) {
		return "", false
	}
	for _, c := range parts[1] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", false
		}
	}
	return parts[1], true
}
