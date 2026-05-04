// Package log provides a structured JSON slog handler that automatically
// injects trace_id and span_id from the active OTel span context, so logs
// can be pivoted to traces in Grafana.
package log

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

type traceHandler struct {
	slog.Handler
}

// Handle attaches trace_id and span_id from the context's active span, when present.
func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{Handler: h.Handler.WithGroup(name)}
}

// New returns a slog.Logger writing JSON to w with the given level.
// Pass os.Stdout in production; pass io.Discard in tests.
func New(w io.Writer, level string) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	lvl := parseLevel(level)
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			// Rename slog defaults to schema-stable names.
			switch a.Key {
			case slog.TimeKey:
				a.Key = "ts"
			case slog.LevelKey:
				a.Key = "severity"
			case slog.MessageKey:
				a.Key = "msg"
			}
			return a
		},
	})
	logger := slog.New(traceHandler{Handler: base}).With(slog.String("service.name", "tracearr"))
	return logger
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
