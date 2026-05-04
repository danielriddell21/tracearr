package spans

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// OTelBuilder satisfies correlate.Builder by translating engine decisions
// into OTel SDK span operations. It owns a map of open spans by span ID
// so the engine can refer to them without holding OTel objects directly.
type OTelBuilder struct {
	tracer trace.Tracer

	mu        sync.Mutex
	openSpans map[[8]byte]trace.Span
}

// NewOTelBuilder returns a builder backed by the given tracer.
func NewOTelBuilder(tracer trace.Tracer) *OTelBuilder {
	return &OTelBuilder{
		tracer:    tracer,
		openSpans: make(map[[8]byte]trace.Span),
	}
}

func toSpanKind(s string) trace.SpanKind {
	switch s {
	case "SERVER":
		return trace.SpanKindServer
	case "CLIENT":
		return trace.SpanKindClient
	case "INTERNAL":
		return trace.SpanKindInternal
	case "PRODUCER":
		return trace.SpanKindProducer
	case "CONSUMER":
		return trace.SpanKindConsumer
	default:
		return trace.SpanKindUnspecified
	}
}

// OpenRoot starts a root span carrying its own trace ID.
func (b *OTelBuilder) OpenRoot(ctx context.Context, name, spanKind string, start time.Time,
	attrs []attribute.KeyValue, links []trace.Link) (context.Context, [16]byte, [8]byte) {
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(toSpanKind(spanKind)),
		trace.WithTimestamp(start),
		trace.WithAttributes(attrs...),
		trace.WithNewRoot(),
	}
	if len(links) > 0 {
		opts = append(opts, trace.WithLinks(links...))
	}
	rootCtx, sp := b.tracer.Start(ctx, name, opts...)
	sc := sp.SpanContext()
	tid := sc.TraceID()
	sid := sc.SpanID()
	b.mu.Lock()
	b.openSpans[sid] = sp
	b.mu.Unlock()
	return rootCtx, tid, sid
}

// OpenChild starts a child span under rootCtx.
func (b *OTelBuilder) OpenChild(rootCtx context.Context, name, spanKind string, start time.Time,
	attrs []attribute.KeyValue) [8]byte {
	_, sp := b.tracer.Start(rootCtx, name,
		trace.WithSpanKind(toSpanKind(spanKind)),
		trace.WithTimestamp(start),
		trace.WithAttributes(attrs...),
	)
	sid := sp.SpanContext().SpanID()
	b.mu.Lock()
	b.openSpans[sid] = sp
	b.mu.Unlock()
	return sid
}

// CloseSpan ends an open span. A zero spanID closes the root carried by rootCtx.
func (b *OTelBuilder) CloseSpan(rootCtx context.Context, spanID [8]byte, end time.Time,
	ok bool, errorReason string, attrs []attribute.KeyValue) {
	var sp trace.Span
	if spanID == ([8]byte{}) {
		sp = trace.SpanFromContext(rootCtx)
	} else {
		b.mu.Lock()
		sp = b.openSpans[spanID]
		delete(b.openSpans, spanID)
		b.mu.Unlock()
	}
	if sp == nil || !sp.SpanContext().IsValid() {
		return
	}
	if len(attrs) > 0 {
		sp.SetAttributes(attrs...)
	}
	if !ok {
		sp.SetStatus(codes.Error, errorReason)
		if errorReason != "" {
			sp.SetAttributes(attribute.String("error.type", errorReason))
		}
	} else {
		sp.SetStatus(codes.Ok, "")
	}
	sp.End(trace.WithTimestamp(end))
}

// SyntheticSpan emits a fully-formed span with explicit start/end. If
// rootCtx is nil, the span has no parent.
func (b *OTelBuilder) SyntheticSpan(rootCtx context.Context, name, spanKind string,
	start, end time.Time, attrs []attribute.KeyValue) {
	ctx := rootCtx
	if ctx == nil {
		ctx = context.Background()
	}
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(toSpanKind(spanKind)),
		trace.WithTimestamp(start),
		trace.WithAttributes(attrs...),
	}
	if rootCtx == nil {
		opts = append(opts, trace.WithNewRoot())
	}
	_, sp := b.tracer.Start(ctx, name, opts...)
	sp.End(trace.WithTimestamp(end))
}

// AddSpanEvent appends a named event to an open span.
func (b *OTelBuilder) AddSpanEvent(rootCtx context.Context, spanID [8]byte, name string, when time.Time,
	attrs []attribute.KeyValue) {
	var sp trace.Span
	if spanID == ([8]byte{}) {
		sp = trace.SpanFromContext(rootCtx)
	} else {
		b.mu.Lock()
		sp = b.openSpans[spanID]
		b.mu.Unlock()
	}
	if sp == nil || !sp.SpanContext().IsValid() {
		return
	}
	sp.AddEvent(name, trace.WithTimestamp(when), trace.WithAttributes(attrs...))
}
