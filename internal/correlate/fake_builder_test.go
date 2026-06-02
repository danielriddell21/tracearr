package correlate

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// recordedSpan is what the fake builder collects per call.
type recordedSpan struct {
	Name      string
	Kind      string
	Start     time.Time
	End       time.Time
	Synthetic bool
	Closed    bool
	Status    string // "" | "ok" | "error:<reason>"
	Attrs     []attribute.KeyValue
	Events    []recordedEvent
	ParentID  [8]byte
	SpanID    [8]byte
	TraceID   [16]byte
	HasParent bool
	Links     []trace.Link
}

type recordedEvent struct {
	Name  string
	When  time.Time
	Attrs []attribute.KeyValue
}

// fakeBuilder records every Builder call. It's the test double for the
// engine and also doubles as a span-tree assertion target.
type fakeBuilder struct {
	counter   uint64
	openByID  map[[8]byte]*recordedSpan
	rootByCtx map[context.Context]*recordedSpan
	all       []*recordedSpan
}

func newFakeBuilder() *fakeBuilder {
	return &fakeBuilder{
		openByID:  map[[8]byte]*recordedSpan{},
		rootByCtx: map[context.Context]*recordedSpan{},
	}
}

func (b *fakeBuilder) nextID() [8]byte {
	n := atomic.AddUint64(&b.counter, 1)
	var out [8]byte
	for i := 0; i < 8; i++ {
		out[7-i] = byte(n >> (i * 8))
	}
	return out
}

func (b *fakeBuilder) nextTraceID() [16]byte {
	n := atomic.AddUint64(&b.counter, 1)
	var out [16]byte
	for i := 0; i < 8; i++ {
		out[15-i] = byte(n >> (i * 8))
	}
	return out
}

type rootCtxKey struct{}

func (b *fakeBuilder) OpenRoot(ctx context.Context, name, kind string, start time.Time,
	attrs []attribute.KeyValue, links []trace.Link) (context.Context, [16]byte, [8]byte) {
	rs := &recordedSpan{
		Name:    name,
		Kind:    kind,
		Start:   start,
		Attrs:   append([]attribute.KeyValue{}, attrs...),
		SpanID:  b.nextID(),
		TraceID: b.nextTraceID(),
		Links:   append([]trace.Link{}, links...),
	}
	b.openByID[rs.SpanID] = rs
	b.all = append(b.all, rs)
	rootCtx := context.WithValue(ctx, rootCtxKey{}, rs)
	b.rootByCtx[rootCtx] = rs
	return rootCtx, rs.TraceID, rs.SpanID
}

func (b *fakeBuilder) OpenChild(rootCtx context.Context, name, kind string, start time.Time,
	attrs []attribute.KeyValue) [8]byte {
	root, _ := rootCtx.Value(rootCtxKey{}).(*recordedSpan)
	rs := &recordedSpan{
		Name:      name,
		Kind:      kind,
		Start:     start,
		Attrs:     append([]attribute.KeyValue{}, attrs...),
		SpanID:    b.nextID(),
		HasParent: true,
	}
	if root != nil {
		rs.ParentID = root.SpanID
		rs.TraceID = root.TraceID
	}
	b.openByID[rs.SpanID] = rs
	b.all = append(b.all, rs)
	return rs.SpanID
}

func (b *fakeBuilder) CloseSpan(rootCtx context.Context, spanID [8]byte, end time.Time,
	ok bool, errReason string, attrs []attribute.KeyValue) {
	var rs *recordedSpan
	if spanID == ([8]byte{}) {
		rs, _ = rootCtx.Value(rootCtxKey{}).(*recordedSpan)
	} else {
		rs = b.openByID[spanID]
		delete(b.openByID, spanID)
	}
	if rs == nil {
		return
	}
	rs.End = end
	rs.Closed = true
	if ok {
		rs.Status = "ok"
	} else {
		rs.Status = "error:" + errReason
	}
	rs.Attrs = append(rs.Attrs, attrs...)
}

func (b *fakeBuilder) SyntheticSpan(rootCtx context.Context, name, kind string,
	start, end time.Time, attrs []attribute.KeyValue) {
	rs := &recordedSpan{
		Name:      name,
		Kind:      kind,
		Start:     start,
		End:       end,
		Synthetic: true,
		Closed:    true,
		Attrs:     append([]attribute.KeyValue{}, attrs...),
		SpanID:    b.nextID(),
	}
	if rootCtx != nil {
		if root, ok := rootCtx.Value(rootCtxKey{}).(*recordedSpan); ok {
			rs.HasParent = true
			rs.ParentID = root.SpanID
			rs.TraceID = root.TraceID
		}
	}
	b.all = append(b.all, rs)
}

func (b *fakeBuilder) AddSpanEvent(rootCtx context.Context, spanID [8]byte, name string,
	when time.Time, attrs []attribute.KeyValue) {
	var rs *recordedSpan
	if spanID == ([8]byte{}) {
		rs, _ = rootCtx.Value(rootCtxKey{}).(*recordedSpan)
	} else {
		rs = b.openByID[spanID]
	}
	if rs == nil {
		return
	}
	rs.Events = append(rs.Events, recordedEvent{Name: name, When: when, Attrs: attrs})
}

// findSpan returns the first span with the given name, or nil.
func (b *fakeBuilder) findSpan(name string) *recordedSpan {
	for _, s := range b.all {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// findAll returns all spans with the given name in emit order.
func (b *fakeBuilder) findAll(name string) []*recordedSpan {
	var out []*recordedSpan
	for _, s := range b.all {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

// hasAttr reports whether the span has an attribute with the given key.
func hasAttr(s *recordedSpan, key attribute.Key) bool {
	for _, a := range s.Attrs {
		if a.Key == key {
			return true
		}
	}
	return false
}

// attrValue returns the string form of the named attribute, or "".
func attrValue(s *recordedSpan, key attribute.Key) string {
	for _, a := range s.Attrs {
		if a.Key == key {
			return a.Value.Emit()
		}
	}
	return ""
}
