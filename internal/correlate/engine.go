package correlate

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/danielriddell21/tracearr/internal/spans"
)

// Builder is what the engine calls to translate decisions into actual OTel
// spans. The production implementation wraps the OTel SDK; tests use a
// recording fake.
type Builder interface {
	// OpenRoot starts a new root span and returns a context carrying it
	// plus the OTel trace and span IDs.
	OpenRoot(ctx context.Context, name, spanKind string, start time.Time,
		attrs []attribute.KeyValue, links []trace.Link) (rootCtx context.Context, traceID [16]byte, spanID [8]byte)

	// OpenChild starts a child span under rootCtx and returns its span ID.
	OpenChild(rootCtx context.Context, name, spanKind string, start time.Time,
		attrs []attribute.KeyValue) (spanID [8]byte)

	// CloseSpan ends an open span by its span ID, recording status/error and final attrs.
	CloseSpan(rootCtx context.Context, spanID [8]byte, end time.Time,
		ok bool, errorReason string, attrs []attribute.KeyValue)

	// SyntheticSpan emits a fully-formed span with explicit start/end times,
	// parented under rootCtx (use rootCtx==nil for orphan spans).
	SyntheticSpan(rootCtx context.Context, name, spanKind string, start, end time.Time,
		attrs []attribute.KeyValue)

	// AddSpanEvent appends a named event to an open span.
	AddSpanEvent(rootCtx context.Context, spanID [8]byte, name string, when time.Time,
		attrs []attribute.KeyValue)
}

// Engine is the correlation brain. One Engine per process.
type Engine struct {
	mu      sync.Mutex
	store   Store
	builder Builder
	log     *slog.Logger

	// rootCtx by trace key, kept for the lifetime of the trace so children
	// inherit the same span context. Held only in memory; v0.2 will need a
	// persistence-aware variant.
	ctxByKey map[MediaKey]context.Context

	// Look-back buffer: download events that arrived before their *arr Grab.
	// Keyed by DownloadID. Replayed on the next Grab carrying the same ID.
	pendingDL    map[string]*pendingDownload
	pendingDLTTL time.Duration
}

type pendingDownload struct {
	event   Event
	addedAt time.Time
}

// NewEngine returns a configured engine.
func NewEngine(store Store, builder Builder, log *slog.Logger, pendingDLTTL time.Duration) *Engine {
	return &Engine{
		store:        store,
		builder:      builder,
		log:          log,
		ctxByKey:     make(map[MediaKey]context.Context),
		pendingDL:    make(map[string]*pendingDownload),
		pendingDLTTL: pendingDLTTL,
	}
}

// Process handles a single Event. Errors are logged and swallowed; receivers
// should always 200 their callers.
func (e *Engine) Process(ctx context.Context, ev Event) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.store.SeenEvent(ev.Source, ev.EventID, ev.OccurredAt) {
		e.log.LogAttrs(ctx, slog.LevelDebug, "duplicate event dropped",
			slog.String("source", string(ev.Source)),
			slog.String("event_id", ev.EventID))
		return
	}

	switch ev.Phase {
	case PhaseRequestOpened:
		e.onRequestOpened(ctx, ev)
	case PhaseRequestApproved:
		e.onRequestApproved(ctx, ev)
	case PhaseRequestClosed:
		e.onRequestClosed(ctx, ev)
	case PhaseGrabbed:
		e.onGrabbed(ctx, ev)
	case PhaseDownloadStart:
		e.onDownloadStart(ctx, ev)
	case PhaseDownloadDone:
		e.onDownloadDone(ctx, ev)
	case PhaseImported:
		e.onImported(ctx, ev)
	case PhaseManualNeeded:
		e.onManualNeeded(ctx, ev)
	default:
		e.log.LogAttrs(ctx, slog.LevelWarn, "unknown phase", slog.String("phase", string(ev.Phase)))
	}
}

// resolveTrace looks up a trace by Key, then by series-only key (for TV
// season-pack/whole-show events landing on episode-level state or vice versa).
func (e *Engine) resolveTrace(k MediaKey) (*TraceState, MediaKey, bool) {
	if s, ok := e.store.Get(k); ok {
		return s, k, true
	}
	if k.Type == MediaTypeTV {
		sk := k.SeriesKey()
		if s, ok := e.store.Get(sk); ok {
			return s, sk, true
		}
	}
	return nil, k, false
}

func (e *Engine) onRequestOpened(ctx context.Context, ev Event) {
	if _, _, ok := e.resolveTrace(ev.Key); ok {
		return // duplicate; nothing to do
	}
	attrs := baseAttrs(ev.Key, ev.Title, 1)
	attrs = append(attrs,
		spans.AttrMediaSource.String(string(ev.Source)),
	)
	if ev.RequestID != "" {
		attrs = append(attrs, spans.AttrMediaRequestID.String(ev.RequestID))
	}
	if ev.RequestedBy != "" {
		attrs = append(attrs, spans.AttrMediaRequestedBy.String(ev.RequestedBy))
	}

	rootCtx, tid, sid := e.builder.OpenRoot(ctx, spans.SpanMediaRequest, "SERVER", ev.OccurredAt, attrs, nil)
	state := &TraceState{
		Key:         ev.Key,
		TraceID:     tid,
		Attempt:     1,
		Source:      ev.Source,
		OpenedAt:    ev.OccurredAt,
		UpdatedAt:   ev.OccurredAt,
		SearchStart: ev.OccurredAt,
		Title:       ev.Title,
	}
	_ = sid
	e.ctxByKey[ev.Key] = rootCtx
	e.store.Put(state)
	e.log.LogAttrs(ctx, slog.LevelInfo, "request opened",
		slog.String("event", "media.request.opened"),
		slog.String("media.title", ev.Title),
		slog.String("trace_id", traceIDHex(tid)))
}

func (e *Engine) onRequestApproved(ctx context.Context, ev Event) {
	state, key, ok := e.resolveTrace(ev.Key)
	if !ok {
		return
	}
	rootCtx := e.ctxByKey[key]
	state.SearchStart = ev.OccurredAt
	state.UpdatedAt = ev.OccurredAt
	e.store.Put(state)
	e.builder.AddSpanEvent(rootCtx, [8]byte{}, "request.approved", ev.OccurredAt, nil)
}

func (e *Engine) onRequestClosed(ctx context.Context, ev Event) {
	state, key, ok := e.resolveTrace(ev.Key)
	if !ok {
		return
	}
	rootCtx := e.ctxByKey[key]
	// Defensive close of any still-open children.
	if state.HasDownload {
		e.builder.CloseSpan(rootCtx, state.DownloadSpanID, ev.OccurredAt, true, "", nil)
		state.HasDownload = false
	}
	if state.HasGrab {
		e.builder.CloseSpan(rootCtx, state.GrabSpanID, ev.OccurredAt, true, "", nil)
		state.HasGrab = false
	}
	okStatus := ev.Outcome == OutcomeAvailable
	e.builder.CloseSpan(rootCtx, [8]byte{}, ev.OccurredAt, okStatus, ev.ErrorReason, nil)
	delete(e.ctxByKey, key)
	e.store.Delete(key)
	e.log.LogAttrs(ctx, slog.LevelInfo, "request closed",
		slog.String("event", "media.request.closed"),
		slog.String("outcome", string(ev.Outcome)),
		slog.String("trace_id", traceIDHex(state.TraceID)))
}

func (e *Engine) onGrabbed(ctx context.Context, ev Event) {
	state, key, ok := e.resolveTrace(ev.Key)
	if !ok {
		// Direct *arr search with no Overseerr request — open a synthetic root
		// so we can still trace the lifecycle.
		state = e.openOrphanRoot(ctx, ev)
		key = ev.Key
	}
	rootCtx := e.ctxByKey[key]

	// Synthetic prowlarr.search from SearchStart -> OccurredAt.
	if !state.SearchStart.IsZero() && state.SearchStart.Before(ev.OccurredAt) {
		searchAttrs := []attribute.KeyValue{}
		if ev.Release.Indexer != "" {
			searchAttrs = append(searchAttrs, spans.AttrMediaIndexer.String(ev.Release.Indexer))
		}
		searchAttrs = append(searchAttrs,
			spans.AttrProwlarrElapsedMs.Int64(ev.OccurredAt.Sub(state.SearchStart).Milliseconds()),
		)
		e.builder.SyntheticSpan(rootCtx, spans.SpanProwlarrSearch, "CLIENT",
			state.SearchStart, ev.OccurredAt, searchAttrs)
	}

	// If a previous grab is still open (re-grab within attempt), close it with error.
	if state.HasGrab {
		e.builder.CloseSpan(rootCtx, state.GrabSpanID, ev.OccurredAt, false, "arr.grab_superseded", nil)
		state.HasGrab = false
	}

	spanName := spans.SpanSonarrGrab
	if ev.Source == SourceRadarr {
		spanName = spans.SpanRadarrGrab
	}
	grabAttrs := releaseAttrs(ev.Release)
	sid := e.builder.OpenChild(rootCtx, spanName, "CLIENT", ev.OccurredAt, grabAttrs)
	state.GrabSpanID = sid
	state.HasGrab = true
	state.UpdatedAt = ev.OccurredAt
	if ev.Release.DownloadID != "" {
		state.DownloadID = ev.Release.DownloadID
	}
	e.store.Put(state)

	// Replay any parked download event that arrived before this grab.
	if pd, parked := e.pendingDL[ev.Release.DownloadID]; parked && ev.Release.DownloadID != "" {
		delete(e.pendingDL, ev.Release.DownloadID)
		// Re-enter the download path now that the trace is established.
		// We're already holding the lock; call inline.
		e.handleDownloadStart(rootCtx, state, key, pd.event)
	}
}

func (e *Engine) onDownloadStart(ctx context.Context, ev Event) {
	state, ok := e.store.GetByDownloadID(ev.Release.DownloadID)
	if !ok {
		// Park for replay; cap by TTL via opportunistic GC.
		now := ev.OccurredAt
		cutoff := now.Add(-e.pendingDLTTL)
		for id, pd := range e.pendingDL {
			if pd.addedAt.Before(cutoff) {
				delete(e.pendingDL, id)
			}
		}
		e.pendingDL[ev.Release.DownloadID] = &pendingDownload{event: ev, addedAt: now}
		return
	}
	rootCtx := e.ctxByKey[state.Key]
	e.handleDownloadStart(rootCtx, state, state.Key, ev)
}

func (e *Engine) handleDownloadStart(rootCtx context.Context, state *TraceState, key MediaKey, ev Event) {
	if state.HasDownload {
		return // already open
	}
	attrs := []attribute.KeyValue{
		spans.AttrMediaDownloadClient.String(ev.Release.DownloadClient),
		spans.AttrMediaDownloadClientID.String(ev.Release.DownloadID),
	}
	if ev.Release.SizeBytes > 0 {
		attrs = append(attrs, spans.AttrMediaSizeBytes.Int64(ev.Release.SizeBytes))
	}
	sid := e.builder.OpenChild(rootCtx, spans.SpanDownloadTransfer, "CLIENT", ev.OccurredAt, attrs)
	state.DownloadSpanID = sid
	state.HasDownload = true
	state.UpdatedAt = ev.OccurredAt
	e.store.Put(state)
}

func (e *Engine) onDownloadDone(ctx context.Context, ev Event) {
	state, ok := e.store.GetByDownloadID(ev.Release.DownloadID)
	if !ok {
		return
	}
	rootCtx := e.ctxByKey[state.Key]
	finalAttrs := []attribute.KeyValue{}
	if ev.Release.SizeBytes > 0 {
		finalAttrs = append(finalAttrs, spans.AttrDownloadBytes.Int64(ev.Release.SizeBytes))
	}
	if state.HasDownload {
		ok := ev.ErrorReason == ""
		e.builder.CloseSpan(rootCtx, state.DownloadSpanID, ev.OccurredAt, ok, ev.ErrorReason, finalAttrs)
		state.HasDownload = false
	} else {
		// Start was missed; emit a synthetic span covering the moment.
		e.builder.SyntheticSpan(rootCtx, spans.SpanDownloadTransfer, "CLIENT",
			ev.OccurredAt, ev.OccurredAt, finalAttrs)
	}
	state.UpdatedAt = ev.OccurredAt
	// Stash download end on the grab attrs via a span event so the import span
	// can anchor against it without us needing a new field. Simpler: keep
	// UpdatedAt as the post-download anchor.
	e.store.Put(state)
}

func (e *Engine) onImported(ctx context.Context, ev Event) {
	state, key, ok := e.resolveTrace(ev.Key)
	if !ok {
		return
	}
	rootCtx := e.ctxByKey[key]

	// Defensively close any still-open download.
	if state.HasDownload {
		e.builder.CloseSpan(rootCtx, state.DownloadSpanID, ev.OccurredAt, true, "", nil)
		state.HasDownload = false
	}

	importAttrs := []attribute.KeyValue{}
	if ev.ImportPath != "" {
		importAttrs = append(importAttrs, spans.AttrMediaImportPath.String(ev.ImportPath))
	}
	if ev.FileSizeBytes > 0 {
		importAttrs = append(importAttrs, spans.AttrMediaFileSizeBytes.Int64(ev.FileSizeBytes))
	}
	if ev.Codec != "" {
		importAttrs = append(importAttrs, spans.AttrMediaCodec.String(ev.Codec))
	}
	if ev.Resolution != "" {
		importAttrs = append(importAttrs, spans.AttrMediaResolution.String(ev.Resolution))
	}
	spanName := spans.SpanSonarrImport
	if ev.Source == SourceRadarr {
		spanName = spans.SpanRadarrImport
	}
	// Anchor start at last known download end (UpdatedAt) so the span has
	// non-zero duration when download_done fired earlier.
	start := state.UpdatedAt
	if !start.Before(ev.OccurredAt) {
		start = ev.OccurredAt
	}
	e.builder.SyntheticSpan(rootCtx, spanName, "INTERNAL", start, ev.OccurredAt, importAttrs)

	// Close the grab as the lifecycle's *arr-side work is done.
	if state.HasGrab {
		e.builder.CloseSpan(rootCtx, state.GrabSpanID, ev.OccurredAt, true, "", nil)
		state.HasGrab = false
	}
	state.UpdatedAt = ev.OccurredAt
	e.store.Put(state)

	// If this trace had no Overseerr (orphan/upgrade), close root immediately.
	if state.Source == SourceSonarr || state.Source == SourceRadarr {
		e.builder.CloseSpan(rootCtx, [8]byte{}, ev.OccurredAt, true, "", nil)
		delete(e.ctxByKey, key)
		e.store.Delete(key)
	}
}

func (e *Engine) onManualNeeded(ctx context.Context, ev Event) {
	state, key, ok := e.resolveTrace(ev.Key)
	if !ok {
		return
	}
	rootCtx := e.ctxByKey[key]
	target := state.GrabSpanID
	if !state.HasGrab {
		target = [8]byte{}
	}
	e.builder.AddSpanEvent(rootCtx, target, "manual_interaction_required", ev.OccurredAt, nil)
	state.LastError = spans.ErrManualInteraction
	state.UpdatedAt = ev.OccurredAt
	e.store.Put(state)
}

// openOrphanRoot creates a root span for a Grab that has no preceding
// Overseerr request — typical for direct user searches in the *arr UI or
// for quality upgrades long after the original request closed.
func (e *Engine) openOrphanRoot(ctx context.Context, ev Event) *TraceState {
	attrs := baseAttrs(ev.Key, ev.Title, 1)
	attrs = append(attrs, spans.AttrMediaSource.String(string(ev.Source)))
	rootCtx, tid, _ := e.builder.OpenRoot(ctx, spans.SpanMediaRequest, "SERVER", ev.OccurredAt, attrs, nil)
	state := &TraceState{
		Key:         ev.Key,
		TraceID:     tid,
		Attempt:     1,
		Source:      ev.Source,
		OpenedAt:    ev.OccurredAt,
		UpdatedAt:   ev.OccurredAt,
		SearchStart: ev.OccurredAt,
		Title:       ev.Title,
	}
	e.ctxByKey[ev.Key] = rootCtx
	e.store.Put(state)
	return state
}

// Sweep force-closes traces older than ttl. Intended to be called from a
// janitor goroutine on a fixed interval.
func (e *Engine) Sweep(now time.Time, ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, k := range e.store.Stale(ttl, now) {
		state, ok := e.store.Get(k)
		if !ok {
			continue
		}
		rootCtx := e.ctxByKey[k]
		if state.HasDownload {
			e.builder.CloseSpan(rootCtx, state.DownloadSpanID, now, false, spans.ErrTracearrTimeout, nil)
		}
		if state.HasGrab {
			e.builder.CloseSpan(rootCtx, state.GrabSpanID, now, false, spans.ErrTracearrTimeout, nil)
		}
		e.builder.CloseSpan(rootCtx, [8]byte{}, now, false, spans.ErrTracearrTimeout, nil)
		delete(e.ctxByKey, k)
		e.store.Delete(k)
		e.log.LogAttrs(context.Background(), slog.LevelWarn, "trace timed out",
			slog.String("event", "tracearr.timeout"),
			slog.String("media.title", state.Title),
			slog.String("trace_id", traceIDHex(state.TraceID)))
	}
}

// baseAttrs returns the required-on-every-span attributes derived from a MediaKey.
func baseAttrs(k MediaKey, title string, attempt int) []attribute.KeyValue {
	out := []attribute.KeyValue{
		spans.AttrMediaType.String(string(k.Type)),
		spans.AttrMediaAttempt.Int(attempt),
	}
	if title != "" {
		out = append(out, spans.AttrMediaTitle.String(title))
	}
	if k.TMDB != 0 {
		out = append(out, spans.AttrMediaTMDBID.Int(k.TMDB))
	}
	if k.TVDB != 0 {
		out = append(out, spans.AttrMediaTVDBID.Int(k.TVDB))
	}
	return out
}

func releaseAttrs(r Release) []attribute.KeyValue {
	out := []attribute.KeyValue{}
	if r.Indexer != "" {
		out = append(out, spans.AttrMediaIndexer.String(r.Indexer))
	}
	if r.Title != "" {
		out = append(out, spans.AttrMediaReleaseTitle.String(r.Title))
	}
	if r.Group != "" {
		out = append(out, spans.AttrMediaReleaseGroup.String(r.Group))
	}
	if r.Quality != "" {
		out = append(out, spans.AttrMediaQuality.String(r.Quality))
	}
	if r.QualityProfile != "" {
		out = append(out, spans.AttrMediaQualityProfile.String(r.QualityProfile))
	}
	if r.SizeBytes > 0 {
		out = append(out, spans.AttrMediaSizeBytes.Int64(r.SizeBytes))
	}
	if r.CustomFormatScore != 0 {
		out = append(out, spans.AttrMediaCustomFormatScore.Int(r.CustomFormatScore))
	}
	if r.DownloadClient != "" {
		out = append(out, spans.AttrMediaDownloadClient.String(r.DownloadClient))
	}
	if r.DownloadID != "" {
		out = append(out, spans.AttrMediaDownloadClientID.String(r.DownloadID))
	}
	if r.Protocol != "" {
		out = append(out, spans.AttrMediaProtocol.String(r.Protocol))
	}
	return out
}

func traceIDHex(tid [16]byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 32)
	for i, b := range tid {
		out[i*2] = hexdigits[b>>4]
		out[i*2+1] = hexdigits[b&0x0f]
	}
	return string(out)
}
