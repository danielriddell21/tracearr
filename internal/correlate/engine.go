package correlate

import (
	"context"
	"log/slog"
	"strings"
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

// Restore rehydrates in-flight traces from the store. Each loaded trace is
// marked Resumed; subsequent events for it will be expressed as synthetic
// spans (the original OTel Span handles do not survive a process restart).
// A reconstructed parent SpanContext is installed in ctxByKey so that any
// child spans we synthesise are correctly placed under the original
// TraceID/RootSpanID. Stale span-id pointers (HasGrab/HasDownload) are
// cleared because the spans they referenced were never End()ed and are
// permanently lost from the backend.
func (e *Engine) Restore(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ts := range e.store.LoadAll() {
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    ts.TraceID,
			SpanID:     ts.RootSpanID,
			TraceFlags: trace.FlagsSampled,
			Remote:     true,
		})
		e.ctxByKey[ts.Key] = trace.ContextWithSpanContext(ctx, sc)
		ts.HasGrab = false
		ts.HasDownload = false
		e.store.Put(ts)
		e.log.LogAttrs(ctx, slog.LevelInfo, "trace resumed",
			slog.String("event", "tracearr.resumed"),
			slog.String("media.title", ts.Title),
			slog.Int("media.attempt", ts.Attempt),
			slog.String("trace_id", traceIDHex(ts.TraceID)))
	}
}

// resumedRootAttrs reconstructs the root span attributes from the persisted
// TraceState fields. Used when synthesising the root span at close time
// for a trace that was resumed from disk.
func resumedRootAttrs(state *TraceState) []attribute.KeyValue {
	attrs := baseAttrs(state.Key, state.Title, state.Attempt)
	attrs = append(attrs, spans.AttrMediaSource.String(string(state.Source)))
	if state.RequestID != "" {
		attrs = append(attrs, spans.AttrMediaRequestID.String(state.RequestID))
	}
	if state.RequestedBy != "" {
		attrs = append(attrs, spans.AttrMediaRequestedBy.String(state.RequestedBy))
	}
	return attrs
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
	attempt := 1
	var links []trace.Link
	if prior, ok := e.store.RecallClosed(ev.Key, ev.OccurredAt); ok {
		attempt = prior.Attempt + 1
		links = []trace.Link{linkToPrior(prior, upgradeReasonForOutcome(prior.Outcome))}
	}
	attrs := baseAttrs(ev.Key, ev.Title, attempt)
	attrs = append(attrs, spans.AttrMediaSource.String(string(ev.Source)))
	if ev.RequestID != "" {
		attrs = append(attrs, spans.AttrMediaRequestID.String(ev.RequestID))
	}
	if ev.RequestedBy != "" {
		attrs = append(attrs, spans.AttrMediaRequestedBy.String(ev.RequestedBy))
	}
	if len(links) > 0 {
		attrs = append(attrs, spans.AttrMediaUpgradeReason.String(upgradeReasonForOutcome(e.mustRecallOutcome(ev.Key, ev.OccurredAt))))
	}

	rootCtx, tid, sid := e.builder.OpenRoot(ctx, spans.SpanMediaRequest, "SERVER", ev.OccurredAt, attrs, links)
	state := &TraceState{
		Key:         ev.Key,
		TraceID:     tid,
		RootSpanID:  sid,
		Attempt:     attempt,
		Source:      ev.Source,
		OpenedAt:    ev.OccurredAt,
		UpdatedAt:   ev.OccurredAt,
		SearchStart: ev.OccurredAt,
		Title:       ev.Title,
		RequestID:   ev.RequestID,
		RequestedBy: ev.RequestedBy,
	}
	e.ctxByKey[ev.Key] = rootCtx
	e.store.Put(state)
	e.log.LogAttrs(ctx, slog.LevelInfo, "request opened",
		slog.String("event", "media.request.opened"),
		slog.String("media.title", ev.Title),
		slog.Int("media.attempt", attempt),
		slog.String("trace_id", traceIDHex(tid)))
}

// mustRecallOutcome returns the prior outcome for k, or OutcomeFailed if
// recall fails (meaning the link should never have been created — defensive).
func (e *Engine) mustRecallOutcome(k MediaKey, now time.Time) Outcome {
	if c, ok := e.store.RecallClosed(k, now); ok {
		return c.Outcome
	}
	return OutcomeFailed
}

func linkToPrior(prior ClosedTrace, reason string) trace.Link {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: prior.TraceID,
		// SpanID intentionally zero — we link to the trace, not a specific span.
		// Backends that require a SpanID (Tempo) tolerate this; otherwise use
		// a fixed pseudo-root id derived from the trace id.
		Remote: true,
	})
	return trace.Link{
		SpanContext: sc,
		Attributes: []attribute.KeyValue{
			spans.AttrMediaUpgradeReason.String(reason),
		},
	}
}

func upgradeReasonForOutcome(o Outcome) string {
	switch o {
	case OutcomeFailed, OutcomeTimeout:
		return spans.UpgradeFailedRetry
	case OutcomeAvailable:
		return spans.UpgradeQuality
	default:
		return spans.UpgradeManualSearch
	}
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
	// Defensive close of any still-open children opened in this process.
	if state.HasDownload {
		e.builder.CloseSpan(rootCtx, state.DownloadSpanID, ev.OccurredAt, true, "", nil)
		state.HasDownload = false
	}
	if state.HasGrab {
		e.builder.CloseSpan(rootCtx, state.GrabSpanID, ev.OccurredAt, true, "", nil)
		state.HasGrab = false
	}
	okStatus := ev.Outcome == OutcomeAvailable
	if state.Resumed {
		// Emit synthetic root with original start time so the reassembled
		// trace shows up in Tempo as a complete tree.
		attrs := resumedRootAttrs(state)
		if !okStatus && ev.ErrorReason != "" {
			attrs = append(attrs, attribute.String("error.type", ev.ErrorReason))
		}
		e.builder.SyntheticSpan(nil, spans.SpanMediaRequest, "SERVER",
			state.OpenedAt, ev.OccurredAt, attrs)
	} else {
		e.builder.CloseSpan(rootCtx, [8]byte{}, ev.OccurredAt, okStatus, ev.ErrorReason, nil)
	}
	delete(e.ctxByKey, key)
	e.store.Delete(key)
	e.store.RememberClosed(key, state.TraceID, state.Attempt, ev.Outcome, ev.OccurredAt)
	e.log.LogAttrs(ctx, slog.LevelInfo, "request closed",
		slog.String("event", "media.request.closed"),
		slog.String("outcome", string(ev.Outcome)),
		slog.Bool("resumed", state.Resumed),
		slog.Int("media.attempt", state.Attempt),
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
	state.DownloadStartedAt = ev.OccurredAt
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
		if state.Resumed {
			e.builder.SyntheticSpan(nil, spans.SpanMediaRequest, "SERVER",
				state.OpenedAt, ev.OccurredAt, resumedRootAttrs(state))
		} else {
			e.builder.CloseSpan(rootCtx, [8]byte{}, ev.OccurredAt, true, "", nil)
		}
		delete(e.ctxByKey, key)
		e.store.Delete(key)
		e.store.RememberClosed(key, state.TraceID, state.Attempt, OutcomeAvailable, ev.OccurredAt)
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
// for quality upgrades long after the original request closed. When a
// prior trace for the same media is recalled, the new root is linked to
// it and `media.upgrade_reason` is set.
func (e *Engine) openOrphanRoot(ctx context.Context, ev Event) *TraceState {
	attempt := 1
	var links []trace.Link
	var reason string
	if prior, ok := e.store.RecallClosed(ev.Key, ev.OccurredAt); ok {
		attempt = prior.Attempt + 1
		switch {
		case ev.Release.IsUpgrade:
			reason = spans.UpgradeQuality
		case prior.Outcome == OutcomeFailed || prior.Outcome == OutcomeTimeout:
			reason = spans.UpgradeFailedRetry
		default:
			reason = spans.UpgradeManualSearch
		}
		links = []trace.Link{linkToPrior(prior, reason)}
	} else if ev.Release.IsUpgrade {
		// Sonarr says it's an upgrade but we have no prior — still mark it.
		reason = spans.UpgradeQuality
	}
	attrs := baseAttrs(ev.Key, ev.Title, attempt)
	attrs = append(attrs, spans.AttrMediaSource.String(string(ev.Source)))
	if reason != "" {
		attrs = append(attrs, spans.AttrMediaUpgradeReason.String(reason))
	}
	rootCtx, tid, sid := e.builder.OpenRoot(ctx, spans.SpanMediaRequest, "SERVER", ev.OccurredAt, attrs, links)
	state := &TraceState{
		Key:         ev.Key,
		TraceID:     tid,
		RootSpanID:  sid,
		Attempt:     attempt,
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

// EmitProwlarrSearch surfaces a Prowlarr indexer-query history entry as a
// span. If query matches the title of an in-flight trace, the span is
// parented under that trace's root; otherwise it is emitted as an orphan
// `prowlarr.unmatched` span so operators can still see search activity.
func (e *Engine) EmitProwlarrSearch(ctx context.Context, query, indexer string, when time.Time, elapsedMs int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	attrs := []attribute.KeyValue{
		spans.AttrMediaIndexer.String(indexer),
		spans.AttrMediaSearchQuery.String(query),
	}
	if elapsedMs > 0 {
		attrs = append(attrs, spans.AttrProwlarrElapsedMs.Int64(elapsedMs))
	}
	// The poller emits at the entry's `date`; we synthesise a near-zero-duration
	// span at that point. Real elapsed data is on the attribute.
	end := when
	if elapsedMs > 0 {
		end = when.Add(time.Duration(elapsedMs) * time.Millisecond)
	}
	if rootCtx, ok := e.lookupByTitle(query); ok {
		e.builder.SyntheticSpan(rootCtx, spans.SpanProwlarrSearch, "CLIENT", when, end, attrs)
		return
	}
	e.builder.SyntheticSpan(nil, "prowlarr.unmatched", "CLIENT", when, end, attrs)
}

// lookupByTitle searches in-flight traces for one whose title appears in q.
// Best-effort, case-insensitive substring on a normalised form. Returns the
// root context for use as a synthetic span parent.
func (e *Engine) lookupByTitle(q string) (context.Context, bool) {
	qn := normaliseTitle(q)
	if qn == "" {
		return nil, false
	}
	for k, ctx := range e.ctxByKey {
		state, ok := e.store.Get(k)
		if !ok || state.Title == "" {
			continue
		}
		tn := normaliseTitle(state.Title)
		if tn == "" {
			continue
		}
		if strings.Contains(qn, tn) || strings.Contains(tn, qn) {
			return ctx, true
		}
	}
	return nil, false
}

func normaliseTitle(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// NoteQueueIssue records a stalled-queue event on the in-flight trace
// matching downloadID. If no trace matches (queue item belongs to a release
// we never saw a Grab webhook for), the call is a no-op. Called by the
// queue poller.
func (e *Engine) NoteQueueIssue(ctx context.Context, downloadID, status, msg string, when time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	state, ok := e.store.GetByDownloadID(downloadID)
	if !ok {
		return
	}
	rootCtx := e.ctxByKey[state.Key]
	target := state.GrabSpanID
	if !state.HasGrab {
		target = [8]byte{}
	}
	attrs := []attribute.KeyValue{
		attribute.String("queue.status", status),
	}
	if msg != "" {
		attrs = append(attrs, attribute.String("queue.message", msg))
	}
	e.builder.AddSpanEvent(rootCtx, target, "queue.stalled", when, attrs)
	state.LastError = "arr.queue_" + status
	state.UpdatedAt = when
	e.store.Put(state)
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
		if state.Resumed {
			attrs := resumedRootAttrs(state)
			attrs = append(attrs, attribute.String("error.type", spans.ErrTracearrTimeout))
			e.builder.SyntheticSpan(nil, spans.SpanMediaRequest, "SERVER",
				state.OpenedAt, now, attrs)
		} else {
			e.builder.CloseSpan(rootCtx, [8]byte{}, now, false, spans.ErrTracearrTimeout, nil)
		}
		delete(e.ctxByKey, k)
		e.store.Delete(k)
		e.store.RememberClosed(k, state.TraceID, state.Attempt, OutcomeTimeout, now)
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
