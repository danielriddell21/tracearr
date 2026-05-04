package correlate

import (
	"sync"
	"time"
)

// TraceState is the per-trace record kept in the store. It is JSON-
// serialised into BoltDB; field names are stable.
type TraceState struct {
	Key       MediaKey  `json:"key"`
	TraceID   [16]byte  `json:"trace_id"`
	RootSpanID [8]byte  `json:"root_span_id"`
	Attempt   int       `json:"attempt"`
	Source    Source    `json:"source"`
	OpenedAt  time.Time `json:"opened_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// References to active child spans by phase. We keep span IDs so receivers
	// can close the right child without re-resolving.
	GrabSpanID     [8]byte `json:"grab_span_id"`
	HasGrab        bool    `json:"has_grab"`
	DownloadSpanID [8]byte `json:"download_span_id"`
	HasDownload    bool    `json:"has_download"`

	// SearchStart is the moment we'd anchor a synthetic prowlarr.search span;
	// set on PhaseRequestApproved (or RequestOpened if no approval event arrives).
	SearchStart time.Time `json:"search_start"`

	// DownloadStartedAt records when the download.transfer span was opened, so
	// that a post-restart download.done can synthesise a span with accurate
	// start time even though the original Span handle is lost.
	DownloadStartedAt time.Time `json:"download_started_at"`

	// DownloadID -> trace mapping is kept in the store directly; this is the
	// last seen DownloadID for this trace so we can clean up index entries.
	DownloadID string `json:"download_id"`

	// LastError is the most recent error reason recorded against this trace.
	LastError string `json:"last_error"`

	// Cached attrs needed to faithfully re-synthesise the root span post-
	// restart (OTel does not let us End() a span whose handle has been lost).
	Title       string `json:"title"`
	RequestID   string `json:"request_id"`
	RequestedBy string `json:"requested_by"`

	// Resumed is true when this state was loaded from disk by Restore(). It
	// flips back to false once the trace closes. While Resumed, the engine
	// uses synthetic spans (with reconstructed parent context) instead of
	// live tracer.Start spans so we don't dangle un-ended span handles.
	Resumed bool `json:"-"`
}

// Store persists in-flight traces. The v0.1 implementation is in-memory
// with a janitor; v0.2 replaces this with a BoltDB-backed implementation
// behind the same interface.
type Store interface {
	// Get returns the trace for k, or false if absent.
	Get(k MediaKey) (*TraceState, bool)

	// GetByDownloadID looks up a trace by SABnzbd nzo_id / NZBGet NZBID.
	GetByDownloadID(downloadID string) (*TraceState, bool)

	// Put inserts or updates a trace record.
	Put(s *TraceState)

	// Delete removes a trace and any download-id index entry pointing at it.
	Delete(k MediaKey)

	// Stale returns trace keys older than ttl by UpdatedAt. The caller is
	// expected to close them and call Delete.
	Stale(ttl time.Duration, now time.Time) []MediaKey

	// SeenEvent atomically records (source, eventID); returns true if this
	// is the first time we have seen it (i.e., not a duplicate). Empty IDs
	// always return true.
	SeenEvent(source Source, eventID string, now time.Time) bool

	// RememberClosed records the just-closed trace's TraceID + outcome so a
	// subsequent attempt for the same media (re-grab, quality upgrade, retry)
	// can be linked to it. Records expire after the store's configured
	// recall TTL.
	RememberClosed(k MediaKey, traceID [16]byte, attempt int, outcome Outcome, when time.Time)

	// RecallClosed returns the most recent closed trace for k, if any record
	// is still within the recall TTL.
	RecallClosed(k MediaKey, now time.Time) (ClosedTrace, bool)

	// LoadAll returns every persisted in-flight trace. Used by Engine.Restore
	// at startup to rehydrate state. Each returned TraceState should have
	// Resumed=true so the engine knows to emit synthetic spans rather than
	// trying to use live OTel handles it doesn't have.
	LoadAll() []*TraceState

	// Close releases resources (closing the BoltDB handle, flushing, etc.).
	Close() error
}

// ClosedTrace is what RecallClosed returns: the bare minimum to construct
// a span Link plus enough metadata for upgrade-reason inference.
type ClosedTrace struct {
	TraceID  [16]byte
	Attempt  int
	Outcome  Outcome
	ClosedAt time.Time
}

// memoryStore is a thread-safe in-memory implementation.
type memoryStore struct {
	mu        sync.Mutex
	byKey     map[MediaKey]*TraceState
	byDLID    map[string]MediaKey
	dedup     map[string]time.Time // "source|eventID" -> firstSeen
	closed    map[MediaKey]ClosedTrace
	dedupTTL  time.Duration
	recallTTL time.Duration
}

// NewMemoryStore returns an in-memory Store. dedupTTL bounds how long an
// (source,eventID) is remembered for deduplication (24h is a reasonable
// default); recallTTL bounds how long a closed trace is recalled for
// upgrade-link purposes (30d is a reasonable default).
func NewMemoryStore(dedupTTL, recallTTL time.Duration) Store {
	return &memoryStore{
		byKey:     make(map[MediaKey]*TraceState),
		byDLID:    make(map[string]MediaKey),
		dedup:     make(map[string]time.Time),
		closed:    make(map[MediaKey]ClosedTrace),
		dedupTTL:  dedupTTL,
		recallTTL: recallTTL,
	}
}

func (m *memoryStore) Get(k MediaKey) (*TraceState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.byKey[k]
	return s, ok
}

func (m *memoryStore) GetByDownloadID(id string) (*TraceState, bool) {
	if id == "" {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k, ok := m.byDLID[id]
	if !ok {
		return nil, false
	}
	s, ok := m.byKey[k]
	return s, ok
}

func (m *memoryStore) Put(s *TraceState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byKey[s.Key] = s
	if s.DownloadID != "" {
		m.byDLID[s.DownloadID] = s.Key
	}
}

func (m *memoryStore) Delete(k MediaKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.byKey[k]; ok && s.DownloadID != "" {
		delete(m.byDLID, s.DownloadID)
	}
	delete(m.byKey, k)
}

func (m *memoryStore) Stale(ttl time.Duration, now time.Time) []MediaKey {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := now.Add(-ttl)
	var out []MediaKey
	for k, s := range m.byKey {
		if s.UpdatedAt.Before(cutoff) {
			out = append(out, k)
		}
	}
	return out
}

func (m *memoryStore) RememberClosed(k MediaKey, tid [16]byte, attempt int, outcome Outcome, when time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Opportunistic GC.
	cutoff := when.Add(-m.recallTTL)
	for k2, c := range m.closed {
		if c.ClosedAt.Before(cutoff) {
			delete(m.closed, k2)
		}
	}
	m.closed[k] = ClosedTrace{TraceID: tid, Attempt: attempt, Outcome: outcome, ClosedAt: when}
}

func (m *memoryStore) RecallClosed(k MediaKey, now time.Time) (ClosedTrace, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.closed[k]
	if !ok {
		// Try the series-level key for TV; episode upgrades should link to the
		// most recent attempt at any level for the same series.
		if k.Type == MediaTypeTV {
			if c2, ok2 := m.closed[k.SeriesKey()]; ok2 {
				c, ok = c2, true
			}
		}
	}
	if !ok {
		return ClosedTrace{}, false
	}
	if c.ClosedAt.Before(now.Add(-m.recallTTL)) {
		delete(m.closed, k)
		return ClosedTrace{}, false
	}
	return c, true
}

func (m *memoryStore) SeenEvent(src Source, id string, now time.Time) bool {
	if id == "" {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Opportunistic GC of expired dedup entries.
	cutoff := now.Add(-m.dedupTTL)
	for k, t := range m.dedup {
		if t.Before(cutoff) {
			delete(m.dedup, k)
		}
	}
	key := string(src) + "|" + id
	if _, ok := m.dedup[key]; ok {
		return false
	}
	m.dedup[key] = now
	return true
}

func (m *memoryStore) LoadAll() []*TraceState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*TraceState, 0, len(m.byKey))
	for _, s := range m.byKey {
		out = append(out, s)
	}
	return out
}

func (m *memoryStore) Close() error { return nil }
