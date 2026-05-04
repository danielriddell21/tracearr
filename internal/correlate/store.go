package correlate

import (
	"sync"
	"time"
)

// TraceState is the per-trace record kept in the store.
type TraceState struct {
	Key       MediaKey
	TraceID   [16]byte // OTel trace id of this attempt's root
	Attempt   int
	Source    Source
	OpenedAt  time.Time
	UpdatedAt time.Time

	// References to active child spans by phase. We keep span IDs so receivers
	// can close the right child without re-resolving.
	GrabSpanID     [8]byte
	HasGrab        bool
	DownloadSpanID [8]byte
	HasDownload    bool

	// SearchStart is the moment we'd anchor a synthetic prowlarr.search span;
	// set on PhaseRequestApproved (or RequestOpened if no approval event arrives).
	SearchStart time.Time

	// DownloadID -> trace mapping is kept in the store directly; this is the
	// last seen DownloadID for this trace so we can clean up index entries.
	DownloadID string

	// LastError is the most recent error reason recorded against this trace.
	LastError string

	// Title is cached for log readability and metrics fallback.
	Title string

	// PreviousTraceID, when set, points at the prior attempt's root for span links.
	PreviousTraceID [16]byte
	HasPrevious     bool
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
}

// memoryStore is a thread-safe in-memory implementation.
type memoryStore struct {
	mu       sync.Mutex
	byKey    map[MediaKey]*TraceState
	byDLID   map[string]MediaKey
	dedup    map[string]time.Time // "source|eventID" -> firstSeen
	dedupTTL time.Duration
}

// NewMemoryStore returns an in-memory Store. dedupTTL bounds how long an
// (source,eventID) is remembered for deduplication (24h is a reasonable default).
func NewMemoryStore(dedupTTL time.Duration) Store {
	return &memoryStore{
		byKey:    make(map[MediaKey]*TraceState),
		byDLID:   make(map[string]MediaKey),
		dedup:    make(map[string]time.Time),
		dedupTTL: dedupTTL,
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
