package correlate

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/danielriddell21/tracearr/internal/spans"
)

// TestBoltStoreRoundtrip verifies a TraceState survives a Close/Open cycle.
func TestBoltStoreRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tracearr.db")
	s1, err := OpenBoltStore(path, time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	want := &TraceState{
		Key:        MediaKey{Type: MediaTypeMovie, TMDB: 42},
		TraceID:    [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		RootSpanID: [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Attempt:    1,
		Source:     SourceOverseerr,
		OpenedAt:   time.Unix(100, 0).UTC(),
		UpdatedAt:  time.Unix(100, 0).UTC(),
		DownloadID: "nzb-xyz",
		Title:      "Restart Test",
	}
	s1.Put(want)
	s1.RememberClosed(MediaKey{Type: MediaTypeMovie, TMDB: 99}, [16]byte{99}, 1, OutcomeAvailable, time.Unix(50, 0).UTC())
	if !s1.SeenEvent(SourceOverseerr, "ev-1", time.Unix(0, 0).UTC()) {
		t.Fatal("first SeenEvent should return true")
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenBoltStore(path, time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, ok := s2.Get(want.Key)
	if !ok {
		t.Fatal("trace lost across reopen")
	}
	if got.Title != want.Title || got.TraceID != want.TraceID || got.RootSpanID != want.RootSpanID {
		t.Errorf("trace round-trip mismatch: got %+v", got)
	}

	if got, ok := s2.GetByDownloadID("nzb-xyz"); !ok || got.Key != want.Key {
		t.Errorf("download index lost: ok=%v got=%+v", ok, got)
	}

	if c, ok := s2.RecallClosed(MediaKey{Type: MediaTypeMovie, TMDB: 99}, time.Unix(60, 0).UTC()); !ok || c.Outcome != OutcomeAvailable {
		t.Errorf("closed recall lost: ok=%v c=%+v", ok, c)
	}

	if s2.SeenEvent(SourceOverseerr, "ev-1", time.Unix(0, 0).UTC()) {
		t.Error("dedup of ev-1 lost across reopen")
	}

	loaded := s2.LoadAll()
	if len(loaded) != 1 || !loaded[0].Resumed {
		t.Errorf("LoadAll should mark Resumed=true; got %+v", loaded)
	}
}

// TestRestartResumeProducesSyntheticRoot opens a trace, simulates a crash
// by spinning up a fresh engine pointed at the same Bolt file, then
// delivers the closing events. Asserts the resumed trace produces a
// synthetic root span with the original start time.
func TestRestartResumeProducesSyntheticRoot(t *testing.T) {
	dir := t.TempDir()
	dbpath := filepath.Join(dir, "trace.db")
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mk := MediaKey{Type: MediaTypeMovie, TMDB: 7}

	// --- Process 1: open trace, get to mid-download, "crash". ---
	store1, err := OpenBoltStore(dbpath, time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	fb1 := newFakeBuilder()
	eng1 := NewEngine(store1, fb1, logger, time.Minute)
	ctx := context.Background()
	eng1.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(100),
		Key: mk, Title: "Resume Movie", RequestID: "r1", RequestedBy: "alice",
		EventID: "r1:p",
	})
	eng1.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseGrabbed, OccurredAt: tsec(120),
		Key: mk, Title: "Resume Movie",
		Release: Release{Indexer: "I", DownloadID: "d1", DownloadClient: "NZBGet", Protocol: "usenet"},
		EventID: "d1:grab",
	})
	eng1.Process(ctx, Event{
		Source: SourceNZBGet, Phase: PhaseDownloadStart, OccurredAt: tsec(125),
		Release: Release{DownloadID: "d1", DownloadClient: "nzbget"},
		EventID: "d1:queue_added",
	})
	originalRoots := fb1.findAll(spans.SpanMediaRequest)
	if len(originalRoots) != 1 {
		t.Fatalf("expected 1 root pre-crash, got %d", len(originalRoots))
	}
	originalTraceID := originalRoots[0].TraceID
	if err := store1.Close(); err != nil {
		t.Fatal(err)
	}

	// --- Process 2: fresh engine, fresh builder, same disk. ---
	store2, err := OpenBoltStore(dbpath, time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	fb2 := newFakeBuilder()
	eng2 := NewEngine(store2, fb2, logger, time.Minute)
	eng2.Restore(ctx)

	// Confirm the trace was rehydrated and marked Resumed.
	loaded, ok := store2.Get(mk)
	if !ok || !loaded.Resumed {
		t.Fatalf("expected Resumed trace; got ok=%v state=%+v", ok, loaded)
	}
	if loaded.HasGrab || loaded.HasDownload {
		t.Error("Restore should clear stale child-span flags")
	}

	// Closing events arrive post-restart.
	eng2.Process(ctx, Event{
		Source: SourceNZBGet, Phase: PhaseDownloadDone, OccurredAt: tsec(900),
		Release: Release{DownloadID: "d1", SizeBytes: 12345},
		EventID: "d1:post_processed",
	})
	eng2.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseImported, OccurredAt: tsec(910),
		Key: mk, Title: "Resume Movie", ImportPath: "/m/x.mkv",
		EventID: "d1:import",
	})
	eng2.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestClosed, OccurredAt: tsec(915),
		Key: mk, Outcome: OutcomeAvailable, EventID: "r1:av",
	})

	// The post-restart trace should contain a synthetic root spanning the
	// original OpenedAt -> close event time.
	roots := fb2.findAll(spans.SpanMediaRequest)
	if len(roots) != 1 {
		t.Fatalf("expected 1 synthetic root post-restart, got %d", len(roots))
	}
	root := roots[0]
	if !root.Synthetic {
		t.Error("post-restart root should be synthetic")
	}
	if root.Start != tsec(100) {
		t.Errorf("synthetic root start = %v, want %v", root.Start, tsec(100))
	}
	if root.End != tsec(915) {
		t.Errorf("synthetic root end = %v, want %v", root.End, tsec(915))
	}
	if !hasAttr(root, spans.AttrMediaRequestedBy) {
		t.Error("synthetic root should carry RequestedBy attr")
	}
	// Trace ID isn't preserved on the synthetic root in our fake (it's a
	// brand-new emission with no parent) — but in the OTel-backed builder
	// we'd pass the original trace context. The fake records ParentID as
	// zero for a no-parent SyntheticSpan; that's expected.
	_ = originalTraceID
}
