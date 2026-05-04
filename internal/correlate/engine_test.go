package correlate

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/danielriddell21/tracearr/internal/spans"
)

func newTestEngine(t *testing.T) (*Engine, *fakeBuilder, Store) {
	t.Helper()
	store := NewMemoryStore(time.Hour, 30*24*time.Hour)
	fb := newFakeBuilder()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	eng := NewEngine(store, fb, logger, time.Minute)
	return eng, fb, store
}

func tsec(s int) time.Time {
	return time.Unix(int64(s), 0).UTC()
}

// TestMovieHappyPath walks an Overseerr → Radarr → NZBGet → import flow
// and asserts the final span tree shape and required attributes.
func TestMovieHappyPath(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()
	mediaKey := MediaKey{Type: MediaTypeMovie, TMDB: 603}

	// 1. Overseerr opens the request.
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(100),
		Key: mediaKey, Title: "The Matrix", RequestID: "req-1", RequestedBy: "alice",
		EventID: "req-1:MEDIA_PENDING",
	})
	// 2. Approval — anchors prowlarr.search start.
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestApproved, OccurredAt: tsec(110),
		Key: mediaKey, EventID: "req-1:MEDIA_APPROVED",
	})
	// 3. Radarr grabs.
	eng.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseGrabbed, OccurredAt: tsec(125),
		Key: mediaKey, Title: "The Matrix",
		Release: Release{
			Title: "The.Matrix.1999.2160p.WEB.x265-FLUX",
			Indexer: "NZBgeek", Group: "FLUX", Quality: "WEBDL-2160p",
			SizeBytes: 12345678901, DownloadClient: "NZBGet", DownloadID: "nzb-aaa",
			Protocol: "usenet",
		},
		EventID: "nzb-aaa:grab",
	})
	// 4. NZBGet starts download.
	eng.Process(ctx, Event{
		Source: SourceNZBGet, Phase: PhaseDownloadStart, OccurredAt: tsec(130),
		Release: Release{DownloadID: "nzb-aaa", DownloadClient: "nzbget", SizeBytes: 12345678901},
		EventID: "nzb-aaa:queue_added",
	})
	// 5. NZBGet finishes download.
	eng.Process(ctx, Event{
		Source: SourceNZBGet, Phase: PhaseDownloadDone, OccurredAt: tsec(900),
		Release: Release{DownloadID: "nzb-aaa", SizeBytes: 12345678901},
		EventID: "nzb-aaa:post_processed",
	})
	// 6. Radarr imports.
	eng.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseImported, OccurredAt: tsec(910),
		Key: mediaKey, Title: "The Matrix",
		ImportPath: "/movies/The Matrix (1999)/movie.mkv", FileSizeBytes: 12345678901,
		Codec: "x265", Resolution: "3840x2160",
		EventID: "nzb-aaa:import",
	})
	// 7. Overseerr closes the request as available.
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestClosed, OccurredAt: tsec(915),
		Key: mediaKey, Outcome: OutcomeAvailable,
		EventID: "req-1:MEDIA_AVAILABLE",
	})

	// Span tree assertions.
	root := fb.findSpan(spans.SpanMediaRequest)
	if root == nil {
		t.Fatal("missing media.request root")
	}
	if root.Kind != "SERVER" {
		t.Errorf("root kind = %q, want SERVER", root.Kind)
	}
	if !root.Closed || root.Status != "ok" {
		t.Errorf("root not closed ok: closed=%v status=%q", root.Closed, root.Status)
	}
	if got := attrValue(root, spans.AttrMediaTMDBID); got != "603" {
		t.Errorf("root tmdb_id = %q, want 603", got)
	}
	if !hasAttr(root, spans.AttrMediaRequestedBy) {
		t.Error("root missing requested_by")
	}

	if got := fb.findSpan(spans.SpanProwlarrSearch); got == nil {
		t.Error("missing synthetic prowlarr.search")
	} else if !got.Synthetic {
		t.Error("prowlarr.search should be synthetic")
	} else if got.Start != tsec(110) || got.End != tsec(125) {
		t.Errorf("prowlarr.search times = [%v,%v], want [%v,%v]", got.Start, got.End, tsec(110), tsec(125))
	}

	grab := fb.findSpan(spans.SpanRadarrGrab)
	if grab == nil {
		t.Fatal("missing radarr.grab")
	}
	if grab.Kind != "CLIENT" || !grab.Closed {
		t.Errorf("grab kind=%q closed=%v", grab.Kind, grab.Closed)
	}
	if grab.ParentID != root.SpanID {
		t.Errorf("grab parent != root")
	}
	if got := attrValue(grab, spans.AttrMediaIndexer); got != "NZBgeek" {
		t.Errorf("grab indexer = %q", got)
	}

	dl := fb.findSpan(spans.SpanDownloadTransfer)
	if dl == nil {
		t.Fatal("missing download.transfer")
	}
	if !dl.Closed {
		t.Error("download.transfer not closed")
	}

	imp := fb.findSpan(spans.SpanRadarrImport)
	if imp == nil {
		t.Fatal("missing radarr.import")
	}
	if imp.Kind != "INTERNAL" {
		t.Errorf("import kind = %q, want INTERNAL", imp.Kind)
	}
	if !imp.Synthetic {
		t.Error("import should be synthetic")
	}
}

// TestTVEpisodeFlow exercises the per-episode key path and series-level
// fallback when the Overseerr request was opened at series level but the
// Sonarr Grab carries per-episode info.
func TestTVEpisodeFlow(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()

	seriesKey := MediaKey{Type: MediaTypeTV, TVDB: 305288}
	epKey := MediaKey{Type: MediaTypeTV, TVDB: 305288, Season: 2, Episode: 4}

	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(100),
		Key: seriesKey, Title: "Severance", RequestID: "req-2",
		EventID: "req-2:MEDIA_PENDING",
	})
	eng.Process(ctx, Event{
		Source: SourceSonarr, Phase: PhaseGrabbed, OccurredAt: tsec(120),
		Key: epKey, Title: "Severance",
		Release: Release{
			Title: "Severance.S02E04.WEBRip.x264-NoGroup",
			Indexer: "DrunkenSlug", DownloadClient: "NZBGet", DownloadID: "nzb-bbb",
			Protocol: "usenet", Quality: "WEBRip-1080p",
		},
		EventID: "nzb-bbb:grab:2:4",
	})
	eng.Process(ctx, Event{
		Source: SourceNZBGet, Phase: PhaseDownloadDone, OccurredAt: tsec(300),
		Release: Release{DownloadID: "nzb-bbb"},
		EventID: "nzb-bbb:post_processed",
	})
	eng.Process(ctx, Event{
		Source: SourceSonarr, Phase: PhaseImported, OccurredAt: tsec(310),
		Key: epKey, Title: "Severance", ImportPath: "/tv/Severance/S02/E04.mkv",
		EventID: "nzb-bbb:import:2:4",
	})
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestClosed, OccurredAt: tsec(320),
		Key: seriesKey, Outcome: OutcomeAvailable, EventID: "req-2:MEDIA_AVAILABLE",
	})

	if got := fb.findSpan(spans.SpanMediaRequest); got == nil || !got.Closed || got.Status != "ok" {
		t.Errorf("root: %+v", got)
	}
	if got := fb.findSpan(spans.SpanSonarrGrab); got == nil {
		t.Error("missing sonarr.grab")
	}
	if got := fb.findSpan(spans.SpanSonarrImport); got == nil {
		t.Error("missing sonarr.import")
	}
}

// TestRaceDownloadStartBeforeGrab verifies the look-back buffer: a download-
// start event that arrives before its parent Grab is parked and replayed.
func TestRaceDownloadStartBeforeGrab(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()
	mk := MediaKey{Type: MediaTypeMovie, TMDB: 1}

	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(0),
		Key: mk, Title: "X", RequestID: "r1", EventID: "r1:p",
	})
	// Download start lands first.
	eng.Process(ctx, Event{
		Source: SourceNZBGet, Phase: PhaseDownloadStart, OccurredAt: tsec(5),
		Release: Release{DownloadID: "early-id", DownloadClient: "nzbget"},
		EventID: "early-id:queue_added",
	})
	// No download.transfer yet — it's parked.
	if got := fb.findSpan(spans.SpanDownloadTransfer); got != nil {
		t.Fatal("download.transfer should not be open before grab")
	}
	// Grab arrives.
	eng.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseGrabbed, OccurredAt: tsec(10),
		Key: mk, Title: "X",
		Release: Release{Indexer: "I", DownloadID: "early-id", DownloadClient: "NZBGet", Protocol: "usenet"},
		EventID: "early-id:grab",
	})
	// Now the parked event should have been replayed.
	if got := fb.findSpan(spans.SpanDownloadTransfer); got == nil {
		t.Error("expected download.transfer after replay")
	}
}

// TestDeduplication verifies that a duplicate (source, eventID) is dropped.
func TestDeduplication(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()
	mk := MediaKey{Type: MediaTypeMovie, TMDB: 1}
	ev := Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(0),
		Key: mk, Title: "X", EventID: "dup-1",
	}
	eng.Process(ctx, ev)
	eng.Process(ctx, ev) // duplicate
	roots := fb.findAll(spans.SpanMediaRequest)
	if len(roots) != 1 {
		t.Errorf("expected 1 root after duplicate, got %d", len(roots))
	}
}

// TestJanitorTimeout asserts a stale trace is force-closed with the right error.
func TestJanitorTimeout(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()
	mk := MediaKey{Type: MediaTypeMovie, TMDB: 1}
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(0),
		Key: mk, Title: "X", EventID: "t1",
	})
	// Sweep with a tiny TTL relative to far-future "now".
	eng.Sweep(tsec(86400), 100*time.Second)
	root := fb.findSpan(spans.SpanMediaRequest)
	if root == nil || !root.Closed {
		t.Fatal("root not closed")
	}
	if root.Status != "error:"+spans.ErrTracearrTimeout {
		t.Errorf("status = %q, want error:%s", root.Status, spans.ErrTracearrTimeout)
	}
}

// TestOrphanGrabOpensRoot verifies that a Sonarr Grab without a prior Overseerr
// request still produces a complete trace (covers manual searches and quality
// upgrades initiated from the *arr UI).
func TestOrphanGrabOpensRoot(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()
	mk := MediaKey{Type: MediaTypeMovie, TMDB: 42}

	eng.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseGrabbed, OccurredAt: tsec(50),
		Key: mk, Title: "Orphan",
		Release: Release{Indexer: "I", DownloadID: "z1", DownloadClient: "NZBGet", Protocol: "usenet"},
		EventID: "z1:grab",
	})
	eng.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseImported, OccurredAt: tsec(60),
		Key: mk, Title: "Orphan", ImportPath: "/movies/x.mkv", EventID: "z1:import",
	})

	root := fb.findSpan(spans.SpanMediaRequest)
	if root == nil {
		t.Fatal("expected synthetic root for orphan grab")
	}
	if !root.Closed || root.Status != "ok" {
		t.Errorf("root not closed ok: %+v", root)
	}
	if attrValue(root, spans.AttrMediaSource) != string(SourceRadarr) {
		t.Errorf("orphan root source = %q, want radarr", attrValue(root, spans.AttrMediaSource))
	}
}

// TestRequiredAttributesEverySpan asserts the schema's "required on every
// span" rules: media.type, media.attempt, media.title, and a TMDB or TVDB id.
func TestRequiredAttributesEverySpan(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()
	mk := MediaKey{Type: MediaTypeMovie, TMDB: 1}
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(0),
		Key: mk, Title: "X", EventID: "r1:p",
	})
	root := fb.findSpan(spans.SpanMediaRequest)
	if root == nil {
		t.Fatal("missing root")
	}
	if !hasAttr(root, spans.AttrMediaType) {
		t.Error("root missing media.type")
	}
	if !hasAttr(root, spans.AttrMediaAttempt) {
		t.Error("root missing media.attempt")
	}
	if !hasAttr(root, spans.AttrMediaTitle) {
		t.Error("root missing media.title")
	}
	if !hasAttr(root, spans.AttrMediaTMDBID) && !hasAttr(root, spans.AttrMediaTVDBID) {
		t.Error("root missing tmdb/tvdb id")
	}
}
