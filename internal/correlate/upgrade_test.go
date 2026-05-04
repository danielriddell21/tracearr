package correlate

import (
	"context"
	"testing"

	"github.com/danielriddell21/tracearr/internal/spans"
)

// TestQualityUpgradeOpensLinkedTrace simulates a Sonarr "isUpgrade=true"
// grab arriving long after the original Overseerr-driven trace closed.
// The new root must carry a span Link to the original trace ID and the
// `media.upgrade_reason` attribute set to quality_upgrade.
func TestQualityUpgradeOpensLinkedTrace(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()
	mk := MediaKey{Type: MediaTypeMovie, TMDB: 99}

	// Original happy-path trace.
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(0),
		Key: mk, Title: "X", RequestID: "r1", EventID: "r1:p",
	})
	eng.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseGrabbed, OccurredAt: tsec(10),
		Key: mk, Title: "X",
		Release: Release{Indexer: "I", DownloadID: "d1", DownloadClient: "NZBGet", Protocol: "usenet", Quality: "WEBDL-1080p"},
		EventID: "d1:grab",
	})
	eng.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseImported, OccurredAt: tsec(100),
		Key: mk, Title: "X", ImportPath: "/m/x.mkv", EventID: "d1:import",
	})
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestClosed, OccurredAt: tsec(110),
		Key: mk, Outcome: OutcomeAvailable, EventID: "r1:av",
	})

	originalTraceID := fb.findSpan(spans.SpanMediaRequest).TraceID

	// Days later, Radarr fires an upgrade Grab — orphan path.
	eng.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseGrabbed, OccurredAt: tsec(86400),
		Key: mk, Title: "X",
		Release: Release{
			Indexer: "I", DownloadID: "d2", DownloadClient: "NZBGet", Protocol: "usenet",
			Quality: "WEBDL-2160p", IsUpgrade: true,
		},
		EventID: "d2:grab",
	})

	roots := fb.findAll(spans.SpanMediaRequest)
	if len(roots) != 2 {
		t.Fatalf("expected 2 root spans, got %d", len(roots))
	}
	upgradeRoot := roots[1]
	if attrValue(upgradeRoot, spans.AttrMediaAttempt) != "2" {
		t.Errorf("attempt = %q, want 2", attrValue(upgradeRoot, spans.AttrMediaAttempt))
	}
	if attrValue(upgradeRoot, spans.AttrMediaUpgradeReason) != spans.UpgradeQuality {
		t.Errorf("upgrade_reason = %q, want %q",
			attrValue(upgradeRoot, spans.AttrMediaUpgradeReason), spans.UpgradeQuality)
	}
	if len(upgradeRoot.Links) != 1 {
		t.Fatalf("expected 1 link, got %d", len(upgradeRoot.Links))
	}
	if upgradeRoot.Links[0].SpanContext.TraceID() != originalTraceID {
		t.Errorf("link trace = %v, want %v", upgradeRoot.Links[0].SpanContext.TraceID(), originalTraceID)
	}
}

// TestFailedRetryClassification verifies a failed-then-retried request is
// linked with reason=failed_retry, not quality_upgrade.
func TestFailedRetryClassification(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()
	mk := MediaKey{Type: MediaTypeMovie, TMDB: 1}

	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(0),
		Key: mk, Title: "X", RequestID: "r1", EventID: "r1:p",
	})
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestClosed, OccurredAt: tsec(50),
		Key: mk, Outcome: OutcomeFailed, EventID: "r1:fail",
	})
	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(100),
		Key: mk, Title: "X", RequestID: "r2", EventID: "r2:p",
	})

	roots := fb.findAll(spans.SpanMediaRequest)
	if len(roots) != 2 {
		t.Fatalf("got %d roots", len(roots))
	}
	if attrValue(roots[1], spans.AttrMediaUpgradeReason) != spans.UpgradeFailedRetry {
		t.Errorf("upgrade_reason = %q, want %q",
			attrValue(roots[1], spans.AttrMediaUpgradeReason), spans.UpgradeFailedRetry)
	}
	if len(roots[1].Links) != 1 {
		t.Errorf("expected 1 link on retry root, got %d", len(roots[1].Links))
	}
}
