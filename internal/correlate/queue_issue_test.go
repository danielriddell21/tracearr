package correlate

import (
	"context"
	"testing"

	"github.com/danielriddell21/tracearr/internal/spans"
)

// TestQueueIssueAppendsSpanEvent verifies a poller-driven NoteQueueIssue
// adds a `queue.stalled` event to the in-flight grab span.
func TestQueueIssueAppendsSpanEvent(t *testing.T) {
	eng, fb, _ := newTestEngine(t)
	ctx := context.Background()
	mk := MediaKey{Type: MediaTypeMovie, TMDB: 1}

	eng.Process(ctx, Event{
		Source: SourceOverseerr, Phase: PhaseRequestOpened, OccurredAt: tsec(0),
		Key: mk, Title: "X", RequestID: "r", EventID: "r:p",
	})
	eng.Process(ctx, Event{
		Source: SourceRadarr, Phase: PhaseGrabbed, OccurredAt: tsec(10),
		Key: mk, Title: "X",
		Release: Release{Indexer: "I", DownloadID: "d1", DownloadClient: "NZBGet", Protocol: "usenet"},
		EventID: "d1:grab",
	})

	eng.NoteQueueIssue(ctx, "d1", "warning", "stalled by indexer", tsec(50))

	grab := fb.findSpan(spans.SpanRadarrGrab)
	if grab == nil {
		t.Fatal("missing grab span")
	}
	if len(grab.Events) != 1 {
		t.Fatalf("expected 1 span event, got %d", len(grab.Events))
	}
	if grab.Events[0].Name != "queue.stalled" {
		t.Errorf("event name = %q", grab.Events[0].Name)
	}
}

// TestQueueIssueUnknownDownloadIDIgnored verifies we don't crash on a
// poller event for a downloadID we never saw a Grab for.
func TestQueueIssueUnknownDownloadIDIgnored(t *testing.T) {
	eng, _, _ := newTestEngine(t)
	eng.NoteQueueIssue(context.Background(), "nope", "failed", "", tsec(0))
}
