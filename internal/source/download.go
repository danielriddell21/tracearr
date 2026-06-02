package source

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/danielriddell21/tracearr/internal/correlate"
	"github.com/danielriddell21/tracearr/internal/spans"
)

// DownloadPayload is the schema we expect from the helper scripts shipped
// for NZBGet and SABnzbd. Both clients can run a script on queue events
// and post-processing; the script wraps their state into this JSON and
// POSTs to /script/<client>.
type DownloadPayload struct {
	Event      string `json:"event"`       // "queue_added" | "post_processed"
	DownloadID string `json:"download_id"` // NZBGet NZBID or SAB nzo_id
	Name       string `json:"name"`
	Category   string `json:"category"` // typically the *arr instance name
	Status     string `json:"status"`   // "SUCCESS" | "FAILURE" | "Completed" | "Failed"
	SizeBytes  int64  `json:"size_bytes"`
	TS         string `json:"ts"` // RFC3339; falls back to now() if blank
}

// ParseDownload converts a script-side payload to one or more events.
func ParseDownload(raw []byte, src correlate.Source) ([]correlate.Event, error) {
	var p DownloadPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode %s script: %w", src, err)
	}
	if p.DownloadID == "" {
		return nil, fmt.Errorf("missing download_id")
	}
	now := time.Now().UTC()
	if p.TS != "" {
		if t, err := time.Parse(time.RFC3339, p.TS); err == nil {
			now = t
		}
	}

	rel := correlate.Release{
		Title:          p.Name,
		DownloadClient: string(src),
		DownloadID:     p.DownloadID,
		SizeBytes:      p.SizeBytes,
		Protocol:       "usenet",
	}

	ev := correlate.Event{
		Source:     src,
		OccurredAt: now,
		Release:    rel,
		EventID:    p.DownloadID + ":" + p.Event,
	}

	switch p.Event {
	case "queue_added":
		ev.Phase = correlate.PhaseDownloadStart
	case "post_processed":
		ev.Phase = correlate.PhaseDownloadDone
		if isFailure(p.Status) {
			ev.ErrorReason = spans.ErrDownloadFailed
		}
	default:
		return nil, fmt.Errorf("unknown event %q", p.Event)
	}
	return []correlate.Event{ev}, nil
}

func isFailure(status string) bool {
	s := strings.ToLower(status)
	return s == "failure" || s == "failed" || s == "error"
}
