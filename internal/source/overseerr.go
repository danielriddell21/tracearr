package source

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/danielriddell21/tracearr/internal/correlate"
	"github.com/danielriddell21/tracearr/internal/spans"
)

// OverseerrPayload mirrors the documented webhook agent payload. Numeric
// IDs ship as strings — Overseerr is loose about types here.
type OverseerrPayload struct {
	NotificationType string `json:"notification_type"`
	Event            string `json:"event"`
	Subject          string `json:"subject"`
	Message          string `json:"message"`
	Media            struct {
		MediaType string `json:"media_type"`
		TMDBID    string `json:"tmdbId"`
		TVDBID    string `json:"tvdbId"`
		Status    string `json:"status"`
	} `json:"media"`
	Request struct {
		RequestID            string `json:"request_id"`
		RequestedByUsername  string `json:"requestedBy_username"`
		RequestedByEmail     string `json:"requestedBy_email"`
	} `json:"request"`
	Extra []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"extra"`
}

// ParseOverseerr converts an Overseerr webhook delivery into a correlate.Event.
// Returns (event, true, nil) on a real lifecycle event, (zero, false, nil) for
// test notifications and other ignorable types, and (zero, false, err) on
// malformed input.
func ParseOverseerr(raw []byte, isJellyseerr bool) (correlate.Event, bool, error) {
	var p OverseerrPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return correlate.Event{}, false, fmt.Errorf("decode overseerr: %w", err)
	}
	source := correlate.SourceOverseerr
	if isJellyseerr {
		source = correlate.SourceJellyseerr
	}

	if p.NotificationType == "" {
		return correlate.Event{}, false, nil
	}
	if p.NotificationType == "TEST_NOTIFICATION" {
		return correlate.Event{}, false, nil
	}

	mediaType := correlate.MediaTypeMovie
	if p.Media.MediaType == "tv" {
		mediaType = correlate.MediaTypeTV
	}
	key := correlate.MediaKey{
		Type: mediaType,
		TMDB: atoi(p.Media.TMDBID),
		TVDB: atoi(p.Media.TVDBID),
	}
	if key.TMDB == 0 && key.TVDB == 0 {
		return correlate.Event{}, false, fmt.Errorf("overseerr event missing tmdb/tvdb ids")
	}

	now := time.Now().UTC()
	ev := correlate.Event{
		Source:      source,
		OccurredAt:  now,
		Key:         key,
		Title:       p.Subject,
		RequestID:   p.Request.RequestID,
		RequestedBy: p.Request.RequestedByUsername,
		EventID:     p.Request.RequestID + ":" + p.NotificationType,
	}

	switch p.NotificationType {
	case "MEDIA_PENDING", "MEDIA_AUTO_APPROVED":
		ev.Phase = correlate.PhaseRequestOpened
	case "MEDIA_APPROVED":
		ev.Phase = correlate.PhaseRequestApproved
	case "MEDIA_AVAILABLE":
		ev.Phase = correlate.PhaseRequestClosed
		ev.Outcome = correlate.OutcomeAvailable
	case "MEDIA_FAILED":
		ev.Phase = correlate.PhaseRequestClosed
		ev.Outcome = correlate.OutcomeFailed
		ev.ErrorReason = spans.ErrOverseerrFailed
	case "MEDIA_DECLINED":
		ev.Phase = correlate.PhaseRequestClosed
		ev.Outcome = correlate.OutcomeDeclined
		ev.ErrorReason = spans.ErrOverseerrDeclined
	default:
		return correlate.Event{}, false, nil
	}
	return ev, true, nil
}
