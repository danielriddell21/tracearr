// Package correlate is the heart of tracearr: it converts a stream of
// vendor-agnostic Events into SpanIntents (open/close/event operations)
// against in-flight traces, keyed by stable media identifiers.
package correlate

import "time"

// MediaType is movie or tv.
type MediaType string

const (
	MediaTypeMovie MediaType = "movie"
	MediaTypeTV    MediaType = "tv"
)

// Phase identifies a lifecycle stage. Receivers map their vendor-specific
// event types to one Phase; the engine decides what spans to open/close.
type Phase string

const (
	PhaseRequestOpened   Phase = "request.opened"   // Overseerr MEDIA_PENDING / MEDIA_AUTO_APPROVED
	PhaseRequestApproved Phase = "request.approved" // Overseerr MEDIA_APPROVED
	PhaseRequestClosed   Phase = "request.closed"   // Overseerr MEDIA_AVAILABLE / MEDIA_FAILED / MEDIA_DECLINED
	PhaseGrabbed         Phase = "arr.grab"         // Sonarr/Radarr Grab
	PhaseDownloadStart   Phase = "download.start"   // NZBGet queue script / SAB pre-queue
	PhaseDownloadDone    Phase = "download.done"    // NZBGet pp-script / SAB notification
	PhaseImported        Phase = "arr.import"       // Sonarr/Radarr Download(=imported)
	PhaseManualNeeded    Phase = "arr.manual"       // ManualInteractionRequired
)

// Source is the originating app.
type Source string

const (
	SourceOverseerr  Source = "overseerr"
	SourceJellyseerr Source = "jellyseerr"
	SourceSonarr     Source = "sonarr"
	SourceRadarr     Source = "radarr"
	SourceProwlarr   Source = "prowlarr"
	SourceNZBGet     Source = "nzbget"
	SourceSABnzbd    Source = "sabnzbd"
)

// MediaKey is the correlation primary key. Either TMDB or TVDB must be
// non-zero. For movies, Season/Episode are 0. For TV, Season=0 means
// whole-series; Episode=0 means season-pack/season-level.
type MediaKey struct {
	Type    MediaType
	TMDB    int
	TVDB    int
	Season  int
	Episode int
}

// SeriesKey returns a series-only key (Season/Episode zeroed). For TV this
// matches both whole-series and per-episode events; for movies it equals
// the original key.
func (k MediaKey) SeriesKey() MediaKey {
	k.Season, k.Episode = 0, 0
	return k
}

// Outcome of a closed root trace, surfaced into metrics labels.
type Outcome string

const (
	OutcomeAvailable Outcome = "available"
	OutcomeFailed    Outcome = "failed"
	OutcomeDeclined  Outcome = "declined"
	OutcomeTimeout   Outcome = "timeout"
)

// Release identifies a single grab/download cycle. DownloadID is the
// SABnzbd nzo_id or NZBGet NZBID; the *arr Grab event carries it.
type Release struct {
	Title             string
	Indexer           string
	Group             string
	Quality           string
	QualityProfile    string
	SizeBytes         int64
	CustomFormatScore int
	DownloadClient    string
	DownloadID        string
	Protocol          string // "usenet" | "torrent"
	Episodes          []EpisodeRef
	IsUpgrade         bool
}

// EpisodeRef is a Sonarr-side episode descriptor.
type EpisodeRef struct {
	TVDBID  int
	Season  int
	Episode int
	Title   string
}

// Event is the vendor-agnostic input to the correlation engine. Every
// receiver produces one of these per webhook delivery.
type Event struct {
	Source     Source
	Phase      Phase
	OccurredAt time.Time
	Key        MediaKey

	// Optional human-readable title; helpful when correlating by title fallback.
	Title string

	// Outcome is set on PhaseRequestClosed / failure cases.
	Outcome Outcome

	// Release is set on PhaseGrabbed and download phases.
	Release Release

	// RequestID / RequestedBy are set on Overseerr events.
	RequestID    string
	RequestedBy  string

	// ImportPath / FileSizeBytes / Codec / Resolution on PhaseImported.
	ImportPath    string
	FileSizeBytes int64
	Codec         string
	Resolution    string

	// ErrorReason populates `error.type` when this event closes a span with status Error.
	ErrorReason string

	// EventID is the source-side delivery ID, used for dedup. Empty if unsupported.
	EventID string
}
