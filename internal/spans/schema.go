// Package spans defines the tracearr span schema: span names, attribute keys,
// and status reasons. These constants are the contract between the
// correlation engine and the OTel exporter, and are also the documented
// surface for dashboard authors.
package spans

import "go.opentelemetry.io/otel/attribute"

// Span names. Phase-prefixed; one per lifecycle stage.
const (
	SpanMediaRequest     = "media.request"
	SpanProwlarrSearch   = "prowlarr.search"
	SpanSonarrGrab       = "sonarr.grab"
	SpanRadarrGrab       = "radarr.grab"
	SpanDownloadTransfer = "download.transfer"
	SpanSonarrImport     = "sonarr.import"
	SpanRadarrImport     = "radarr.import"
)

// Required-on-every-span attribute keys.
const (
	AttrMediaType    = attribute.Key("media.type")
	AttrMediaAttempt = attribute.Key("media.attempt")
	AttrMediaTMDBID  = attribute.Key("media.tmdb_id")
	AttrMediaTVDBID  = attribute.Key("media.tvdb_id")
	AttrMediaIMDBID  = attribute.Key("media.imdb_id")
	AttrMediaTitle   = attribute.Key("media.title")
)

// media.request attributes.
const (
	AttrMediaRequestID   = attribute.Key("media.request_id")
	AttrMediaRequestedBy = attribute.Key("media.requested_by")
	AttrMediaSource      = attribute.Key("media.source")
	AttrMediaSeasons     = attribute.Key("media.seasons")
)

// Grab/search attributes.
const (
	AttrMediaIndexer           = attribute.Key("media.indexer")
	AttrMediaIndexerID         = attribute.Key("media.indexer_id")
	AttrMediaSearchQuery       = attribute.Key("media.search_query")
	AttrMediaReleaseTitle      = attribute.Key("media.release_title")
	AttrMediaReleaseGroup      = attribute.Key("media.release_group")
	AttrMediaQuality           = attribute.Key("media.quality")
	AttrMediaQualityProfile    = attribute.Key("media.quality_profile")
	AttrMediaSizeBytes         = attribute.Key("media.size_bytes")
	AttrMediaCustomFormatScore = attribute.Key("media.custom_format_score")
	AttrMediaDownloadClient    = attribute.Key("media.download_client")
	AttrMediaDownloadClientID  = attribute.Key("media.download_client_id")
	AttrMediaProtocol          = attribute.Key("media.protocol")
	AttrMediaEpisodes          = attribute.Key("media.episodes")
	AttrProwlarrElapsedMs      = attribute.Key("prowlarr.elapsed_ms")
)

// Download transfer attributes.
const (
	AttrMediaCategory      = attribute.Key("media.category")
	AttrDownloadBytes      = attribute.Key("download.bytes_downloaded")
	AttrDownloadDurationMs = attribute.Key("download.duration_ms")
)

// Import attributes.
const (
	AttrMediaImportPath    = attribute.Key("media.import_path")
	AttrMediaFileSizeBytes = attribute.Key("media.file_size_bytes")
	AttrMediaCodec         = attribute.Key("media.codec")
	AttrMediaResolution    = attribute.Key("media.resolution")
)

// Upgrade-link attributes (set on root spans of subsequent attempt traces).
const (
	AttrMediaUpgradeReason = attribute.Key("media.upgrade_reason")
)

// Error reason values (used as `error.type` attribute).
const (
	ErrTracearrTimeout   = "tracearr.timeout"
	ErrManualInteraction = "arr.manual_interaction_required"
	ErrImportFailed      = "arr.import_failed"
	ErrOverseerrDeclined = "overseerr.declined"
	ErrOverseerrFailed   = "overseerr.failed"
	ErrDownloadFailed    = "download.failed"
)

// Source values for AttrMediaSource.
const (
	SourceOverseerr  = "overseerr"
	SourceJellyseerr = "jellyseerr"
)

// Protocol values for AttrMediaProtocol.
const (
	ProtocolUsenet  = "usenet"
	ProtocolTorrent = "torrent"
)

// Upgrade reasons for AttrMediaUpgradeReason.
const (
	UpgradeQuality      = "quality_upgrade"
	UpgradeFailedRetry  = "failed_retry"
	UpgradeManualSearch = "manual"
)
