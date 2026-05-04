package source

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/danielriddell21/tracearr/internal/correlate"
)

// SonarrPayload covers the v3 webhook surface we use. Many fields are
// omitted; we decode only what the engine needs.
type SonarrPayload struct {
	EventType string `json:"eventType"`
	Series    struct {
		ID     int    `json:"id"`
		Title  string `json:"title"`
		TVDBID int    `json:"tvdbId"`
		TMDBID int    `json:"tmdbId"`
		IMDBID string `json:"imdbId"`
	} `json:"series"`
	Episodes []struct {
		ID            int    `json:"id"`
		EpisodeNumber int    `json:"episodeNumber"`
		SeasonNumber  int    `json:"seasonNumber"`
		Title         string `json:"title"`
	} `json:"episodes"`
	Release struct {
		Quality           string `json:"quality"`
		QualityVersion    int    `json:"qualityVersion"`
		ReleaseGroup      string `json:"releaseGroup"`
		ReleaseTitle      string `json:"releaseTitle"`
		Indexer           string `json:"indexer"`
		Size              int64  `json:"size"`
		CustomFormatScore int    `json:"customFormatScore"`
	} `json:"release"`
	EpisodeFile struct {
		RelativePath string `json:"relativePath"`
		Path         string `json:"path"`
		Quality      string `json:"quality"`
		Size         int64  `json:"size"`
		MediaInfo    struct {
			VideoCodec string `json:"videoCodec"`
			Resolution string `json:"resolution"`
		} `json:"mediaInfo"`
	} `json:"episodeFile"`
	DownloadClient     string `json:"downloadClient"`
	DownloadClientType string `json:"downloadClientType"`
	DownloadID         string `json:"downloadId"`
	IsUpgrade          bool   `json:"isUpgrade"`
}

// ParseSonarr converts a Sonarr webhook to one or more correlate.Events.
// Sonarr sometimes targets multiple episodes in one delivery; we emit one
// Event per episode (each carrying the same Release) so per-episode keys
// can be addressed directly. The engine de-duplicates via SeenEvent.
func ParseSonarr(raw []byte) ([]correlate.Event, error) {
	return parseArr(raw, correlate.SourceSonarr)
}

// ParseRadarr is the symmetric entrypoint for Radarr. Radarr's payload
// shape differs in the top-level entity (movie vs series, movieFile vs
// episodeFile) — we re-decode through a Radarr-specific struct internally.
func ParseRadarr(raw []byte) ([]correlate.Event, error) {
	return parseRadarr(raw)
}

func parseArr(raw []byte, src correlate.Source) ([]correlate.Event, error) {
	var p SonarrPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode sonarr: %w", err)
	}
	if p.EventType == "" {
		return nil, fmt.Errorf("missing eventType")
	}
	now := time.Now().UTC()

	switch p.EventType {
	case "Test":
		return nil, nil
	case "Grab":
		return sonarrGrabs(p, src, now), nil
	case "Download":
		return sonarrImports(p, src, now), nil
	case "ManualInteractionRequired":
		return sonarrManual(p, src, now), nil
	default:
		return nil, nil
	}
}

func sonarrGrabs(p SonarrPayload, src correlate.Source, now time.Time) []correlate.Event {
	rel := correlate.Release{
		Title:             p.Release.ReleaseTitle,
		Indexer:           p.Release.Indexer,
		Group:             p.Release.ReleaseGroup,
		Quality:           p.Release.Quality,
		SizeBytes:         p.Release.Size,
		CustomFormatScore: p.Release.CustomFormatScore,
		DownloadClient:    p.DownloadClient,
		DownloadID:        p.DownloadID,
		Protocol:          arrProtocol(p.DownloadClientType),
		IsUpgrade:         p.IsUpgrade,
	}
	for _, ep := range p.Episodes {
		rel.Episodes = append(rel.Episodes, correlate.EpisodeRef{
			Season:  ep.SeasonNumber,
			Episode: ep.EpisodeNumber,
			Title:   ep.Title,
		})
	}
	out := make([]correlate.Event, 0, max(len(p.Episodes), 1))
	if len(p.Episodes) == 0 {
		// Movie path — Radarr reuses this code via parseRadarr's adapter.
		out = append(out, correlate.Event{
			Source:     src,
			Phase:      correlate.PhaseGrabbed,
			OccurredAt: now,
			Key: correlate.MediaKey{
				Type: correlate.MediaTypeMovie,
				TMDB: p.Series.TMDBID,
			},
			Title:   p.Series.Title,
			Release: rel,
			EventID: p.DownloadID + ":grab",
		})
		return out
	}
	for _, ep := range p.Episodes {
		out = append(out, correlate.Event{
			Source:     src,
			Phase:      correlate.PhaseGrabbed,
			OccurredAt: now,
			Key: correlate.MediaKey{
				Type:    correlate.MediaTypeTV,
				TVDB:    p.Series.TVDBID,
				TMDB:    p.Series.TMDBID,
				Season:  ep.SeasonNumber,
				Episode: ep.EpisodeNumber,
			},
			Title:   p.Series.Title,
			Release: rel,
			EventID: fmt.Sprintf("%s:grab:%d:%d", p.DownloadID, ep.SeasonNumber, ep.EpisodeNumber),
		})
	}
	return out
}

func sonarrImports(p SonarrPayload, src correlate.Source, now time.Time) []correlate.Event {
	out := make([]correlate.Event, 0, max(len(p.Episodes), 1))
	importPath := p.EpisodeFile.Path
	if importPath == "" {
		importPath = p.EpisodeFile.RelativePath
	}
	if len(p.Episodes) == 0 {
		out = append(out, correlate.Event{
			Source:        src,
			Phase:         correlate.PhaseImported,
			OccurredAt:    now,
			Key:           correlate.MediaKey{Type: correlate.MediaTypeMovie, TMDB: p.Series.TMDBID},
			Title:         p.Series.Title,
			ImportPath:    importPath,
			FileSizeBytes: p.EpisodeFile.Size,
			Codec:         p.EpisodeFile.MediaInfo.VideoCodec,
			Resolution:    p.EpisodeFile.MediaInfo.Resolution,
			EventID:       p.DownloadID + ":import",
		})
		return out
	}
	for _, ep := range p.Episodes {
		out = append(out, correlate.Event{
			Source:     src,
			Phase:      correlate.PhaseImported,
			OccurredAt: now,
			Key: correlate.MediaKey{
				Type:    correlate.MediaTypeTV,
				TVDB:    p.Series.TVDBID,
				TMDB:    p.Series.TMDBID,
				Season:  ep.SeasonNumber,
				Episode: ep.EpisodeNumber,
			},
			Title:         p.Series.Title,
			ImportPath:    importPath,
			FileSizeBytes: p.EpisodeFile.Size,
			Codec:         p.EpisodeFile.MediaInfo.VideoCodec,
			Resolution:    p.EpisodeFile.MediaInfo.Resolution,
			EventID:       fmt.Sprintf("%s:import:%d:%d", p.DownloadID, ep.SeasonNumber, ep.EpisodeNumber),
		})
	}
	return out
}

func sonarrManual(p SonarrPayload, src correlate.Source, now time.Time) []correlate.Event {
	if len(p.Episodes) == 0 {
		return []correlate.Event{{
			Source:     src,
			Phase:      correlate.PhaseManualNeeded,
			OccurredAt: now,
			Key:        correlate.MediaKey{Type: correlate.MediaTypeMovie, TMDB: p.Series.TMDBID},
			Title:      p.Series.Title,
			EventID:    p.DownloadID + ":manual",
		}}
	}
	out := make([]correlate.Event, 0, len(p.Episodes))
	for _, ep := range p.Episodes {
		out = append(out, correlate.Event{
			Source:     src,
			Phase:      correlate.PhaseManualNeeded,
			OccurredAt: now,
			Key: correlate.MediaKey{
				Type:    correlate.MediaTypeTV,
				TVDB:    p.Series.TVDBID,
				TMDB:    p.Series.TMDBID,
				Season:  ep.SeasonNumber,
				Episode: ep.EpisodeNumber,
			},
			Title:   p.Series.Title,
			EventID: fmt.Sprintf("%s:manual:%d:%d", p.DownloadID, ep.SeasonNumber, ep.EpisodeNumber),
		})
	}
	return out
}

func arrProtocol(clientType string) string {
	switch strings.ToLower(clientType) {
	case "nzbget", "sabnzbd", "usenet", "nzbvortex":
		return "usenet"
	case "qbittorrent", "deluge", "transmission", "rtorrent", "torrent":
		return "torrent"
	default:
		return ""
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
