package source

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/danielriddell21/tracearr/internal/correlate"
)

// RadarrPayload mirrors the v3 webhook surface for movies.
type RadarrPayload struct {
	EventType string `json:"eventType"`
	Movie     struct {
		ID     int    `json:"id"`
		Title  string `json:"title"`
		Year   int    `json:"year"`
		TMDBID int    `json:"tmdbId"`
		IMDBID string `json:"imdbId"`
	} `json:"movie"`
	Release struct {
		Quality           string `json:"quality"`
		ReleaseGroup      string `json:"releaseGroup"`
		ReleaseTitle      string `json:"releaseTitle"`
		Indexer           string `json:"indexer"`
		Size              int64  `json:"size"`
		CustomFormatScore int    `json:"customFormatScore"`
	} `json:"release"`
	MovieFile struct {
		RelativePath string `json:"relativePath"`
		Path         string `json:"path"`
		Quality      string `json:"quality"`
		Size         int64  `json:"size"`
		MediaInfo    struct {
			VideoCodec string `json:"videoCodec"`
			Resolution string `json:"resolution"`
		} `json:"mediaInfo"`
	} `json:"movieFile"`
	DownloadClient     string `json:"downloadClient"`
	DownloadClientType string `json:"downloadClientType"`
	DownloadID         string `json:"downloadId"`
	IsUpgrade          bool   `json:"isUpgrade"`
}

func parseRadarr(raw []byte) ([]correlate.Event, error) {
	var p RadarrPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode radarr: %w", err)
	}
	if p.EventType == "" {
		return nil, fmt.Errorf("missing eventType")
	}
	now := time.Now().UTC()
	key := correlate.MediaKey{Type: correlate.MediaTypeMovie, TMDB: p.Movie.TMDBID}
	title := p.Movie.Title
	if p.Movie.Year != 0 {
		title = fmt.Sprintf("%s (%d)", p.Movie.Title, p.Movie.Year)
	}

	switch p.EventType {
	case "Test":
		return nil, nil
	case "Grab":
		return []correlate.Event{{
			Source:     correlate.SourceRadarr,
			Phase:      correlate.PhaseGrabbed,
			OccurredAt: now,
			Key:        key,
			Title:      title,
			Release: correlate.Release{
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
			},
			EventID: p.DownloadID + ":grab",
		}}, nil
	case "Download":
		path := p.MovieFile.Path
		if path == "" {
			path = p.MovieFile.RelativePath
		}
		return []correlate.Event{{
			Source:        correlate.SourceRadarr,
			Phase:         correlate.PhaseImported,
			OccurredAt:    now,
			Key:           key,
			Title:         title,
			ImportPath:    path,
			FileSizeBytes: p.MovieFile.Size,
			Codec:         p.MovieFile.MediaInfo.VideoCodec,
			Resolution:    p.MovieFile.MediaInfo.Resolution,
			EventID:       p.DownloadID + ":import",
		}}, nil
	case "ManualInteractionRequired":
		return []correlate.Event{{
			Source:     correlate.SourceRadarr,
			Phase:      correlate.PhaseManualNeeded,
			OccurredAt: now,
			Key:        key,
			Title:      title,
			EventID:    p.DownloadID + ":manual",
		}}, nil
	}
	return nil, nil
}
