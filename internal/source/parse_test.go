package source

import (
	"testing"

	"github.com/danielriddell21/tracearr/internal/correlate"
)

func TestParseOverseerrPending(t *testing.T) {
	raw := []byte(`{
		"notification_type": "MEDIA_PENDING",
		"subject": "The Matrix (1999)",
		"media": {"media_type": "movie", "tmdbId": "603", "tvdbId": "0", "status": "PENDING"},
		"request": {"request_id": "42", "requestedBy_username": "alice"}
	}`)
	ev, ok, err := ParseOverseerr(raw, false)
	if err != nil || !ok {
		t.Fatalf("parse: ok=%v err=%v", ok, err)
	}
	if ev.Phase != correlate.PhaseRequestOpened {
		t.Errorf("phase = %v", ev.Phase)
	}
	if ev.Key.TMDB != 603 || ev.Key.Type != correlate.MediaTypeMovie {
		t.Errorf("key = %+v", ev.Key)
	}
	if ev.RequestedBy != "alice" {
		t.Errorf("requested_by = %q", ev.RequestedBy)
	}
}

func TestParseOverseerrTestNotificationIgnored(t *testing.T) {
	raw := []byte(`{"notification_type": "TEST_NOTIFICATION"}`)
	_, ok, err := ParseOverseerr(raw, false)
	if err != nil || ok {
		t.Errorf("expected ok=false err=nil, got ok=%v err=%v", ok, err)
	}
}

func TestParseSonarrGrabMultiEpisode(t *testing.T) {
	raw := []byte(`{
		"eventType": "Grab",
		"series": {"id":1,"title":"Show","tvdbId":1234,"tmdbId":5678,"imdbId":"tt0"},
		"episodes": [
			{"id":11,"episodeNumber":1,"seasonNumber":2,"title":"a"},
			{"id":12,"episodeNumber":2,"seasonNumber":2,"title":"b"}
		],
		"release": {"quality":"WEBDL-1080p","releaseGroup":"GRP","releaseTitle":"rel.title","indexer":"X","size":1000,"customFormatScore":50},
		"downloadClient":"NZBGet","downloadClientType":"Nzbget","downloadId":"d1"
	}`)
	events, err := ParseSonarr(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	for _, ev := range events {
		if ev.Phase != correlate.PhaseGrabbed {
			t.Errorf("phase = %v", ev.Phase)
		}
		if ev.Key.TVDB != 1234 || ev.Key.Type != correlate.MediaTypeTV {
			t.Errorf("key = %+v", ev.Key)
		}
		if ev.Release.Indexer != "X" {
			t.Errorf("indexer = %q", ev.Release.Indexer)
		}
		if ev.Release.Protocol != "usenet" {
			t.Errorf("protocol = %q", ev.Release.Protocol)
		}
	}
}

func TestParseRadarrGrab(t *testing.T) {
	raw := []byte(`{
		"eventType": "Grab",
		"movie": {"id":1,"title":"X","year":2020,"tmdbId":99,"imdbId":"tt99"},
		"release": {"quality":"WEBDL-1080p","releaseGroup":"G","releaseTitle":"r","indexer":"I","size":42},
		"downloadClient":"SABnzbd","downloadClientType":"Sabnzbd","downloadId":"sab-1"
	}`)
	events, err := ParseRadarr(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.Key.TMDB != 99 || ev.Key.Type != correlate.MediaTypeMovie {
		t.Errorf("key = %+v", ev.Key)
	}
	if ev.Release.Protocol != "usenet" {
		t.Errorf("protocol = %q", ev.Release.Protocol)
	}
	if ev.Title != "X (2020)" {
		t.Errorf("title = %q", ev.Title)
	}
}

func TestParseDownloadPostProcessed(t *testing.T) {
	raw := []byte(`{"event":"post_processed","download_id":"x","name":"r","category":"sonarr","status":"SUCCESS","size_bytes":100}`)
	events, err := ParseDownload(raw, correlate.SourceNZBGet)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Phase != correlate.PhaseDownloadDone {
		t.Errorf("got %+v", events)
	}
	if events[0].ErrorReason != "" {
		t.Errorf("unexpected error reason: %q", events[0].ErrorReason)
	}
}

func TestParseDownloadFailed(t *testing.T) {
	raw := []byte(`{"event":"post_processed","download_id":"x","status":"Failed"}`)
	events, err := ParseDownload(raw, correlate.SourceSABnzbd)
	if err != nil {
		t.Fatal(err)
	}
	if events[0].ErrorReason == "" {
		t.Error("expected error reason on failure")
	}
}
