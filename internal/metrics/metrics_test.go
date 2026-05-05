package metrics

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestPrometheusEndpointExposesEmittedMetrics verifies the metric registry
// + Prometheus handler round-trip: emit a request observation, scrape, and
// confirm the metric appears in the text-format output with our labels.
func TestPrometheusEndpointExposesEmittedMetrics(t *testing.T) {
	m, err := New("tracearr", "media", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())

	m.ObserveRequest(context.Background(), "movie", "overseerr", "available", 42*time.Second)
	m.CountAttempt(context.Background(), "movie", "available", "")
	m.AdjustInFlight(context.Background(), "request", 1)
	m.CountWebhook(context.Background(), "sonarr", "arr.grab", "ok")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	out := string(body)

	wantSubstrings := []string{
		`tracearr_request_duration_seconds`,
		`media_type="movie"`,
		`source="overseerr"`,
		`outcome="available"`,
		`tracearr_attempts_total`,
		`tracearr_in_flight`,
		`tracearr_webhook_received_total`,
		`event_type="arr.grab"`,
	}
	for _, w := range wantSubstrings {
		if !strings.Contains(out, w) {
			t.Errorf("scrape missing %q\n--- output ---\n%s", w, out)
		}
	}
}
