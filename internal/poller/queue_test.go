package poller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type recordedIssue struct {
	DownloadID, Status, Message string
	When                        time.Time
}

type recordingObserver struct {
	mu    sync.Mutex
	calls []recordedIssue
}

func (r *recordingObserver) NoteQueueIssue(_ context.Context, id, status, msg string, when time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recordedIssue{id, status, msg, when})
}

func (r *recordingObserver) snapshot() []recordedIssue {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedIssue, len(r.calls))
	copy(out, r.calls)
	return out
}

func TestQueuePollerEmitsStalledOnly(t *testing.T) {
	body := `{"records":[
		{"downloadId":"a","status":"downloading"},
		{"downloadId":"b","status":"warning","statusMessages":[{"title":"x","messages":["stalled"]}]},
		{"downloadId":"c","status":"failed","errorMessage":"corrupt"}
	]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "k" {
			http.Error(w, "no key", 401)
			return
		}
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	obs := &recordingObserver{}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	p := NewQueuePoller("sonarr", srv.URL, "k", time.Millisecond, obs, logger)
	if err := p.tick(context.Background(), time.Unix(0, 0)); err != nil {
		t.Fatalf("tick: %v", err)
	}
	calls := obs.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 stalled events, got %d", len(calls))
	}
	statuses := map[string]string{}
	for _, c := range calls {
		statuses[c.DownloadID] = c.Status
	}
	if statuses["b"] != "warning" || statuses["c"] != "failed" {
		t.Errorf("unexpected status map: %+v", statuses)
	}
}

func TestQueuePollerSuppressesRepeats(t *testing.T) {
	body := `{"records":[{"downloadId":"b","status":"warning"}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	obs := &recordingObserver{}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	p := NewQueuePoller("sonarr", srv.URL, "k", time.Millisecond, obs, logger)
	for i := 0; i < 3; i++ {
		if err := p.tick(context.Background(), time.Unix(int64(i), 0)); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(obs.snapshot()); got != 1 {
		t.Errorf("expected 1 emitted event after 3 polls of same status, got %d", got)
	}
}
