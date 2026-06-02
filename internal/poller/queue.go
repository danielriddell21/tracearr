// Package poller adds REST-API-backed event sources for things webhooks
// don't cover: Sonarr/Radarr queue stalls, Prowlarr search history.
package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/danielriddell21/tracearr/internal/correlate"
)

// QueueObserver is what the queue poller calls when it detects a stalled
// item. The engine's NoteQueueIssue is the production implementation.
type QueueObserver interface {
	NoteQueueIssue(ctx context.Context, downloadID, status, message string, when time.Time)
}

// QueuePoller polls a Sonarr- or Radarr-shaped /api/v3/queue endpoint and
// emits span events for items in non-progress states.
type QueuePoller struct {
	app      string // "sonarr" | "radarr" (used in logs only)
	baseURL  string
	apiKey   string
	interval time.Duration
	client   *http.Client
	obs      QueueObserver
	log      *slog.Logger

	mu   sync.Mutex
	seen map[string]string // downloadID -> last reported status (suppress repeats)
}

// NewQueuePoller wires a poller. The same shape works for Sonarr v3+ and
// Radarr v3+ — the queue payload schema is identical for our purposes.
func NewQueuePoller(app, baseURL, apiKey string, interval time.Duration,
	obs QueueObserver, log *slog.Logger) *QueuePoller {
	return &QueuePoller{
		app:      app,
		baseURL:  baseURL,
		apiKey:   apiKey,
		interval: interval,
		client:   &http.Client{Timeout: 10 * time.Second},
		obs:      obs,
		log:      log,
		seen:     map[string]string{},
	}
}

// Run blocks until ctx is cancelled.
func (p *QueuePoller) Run(ctx context.Context) {
	if p.interval <= 0 {
		p.interval = 30 * time.Second
	}
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			if err := p.tick(ctx, now); err != nil {
				p.log.LogAttrs(ctx, slog.LevelWarn, "queue poll failed",
					slog.String("event", p.app+".queue_poll_error"),
					slog.String("error", err.Error()))
			}
		}
	}
}

type queueResponse struct {
	Records []queueRecord `json:"records"`
}

type queueRecord struct {
	DownloadID            string `json:"downloadId"`
	Status                string `json:"status"` // queued | downloading | completed | warning | failed | delay
	TrackedDownloadStatus string `json:"trackedDownloadStatus"`
	StatusMessages        []struct {
		Title    string   `json:"title"`
		Messages []string `json:"messages"`
	} `json:"statusMessages"`
	ErrorMessage string `json:"errorMessage"`
}

func (p *QueuePoller) tick(ctx context.Context, now time.Time) error {
	u, err := url.Parse(p.baseURL)
	if err != nil {
		return fmt.Errorf("parse base_url: %w", err)
	}
	u.Path = "/api/v3/queue"
	q := u.Query()
	q.Set("pageSize", "200")
	u.RawQuery = q.Encode()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	req.Header.Set("X-Api-Key", p.apiKey)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	var body queueResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	stalledStatuses := map[string]bool{
		"warning": true, "failed": true, "delay": true,
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	live := map[string]bool{}
	for _, r := range body.Records {
		if r.DownloadID == "" {
			continue
		}
		live[r.DownloadID] = true
		if !stalledStatuses[r.Status] {
			delete(p.seen, r.DownloadID)
			continue
		}
		// Suppress repeat events at the same status.
		if prev, ok := p.seen[r.DownloadID]; ok && prev == r.Status {
			continue
		}
		p.seen[r.DownloadID] = r.Status
		msg := r.ErrorMessage
		if msg == "" && len(r.StatusMessages) > 0 && len(r.StatusMessages[0].Messages) > 0 {
			msg = r.StatusMessages[0].Messages[0]
		}
		p.obs.NoteQueueIssue(ctx, r.DownloadID, r.Status, msg, now)
	}
	// Drop bookkeeping for items that have left the queue.
	for id := range p.seen {
		if !live[id] {
			delete(p.seen, id)
		}
	}
	return nil
}

// EngineNoter adapts a *correlate.Engine to QueueObserver. Pulled out so
// the poller can be tested with a fake observer without depending on the
// engine package at test time.
type EngineNoter struct{ Engine *correlate.Engine }

// NoteQueueIssue forwards the queue issue to the correlation engine.
func (e EngineNoter) NoteQueueIssue(ctx context.Context, downloadID, status, msg string, when time.Time) {
	e.Engine.NoteQueueIssue(ctx, downloadID, status, msg, when)
}
