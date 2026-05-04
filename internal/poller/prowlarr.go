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

// SearchEmitter is what the Prowlarr poller calls per search history entry.
// The engine implementation attempts title-based correlation; orphan
// searches surface as `prowlarr.unmatched` spans.
type SearchEmitter interface {
	EmitProwlarrSearch(ctx context.Context, query, indexer string, when time.Time, elapsedMs int64)
}

// ProwlarrPoller polls /api/v1/history?eventType=indexerQuery and emits
// one search event per new entry since the last poll.
type ProwlarrPoller struct {
	baseURL  string
	apiKey   string
	interval time.Duration
	client   *http.Client
	emitter  SearchEmitter
	log      *slog.Logger

	mu     sync.Mutex
	cursor time.Time // last processed entry's date
}

func NewProwlarrPoller(baseURL, apiKey string, interval time.Duration,
	emitter SearchEmitter, log *slog.Logger) *ProwlarrPoller {
	return &ProwlarrPoller{
		baseURL:  baseURL,
		apiKey:   apiKey,
		interval: interval,
		client:   &http.Client{Timeout: 10 * time.Second},
		emitter:  emitter,
		log:      log,
	}
}

func (p *ProwlarrPoller) Run(ctx context.Context) {
	if p.interval <= 0 {
		p.interval = 30 * time.Second
	}
	t := time.NewTicker(p.interval)
	defer t.Stop()
	// Anchor cursor to startup time so we don't replay history.
	p.cursor = time.Now().UTC()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := p.tick(ctx); err != nil {
				p.log.LogAttrs(ctx, slog.LevelWarn, "prowlarr poll failed",
					slog.String("event", "prowlarr.poll_error"),
					slog.String("error", err.Error()))
			}
		}
	}
}

type prowlarrHistoryResponse struct {
	Records []prowlarrHistoryRecord `json:"records"`
}

type prowlarrHistoryRecord struct {
	Date    time.Time `json:"date"`
	Indexer string    `json:"indexer"`
	Data    struct {
		Query     string `json:"query"`
		ElapsedMs string `json:"elapsedTime"`
	} `json:"data"`
	EventType string `json:"eventType"`
}

func (p *ProwlarrPoller) tick(ctx context.Context) error {
	u, err := url.Parse(p.baseURL)
	if err != nil {
		return err
	}
	u.Path = "/api/v1/history"
	q := u.Query()
	q.Set("eventType", "indexerQuery")
	q.Set("pageSize", "100")
	q.Set("sortKey", "date")
	q.Set("sortDirection", "descending")
	u.RawQuery = q.Encode()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	req.Header.Set("X-Api-Key", p.apiKey)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	var body prowlarrHistoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return err
	}
	p.mu.Lock()
	prev := p.cursor
	var newest time.Time
	for _, r := range body.Records {
		if !r.Date.After(prev) {
			continue
		}
		if r.Date.After(newest) {
			newest = r.Date
		}
		var elapsed int64
		if n, err := time.ParseDuration(r.Data.ElapsedMs + "ms"); err == nil {
			elapsed = n.Milliseconds()
		}
		p.emitter.EmitProwlarrSearch(ctx, r.Data.Query, r.Indexer, r.Date, elapsed)
	}
	if !newest.IsZero() {
		p.cursor = newest
	}
	p.mu.Unlock()
	return nil
}

// EngineProwlarr adapts a *correlate.Engine to SearchEmitter.
type EngineProwlarr struct{ Engine *correlate.Engine }

func (e EngineProwlarr) EmitProwlarrSearch(ctx context.Context, query, indexer string, when time.Time, elapsedMs int64) {
	e.Engine.EmitProwlarrSearch(ctx, query, indexer, when, elapsedMs)
}

