package receiver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/danielriddell21/tracearr/internal/config"
	"github.com/danielriddell21/tracearr/internal/correlate"
	"github.com/danielriddell21/tracearr/internal/source"
)

// Engine is what the handlers feed events into.
type Engine interface {
	Process(ctx context.Context, ev correlate.Event)
}

// Metrics is what the receivers call to count webhook receipts.
type Metrics interface {
	CountWebhook(ctx context.Context, source, eventType, result string)
}

type noopMetrics struct{}

func (noopMetrics) CountWebhook(context.Context, string, string, string) {}

// Server holds the dependencies for all HTTP handlers and exposes Mux().
type Server struct {
	cfg     *config.Config
	engine  Engine
	log     *slog.Logger
	metrics Metrics
	scrape  http.Handler
}

// NewServer wires a Server given config and a correlation engine.
func NewServer(cfg *config.Config, engine Engine, log *slog.Logger) *Server {
	return &Server{cfg: cfg, engine: engine, log: log, metrics: noopMetrics{}}
}

// SetMetrics installs the webhook-counter sink. Pass nil for no-op.
func (s *Server) SetMetrics(m Metrics) {
	if m == nil {
		s.metrics = noopMetrics{}
		return
	}
	s.metrics = m
}

// SetScrapeHandler mounts the given handler at /metrics. Typically the
// Prometheus exporter from internal/metrics.
func (s *Server) SetScrapeHandler(h http.Handler) { s.scrape = h }

// Mux returns the http.Handler for the configured routes.
func (s *Server) Mux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook/overseerr", s.handleOverseerr(false))
	mux.HandleFunc("POST /webhook/jellyseerr", s.handleOverseerr(true))
	mux.HandleFunc("POST /webhook/sonarr", s.handleSonarr)
	mux.HandleFunc("POST /webhook/radarr", s.handleRadarr)
	mux.HandleFunc("POST /script/nzbget", s.handleScript(correlate.SourceNZBGet))
	mux.HandleFunc("POST /script/sabnzbd", s.handleScript(correlate.SourceSABnzbd))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if s.scrape != nil {
		mux.Handle("GET /metrics", s.scrape)
	}
	return logMiddleware(s.log, payloadCap(1<<20, mux)) // 1 MiB cap
}

// payloadCap rejects requests with bodies larger than the cap, before any
// authentication, to avoid wasting cycles on hostile callers.
func payloadCap(maxBytes int64, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		h.ServeHTTP(w, r)
	})
}

func logMiddleware(log *slog.Logger, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.LogAttrs(r.Context(), slog.LevelDebug, "http request",
			slog.String("event", "http.request"),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path))
		h.ServeHTTP(w, r)
	})
}

// readBody pulls the body up to the cap installed in payloadCap.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload too large")
			return nil, err
		}
		writeError(w, http.StatusBadRequest, "read error")
		return nil, err
	}
	return body, nil
}

func (s *Server) handleOverseerr(isJellyseerr bool) http.HandlerFunc {
	app := s.cfg.Sources.Overseerr
	srcLabel := "overseerr"
	if isJellyseerr {
		app = s.cfg.Sources.Jellyseerr
		srcLabel = "jellyseerr"
	}
	return func(w http.ResponseWriter, r *http.Request) {
		eventType, result := "*", "ok"
		defer func() { s.metrics.CountWebhook(r.Context(), srcLabel, eventType, result) }()
		if !app.Enabled {
			result = "dropped"
			writeError(w, http.StatusNotFound, "source disabled")
			return
		}
		if err := verifyBearer(r.Header.Get("Authorization"), app.WebhookSecret); err != nil {
			result = "unauthorized"
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		body, err := readBody(w, r)
		if err != nil {
			result = "bad_payload"
			return
		}
		ev, ok, err := source.ParseOverseerr(body, isJellyseerr)
		if err != nil {
			result = "bad_payload"
			s.log.LogAttrs(r.Context(), slog.LevelWarn, "parse error",
				slog.String("event", "overseerr.parse_error"),
				slog.String("error", err.Error()))
			writeError(w, http.StatusBadRequest, "bad payload")
			return
		}
		if !ok {
			result = "dropped"
			w.WriteHeader(http.StatusOK)
			return
		}
		eventType = string(ev.Phase)
		s.engine.Process(r.Context(), ev)
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) handleArr(srcLabel string, app config.AppSource,
	parser func([]byte) ([]correlate.Event, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		eventType, result := "*", "ok"
		defer func() { s.metrics.CountWebhook(r.Context(), srcLabel, eventType, result) }()
		if !app.Enabled {
			result = "dropped"
			writeError(w, http.StatusNotFound, "source disabled")
			return
		}
		body, err := readBody(w, r)
		if err != nil {
			result = "bad_payload"
			return
		}
		if err := verifyHMACSHA256(r.Header.Get("X-Webhook-Signature"), app.WebhookSecret, body); err != nil {
			result = "unauthorized"
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		events, err := parser(body)
		if err != nil {
			result = "bad_payload"
			s.log.LogAttrs(r.Context(), slog.LevelWarn, "parse error",
				slog.String("event", srcLabel+".parse_error"),
				slog.String("error", err.Error()))
			writeError(w, http.StatusBadRequest, "bad payload")
			return
		}
		if len(events) == 0 {
			result = "dropped"
		} else {
			eventType = string(events[0].Phase)
		}
		for _, ev := range events {
			s.engine.Process(r.Context(), ev)
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) handleSonarr(w http.ResponseWriter, r *http.Request) {
	s.handleArr("sonarr", s.cfg.Sources.Sonarr, source.ParseSonarr)(w, r)
}

func (s *Server) handleRadarr(w http.ResponseWriter, r *http.Request) {
	s.handleArr("radarr", s.cfg.Sources.Radarr, source.ParseRadarr)(w, r)
}

func (s *Server) handleScript(src correlate.Source) http.HandlerFunc {
	app := s.cfg.Sources.NZBGet
	if src == correlate.SourceSABnzbd {
		app = s.cfg.Sources.SABnzbd
	}
	srcLabel := string(src)
	return func(w http.ResponseWriter, r *http.Request) {
		eventType, result := "*", "ok"
		defer func() { s.metrics.CountWebhook(r.Context(), srcLabel, eventType, result) }()
		if !app.Enabled {
			result = "dropped"
			writeError(w, http.StatusNotFound, "source disabled")
			return
		}
		if err := verifyBearer(r.Header.Get("Authorization"), app.ScriptToken); err != nil {
			result = "unauthorized"
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		body, err := readBody(w, r)
		if err != nil {
			result = "bad_payload"
			return
		}
		events, err := source.ParseDownload(body, src)
		if err != nil {
			result = "bad_payload"
			s.log.LogAttrs(r.Context(), slog.LevelWarn, "parse error",
				slog.String("event", string(src)+".parse_error"),
				slog.String("error", err.Error()))
			writeError(w, http.StatusBadRequest, "bad payload")
			return
		}
		if len(events) == 0 {
			result = "dropped"
		} else {
			eventType = string(events[0].Phase)
		}
		for _, ev := range events {
			s.engine.Process(r.Context(), ev)
		}
		w.WriteHeader(http.StatusOK)
	}
}
