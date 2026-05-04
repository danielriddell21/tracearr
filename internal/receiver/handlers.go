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

// Server holds the dependencies for all HTTP handlers and exposes Mux().
type Server struct {
	cfg    *config.Config
	engine Engine
	log    *slog.Logger
}

// NewServer wires a Server given config and a correlation engine.
func NewServer(cfg *config.Config, engine Engine, log *slog.Logger) *Server {
	return &Server{cfg: cfg, engine: engine, log: log}
}

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
	if isJellyseerr {
		app = s.cfg.Sources.Jellyseerr
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if !app.Enabled {
			writeError(w, http.StatusNotFound, "source disabled")
			return
		}
		if err := verifyBearer(r.Header.Get("Authorization"), app.WebhookSecret); err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		body, err := readBody(w, r)
		if err != nil {
			return
		}
		ev, ok, err := source.ParseOverseerr(body, isJellyseerr)
		if err != nil {
			s.log.LogAttrs(r.Context(), slog.LevelWarn, "parse error",
				slog.String("event", "overseerr.parse_error"),
				slog.String("error", err.Error()))
			writeError(w, http.StatusBadRequest, "bad payload")
			return
		}
		if !ok {
			w.WriteHeader(http.StatusOK)
			return
		}
		s.engine.Process(r.Context(), ev)
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) handleSonarr(w http.ResponseWriter, r *http.Request) {
	app := s.cfg.Sources.Sonarr
	if !app.Enabled {
		writeError(w, http.StatusNotFound, "source disabled")
		return
	}
	body, err := readBody(w, r)
	if err != nil {
		return
	}
	if err := verifyHMACSHA256(r.Header.Get("X-Webhook-Signature"), app.WebhookSecret, body); err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	events, err := source.ParseSonarr(body)
	if err != nil {
		s.log.LogAttrs(r.Context(), slog.LevelWarn, "parse error",
			slog.String("event", "sonarr.parse_error"),
			slog.String("error", err.Error()))
		writeError(w, http.StatusBadRequest, "bad payload")
		return
	}
	for _, ev := range events {
		s.engine.Process(r.Context(), ev)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleRadarr(w http.ResponseWriter, r *http.Request) {
	app := s.cfg.Sources.Radarr
	if !app.Enabled {
		writeError(w, http.StatusNotFound, "source disabled")
		return
	}
	body, err := readBody(w, r)
	if err != nil {
		return
	}
	if err := verifyHMACSHA256(r.Header.Get("X-Webhook-Signature"), app.WebhookSecret, body); err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	events, err := source.ParseRadarr(body)
	if err != nil {
		s.log.LogAttrs(r.Context(), slog.LevelWarn, "parse error",
			slog.String("event", "radarr.parse_error"),
			slog.String("error", err.Error()))
		writeError(w, http.StatusBadRequest, "bad payload")
		return
	}
	for _, ev := range events {
		s.engine.Process(r.Context(), ev)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleScript(src correlate.Source) http.HandlerFunc {
	app := s.cfg.Sources.NZBGet
	if src == correlate.SourceSABnzbd {
		app = s.cfg.Sources.SABnzbd
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if !app.Enabled {
			writeError(w, http.StatusNotFound, "source disabled")
			return
		}
		if err := verifyBearer(r.Header.Get("Authorization"), app.ScriptToken); err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		body, err := readBody(w, r)
		if err != nil {
			return
		}
		events, err := source.ParseDownload(body, src)
		if err != nil {
			s.log.LogAttrs(r.Context(), slog.LevelWarn, "parse error",
				slog.String("event", string(src)+".parse_error"),
				slog.String("error", err.Error()))
			writeError(w, http.StatusBadRequest, "bad payload")
			return
		}
		for _, ev := range events {
			s.engine.Process(r.Context(), ev)
		}
		w.WriteHeader(http.StatusOK)
	}
}
