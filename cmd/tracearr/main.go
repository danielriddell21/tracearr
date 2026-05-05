// Command tracearr is the entrypoint for the tracearr service. It loads
// config, wires the OTel exporter, correlation engine, and HTTP receivers,
// then runs until SIGTERM/SIGINT.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danielriddell21/tracearr/internal/config"
	"github.com/danielriddell21/tracearr/internal/correlate"
	"github.com/danielriddell21/tracearr/internal/exporter"
	"github.com/danielriddell21/tracearr/internal/log"
	"github.com/danielriddell21/tracearr/internal/metrics"
	"github.com/danielriddell21/tracearr/internal/poller"
	"github.com/danielriddell21/tracearr/internal/receiver"
	"github.com/danielriddell21/tracearr/internal/spans"
)

// version is set by the build (goreleaser ldflags).
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "tracearr: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "/etc/tracearr/config.yaml", "path to YAML config")
	flag.Parse()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := log.New(os.Stdout, cfg.Logs.Level)
	logger.Info("starting tracearr", "version", version, "config", cfgPath)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	provider, err := exporter.New(ctx, exporter.Config{
		Endpoint:    cfg.Exporter.OTLP.Endpoint,
		Protocol:    cfg.Exporter.OTLP.Protocol,
		Insecure:    cfg.Exporter.OTLP.Insecure,
		HeadersEnv:  cfg.Exporter.OTLP.HeadersEnv,
		CAFile:      cfg.Exporter.OTLP.TLS.CAFile,
		CertFile:    cfg.Exporter.OTLP.TLS.CertFile,
		KeyFile:     cfg.Exporter.OTLP.TLS.KeyFile,
		ServiceName: "tracearr",
		Namespace:   "media",
		Version:     version,
	})
	if err != nil {
		return fmt.Errorf("init exporter: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(shutCtx)
	}()

	tracer := provider.Tracer("tracearr")
	builder := spans.NewOTelBuilder(tracer)

	var store correlate.Store
	switch cfg.Storage.Backend {
	case "bolt":
		s, err := correlate.OpenBoltStore(cfg.Storage.Path, 24*time.Hour, 30*24*time.Hour)
		if err != nil {
			return fmt.Errorf("open bolt store: %w", err)
		}
		store = s
	default:
		store = correlate.NewMemoryStore(24*time.Hour, 30*24*time.Hour)
	}
	defer func() { _ = store.Close() }()

	engine := correlate.NewEngine(store, builder, logger, cfg.Correlation.DownloadLookback)

	var metricsRegistry *metrics.Metrics
	if cfg.Metrics.Enabled {
		metricsRegistry, err = metrics.New("tracearr", "media", version)
		if err != nil {
			return fmt.Errorf("init metrics: %w", err)
		}
		defer func() {
			shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = metricsRegistry.Shutdown(shutCtx)
		}()
		engine.SetMetrics(metricsRegistry)
	}
	engine.Restore(ctx)

	// Janitor.
	janitorDone := make(chan struct{})
	go func() {
		defer close(janitorDone)
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				engine.Sweep(t.UTC(), cfg.Storage.MaxTraceDuration)
			}
		}
	}()

	// Pollers. Each runs in its own goroutine; ctx cancellation stops them.
	if app := cfg.Sources.Sonarr; app.Enabled && app.BaseURL != "" && app.APIKey != "" {
		p := poller.NewQueuePoller("sonarr", app.BaseURL, app.APIKey, app.QueuePollInterval,
			poller.EngineNoter{Engine: engine}, logger)
		go p.Run(ctx)
	}
	if app := cfg.Sources.Radarr; app.Enabled && app.BaseURL != "" && app.APIKey != "" {
		p := poller.NewQueuePoller("radarr", app.BaseURL, app.APIKey, app.QueuePollInterval,
			poller.EngineNoter{Engine: engine}, logger)
		go p.Run(ctx)
	}
	if app := cfg.Sources.Prowlarr; app.Enabled && app.BaseURL != "" && app.APIKey != "" {
		p := poller.NewProwlarrPoller(app.BaseURL, app.APIKey, app.HistoryPollInterval,
			poller.EngineProwlarr{Engine: engine}, logger)
		go p.Run(ctx)
	}

	rcv := receiver.NewServer(cfg, engine, logger)
	if metricsRegistry != nil {
		rcv.SetMetrics(metricsRegistry)
		rcv.SetScrapeHandler(metricsRegistry.Handler())
	}
	srv := &http.Server{
		Addr:        cfg.Server.Listen,
		Handler:     rcv.Mux(),
		ReadTimeout: cfg.Server.ReadTimeout,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http listening", "addr", cfg.Server.Listen)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Warn("http shutdown error", "error", err)
	}
	<-janitorDone
	logger.Info("stopped")
	return nil
}
