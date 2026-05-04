// Package config loads tracearr's YAML config and resolves env-indirected
// secrets. Secrets MUST be supplied via env vars (`*_env` keys); inline
// values are rejected at load time.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Storage     StorageConfig     `yaml:"storage"`
	Correlation CorrelationConfig `yaml:"correlation"`
	Sources     SourcesConfig     `yaml:"sources"`
	Exporter    ExporterConfig    `yaml:"exporter"`
	Sampling    SamplingConfig    `yaml:"sampling"`
	Logs        LogsConfig        `yaml:"logs"`
	Metrics     MetricsConfig     `yaml:"metrics"`
}

type ServerConfig struct {
	Listen       string        `yaml:"listen"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
}

type StorageConfig struct {
	Backend            string        `yaml:"backend"`
	Path               string        `yaml:"path"`
	MaxTraceDuration   time.Duration `yaml:"max_trace_duration"`
}

type CorrelationConfig struct {
	ProwlarrBuffer  time.Duration `yaml:"prowlarr_buffer"`
	DownloadLookback time.Duration `yaml:"download_lookback"`
}

type SourcesConfig struct {
	Overseerr  AppSource `yaml:"overseerr"`
	Jellyseerr AppSource `yaml:"jellyseerr"`
	Sonarr     AppSource `yaml:"sonarr"`
	Radarr     AppSource `yaml:"radarr"`
	Prowlarr   AppSource `yaml:"prowlarr"`
	NZBGet     AppSource `yaml:"nzbget"`
	SABnzbd    AppSource `yaml:"sabnzbd"`
}

type AppSource struct {
	Enabled             bool          `yaml:"enabled"`
	BaseURL             string        `yaml:"base_url"`
	APIKeyEnv           string        `yaml:"api_key_env"`
	WebhookSecretEnv    string        `yaml:"webhook_secret_env"`
	ScriptTokenEnv      string        `yaml:"script_token_env"`
	QueuePollInterval   time.Duration `yaml:"queue_poll_interval"`
	HistoryPollInterval time.Duration `yaml:"history_poll_interval"`

	// Resolved at load time from the *_env keys.
	APIKey        string `yaml:"-"`
	WebhookSecret string `yaml:"-"`
	ScriptToken   string `yaml:"-"`
}

type ExporterConfig struct {
	OTLP OTLPConfig `yaml:"otlp"`
}

type OTLPConfig struct {
	Endpoint   string    `yaml:"endpoint"`
	Protocol   string    `yaml:"protocol"`
	Insecure   bool      `yaml:"insecure"`
	HeadersEnv string    `yaml:"headers_env"`
	TLS        TLSConfig `yaml:"tls"`
}

type TLSConfig struct {
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type SamplingConfig struct {
	Strategy string `yaml:"strategy"`
}

type LogsConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	OTLP   struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"otlp"`
}

type MetricsConfig struct {
	Enabled        bool          `yaml:"enabled"`
	ExportInterval time.Duration `yaml:"export_interval"`
}

// Load reads, parses, validates, and resolves env-indirected secrets.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.resolveSecrets(); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Server.Listen == "" {
		c.Server.Listen = "0.0.0.0:8080"
	}
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = 10 * time.Second
	}
	if c.Storage.Backend == "" {
		c.Storage.Backend = "memory"
	}
	if c.Storage.MaxTraceDuration == 0 {
		c.Storage.MaxTraceDuration = 14 * 24 * time.Hour
	}
	if c.Correlation.DownloadLookback == 0 {
		c.Correlation.DownloadLookback = 60 * time.Second
	}
	if c.Correlation.ProwlarrBuffer == 0 {
		c.Correlation.ProwlarrBuffer = 30 * time.Second
	}
	if c.Exporter.OTLP.Protocol == "" {
		c.Exporter.OTLP.Protocol = "grpc"
	}
	if c.Sampling.Strategy == "" {
		c.Sampling.Strategy = "always_on"
	}
	if c.Logs.Level == "" {
		c.Logs.Level = "info"
	}
	if c.Logs.Format == "" {
		c.Logs.Format = "json"
	}
	if c.Metrics.ExportInterval == 0 {
		c.Metrics.ExportInterval = 60 * time.Second
	}
}

func (c *Config) resolveSecrets() error {
	resolve := func(name string, app *AppSource) error {
		if !app.Enabled {
			return nil
		}
		if app.APIKeyEnv != "" {
			app.APIKey = os.Getenv(app.APIKeyEnv)
			if app.APIKey == "" {
				return fmt.Errorf("%s.api_key_env=%s but env var is unset", name, app.APIKeyEnv)
			}
		}
		if app.WebhookSecretEnv != "" {
			app.WebhookSecret = os.Getenv(app.WebhookSecretEnv)
			// Webhook secret may legitimately be empty (not all *arr versions support signing).
		}
		if app.ScriptTokenEnv != "" {
			app.ScriptToken = os.Getenv(app.ScriptTokenEnv)
			if app.ScriptToken == "" {
				return fmt.Errorf("%s.script_token_env=%s but env var is unset", name, app.ScriptTokenEnv)
			}
		}
		return nil
	}
	if err := resolve("overseerr", &c.Sources.Overseerr); err != nil {
		return err
	}
	if err := resolve("jellyseerr", &c.Sources.Jellyseerr); err != nil {
		return err
	}
	if err := resolve("sonarr", &c.Sources.Sonarr); err != nil {
		return err
	}
	if err := resolve("radarr", &c.Sources.Radarr); err != nil {
		return err
	}
	if err := resolve("prowlarr", &c.Sources.Prowlarr); err != nil {
		return err
	}
	if err := resolve("nzbget", &c.Sources.NZBGet); err != nil {
		return err
	}
	if err := resolve("sabnzbd", &c.Sources.SABnzbd); err != nil {
		return err
	}
	return nil
}

func (c *Config) validate() error {
	if c.Exporter.OTLP.Endpoint == "" {
		return errors.New("exporter.otlp.endpoint is required")
	}
	if c.Storage.Backend != "memory" && c.Storage.Backend != "bolt" {
		return fmt.Errorf("storage.backend %q not supported (memory|bolt)", c.Storage.Backend)
	}
	if c.Storage.Backend == "bolt" && c.Storage.Path == "" {
		return errors.New("storage.path required when backend=bolt")
	}
	return nil
}
