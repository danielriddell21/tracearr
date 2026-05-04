// Package exporter wires the OTel SDK (TracerProvider, batch processor,
// OTLP exporter) for tracearr. v0.1 supports gRPC and HTTP/protobuf.
package exporter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc/credentials"
)

// Config controls the OTLP exporter.
type Config struct {
	Endpoint    string        // host:port for gRPC, host:port or full URL for HTTP
	Protocol    string        // "grpc" | "http/protobuf"
	Insecure    bool          // disable TLS
	HeadersEnv  string        // env var holding headers like "key1=value1,key2=value2"
	CAFile      string
	CertFile    string
	KeyFile     string
	ServiceName string        // resource service.name; default "tracearr"
	Namespace   string        // resource service.namespace; default "media"
	Version     string        // resource service.version
	BatchTimeout time.Duration // BatchSpanProcessor max delay
}

// Provider is the configured OTel TracerProvider plus a Shutdown handle.
type Provider struct {
	*sdktrace.TracerProvider
}

// New builds a TracerProvider, OTLP exporter, batch span processor, and
// resource. Caller must call Shutdown on process exit.
func New(ctx context.Context, c Config) (*Provider, error) {
	if c.ServiceName == "" {
		c.ServiceName = "tracearr"
	}
	if c.Namespace == "" {
		c.Namespace = "media"
	}
	if c.BatchTimeout == 0 {
		c.BatchTimeout = 5 * time.Second
	}

	exp, err := buildExporter(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(c.ServiceName),
		semconv.ServiceNamespace(c.Namespace),
		semconv.ServiceVersion(c.Version),
	))
	if err != nil {
		return nil, fmt.Errorf("resource merge: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(c.BatchTimeout)),
		sdktrace.WithResource(res),
		// Always-on sampling — homelab volumes do not justify head/tail sampling.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	return &Provider{TracerProvider: tp}, nil
}

func buildExporter(ctx context.Context, c Config) (*otlptrace.Exporter, error) {
	headers := parseHeaders(os.Getenv(c.HeadersEnv))
	switch strings.ToLower(c.Protocol) {
	case "", "grpc":
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(c.Endpoint),
			otlptracegrpc.WithHeaders(headers),
		}
		tlsCfg, err := loadTLS(c)
		if err != nil {
			return nil, err
		}
		switch {
		case c.Insecure:
			opts = append(opts, otlptracegrpc.WithInsecure())
		case tlsCfg != nil:
			opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg)))
		}
		return otlptracegrpc.New(ctx, opts...)
	case "http", "http/protobuf":
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(c.Endpoint),
			otlptracehttp.WithHeaders(headers),
		}
		tlsCfg, err := loadTLS(c)
		if err != nil {
			return nil, err
		}
		switch {
		case c.Insecure:
			opts = append(opts, otlptracehttp.WithInsecure())
		case tlsCfg != nil:
			opts = append(opts, otlptracehttp.WithTLSClientConfig(tlsCfg))
		}
		return otlptracehttp.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("unknown otlp protocol %q", c.Protocol)
	}
}

func parseHeaders(s string) map[string]string {
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		if i := strings.Index(kv, "="); i > 0 {
			out[strings.TrimSpace(kv[:i])] = strings.TrimSpace(kv[i+1:])
		}
	}
	return out
}

func loadTLS(c Config) (*tls.Config, error) {
	if c.CAFile == "" && c.CertFile == "" && c.KeyFile == "" {
		return nil, nil
	}
	cfg := &tls.Config{}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA file contained no certificates")
		}
		cfg.RootCAs = pool
	}
	if c.CertFile != "" || c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load keypair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
