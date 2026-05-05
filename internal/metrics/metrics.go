// Package metrics owns tracearr's derived metric instruments. Every metric
// here is computed from completed spans (or webhook receipts) and adds
// information the existing exportarr Prometheus exporter does not provide.
package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics groups the instruments. A nil *Metrics is a valid no-op so call
// sites can blindly invoke without nil-checking.
type Metrics struct {
	requestDuration  metric.Float64Histogram
	grabDuration     metric.Float64Histogram
	downloadDuration metric.Float64Histogram
	importDuration   metric.Float64Histogram
	attempts         metric.Int64Counter
	inFlight         metric.Int64UpDownCounter
	webhookReceived  metric.Int64Counter

	provider *sdkmetric.MeterProvider
	handler  http.Handler
}

// New wires the OTel meter provider with a Prometheus reader, mounts the
// /metrics handler, and creates all instruments. The returned Metrics is
// safe to call from any goroutine.
func New(serviceName, namespace, version string) (*Metrics, error) {
	exporter, err := otelprom.New()
	if err != nil {
		return nil, err
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("tracearr")

	m := &Metrics{
		provider: provider,
		handler:  promhttp.Handler(),
	}

	if m.requestDuration, err = meter.Float64Histogram(
		"tracearr_request_duration_seconds",
		metric.WithDescription("End-to-end request -> on-disk latency."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.grabDuration, err = meter.Float64Histogram(
		"tracearr_grab_duration_seconds",
		metric.WithDescription("Time from approval to grab; surfaces slow indexers."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.downloadDuration, err = meter.Float64Histogram(
		"tracearr_download_duration_seconds",
		metric.WithDescription("Pure transfer time, indexer-independent."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.importDuration, err = meter.Float64Histogram(
		"tracearr_import_duration_seconds",
		metric.WithDescription("Time from download done to imported; flags filesystem/scan issues."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.attempts, err = meter.Int64Counter(
		"tracearr_attempts_total",
		metric.WithDescription("Trace attempt closures, including upgrades and retries."),
	); err != nil {
		return nil, err
	}
	if m.inFlight, err = meter.Int64UpDownCounter(
		"tracearr_in_flight",
		metric.WithDescription("In-flight traces by phase."),
	); err != nil {
		return nil, err
	}
	if m.webhookReceived, err = meter.Int64Counter(
		"tracearr_webhook_received_total",
		metric.WithDescription("Webhook deliveries received, segmented by source/event/result."),
	); err != nil {
		return nil, err
	}
	_ = serviceName
	_ = namespace
	_ = version
	return m, nil
}

// Handler returns the http.Handler exposing /metrics in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return m.handler
}

// Shutdown flushes the exporter.
func (m *Metrics) Shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	return m.provider.Shutdown(ctx)
}
