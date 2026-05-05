package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Phase enum matches what the engine reports. Kept stringly to avoid an
// extra type import on the call site.
const (
	PhaseRequest  = "request"
	PhaseGrab     = "grab"
	PhaseDownload = "download"
	PhaseImport   = "import"
)

// ObserveRequest closes out a request-lifecycle measurement.
func (m *Metrics) ObserveRequest(ctx context.Context, mediaType, source, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.requestDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String("media_type", mediaType),
			attribute.String("source", source),
			attribute.String("outcome", outcome),
		))
}

// ObserveGrab records the approval -> grab duration.
func (m *Metrics) ObserveGrab(ctx context.Context, mediaType, indexer string, duration time.Duration) {
	if m == nil || duration <= 0 {
		return
	}
	m.grabDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String("media_type", mediaType),
			attribute.String("indexer", indexer),
		))
}

// ObserveDownload records the download.transfer duration.
func (m *Metrics) ObserveDownload(ctx context.Context, client, protocol string, duration time.Duration) {
	if m == nil || duration <= 0 {
		return
	}
	m.downloadDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String("client", client),
			attribute.String("protocol", protocol),
		))
}

// ObserveImport records the post-download -> imported duration.
func (m *Metrics) ObserveImport(ctx context.Context, app string, duration time.Duration) {
	if m == nil || duration <= 0 {
		return
	}
	m.importDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(attribute.String("app", app)))
}

// CountAttempt records a single trace closure.
func (m *Metrics) CountAttempt(ctx context.Context, mediaType, outcome, upgradeReason string) {
	if m == nil {
		return
	}
	m.attempts.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("media_type", mediaType),
			attribute.String("outcome", outcome),
			attribute.String("upgrade_reason", upgradeReason),
		))
}

// AdjustInFlight increments or decrements the in-flight gauge for a phase.
func (m *Metrics) AdjustInFlight(ctx context.Context, phase string, delta int64) {
	if m == nil {
		return
	}
	m.inFlight.Add(ctx, delta, metric.WithAttributes(attribute.String("phase", phase)))
}

// CountWebhook records a webhook receipt with the result classification.
// result is one of "ok" | "dropped" | "dedup" | "unauthorized" | "bad_payload".
func (m *Metrics) CountWebhook(ctx context.Context, source, eventType, result string) {
	if m == nil {
		return
	}
	m.webhookReceived.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("source", source),
			attribute.String("event_type", eventType),
			attribute.String("result", result),
		))
}
