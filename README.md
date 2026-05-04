# tracearr

OpenTelemetry trace collector for the *arr stack. Single Go binary that turns
Overseerr/Sonarr/Radarr webhooks and NZBGet/SABnzbd post-processing callbacks
into causal, W3C-compliant traces of a media request's full lifecycle —
*request → search → grab → download → import* — and ships them via OTLP to any
backend (Tempo, Jaeger, SigNoz, …).

Designed to be **complementary** to existing observability:
[`exportarr`](https://github.com/onedr0p/exportarr) keeps owning Prometheus
metrics; Promtail/Vector/Loki keep owning logs. tracearr only adds traces and
the small set of metrics that *derive from* traces and have no exportarr
equivalent.

## Status

**v0.1** — webhooks → spans → OTLP export. In-memory state, synthetic
`prowlarr.search` derived from grab timing, NZBGet/SABnzbd via helper scripts,
multi-arch container.

See [`PLAN.md`](docs/PLAN.md) (or the planner's notes) for the v0.2/v0.3
roadmap (BoltDB persistence, real Prowlarr polling, derived metrics, dashboards).

## Span tree

```
media.request            (SERVER, root)        Overseerr MEDIA_PENDING
├─ prowlarr.search       (CLIENT, synthetic)   approval -> grab interval
├─ sonarr.grab|radarr.grab (CLIENT)            Sonarr/Radarr Grab
│  └─ download.transfer  (CLIENT)              NZBGet/SABnzbd
└─ sonarr.import|radarr.import (INTERNAL)      Sonarr/Radarr Download(=imported)
```

All custom attributes live under `media.*` / `download.*` / `prowlarr.*`. See
`internal/spans/schema.go` for the canonical attribute list.

## Quickstart

1. Copy `config.example.yaml` to `config.yaml` and edit the source list.
2. Set the env vars referenced by `*_env` keys (API keys, webhook secrets,
   script tokens). Secrets MUST come from env — tracearr refuses to load if a
   referenced env var is unset.
3. Run an OTel Collector and your trace backend (see
   `deploy/docker-compose.yaml` for a Tempo-based reference).
4. `docker run -v $(pwd)/config.yaml:/etc/tracearr/config.yaml ghcr.io/danielriddell21/tracearr:latest`
5. In Overseerr → Settings → Notifications → Webhook, set the URL to
   `http://tracearr:8080/webhook/overseerr` and the Authorization header to
   `Bearer <OVERSEERR_WEBHOOK_SECRET>`.
6. In Sonarr/Radarr → Settings → Connect → Webhook, add
   `http://tracearr:8080/webhook/{sonarr,radarr}` and enable: *On Grab*,
   *On Download*, *On Manual Interaction Required*.
7. In NZBGet/SABnzbd, install the helper script from `scripts/` and set
   `TRACEARR_URL` and `TRACEARR_TOKEN` env vars.

## Build

```sh
go build ./cmd/tracearr
```

Tests:

```sh
go test ./... -race
```

## Endpoints

| Path | Method | Auth |
|---|---|---|
| `/webhook/overseerr` | POST | `Authorization: Bearer <secret>` |
| `/webhook/jellyseerr` | POST | `Authorization: Bearer <secret>` |
| `/webhook/sonarr` | POST | `X-Webhook-Signature: <hex hmac-sha256>` (optional) |
| `/webhook/radarr` | POST | `X-Webhook-Signature: <hex hmac-sha256>` (optional) |
| `/script/nzbget` | POST | `Authorization: Bearer <token>` |
| `/script/sabnzbd` | POST | `Authorization: Bearer <token>` |
| `/healthz` | GET | none |
