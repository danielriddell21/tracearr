# tracearr

> *tracearr* — traces for the \*arr stack.

[![CI](https://github.com/danielriddell21/tracearr/actions/workflows/ci.yaml/badge.svg)](https://github.com/danielriddell21/tracearr/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/danielriddell21/tracearr/graph/badge.svg)](https://codecov.io/gh/danielriddell21/tracearr)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=danielriddell21_tracearr&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=danielriddell21_tracearr)
[![Go 1.26](https://img.shields.io/badge/go-1.26-blue)](https://go.dev)
[![MIT License](https://img.shields.io/badge/licence-MIT-green)](LICENSE)

OpenTelemetry trace collector for the \*arr stack. A single Go binary that turns Overseerr/Sonarr/Radarr webhooks and NZBGet/SABnzbd post-processing callbacks into causal, W3C-compliant traces of a media request's full lifecycle — *request → search → grab → download → import* — and ships them via OTLP to any backend (Tempo, Jaeger, SigNoz, …).

Complementary to existing observability: [`exportarr`](https://github.com/onedr0p/exportarr) keeps owning Prometheus metrics and Promtail/Vector/Loki keep owning logs. tracearr only adds traces, and the small set of metrics that derive from them.

## Span tree

```
media.request            (SERVER, root)        Overseerr MEDIA_PENDING
├─ prowlarr.search       (CLIENT, synthetic)   approval -> grab interval
├─ sonarr.grab|radarr.grab (CLIENT)            Sonarr/Radarr Grab
│  └─ download.transfer  (CLIENT)              NZBGet/SABnzbd
└─ sonarr.import|radarr.import (INTERNAL)      Sonarr/Radarr Download(=imported)
```

Custom attributes live under `media.*` / `download.*` / `prowlarr.*`; the canonical list is in `internal/spans/schema.go`.

## Quickstart

1. Copy `config.example.yaml` to `config.yaml` and edit the source list.
2. Set the env vars referenced by `*_env` keys. Secrets **must** come from env — tracearr refuses to load if a referenced var is unset.
3. Run an OTel Collector and a trace backend (`deploy/docker-compose.yaml` is a Tempo-based reference).
4. Run it:

```sh
docker run -v $(pwd)/config.yaml:/etc/tracearr/config.yaml ghcr.io/danielriddell21/tracearr:latest
```

5. Point the webhooks at it — Overseerr → Notifications → Webhook, and Sonarr/Radarr → Connect → Webhook with *On Grab*, *On Download* and *On Manual Interaction Required* enabled. For NZBGet/SABnzbd, install the helper from `scripts/` and set `TRACEARR_URL` and `TRACEARR_TOKEN`.

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
| `/metrics` | GET | none (when `metrics.enabled: true`) |
