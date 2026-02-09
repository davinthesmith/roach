# UniFi Protect Kafka Service

Subscribes to UniFi Protect events via WebSocket and publishes smart detection, audio, and motion events to Kafka.

## Overview

This service:
- Connects to a UniFi Protect NVR via the Integration API (WebSocket)
- Classifies events into smart video, audio, and motion categories
- Publishes raw event payloads to category-specific Kafka topics
- Resolves camera IDs to friendly names for Kafka message keys
- Auto-reconnects with configurable exponential backoff

## Architecture

```
UniFi Protect NVR (WebSocket) → ubiquiti-kafka → Kafka Topics
         ↑                            ↑
   Camera discovery            Event classification
   (HTTP /cameras)            (smart/audio/motion)
```

## Topics Published

### Event Topics (real-time via WebSocket)

- **ubiquiti.protect.smart** — Smart video AI detections (person, vehicle, animal, package)
- **ubiquiti.protect.audio** — Smart audio AI detections (babyCry, coAlarm, smoke, speak)
- **ubiquiti.protect.motion** — Motion events

### Message Key

Format: `{camera_name}:{timestamp}`

Camera names are sanitized to lowercase with underscores (e.g., `Front Door` → `front_door`).

### Message Headers

| Header | Description |
|---|---|
| `schema_version` | Schema version (`"1"`) |
| `camera_id` | Camera/device ID from Protect API |
| `camera_name` | Sanitized camera name |
| `event_type` | Category: `smart`, `audio`, or `motion` |
| `detection_type` | Specific type (e.g., `person`, `babyCry`, `motion`) |
| `timestamp` | Event timestamp (Unix seconds) |
| `source` | `"unifi-protect"` |

### Message Body

Raw JSON event payload from the UniFi Protect API, forwarded unchanged.

### Detection Types

| Category | Detection Types |
|---|---|
| Smart (video) | `person`, `vehicle`, `animal`, `package` |
| Audio | `babyCry`, `coAlarm`, `smoke`, `speak` |
| Motion | `motion` |

## Configuration

Environment variables:

```bash
# Required
UNIFI_API_KEY=your_unifi_api_key
UNIFI_HOST=https://192.168.1.1        # NVR URL

# Optional
KAFKA_BROKER=kafka:29092              # Kafka broker address (default)
RECONNECT_BACKOFF=1s,5s,30s           # Comma-separated reconnect delays (default)
LOG_LEVEL=info                        # debug, info, warn, error (default: info)
```

## How It Works

1. Loads configuration and validates required environment variables
2. Connects to the UniFi Protect NVR (`UNIFI_HOST`) with API key authentication
3. Fetches camera metadata via `GET /proxy/protect/integration/v1/cameras` for name resolution
4. Opens a WebSocket connection to `/proxy/protect/integration/v1/subscribe/events`
5. For each event received:
   - Unwraps the API envelope (`{"type": "...", "item": {...}}`)
   - Filters to `modelKey == "event"` only
   - Classifies by `smartDetectTypes` (smart/audio) or `type` (motion)
   - Publishes raw JSON to the appropriate Kafka topic with metadata headers
6. On disconnect: reconnects with exponential backoff (resets after 60s of stable connection)
7. On shutdown (SIGINT/SIGTERM): flushes pending Kafka messages and exits

### Kafka Producer

Uses `confluent-kafka-go` with:
- Idempotent producer (`enable.idempotence=true`, `acks=all`)
- LZ4 compression
- Batching (`linger.ms=50`, `batch.size=100000`)
- Background delivery report handling

## Running Locally

### With Docker Compose (Recommended)

```bash
# From project root
docker compose up ubiquiti-kafka
```

### Standalone with Go

```bash
cd services/ubiquiti-kafka

go mod download

export UNIFI_API_KEY=your_api_key
export UNIFI_HOST=https://192.168.1.1
export KAFKA_BROKER=localhost:9092

# Run (requires librdkafka for confluent-kafka-go)
go run main.go
```

## Troubleshooting

### Service Won't Start

```bash
docker compose logs ubiquiti-kafka
```

Common issues:
1. Missing `UNIFI_API_KEY` or `UNIFI_HOST` — service exits with fatal error
2. Kafka not ready — wait for health check, then restart
3. NVR unreachable — verify `UNIFI_HOST` URL and network connectivity

### No Events Published

1. Verify API key has access to the Protect Integration API
2. Check NVR is reachable: `curl -k https://YOUR_NVR/proxy/protect/integration/v1/cameras -H "X-API-Key: YOUR_KEY"`
3. Ensure cameras have smart detection or motion enabled
4. Set `LOG_LEVEL=debug` for verbose event logging

### WebSocket Disconnects

The service auto-reconnects with backoff. Frequent disconnects may indicate:
- Network instability between Docker host and NVR
- NVR firmware updates or restarts
- API key permissions changed

### Build Issues

The `confluent-kafka-go` library requires CGO and `librdkafka`:
- Build stage installs: `gcc`, `musl-dev`, `pkgconfig`, `librdkafka-dev`, `cyrus-sasl-dev`
- Runtime stage installs: `librdkafka`
- Binary built with `CGO_ENABLED=1 -tags dynamic`

## Related Services

- **homeassistant-kafka**: Home Assistant → Kafka event ingestion
- **weatherlink-kafka**: WeatherLink → Kafka data ingestion
