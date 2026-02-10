# unifi-video-kafka

UniFi Protect RTSPS video streams → Kafka. Captures **1 frame per second** per camera via ffmpeg and publishes JPEG frames to per-camera Kafka topics with **30-minute rolling retention**.

## How it works

1. Fetches camera list from UniFi Protect API (`GET /v1/cameras`)
2. For each camera, requests an RTSPS stream URL (`POST /v1/cameras/{id}/rtsps-stream`)
3. Starts one ffmpeg process per camera: decodes RTSPS → outputs 1 MJPEG frame/sec to stdout
4. Parses JPEG frames from ffmpeg stdout (SOI/EOI markers) and publishes each to Kafka
5. On stream failure: backs off, re-fetches RTSPS URL (token may expire), restarts ffmpeg

## Topics

**Pattern**: `unifi.protect.video.{camera_name}` (e.g. `unifi.protect.video.courtyard`, `unifi.protect.video.front_door`)

Camera names are sanitized: lowercase, spaces and dashes → underscores.

**Retention**: 30 minutes (`retention.ms=1800000`). Topics are created by the service via Kafka Admin API. No additional cleanup required.

**Key**: `camera_id:timestamp` (e.g. `abc123:1707123456`)

**Value**: Raw JPEG frame bytes (binary)

**Headers**:
| Header | Example |
|--------|---------|
| schema_version | 1 |
| camera_id | abc123 |
| camera_name | courtyard |
| timestamp | 1707123456 |
| source | unifi-protect-video |
| content_type | image/jpeg |

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| UNIFI_HOST | Yes | — | NVR URL (e.g. `https://192.168.1.1`) |
| UNIFI_API_KEY | Yes | — | UniFi integration API key |
| KAFKA_BROKER | No | kafka:29092 | Kafka bootstrap servers |
| LOG_LEVEL | No | info | Log level (info, debug) |
| RECONNECT_BACKOFF | No | 1s,5s,30s | Comma-separated backoff durations |

## Running

### With Docker Compose (recommended)

The service is defined in `docker-compose.yml`. Start with the rest of the stack:

```bash
./scripts/start-all.sh build
```

### Standalone (development)

```bash
cd services/unifi-video-kafka
UNIFI_HOST=https://192.168.1.1 UNIFI_API_KEY=... KAFKA_BROKER=localhost:9092 go run .
```

Requires ffmpeg installed locally and Kafka accessible.

## Troubleshooting

| Issue | Action |
|-------|--------|
| ffmpeg not found | Ensure ffmpeg is installed: `apk add ffmpeg` (Docker) or `brew install ffmpeg` (local) |
| RTSPS 401/403 | Check UNIFI_API_KEY is valid; regenerate in UniFi console |
| No frames published | Check ffmpeg stderr in debug logs (`LOG_LEVEL=debug`); verify RTSPS URL works: `ffmpeg -rtsp_transport tcp -i "rtsps://..." -frames:v 1 test.jpg` |
| Stream keeps reconnecting | RTSPS tokens expire; this is normal. Check backoff logs. If too frequent, check NVR connectivity |
| Large messages rejected | Kafka `message.max.bytes` may need increasing; producer is configured for 10MB max |
