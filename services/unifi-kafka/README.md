# unifi-kafka

Subscribes to UniFi Protect event stream (Integration API WebSocket) and publishes smart, audio, and motion events to Kafka. Resolves camera IDs to friendly names; auto-reconnects with configurable backoff.

## Architecture

```
UniFi Protect NVR (WebSocket) → unifi-kafka → Kafka
         ↑                            ↑
   GET /cameras (name resolution)   Classify: smartDetectTypes + type
   /subscribe/events                Key: camera_name:timestamp
```

**Flow**: Load config (UNIFI_API_KEY, UNIFI_HOST required) → Fetch cameras for name resolution → Subscribe to `/proxy/protect/integration/v1/subscribe/events` → For each message: unwrap envelope, filter `modelKey == "event"`, classify by smartDetectTypes (smart/audio) or type (motion) → Publish raw JSON to topic with headers → On disconnect: backoff (reset after 60s stable), reconnect. On shutdown: flush producer, close.

## Topics

| Topic | Content |
|-------|---------|
| `unifi.protect.smart` | Video AI: person, vehicle, animal, package |
| `unifi.protect.audio` | Audio AI: babyCry, coAlarm, smoke, speak |
| `unifi.protect.motion` | Motion events |

**Key**: `{camera_name}:{unix_timestamp}`. Camera name = sanitized (lowercase, spaces/dashes → underscores); fallback = camera ID if discovery failed.

**Headers**: `schema_version`, `camera_id`, `camera_name`, `event_type`, `detection_type`, `timestamp`, `source=unifi-protect`.  
**Body**: Raw UniFi Protect event JSON.

## Configuration

**Required**:

```bash
UNIFI_HOST=https://192.168.1.1
UNIFI_API_KEY=your_api_key
```

**Optional**:

```bash
KAFKA_BROKER=kafka:29092
RECONNECT_BACKOFF=1s,5s,30s
LOG_LEVEL=info
```

## Run

```bash
# From repo root
docker compose up unifi-kafka

# Standalone (requires librdkafka / CGO)
cd services/unifi-kafka
export UNIFI_HOST=... UNIFI_API_KEY=... KAFKA_BROKER=localhost:9092
go run main.go
```

## Producer

Uses `confluent-kafka-go`: idempotent producer, acks=all, LZ4 compression, linger.ms=50, batch.size=100KB. Shutdown flushes with 10s timeout.

## Troubleshooting

| Issue | Action |
|-------|--------|
| Won't start | Set UNIFI_API_KEY, UNIFI_HOST; ensure Kafka and NVR reachable |
| No events | Verify API key has Integration API access; cameras have smart/motion enabled; `LOG_LEVEL=debug` |
| Disconnects | Auto-reconnect with backoff; check network, NVR restarts, API key |

**Build**: CGO and librdkafka required (Dockerfile installs librdkafka-dev at build, librdkafka at runtime).
