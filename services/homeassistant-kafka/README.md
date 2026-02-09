# Home Assistant Kafka Service

Streams Home Assistant `state_changed` events and publishes Ecobee-related updates to Kafka.

## Overview

This service:
- Connects to Home Assistant via WebSocket
- Subscribes to `state_changed` events
- Filters for Ecobee entities
- Publishes full HA event payloads to Kafka topics
- Optionally polls the REST API as a fallback

## Topics Published

- `homeassistant.ecobee.thermostat.climate` — climate entities (thermostat state)
- `homeassistant.ecobee.weather` — weather domain entities (forecast data)
- `homeassistant.ecobee.sensor.temperature` — temperature sensors
- `homeassistant.ecobee.sensor.humidity` — humidity sensors
- `homeassistant.ecobee.sensor.presence` — occupancy/presence binary sensors
- `homeassistant.ecobee.sensor.battery` — battery sensors (if available)
- `homeassistant.ecobee.other` — fallback for unclassified entities

### Message Key Format

Keys use a friendly device name with a Unix timestamp:

```
{friendly_name}:{timestamp}
```

The friendly name is derived from the entity ID by stripping the HA domain prefix
and any sensor-type suffix that is redundant with the topic. Examples:

| Entity ID | Topic | Key |
|---|---|---|
| `climate.sneaux` | `thermostat.climate` | `sneaux:1706140800` |
| `weather.sneaux` | `weather` | `sneaux:1706140800` |
| `sensor.sneaux_humidity` | `sensor.humidity` | `sneaux:1706140800` |
| `sensor.jadyn_s_room_temperature` | `sensor.temperature` | `jadyn_s_room:1706140800` |
| `binary_sensor.kitchen_occupancy` | `sensor.presence` | `kitchen:1706140800` |

## Configuration

Environment variables:

```bash
# Required
HA_URL=http://homeassistant:8123
HA_TOKEN=your_long_lived_access_token
KAFKA_BROKER=kafka:29092

# Optional
HA_WS_URL=ws://homeassistant:8123/api/websocket  # Override derived URL
WS_RECONNECT_BACKOFF=1s,5s,30s                   # Reconnect delays
POLL_ENABLED=false                               # REST polling fallback
POLL_INTERVAL=60s                                # Poll interval
POLL_ENTITY_FILTER=climate.ecobee,sensor.ecobee_temp
LOG_LEVEL=info
```

### Entity Filtering

By default, only entities with `ecobee` in their entity_id are published. If `POLL_ENTITY_FILTER`
contains a comma-separated list, only those exact entity_ids are allowed.

## Message Format

**Headers**:
- `schema_version`
- `entity_id`
- `domain`
- `timestamp`
- `source`
- `event_type`

**Body**:
- Full Home Assistant event payload (JSON)

## Running Locally

### With Docker Compose (Recommended)

```bash
# From project root
docker compose up homeassistant-kafka
```

### Standalone with Go

```bash
cd services/homeassistant-kafka

go mod download

export HA_URL=http://localhost:8123
export HA_TOKEN=your_token
export KAFKA_BROKER=localhost:9092

# Run
go run main.go
```
