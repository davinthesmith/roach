# homeassistant-kafka

Streams Home Assistant `state_changed` events to Kafka. Connects via WebSocket (or optional REST poll fallback). Filters Ecobee entities; publishes full HA event payloads to topic-per-type.

## Architecture

```
Home Assistant (WebSocket or REST) → homeassistant-kafka → Kafka
         ↓                                    ↓
   state_changed events              Topic by entity domain/type
   Entity filter: ecobee (or POLL_ENTITY_FILTER)
```

**Flow**: Load config → Validate HA_URL, HA_TOKEN, KAFKA_BROKER, HA_WS_URL (derived from HA_URL if unset) → Discover Ecobee entities from HA entity registry (unless POLL_ENTITY_FILTER set) → Subscribe to state_changed (WebSocket) or poll loop (REST) → On event, filter Ecobee → Route to topic → Publish with key `{friendly_name}:{timestamp}`.

## Topics

| Topic | Entity type |
|-------|-------------|
| `homeassistant.ecobee.thermostat.climate` | climate |
| `homeassistant.ecobee.weather` | weather |
| `homeassistant.ecobee.sensor.temperature` | sensor (temperature) |
| `homeassistant.ecobee.sensor.humidity` | sensor (humidity) |
| `homeassistant.ecobee.sensor.presence` | binary_sensor (occupancy/presence) |
| `homeassistant.ecobee.sensor.battery` | sensor (battery) |
| `homeassistant.ecobee.other` | fallback |

**Key**: `{friendly_name}:{unix_timestamp}`. Friendly name = entity_id minus domain and topic-redundant suffix (e.g. `_temperature`, `_humidity`).

## Configuration

**Required**:

```bash
HA_URL=http://homeassistant:8123
HA_TOKEN=your_long_lived_access_token
KAFKA_BROKER=kafka:29092
```

**Optional**:

```bash
HA_WS_URL=ws://homeassistant:8123/api/websocket   # Default: derived from HA_URL (http→ws, https→wss)
WS_RECONNECT_BACKOFF=1s,5s,30s
POLL_ENABLED=false                                # Use REST polling instead of WebSocket
POLL_INTERVAL=60s
POLL_ENTITY_FILTER=climate.ecobee,sensor.ecobee_temp   # Exact entity_ids only (overrides discovery)
LOG_LEVEL=info
```

**Entity filtering**: If POLL_ENTITY_FILTER is set, only those entity_ids are published. Otherwise, entities are discovered from HA entity registry (search "ecobee") or fallback: entity_id contains "ecobee".

## Message Format

**Headers**: `schema_version`, `entity_id`, `domain`, `timestamp`, `source`, `event_type`.  
**Body**: Full HA event JSON.

## Run

```bash
# From repo root
docker compose up homeassistant-kafka

# Standalone
cd services/homeassistant-kafka
export HA_URL=http://localhost:8123 HA_TOKEN=... KAFKA_BROKER=localhost:9092
go run main.go
```
