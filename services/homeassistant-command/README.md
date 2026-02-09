# homeassistant-command

Consumes commands from Kafka and executes them against Home Assistant via WebSocket `call_service`. Single topic; persistent WebSocket with reconnect and keepalive.

## Architecture

```
Kafka (homeassistant.command) → homeassistant-command → Home Assistant WebSocket
         ↓                              ↓
   JSON: domain, service,          call_service
   entity_id, data                 Reconnect on failure; 30s keepalive ping
```

**Flow**: Load config → Connect HA WebSocket (with retry) → Start keepalive goroutine → Consume from topic → Parse command → Validate domain/service/entity_id → CallService; on failure clear connection, reconnect, retry once → Commit offset.

## Topic

- **Consumed**: `homeassistant.command` (override: `KAFKA_TOPIC`)

**Message format** (JSON):

```json
{
  "domain": "climate",
  "service": "set_temperature",
  "entity_id": "climate.sneaux",
  "data": { "temperature": 72 }
}
```

**Supported** (climate domain): `set_temperature`, `set_hvac_mode`, `set_preset_mode`, `set_fan_mode`, `turn_on`, `turn_off`. Any HA `call_service` payload is forwarded; validation is domain/service/entity_id required.

## Configuration

**Required**:

```bash
HA_URL=http://homeassistant:8123
HA_TOKEN=your_long_lived_access_token
KAFKA_BROKER=kafka:29092
```

**Optional**:

```bash
HA_WS_URL=ws://homeassistant:8123/api/websocket   # Default: derived from HA_URL
KAFKA_TOPIC=homeassistant.command
KAFKA_CONSUMER_GROUP=homeassistant-command
WS_RECONNECT_BACKOFF=1s,5s,30s
LOG_LEVEL=info
```

## Run

```bash
# From repo root
docker compose up homeassistant-command

# Standalone
cd services/homeassistant-command
export HA_URL=... HA_TOKEN=... KAFKA_BROKER=...
go run main.go
```

## Testing

```bash
./scripts/homeassistant/send-command.sh set_temperature 72
./scripts/homeassistant/send-command.sh set_hvac_mode heat
./scripts/homeassistant/send-command.sh set_preset_mode away
./scripts/homeassistant/send-command.sh set_temperature 68 --entity climate.sneaux
./scripts/homeassistant/send-command.sh turn_off
```
