# Home Assistant Command Service

Consumes thermostat commands from a Kafka topic and executes them against Home Assistant via the WebSocket `call_service` API.

## Overview

This service:
- Consumes messages from the `homeassistant.command` Kafka topic
- Translates each message into a Home Assistant WebSocket `call_service` command
- Maintains a persistent WebSocket connection with reconnect and keepalive
- Supports all climate domain services (set_temperature, set_hvac_mode, etc.)

## Topic Consumed

- `homeassistant.command` — thermostat control commands

### Message Format

```json
{
  "domain": "climate",
  "service": "set_temperature",
  "entity_id": "climate.sneaux",
  "data": {
    "temperature": 72
  }
}
```

### Supported Services

| Service | Data Fields | Example |
|---|---|---|
| `set_temperature` | `temperature` (number) | `{"temperature": 72}` |
| `set_hvac_mode` | `hvac_mode` (off, heat, cool, heat_cool, auto) | `{"hvac_mode": "heat"}` |
| `set_preset_mode` | `preset_mode` (away, home, sleep) | `{"preset_mode": "away"}` |
| `set_fan_mode` | `fan_mode` (auto, on) | `{"fan_mode": "auto"}` |
| `turn_on` | _(none)_ | `{}` |
| `turn_off` | _(none)_ | `{}` |

## Configuration

Environment variables:

```bash
# Required
HA_URL=http://homeassistant:8123
HA_TOKEN=your_long_lived_access_token
KAFKA_BROKER=kafka:29092

# Optional
HA_WS_URL=ws://homeassistant:8123/api/websocket  # Override derived URL
KAFKA_TOPIC=homeassistant.command                 # Topic to consume
KAFKA_CONSUMER_GROUP=homeassistant-command         # Consumer group ID
WS_RECONNECT_BACKOFF=1s,5s,30s                   # Reconnect delays
LOG_LEVEL=info
```

## How It Works

1. Connects to Home Assistant WebSocket API and authenticates
2. Starts consuming from the `homeassistant.command` Kafka topic
3. For each message: parses command, sends `call_service` via WebSocket, waits for result
4. On WebSocket failure: automatically reconnects and retries the command
5. Periodic ping/pong keepalive maintains the connection during idle periods

### WebSocket Translation

Each Kafka message is translated to a HA WebSocket `call_service` command:

```json
{
  "id": 1,
  "type": "call_service",
  "domain": "climate",
  "service": "set_temperature",
  "target": {
    "entity_id": "climate.sneaux"
  },
  "service_data": {
    "temperature": 72
  }
}
```

## Testing

Use the helper script to send commands via Kafka:

```bash
# Set temperature
./scripts/homeassistant/send-command.sh set_temperature 72

# Set HVAC mode
./scripts/homeassistant/send-command.sh set_hvac_mode heat

# Set preset mode
./scripts/homeassistant/send-command.sh set_preset_mode away

# Target a specific entity
./scripts/homeassistant/send-command.sh set_temperature 68 --entity climate.sneaux

# Turn off
./scripts/homeassistant/send-command.sh turn_off
```

## Running Locally

### With Docker Compose (Recommended)

```bash
# From project root
docker compose up homeassistant-command
```

### Standalone with Go

```bash
cd services/homeassistant-command

go mod download

export HA_URL=http://localhost:8123
export HA_TOKEN=your_token
export KAFKA_BROKER=localhost:9092

# Run
go run main.go
```
