# Kafka Topics

## Topic Organization

### Naming Convention
Format: `namespace.category[.subcategory]`

### Current Topics

#### Weather Data Topics
Published on `FETCH_INTERVAL` (configurable)

##### weather.iss
**Integrated Sensor Suite** - Outdoor weather station

Fields:
- `temp` - Temperature (°F)
- `hum` - Humidity (%)
- `wind_speed_last` - Current wind speed (mph)
- `wind_dir_last` - Wind direction (degrees)
- `rain_rate_last` - Current rain rate (in/hr)
- `rain_size` - Rain collector type (1=0.01", 2=0.2mm)
- `rain_15_min` - Last 15 min rainfall (in)
- `rain_60_min` - Last hour rainfall (in)
- `rain_24_hr` - Last 24 hr rainfall (in)
- `solar_rad` - Solar radiation (W/m²)
- `uv_index` - UV index
- `dew_point` - Dew point (°F)
- `heat_index` - Heat index (°F)
- `wind_chill` - Wind chill (°F)
- `thw_index` - Temperature-Humidity-Wind index
- `thsw_index` - Temperature-Humidity-Sun-Wind index

##### weather.barometer
**Barometric Pressure**

Fields:
- `bar_sea_level` - Sea level pressure (inHg)
- `bar_absolute` - Absolute pressure (inHg)
- `bar_trend` - Pressure trend (inHg)

##### weather.indoor
**Indoor Conditions**

Fields:
- `temp_in` - Indoor temperature (°F)
- `hum_in` - Indoor humidity (%)
- `dew_point_in` - Indoor dew point (°F)
- `heat_index_in` - Indoor heat index (°F)

##### weather.health
**Console Health Metrics**

Fields:
- `battery_voltage` - Battery voltage (V)
- `input_voltage` - Input voltage (V)
- `uptime` - Uptime (seconds)
- `wifi_rssi` - WiFi signal strength (dBm)
- `link_uptime` - Network uptime (seconds)
- `rx_bytes` - Bytes received
- `tx_bytes` - Bytes transmitted
- `touchpad_wakeups` - Touchpad interaction count
- `bootloader_version` - Bootloader version
- `firmware_version` - Firmware version
- `free_mem` - Free memory (bytes)
- `queue_kilobytes` - Queue size (KB)
- `internal_free_mem` - Internal free memory (bytes)

##### weather.other
**Fallback** - Unknown or unmapped categories

#### Metadata Topics
Published on `METADATA_FETCH_INTERVAL`, deduped by Kafka key cache

##### weather.metadata.sensors
**Sensor Configuration**

Contains: Individual sensor details, LSIDs, configurations

Key format: `lsid:weekStart`

##### weather.metadata.catalog
**Sensor Type Catalog**

Contains: Data structure definitions, field schemas for sensor types

Published: On change or first run

**Message Structure**: Catalog is published as multiple messages (one per sensor type) instead of a single large message. This avoids Kafka size limits and allows incremental processing.

Message key format: `sensor_type:max_data_structure_type`

##### weather.metadata.station
**Station Information**

Contains: Station name, location, timezone, registration details

Key format: `station_id:weekStart`

#### UniFi Protect Topics
Published in real-time via WebSocket event stream from the UniFi Protect NVR Integration API.

Key format: `{camera_name}:{timestamp}` — the camera name is sanitized to lowercase with underscores (e.g., `Front Door` → `front_door`). Timestamp is Unix seconds derived from the event's `start` field.

##### ubiquiti.protect.smart
**Smart Video AI Detections** - camera AI events

Detection types: `person`, `vehicle`, `animal`, `package`

##### ubiquiti.protect.audio
**Smart Audio AI Detections** - camera audio AI events

Detection types: `babyCry`, `coAlarm`, `smoke`, `speak`

##### ubiquiti.protect.motion
**Motion Events** - camera motion triggers

Detection type: `motion`

#### Home Assistant (Ecobee) Topics
Published on Home Assistant `state_changed` events (WebSocket) or optional polling

Key format: `{friendly_name}:{timestamp}` — the friendly name is derived from the entity ID by stripping the HA domain prefix and any sensor-type suffix redundant with the topic (e.g., `sensor.jadyn_s_room_temperature` on the temperature topic becomes `jadyn_s_room`).

##### homeassistant.ecobee.thermostat.climate
**Thermostat Climate** - `climate` entities

##### homeassistant.ecobee.weather
**Weather** - `weather` domain entities (forecast data)

##### homeassistant.ecobee.sensor.temperature
**Temperature** - sensor entities with temperature units or device_class

##### homeassistant.ecobee.sensor.humidity
**Humidity** - sensor entities with humidity units or device_class

##### homeassistant.ecobee.sensor.presence
**Presence** - binary_sensor entities with occupancy/presence device_class

##### homeassistant.ecobee.sensor.battery
**Battery** - sensor entities with battery device_class

##### homeassistant.ecobee.other
**Fallback** - Unclassified Ecobee-related entities

#### Home Assistant Command Topics
Consumed by the `homeassistant-command` service to control Home Assistant devices via WebSocket `call_service` API

##### homeassistant.command
**Thermostat Commands** - Service calls forwarded to Home Assistant

Consumer group: `homeassistant-command`

Body format:
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

Supported services:
- `climate.set_temperature` - Set target temperature (`data.temperature`)
- `climate.set_hvac_mode` - Set HVAC mode (`data.hvac_mode`: off, heat, cool, heat_cool, auto)
- `climate.set_preset_mode` - Set preset mode (`data.preset_mode`: away, home, sleep)
- `climate.set_fan_mode` - Set fan mode (`data.fan_mode`: auto, on)
- `climate.turn_on` - Turn on the thermostat
- `climate.turn_off` - Turn off the thermostat

Testing: Use `scripts/homeassistant/send-command.sh` to produce test messages.

## Message Structure

### Message Format
All messages are JSON with Kafka headers

### Headers

**Data topics (optimized)**:
- `schema_version` - Schema version (string, e.g., "1")
- `lsid` - Logical Sensor ID (integer)
- `timestamp` - Unix timestamp in seconds (integer)
- `sensor_type` - Sensor type code (integer)
- `data_structure_type` - Data structure type (integer)

**Removed Headers** (available via metadata lookup):
- ~~`station_id`~~ - Use metadata lookup via LSID
- ~~`station_id_uuid`~~ - Use metadata lookup via LSID
- ~~`category`~~ - Derive from sensor_type
- ~~`product_name`~~ - Use metadata lookup via LSID

See [kafka-standards.md](kafka-standards.md) for header optimization rationale.

**Home Assistant event topics**:
- `schema_version` - Schema version (string, e.g., "1")
- `entity_id` - Home Assistant entity ID (string)
- `domain` - Entity domain (string, e.g., "sensor", "climate")
- `timestamp` - Unix timestamp in seconds (integer)
- `source` - Message source (string, "homeassistant")
- `event_type` - Home Assistant event type (string, e.g., "state_changed")

**UniFi Protect event topics**:
- `schema_version` - Schema version (string, e.g., "1")
- `camera_id` - Camera/device ID (string)
- `camera_name` - Sanitized camera name (string)
- `event_type` - Event category (string: "smart", "audio", "motion")
- `detection_type` - Specific detection type (string, e.g., "person", "babyCry", "motion")
- `timestamp` - Unix timestamp in seconds (integer)
- `source` - Message source (string, "unifi-protect")

### Body Example (weather.iss)
```json
{
  "txid": 1,
  "temp": 62.3,
  "hum": 55.2,
  "dew_point": 45.1,
  "wind_speed_last": 5.0,
  "wind_dir_last": 180,
  "rain_rate_last": 0.0,
  "solar_rad": 450,
  "uv_index": 3.2,
  "ts": 1706140800
}
```

### Body Example (homeassistant.ecobee.sensor.temperature)
```json
{
  "event_type": "state_changed",
  "data": {
    "entity_id": "sensor.ecobee_temperature",
    "old_state": {
      "entity_id": "sensor.ecobee_temperature",
      "state": "71.2",
      "attributes": {
        "device_class": "temperature",
        "unit_of_measurement": "°F"
      },
      "last_changed": "2026-02-01T12:00:00Z",
      "last_updated": "2026-02-01T12:00:00Z"
    },
    "new_state": {
      "entity_id": "sensor.ecobee_temperature",
      "state": "71.6",
      "attributes": {
        "device_class": "temperature",
        "unit_of_measurement": "°F"
      },
      "last_changed": "2026-02-01T12:05:00Z",
      "last_updated": "2026-02-01T12:05:00Z"
    }
  },
  "origin": "LOCAL",
  "time_fired": "2026-02-01T12:05:00Z",
  "context": {
    "id": "abcdef123456",
    "user_id": "1234abcd"
  }
}
```

### Body Example (ubiquiti.protect.smart)
```json
{
  "id": "abc123def456",
  "modelKey": "event",
  "type": "smartDetectZone",
  "start": 1706140800000,
  "end": 1706140810000,
  "smartDetectTypes": ["person"],
  "camera": "60a1b2c3d4e5f6"
}
```

## Consuming Topics

### Console Consumer (Development)
```bash
# Read latest messages
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --from-beginning \
  --max-messages 5

# With headers
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --property print.headers=true \
  --from-beginning
```

### Programmatic Consumer (Go Example)
```go
import "github.com/segmentio/kafka-go"

reader := kafka.NewReader(kafka.ReaderConfig{
    Brokers: []string{"kafka:29092"},
    Topic:   "weather.iss",
    GroupID: "my-consumer-group",
})

msg, err := reader.ReadMessage(context.Background())
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Message: %s\n", string(msg.Value))
for _, h := range msg.Headers {
    fmt.Printf("Header: %s = %s\n", h.Key, string(h.Value))
}
```

## Topic Configuration

### Auto-Creation
Topics are automatically created when first message is published

### Retention
All topics: Infinite retention (configured at broker level)

### Replication
Single broker: Replication factor = 1

### Partitions
Default: 1 partition per topic (single broker setup)

## Adding New Topics

### From New Service

1. Define topic naming:
```
namespace.category.subcategory
```

2. Publish to topic (auto-created):
```go
writer := kafka.NewWriter(kafka.WriterConfig{
    Brokers: []string{"kafka:29092"},
    Topic:   "home.hvac.temperature",
})
```

3. Include meaningful headers:
```go
msg := kafka.Message{
    Key:   []byte("sensor-id"),
    Value: []byte(jsonData),
    Headers: []kafka.Header{
        {Key: "sensor_id", Value: []byte("hvac-01")},
        {Key: "location", Value: []byte("living-room")},
        {Key: "timestamp", Value: []byte("1706140800")},
    },
}
```

### Suggested Topic Patterns

#### Home Automation
- `home.hvac.temperature`
- `home.hvac.humidity`
- `home.hvac.setpoint`

#### Security
- `home.security.motion`
- `home.security.door`
- `home.security.camera`

#### Energy
- `home.energy.consumption`
- `home.energy.solar`
- `home.energy.battery`

## Monitoring Topics

### Via Kafka UI
1. Open http://localhost:8080
2. Navigate to Topics
3. Select topic to view messages, partitions, configuration

### Via CLI
```bash
# List all topics
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# Topic details
docker exec roach-kafka kafka-topics --describe --topic weather.iss --bootstrap-server localhost:29092

# Message count (offset info)
docker exec roach-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 \
  --topic weather.iss
```
