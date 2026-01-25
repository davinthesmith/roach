# Kafka Topics

## Topic Organization

### Naming Convention
Format: `namespace.category[.subcategory]`

### Current Topics

#### Weather Data Topics
Published every 5 minutes (configurable)

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

#### Metadata Topics
Published only when changes detected (hash-based comparison)

##### weather.metadata.sensors
**Sensor Configuration**

Contains: Individual sensor details, LSIDs, configurations

Published: On change or first run

##### weather.metadata.catalog
**Sensor Type Catalog**

Contains: Data structure definitions, field schemas for sensor types

Published: On change or first run

**Message Structure**: As of January 2026, catalog is published as multiple messages (one per sensor type) instead of a single large message. This avoids Kafka size limits and allows incremental processing.

Message key format: `sensor_type:{sensor_type_id}`

##### weather.metadata.station
**Station Information**

Contains: Station name, location, timezone, registration details

Published: On change or first run

## Message Structure

### Message Format
All messages are JSON with Kafka headers

### Headers

**Current (Optimized - January 2026)**:
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

### Body Example (weather.iss)
```json
{
  "lsid": 555566,
  "data_structure_type": 1,
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
