# WeatherLink Kafka Service

Ingests WeatherLink v2 API data and publishes current conditions plus metadata to Kafka, with optional historical backfill.

## Overview

This service:
- Fetches current conditions from your WeatherLink station on a fixed interval
- Publishes sensor readings to category-specific Kafka topics
- Publishes sensor, station, and catalog metadata (deduped across restarts)
- Optionally backfills historical data across a time range

## Architecture

```
WeatherLink API → weatherlink-kafka → Kafka Topics
                       ↓
                 Metadata + Backfill
```

## Topics Published

### Weather Data (interval-driven)
- **weather.iss**: Outdoor weather (ISS - Integrated Sensor Suite)
- **weather.barometer**: Barometric pressure
- **weather.indoor**: Indoor conditions
- **weather.health**: Console health metrics
- **weather.other**: Fallback topic for unknown categories

### Metadata (deduped by key)
- **weather.metadata.sensors**: Sensor configuration (keyed by `lsid:weekStart`)
- **weather.metadata.catalog**: Sensor catalog entries (keyed by `sensor_type:max_data_structure_type`)
- **weather.metadata.station**: Station information (keyed by `station_id:weekStart`)

## Configuration

### Environment Variables

Create a `.env` file (or use Docker Compose environment):

```bash
# Required
WEATHERLINK_API_KEY=your_api_key_here
WEATHERLINK_API_SECRET=your_api_secret_here
WEATHERLINK_STATION_ID=your_station_id_here

# Optional
KAFKA_BROKER=kafka:29092           # Kafka broker address
POSTGRES_DSN=host=postgres...      # Currently unused by this service
FETCH_INTERVAL=5m                  # How often to fetch data
METADATA_FETCH_INTERVAL=168h       # How often to refresh metadata
LOG_LEVEL=info                     # Logging level (debug, info, warn, error)

# Backfill (optional)
KAFKA_BACKFILL_ENABLED=true        # Enable historical backfill
BACKFILL_START_TS=0                # Unix timestamp (seconds)
BACKFILL_END_TS=0                  # Unix timestamp (seconds, 0 = now)
```

## Running Locally

### With Docker Compose (Recommended)

```bash
# From project root
docker compose up weatherlink-kafka
```

### Standalone with Go

```bash
cd services/weatherlink-kafka

go mod download

export WEATHERLINK_API_KEY=your_key
export WEATHERLINK_API_SECRET=your_secret
export WEATHERLINK_STATION_ID=your_station_id
export KAFKA_BROKER=localhost:9092

# Run
go run main.go
```

## How It Works

### Startup Sequence

1. Load configuration from environment
2. Connect to Kafka broker
3. Fetch sensor metadata from WeatherLink API
4. Fetch sensor catalog (filtered to active sensor types)
5. Fetch station information
6. Publish metadata to respective topics (deduped by key)
7. Start periodic data fetch loop
8. If enabled, start backfill workers in parallel

### Data Collection Loop

Every `FETCH_INTERVAL`:

1. Fetch current conditions from WeatherLink API
2. For each sensor:
   - Look up sensor metadata (category)
   - Determine target topic based on category
   - For each data point in `sensor.data`:
     - Extract timestamp from `ts`
     - Deduplicate against existing Kafka keys (`lsid:timestamp`)
     - Publish message with headers

### Metadata Updates

Metadata is refreshed on `METADATA_FETCH_INTERVAL`. Keys include the current week start (UTC)
for sensors/station metadata so the same data is only published once per week.

### Deduplication

**Kafka key cache**:
- Scans weather topics on startup to build an in-memory set of existing keys (`lsid:timestamp`)
- Skips publishing if the key is already present in Kafka
- Adds new keys to the cache after successful publish

**Metadata key cache**:
- Scans metadata topics on startup to avoid re-publishing identical keys across restarts

### Backfill

When enabled, the service:
- Splits the time range into 24-hour windows
- Runs a worker pool in parallel across windows
- Uses a token bucket rate limiter with exponential backoff on 429 errors
- Publishes backfilled data asynchronously to Kafka

## Message Format

### Weather Data Message

**Headers**:
```
schema_version: 1
lsid: 918290
timestamp: 1769295600
sensor_type: 43
data_structure_type: 23
```

**Body (JSON):**
```json
{
  "temp": 49.7,
  "hum": 93.8,
  "wind_speed_last": 0,
  "wind_dir_last": 316,
  "rainfall_day_in": 2.06,
  "dew_point": 48,
  "heat_index": 50.2,
  "ts": 1769295600
}
```

### Metadata Message

**weather.metadata.sensors:**
```json
{
  "lsid": 918290,
  "sensor_type": 43,
  "category": "ISS",
  "manufacturer": "Davis Instruments",
  "product_name": "Vantage Pro2, Wireless",
  "station_id": 228773,
  "station_id_uuid": "a7a3248e-d78f-4ab4-9785-a96abd084493",
  "station_name": "Bellevue",
  "latitude": 30.151285,
  "longitude": -92.07978,
  "elevation": 76.638
}
```

## Troubleshooting

### Service Won't Start

```bash
docker compose logs weatherlink-kafka
```

Common issues:
1. Missing API credentials
2. Kafka not ready (wait 30s after starting kafka)
3. Invalid station ID

### No Data Published

1. Verify API credentials are correct
2. Check station ID matches your WeatherLink account
3. Ensure Kafka is reachable at configured broker address
4. Look for errors in logs: `docker compose logs -f weatherlink-kafka`

### API Errors

- **401 Unauthorized**: Invalid API key or secret
- **404 Not Found**: Invalid station ID
- **429 Too Many Requests**: Rate limited (reduce fetch interval or backfill settings)

## Extending

### Adding New Topics

Modify `GetTopicForCategory()` in `util/topic.go`:

```go
func GetTopicForCategory(category string) string {
    switch strings.ToUpper(category) {
    case "ISS":
        return "weather.iss"
    case "YOUR_NEW_CATEGORY":
        return "weather.your_topic"
    // ...
    }
}
```

### Customizing Fetch Interval

Set `FETCH_INTERVAL` environment variable:
- `1m` - Every minute
- `5m` - Every 5 minutes (default)
- `15m` - Every 15 minutes
- `1h` - Every hour

## Security

- API credentials stored in environment variables (never in code)
- HTTPS for all WeatherLink API calls
- Kafka communication over Docker network (plaintext internally)

## Related Services

- **weatherlink-sql**: Kafka → SQL materialization
- **weatherlink-sql-backfill**: SQL backfill from Kafka
