# WeatherLink Ingest Service

Real-time data ingestion service that fetches weather data from the WeatherLink v2 API and publishes to Kafka topics.

## Overview

This service:
- Fetches current conditions from your WeatherLink weather station every 5 minutes
- Publishes sensor data to topic-specific Kafka streams
- Maintains metadata about sensors and data structures
- Detects and publishes configuration changes

## Architecture

```
WeatherLink API → weatherlink-kafka → Kafka Topics
                       ↓
                 Change Detection
                 Hash Comparison
```

## Topics Published

### Weather Data (Every 5 minutes)
- **weather.iss**: Outdoor weather (ISS - Integrated Sensor Suite)
  - Temperature, humidity, wind speed/direction, rainfall, solar radiation, UV index
- **weather.barometer**: Barometric pressure
  - Absolute pressure, sea level pressure, trend
- **weather.indoor**: Indoor conditions
  - Temperature, humidity, dew point, heat index
- **weather.health**: Console health metrics
  - Battery status, WiFi signal, uptime, memory usage

### Metadata (On changes only)
- **weather.metadata.sensors**: Sensor configuration
- **weather.metadata.catalog**: Sensor type definitions and field schemas
- **weather.metadata.station**: Station information and location

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
POSTGRES_DSN=host=postgres...      # PostgreSQL for cache rehydration
FETCH_INTERVAL=5m                  # How often to fetch data
METADATA_FETCH_INTERVAL=168h       # How often to refresh metadata
LOG_LEVEL=info                     # Logging level (debug, info, warn, error)
```

### Getting WeatherLink Credentials

1. Log in to https://www.weatherlink.com/account
2. Navigate to API Token section
3. Generate v2 API token
4. Copy API Key and API Secret
5. Find your Station ID in the Devices section

## Running Locally

### With Docker Compose (Recommended)

```bash
# From project root
docker compose up weatherlink-kafka
```

### Standalone with Go

```bash
cd services/weatherlink-kafka

# Install dependencies
go mod download

# Set environment variables
export WEATHERLINK_API_KEY=your_key
export WEATHERLINK_API_SECRET=your_secret
export WEATHERLINK_STATION_ID=your_station_id
export KAFKA_BROKER=localhost:9092
export POSTGRES_DSN="host=localhost port=5432 user=roach password=yourpass dbname=roach sslmode=disable"

# Run
go run main.go
```

## Development

### Project Structure

```
services/weatherlink-kafka/
├── main.go              # Entry point
├── go.mod               # Go module
├── go.sum               # Dependency checksums
├── Dockerfile           # Container build
├── .env.example         # Environment template
├── README.md            # This file
├── api/                 # WeatherLink API client
│   ├── client.go        # HTTP client wrapper
│   └── weatherlink.go   # API endpoints
├── config/              # Configuration management
│   └── config.go
├── kafka/               # Kafka producer utilities
│   ├── producer.go      # Idempotent producer
│   └── consumer.go      # Scanner utilities
├── models/              # Data models and types
│   └── types.go
├── util/                # Hash utilities
│   └── hash.go
└── service/             # Core service logic
    ├── cache.go         # Deduplication cache with PostgreSQL rehydration
    ├── conditions.go    # Current conditions fetching
    ├── metadata.go      # Metadata management
    └── service.go       # Main service implementation
```

### Dependencies

- `github.com/confluentinc/confluent-kafka-go/v2` - Kafka client (idempotent producer)
- `github.com/lib/pq` - PostgreSQL driver

### Building

```bash
# Build binary
go build -o weatherlink-kafka main.go

# Build Docker image
docker build -t roach-weatherlink-kafka .
```

## How It Works

### Startup Sequence

1. Load configuration from environment
2. Connect to Kafka broker
3. Connect to PostgreSQL (for cache rehydration)
4. Rehydrate timestamp cache from database (last 24 hours)
5. Fetch sensor metadata from WeatherLink API
6. Fetch sensor catalog (field definitions)
7. Fetch station information
8. Publish metadata to respective topics (if changed)
9. Build sensor routing map (LSID → topic)
10. Start periodic data fetch loop

### Data Collection Loop

Every 5 minutes (configurable):

1. Fetch current conditions from WeatherLink API
2. Parse response containing multiple sensors
3. For each sensor:
   - Look up sensor metadata (category, product name)
   - Determine target topic based on category
   - For each data point in sensor.data array:
     - Check timestamp cache for duplicates
     - Extract metadata and create headers
     - Publish message to topic with headers

### Metadata Updates

Daily at midnight:

1. Re-fetch all metadata from WeatherLink API
2. Calculate hash of each metadata type
3. Compare with previous hash
4. Only publish to Kafka if changed
5. Update in-memory cache

This ensures metadata topics only contain meaningful change events, not duplicate data.

### Deduplication

**Kafka key cache (primary)**:
- Scans weather topics on startup to build an in-memory set of existing keys (`lsid:timestamp`)
- Skips publishing if the key is already present in Kafka
- Adds new keys to the cache after successful publish

**Timestamp-based cache (secondary)**:
- In-memory cache: `map[LSID]map[data_structure_type]timestamp`
- Rehydrated from PostgreSQL on startup (last 24 hours)
- Prevents duplicate messages within the current runtime window

## Message Format

### Weather Data Message

**Headers** (optimized January 2026):
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
# Check logs
docker compose logs weatherlink-kafka

# Common issues:
# 1. Missing API credentials
# 2. Kafka not ready (wait 30s after starting kafka)
# 3. Invalid station ID
```

### No Data Published

1. Verify API credentials are correct
2. Check station ID matches your WeatherLink account
3. Ensure Kafka is reachable at configured broker address
4. Look for errors in logs: `docker compose logs -f weatherlink-kafka`

### API Errors

- **401 Unauthorized**: Invalid API key or secret
- **404 Not Found**: Invalid station ID
- **429 Too Many Requests**: Rate limited (reduce fetch interval)

### Connection Issues

```bash
# Test Kafka connection
docker compose exec weatherlink-kafka nc -zv kafka 29092

# Test PostgreSQL connection
docker compose exec weatherlink-kafka nc -zv postgres 5432

# Test WeatherLink API
curl "https://api.weatherlink.com/v2/version?api-key=YOUR_KEY"
```

## Monitoring

### Check Service Health

```bash
# View logs in real-time
docker compose logs -f weatherlink-kafka

# Check if service is running
docker compose ps weatherlink-kafka

# Restart service
docker compose restart weatherlink-kafka
```

### Verify Data Flow

1. Open Kafka UI: http://localhost:8080
2. Navigate to topics (weather.iss, weather.barometer, etc.)
3. View recent messages
4. Check message headers for metadata

### Expected Log Output

```
Starting ROACH Weather Service...
Configuration loaded:
  - Station ID: 228773
  - Kafka Broker: kafka:29092
  - Fetch Interval: 5m
Rehydrating cache from PostgreSQL...
Fetching sensor metadata...
Published metadata for 4 sensors
Fetching sensor catalog...
Published sensor catalog (filtered by active sensor types)
Fetching station info...
Published station info
Fetching current conditions...
Published 4 sensor readings, skipped 0 duplicates
```

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

WeatherLink API rate limits: ~300 requests per hour per station.

## Performance

- **CPU**: Minimal (~1-2% average)
- **Memory**: ~20-50 MB
- **Network**: ~50-100 KB per fetch (varies by station)
- **Kafka Messages**: 4-10 messages per fetch (depends on sensor count)
- **Storage**: ~110 MB/year with LZ4 compression

## Security

- API credentials stored in environment variables (never in code)
- HTTPS for all WeatherLink API calls
- Kafka communication over Docker network (plaintext internally)
- External Kafka access via SSL/TLS

## Related Services

- **weatherlink-materialize-to-sql**: Real-time Kafka→PostgreSQL materialization
- **weatherlink-backfill-to-kafka**: Historical API→Kafka backfill
- **weatherlink-backfill-to-sql**: Kafka→PostgreSQL backfill

## References

- [WeatherLink v2 API Documentation](https://weatherlink.github.io/v2-api/)
- [Confluent Kafka Go Client](https://github.com/confluentinc/confluent-kafka-go)
- [Davis Instruments](https://www.davisinstruments.com/)
