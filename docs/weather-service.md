# Weather Service

## Overview

Go-based service that fetches weather data from WeatherLink v2 API and publishes to Kafka.

## Implementation

**Language**: Go 1.21+
**Location**: `services/weather/`
**Entry Point**: `main.go`

## Functionality

### Data Fetching
- **Interval**: 5 minutes (configurable via `FETCH_INTERVAL`)
- **API**: WeatherLink v2 REST API
- **Authentication**: HMAC-SHA256 signed requests

### Data Publishing
Publishes to 7 Kafka topics:
- 4 data topics (every interval)
- 3 metadata topics (on change only)

See [kafka-topics.md](kafka-topics.md) for topic details.

### Change Detection
Metadata topics use SHA-256 hash comparison:
1. Fetch metadata from API
2. Calculate hash of response
3. Compare with last known hash
4. Publish only if changed
5. Store new hash

## Configuration

### Environment Variables

```bash
# Required
WEATHERLINK_API_KEY=<api_key>
WEATHERLINK_API_SECRET=<api_secret>
WEATHERLINK_STATION_ID=<station_id>

# Optional
KAFKA_BROKER=kafka:29092   # Default: kafka:29092
FETCH_INTERVAL=5m           # Default: 5m (Go duration format)
LOG_LEVEL=info              # Default: info (debug|info|warn|error)
```

### Getting Credentials

1. Visit https://www.weatherlink.com/account
2. Navigate to API Tokens
3. Generate v2 API token
4. Copy API Key and Secret
5. Find Station ID in Devices section

## Building

### Docker Build
```bash
# From project root
docker compose build weather

# With no cache
docker compose build --no-cache weather
```

### Local Build
```bash
cd services/weather
go mod download
go build -o weather-service main.go
```

## Running

### In Docker (Recommended)
```bash
# From project root
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up weather
```

### Standalone
```bash
cd services/weather
export WEATHERLINK_API_KEY=your_key
export WEATHERLINK_API_SECRET=your_secret
export WEATHERLINK_STATION_ID=your_station
export KAFKA_BROKER=localhost:9092
go run main.go
```

## Dependencies

```go
github.com/segmentio/kafka-go  // Kafka client
```

See `go.mod` for complete dependency list.

## Logging

### Log Levels
- `debug` - All API requests/responses, Kafka operations
- `info` - Startup, fetch cycles, publish confirmations
- `warn` - Recoverable errors, API throttling
- `error` - Fatal errors, connection failures

### Log Output
```
2026/01/25 00:00:00 Starting ROACH Weather Service...
2026/01/25 00:00:00 Configuration loaded:
2026/01/25 00:00:00   - Station ID: 228773
2026/01/25 00:00:00   - Kafka Broker: kafka:29092
2026/01/25 00:00:00   - Fetch Interval: 5m0s
2026/01/25 00:00:00 Fetching sensor metadata...
2026/01/25 00:00:05 Published metadata for 4 sensors
2026/01/25 00:00:05 Fetching current conditions...
2026/01/25 00:00:10 Published 4 sensor readings
```

## Error Handling

### Retry Logic
- API failures: Log and continue to next cycle
- Kafka failures: Retry 3 times with backoff
- Network errors: Logged, service continues

### Graceful Degradation
- If metadata fetch fails, data publishing continues
- If one sensor fails, others still published
- Service remains running through transient failures

## Performance

### Resource Usage
- **CPU**: <1% average, spikes to 5% during fetch
- **Memory**: 20-50 MB
- **Network**: ~50-100 KB per fetch cycle

### API Rate Limits
WeatherLink API limits:
- 200 requests per day (free tier)
- At 5-minute intervals: 288 requests/day (exceeds limit)
- Solution: Monitor and adjust interval if needed

## Development

### Code Structure
```go
// main.go structure
func main()
    └─ for every FETCH_INTERVAL
        ├─ fetchAndPublishMetadata()
        │   ├─ fetchSensors()
        │   ├─ fetchCatalog()
        │   └─ fetchStation()
        └─ fetchAndPublishCurrent()
            └─ for each sensor
                └─ publishToKafka()
```

### Key Functions
- `fetchFromAPI()` - HTTP client with HMAC auth
- `generateSignature()` - HMAC-SHA256 signing
- `publishToKafka()` - Kafka producer with headers
- `hashData()` - SHA-256 for change detection

### Adding New Endpoints

1. Add fetch function:
```go
func fetchNewData(apiKey, apiSecret, stationID string) ([]byte, error) {
    endpoint := fmt.Sprintf("/v2/new-endpoint/%s", stationID)
    return fetchFromAPI(endpoint, apiKey, apiSecret, nil)
}
```

2. Add publish logic:
```go
func publishNewData(data []byte, broker string) error {
    return publishToKafka(broker, "weather.new-topic", "key", data, headers)
}
```

3. Add to main loop:
```go
// In main()
newData, err := fetchNewData(apiKey, apiSecret, stationID)
if err == nil {
    publishNewData(newData, kafkaBroker)
}
```

## Testing

### Unit Tests
```bash
cd services/weather
go test ./...
```

### Integration Test
```bash
# Start infrastructure
./scripts/start-infra.sh

# Run service with test credentials
export WEATHERLINK_API_KEY=test
export WEATHERLINK_API_SECRET=test
export WEATHERLINK_STATION_ID=test
export KAFKA_BROKER=localhost:9092
go run main.go
```

### API Testing
Use Postman collection:
- `services/weather/postman/WeatherLink v2 API.postman_collection.json`
- `services/weather/postman/WeatherLink v2 API.postman_environment.json`

## Troubleshooting

### Service Won't Start
Check environment variables:
```bash
docker exec roach-weather env | grep WEATHERLINK
```

### No Data Published
Check logs:
```bash
./scripts/logs.sh weather
```

### API Errors
- 401: Invalid credentials
- 429: Rate limited
- 500: WeatherLink API issue

See [troubleshooting.md](troubleshooting.md) for more details.
