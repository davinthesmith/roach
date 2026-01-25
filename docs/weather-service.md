# Weather Service

## Overview

Go-based service that fetches weather data from WeatherLink v2 API and publishes to Kafka.

## Implementation

**Language**: Go 1.21+
**Location**: `services/weather/`
**Entry Point**: `main.go` (~85 lines)
**Architecture**: Modular package structure with clear separation of concerns

> **See [go-standards.md](go-standards.md) for detailed Go code organization standards used in this service.**

### Package Structure

```
weather/
├── main.go              # Entry point, dependency wiring
├── config/              # Configuration loading
├── models/              # API response structures
├── api/                 # WeatherLink API client
├── kafka/               # Kafka producer
├── service/             # Business logic
└── internal/            # Internal utilities
```

**Packages**:
- `config/` - Environment variable parsing and validation
- `models/` - Data structures for API responses (CurrentConditionsResponse, SensorMetadata, etc.)
- `api/` - WeatherLink API client with authentication (HMAC-SHA256)
- `kafka/` - Kafka message publishing
- `service/` - Core business logic (fetching, caching, metadata management)
- `internal/` - Internal utilities (hash calculation)
- `main.go` - Minimal entry point that wires dependencies and handles graceful shutdown

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

The service follows clean architecture principles:

```
main.go
 └─ service.Start(ctx)
     ├─ fetchSensorMetadata()        // service/metadata.go
     ├─ fetchSensorCatalog()          // service/metadata.go
     ├─ fetchStationInfo()            // service/metadata.go
     ├─ metadataUpdateLoop()          // Background goroutine
     └─ fetchCurrentConditions()      // service/conditions.go
         └─ producer.Publish()        // kafka/producer.go
```

### Key Components

#### API Client (`api/`)
- `client.go` - HTTP client wrapper with timeout configuration
- `auth.go` - HMAC-SHA256 signature generation for authenticated requests
- `weatherlink.go` - Type-safe API endpoint methods
  - `FetchCurrentConditions()` → `CurrentConditionsResponse`
  - `FetchSensorMetadata()` → `SensorsResponse`
  - `FetchSensorCatalog()` → `SensorCatalogResponse`
  - `FetchStationInfo()` → `StationResponse`

#### Service Layer (`service/`)
- `service.go` - Service orchestration and main loop
- `metadata.go` - Metadata fetching with hash-based change detection
- `conditions.go` - Current conditions fetching and publishing
- `cache.go` - Timestamp deduplication cache with PostgreSQL rehydration

#### Kafka Producer (`kafka/`)
- `producer.go` - Generic message publishing with headers
- Handles JSON marshalling and header conversion
- Synchronous publishing for reliability

#### Models (`models/`)
- Type-safe structs for all API responses
- JSON tags for automatic marshalling/unmarshalling
- Clear naming conventions matching API documentation

### Adding New Endpoints

1. Add API method to `api/weatherlink.go`:
```go
func (c *Client) FetchNewData() (*models.NewDataResponse, error) {
    url := fmt.Sprintf("https://api.weatherlink.com/v2/new-endpoint?api-key=%s", c.apiKey)
    body, err := c.makeRequest(url)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch new data: %w", err)
    }
    
    var response models.NewDataResponse
    if err := json.Unmarshal(body, &response); err != nil {
        return nil, fmt.Errorf("failed to parse new data: %w", err)
    }
    
    return &response, nil
}
```

2. Add service method to `service/metadata.go` or new file:
```go
func (s *Service) fetchNewData(ctx context.Context) error {
    response, err := s.apiClient.FetchNewData()
    if err != nil {
        return err
    }
    
    // Add hash-based change detection if needed
    hash := internal.CalculateHash(body)
    if s.lastMetadataHash["new-data"] == hash {
        log.Println("New data unchanged, skipping publish")
        return nil
    }
    
    // Publish to Kafka
    if err := s.producer.Publish(ctx, "weather.new-topic", response, headers); err != nil {
        return err
    }
    
    s.lastMetadataHash["new-data"] = hash
    return nil
}
```

3. Call from `service.Start()` or add to metadata update loop

### Design Patterns

#### Dependency Injection
All dependencies passed via constructors:
```go
svc := service.New(cfg, apiClient, producer, db)
```

#### Error Wrapping
Errors wrapped with context at each layer:
```go
return fmt.Errorf("failed to fetch sensor metadata: %w", err)
```

#### Context Propagation
All operations accept context for cancellation:
```go
func (s *Service) Start(ctx context.Context) error
```

#### Deduplication Strategy
Two-level deduplication:
1. In-memory timestamp cache (last 5 minutes in memory)
2. PostgreSQL rehydration on startup (last 24 hours)

### Testing

#### Package Testing
Each package can be tested independently:
```bash
cd services/weather
go test ./config
go test ./api
go test ./kafka
go test ./service
```

#### Mock Interfaces (Future)
Interfaces can be extracted for testing:
```go
type APIClient interface {
    FetchCurrentConditions() (*models.CurrentConditionsResponse, error)
}
```

## Testing

### Unit Tests
Test individual packages:
```bash
cd services/weather
go test ./config -v
go test ./api -v
go test ./service -v
go test ./kafka -v
```

### Integration Test
Full service test with infrastructure:
```bash
# Start infrastructure
./scripts/start-infra.sh

# Run service with test credentials
cd services/weather
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

---

# Weather SQL Service

## Overview

Go-based service that materializes Kafka messages to PostgreSQL with automatic tag creation and metadata enrichment.

## Implementation

**Language**: Go 1.21+
**Location**: `services/weather-sql/`
**Entry Point**: `main.go` (~75 lines)
**Architecture**: Modular package structure with repository pattern

> **See [go-standards.md](go-standards.md) for detailed Go code organization standards used in this service.**

### Package Structure

```
weather-sql/
├── main.go              # Entry point, dependency wiring
├── config/              # Configuration loading
├── models/              # Data structures
├── cache/               # In-memory caching
├── repository/          # Database operations
├── service/             # Business logic
└── kafka/               # Kafka consumers
```

**Packages**:
- `config/` - Environment variable parsing
- `models/` - Data structures (Device, Tag, FieldMetadata)
- `cache/` - Thread-safe in-memory caching for devices, tags, and catalog metadata
- `repository/` - Database CRUD operations with SQL queries
- `service/` - Business logic processors (metadata, catalog, data, enrichment)
- `kafka/` - Kafka reader creation utilities
- `main.go` - Minimal entry point that wires dependencies

### Architecture

```
Materializer (service/materializer.go)
├── MetadataProcessor    # Listens to weather.metadata.sensors
│   └── Upserts devices to DB and cache
├── CatalogProcessor     # Listens to weather.metadata.catalog
│   └── Upserts catalog to DB and cache
├── DataProcessor        # Listens to weather.* data topics
│   ├── Creates tags on-the-fly with catalog enrichment
│   └── Inserts records to typed tables
└── Enricher             # Backfills existing tags
    └── Queries tags missing metadata and enriches from catalog
```

### Key Components

#### Repository Layer (`repository/`)
- `devices.go` - Device CRUD operations
- `tags.go` - Tag CRUD and enrichment queries
- `catalog.go` - Catalog storage and retrieval
- `records.go` - Record insertion (numeric, text, null)
- `orphans.go` - Orphaned message tracking

#### Service Layer (`service/`)
- `materializer.go` - Service orchestration, lifecycle management
- `metadata.go` - Metadata processor (processes device metadata)
- `catalog.go` - Catalog processor (processes sensor catalog)
- `data.go` - Data processor (creates tags, inserts records)
- `enrichment.go` - Tag enricher (backfills metadata)

#### Cache Layer (`cache/`)
- Thread-safe in-memory cache using `sync.RWMutex`
- Caches devices by LSID
- Caches tags by (device_id, tag_name)
- Caches catalog metadata by (sensor_type, data_structure_type, field_name)

### Functionality

#### Metadata Processing
Listens to `weather.metadata.sensors`:
1. Parses sensor metadata JSON
2. Upserts device to database
3. Updates device cache
4. Logs device update

#### Catalog Processing
Listens to `weather.metadata.catalog`:
1. Parses sensor catalog JSON
2. Iterates through sensor types and data structures
3. Upserts field metadata to `sensor_catalog` table
4. Updates catalog cache
5. Triggers tag enrichment

#### Data Processing
Listens to `weather.*` data topics:
1. Extracts LSID and metadata from message headers
2. Looks up device in cache
3. Parses message body (JSON with field → value)
4. For each field:
   - Looks up tag in cache
   - If tag missing, creates tag with catalog enrichment
   - Determines data type (numeric, text, null)
   - Inserts record to appropriate typed table
5. If device not found, saves to `orphaned_messages`

#### Tag Enrichment
After catalog updates:
1. Queries tags missing unit/description/metadata
2. Joins with devices to get sensor_type and data_structure_type
3. Looks up field metadata from catalog cache
4. Updates tags with enriched metadata
5. Updates tag cache

### Configuration

Environment variables:
```bash
# Required
POSTGRES_DSN=host=postgres port=5432 user=roach password=xxx dbname=roach sslmode=disable

# Optional
KAFKA_BROKER=kafka:29092   # Default: kafka:29092
BATCH_SIZE=100              # Default: 100
LOG_LEVEL=info              # Default: info
```

### Building

```bash
# Docker build (recommended)
docker compose build weather-sql

# Local build
cd services/weather-sql
go build -o weather-sql .
```

### Design Patterns

#### Repository Pattern
All database operations encapsulated in repository layer:
```go
deviceRepo := repository.NewDeviceRepository(db)
devices, err := deviceRepo.LoadAll(ctx)
```

#### Processor Pattern
Each message type has dedicated processor:
- MetadataProcessor for device metadata
- CatalogProcessor for sensor catalog
- DataProcessor for time-series data
- Enricher for tag backfilling

#### Cache-Aside Pattern
1. Check cache first
2. If miss, query database
3. Update cache
4. Return result

### Error Handling

#### Orphaned Messages
Messages that can't be processed are saved to `orphaned_messages`:
- Missing device → `reason: missing_device`
- Failed tag creation → `reason: failed_to_create_tag`
- Can be reprocessed after fixing issues using `scripts/db/reload-orphans.sh`

#### Graceful Degradation
- If cache load fails, service continues (cache starts empty)
- If enrichment fails, service continues (tags lack metadata temporarily)
- All errors logged but don't stop message processing

### Performance

**Resource Usage**:
- CPU: 1-3% average
- Memory: 50-100 MB
- Cache size: ~1-10 MB (devices + tags + catalog)

**Optimizations**:
- In-memory caching reduces DB queries
- Batch inserts for records (future enhancement)
- Typed record tables for efficient storage
- Indexes on (tag_id, timestamp) for fast queries

## Testing

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
