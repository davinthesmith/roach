# AI Context - ROACH System

**Purpose**: Single-file context covering 80% of what an AI agent needs to understand and work with ROACH.

## System Overview

ROACH (Real-time Observability Aggregation Conduit for the Home) is a Kafka-based data aggregation system for home IoT devices with infinite data persistence and PostgreSQL materialization.

**Key Features**:
- Infinite data retention in Kafka
- PostgreSQL materialization with Device/Tag/Record hierarchy
- Rich metadata capture with units and descriptions
- Database migration framework
- Timestamp-based deduplication
- Change detection for metadata
- Real-time streaming and SQL storage

**Current Implementation**: WeatherLink weather station integration with 7 Kafka topics, 2 Go services, optimized for storage efficiency (70% reduction via compression and header optimization)

**Technology Stack**: Docker Compose, Kafka 7.5.0, PostgreSQL 16, Zookeeper, Go 1.21+

## Quick Start

```bash
# Configure credentials
cp .env.example .env
vim .env  # Add WEATHERLINK_API_KEY, WEATHERLINK_API_SECRET, WEATHERLINK_STATION_ID, POSTGRES_PASSWORD

# Start system
./scripts/start-all.sh          # Normal start
./scripts/start-all.sh build    # With rebuild (after code changes)

# Monitor
./scripts/status.sh             # Check health
./scripts/logs.sh weather-publish  # View service logs
docker ps                       # Check containers

# Stop
./scripts/stop-all.sh

# Access
# Kafka UI: http://localhost:8080
# PostgreSQL: localhost:5432 (user: roach, db: roach)
```

**Network Endpoints**:
- Kafka (internal): `kafka:29092`
- Kafka (external): `localhost:9092`
- PostgreSQL: `postgres:5432` (internal), `localhost:5432` (external)
- Kafka UI: `http://localhost:8080`

## Architecture Essentials

### Infrastructure Layer

**Zookeeper** (`roach-zookeeper`)
- Port: 2181
- Purpose: Kafka cluster coordination
- Data: `./data/zookeeper`

**Kafka Broker** (`roach-kafka`)
- Ports: 9092 (external), 29092 (internal)
- Retention: Infinite (`-1`)
- Data: `./data/kafka`
- Auto-create topics: Enabled
- Network: `roach-network`

**PostgreSQL** (`roach-postgres`)
- Port: 5432
- Database: `roach`, User: `roach`
- Data: `./data/postgres`
- Schema: Auto-initialized from `scripts/db/init/01-schema.sql`

**Kafka UI** (`roach-kafka-ui`)
- Port: 8080
- Purpose: Web-based Kafka monitoring

### Application Layer

**weather-publish** (`roach-weather-publish`)
- Language: Go
- Purpose: Fetch WeatherLink data, publish to Kafka with deduplication
- Interval: 5 minutes (configurable via `FETCH_INTERVAL`)
- Topics Published: 7 (4 data, 3 metadata)
- Deduplication: Timestamp-based cache with PostgreSQL rehydration

**weather-sql** (`roach-weather-sql`)
- Language: Go
- Purpose: Materialize Kafka messages to PostgreSQL
- Consumers: All `weather.*` topics
- Features: Auto-tag creation, metadata enrichment, orphaned message tracking

### Data Flow

```
WeatherLink API (HTTPS)
    ↓ Every 5 minutes
Weather Publisher (Go) - Deduplication
    ↓ Publish JSON messages
Kafka Broker (Infinite retention)
    ↓ Stream to consumers
Weather SQL (Go) - Materialization
    ↓ Insert records
PostgreSQL Database
    ↓ Query layer
Devices → Tags → Records
```

### Network Architecture

```
roach-network (Docker bridge)
├── Infrastructure
│   ├── zookeeper:2181
│   ├── kafka:29092 (internal), localhost:9092 (external)
│   ├── postgres:5432 (internal), localhost:5432 (external)
│   └── kafka-ui:8080
└── Applications
    ├── weather-publish (connects to kafka:29092, postgres:5432)
    └── weather-sql (connects to kafka:29092, postgres:5432)
```

## Configuration

### Required Environment Variables

In `.env` file:
```bash
# WeatherLink API (Required)
WEATHERLINK_API_KEY=<your_api_key>
WEATHERLINK_API_SECRET=<your_api_secret>
WEATHERLINK_STATION_ID=<your_station_id>

# PostgreSQL (Required)
POSTGRES_PASSWORD=<secure_password>
```

### Optional Environment Variables

**weather-publish**:
```bash
KAFKA_BROKER=kafka:29092        # Default
POSTGRES_DSN=host=postgres...   # Default provided
FETCH_INTERVAL=5m               # 5 minutes default (Go duration format)
LOG_LEVEL=info                  # debug|info|warn|error
```

**weather-sql**:
```bash
KAFKA_BROKER=kafka:29092        # Default
POSTGRES_DSN=host=postgres...   # Default provided
LOG_LEVEL=info                  # debug|info|warn|error
BATCH_SIZE=100                  # Default
```

### File Locations

- Credentials: `.env` (project root)
- Infrastructure config: `docker-compose.infrastructure.yml`
- Services config: `docker-compose.yml`
- Data: `./data/kafka`, `./data/zookeeper`, `./data/postgres`
- Scripts: `./scripts/*.sh`
- Documentation: `./docs/*.md`
- Services: `./services/weather-publish/`, `./services/weather-sql/`

### Common Customizations

**Change fetch interval**:
```yaml
# docker-compose.yml
services:
  weather-publish:
    environment:
      - FETCH_INTERVAL=10m  # Change from 5m to 10m
```

**Limit data retention** (not recommended, default is infinite):
```yaml
# docker-compose.infrastructure.yml
kafka:
  environment:
    KAFKA_LOG_RETENTION_MS: 2592000000  # 30 days
```

## Services Overview

### weather-publish Service

**Purpose**: Fetch weather data from WeatherLink v2 API and publish to Kafka

**Package Structure**:
```
weather-publish/
├── main.go              # Entry point (~85 lines)
├── config/              # Environment variable parsing
│   └── config.go
├── models/              # API response structures
│   └── types.go
├── api/                 # WeatherLink API client
│   ├── client.go        # HTTP client wrapper
│   ├── auth.go          # HMAC-SHA256 authentication
│   └── weatherlink.go   # API endpoint methods
├── kafka/               # Kafka producer
│   └── producer.go
├── service/             # Business logic
│   ├── service.go       # Orchestration
│   ├── metadata.go      # Metadata fetching
│   ├── conditions.go    # Current conditions
│   └── cache.go         # Timestamp cache
└── internal/            # Internal utilities
    └── hash.go          # SHA-256 hashing
```

**Key Operations**:
1. Fetch sensor metadata (on change via hash comparison)
2. Fetch sensor catalog (on change via hash comparison)
3. Fetch station info (on change via hash comparison)
4. Fetch current conditions (every 5 minutes)
5. Deduplicate via timestamp cache
6. Publish to Kafka topics

**Dependencies**: `github.com/segmentio/kafka-go`, `github.com/lib/pq`

### weather-sql Service

**Purpose**: Materialize Kafka messages to PostgreSQL with automatic tag creation

**Package Structure**:
```
weather-sql/
├── main.go              # Entry point (~75 lines)
├── config/              # Configuration
│   └── config.go
├── models/              # Data structures
│   └── types.go         # Device, Tag, FieldMetadata
├── cache/               # In-memory caching
│   └── cache.go         # Thread-safe cache
├── repository/          # Database operations
│   ├── devices.go       # Device CRUD
│   ├── tags.go          # Tag CRUD and enrichment
│   ├── catalog.go       # Catalog storage
│   ├── records.go       # Record insertion
│   └── orphans.go       # Orphaned message tracking
├── service/             # Business logic
│   ├── materializer.go  # Service orchestration
│   ├── metadata.go      # Metadata processor
│   ├── catalog.go       # Catalog processor
│   ├── data.go          # Data processor
│   └── enrichment.go    # Tag enrichment
└── kafka/               # Kafka consumers
    └── consumer.go
```

**Key Operations**:
1. Consume `weather.metadata.sensors` → Upsert devices
2. Consume `weather.metadata.catalog` → Upsert catalog metadata
3. Consume `weather.*` data topics → Create tags + insert records
4. Enrich tags with units/descriptions from catalog
5. Track orphaned messages (missing device)

**Dependencies**: `github.com/segmentio/kafka-go`, `github.com/lib/pq`

## Kafka Topics

### Naming Convention
Format: `namespace.category[.subcategory]`

### Data Topics (Published every 5 minutes)

**weather.iss** - Outdoor weather (Integrated Sensor Suite)
- Key fields: `temp`, `hum`, `wind_speed_last`, `wind_dir_last`, `rain_rate_last`, `solar_rad`, `uv_index`, `dew_point`, `heat_index`

**weather.barometer** - Barometric pressure
- Key fields: `bar_sea_level`, `bar_absolute`, `bar_trend`

**weather.indoor** - Indoor conditions
- Key fields: `temp_in`, `hum_in`, `dew_point_in`, `heat_index_in`

**weather.health** - Console health metrics
- Key fields: `battery_voltage`, `wifi_rssi`, `uptime`, `firmware_version`, `free_mem`

### Metadata Topics (Published on change only)

**weather.metadata.sensors** - Sensor configuration (LSIDs, sensor details)
**weather.metadata.catalog** - Sensor type catalog (field schemas, units, descriptions)
**weather.metadata.station** - Station information (name, location, timezone)

### Message Structure

**Headers** (every message - optimized January 2026):
- `schema_version` - Schema version (e.g., "1")
- `lsid` - Logical Sensor ID
- `timestamp` - Unix timestamp (seconds)
- `sensor_type` - Sensor type code
- `data_structure_type` - Data structure type

**Note**: Removed redundant headers (`station_id`, `station_id_uuid`, `category`, `product_name`) for storage optimization. These are available via metadata lookup. See [kafka-standards.md](docs/kafka-standards.md).

**Body** (JSON):
```json
{
  "lsid": 555566,
  "data_structure_type": 1,
  "temp": 62.3,
  "hum": 55.2,
  "wind_speed_last": 5.0,
  "ts": 1706140800
}
```

## Database Schema

### Core Tables

**devices** - Sensor registry with full metadata
- Primary key: `id` (serial)
- Unique: `lsid` (Logical Sensor ID)
- Core fields: `sensor_type`, `category`, `manufacturer`, `product_name`
- Extended: `product_number`, `rain_collector_type`, `active`, `tx_id`, `port_number`
- Parent device: `parent_device_type`, `parent_device_name`, `parent_device_id`, `parent_device_id_hex`
- Location: `station_id`, `station_name`, `latitude`, `longitude`, `elevation`
- Timestamps: `created_date`, `modified_date` (from API), `created_at`, `updated_at` (database)
- Metadata: `metadata` (JSONB)
- Real-time: `rt_data_structure_type` (populated from current data messages, not sensors metadata)

**tags** - Field definitions with units and descriptions
- Primary key: `id` (serial)
- Foreign key: `device_id` → `devices(id)`
- Unique: `(device_id, tag_name)`
- Fields: `tag_name`, `data_type`, `unit`, `description`, `metadata` (JSONB)

**sensor_catalog** - Field metadata from WeatherLink API
- Unique: `(sensor_type, data_structure_type, field_name)`
- Fields: `field_type`, `units`, `description`
- Purpose: Source of truth for tag enrichment

**records_numeric** - Numeric time-series data
- Fields: `tag_id`, `device_id`, `value` (numeric), `timestamp`
- Index: `(tag_id, timestamp)`

**records_text** - Text time-series data
- Fields: `tag_id`, `device_id`, `value` (text), `timestamp`

**records_null** - Null value tracking
- Fields: `tag_id`, `device_id`, `timestamp`

**records** (view) - Unified query interface over all record types

**orphaned_messages** - Messages that couldn't be processed
- Fields: `topic`, `partition`, `offset`, `reason`, `headers`, `body`, `created_at`

**schema_migrations** - Migration tracking
- Fields: `version`, `name`, `applied_at`, `checksum`

### Relationships

```
devices (1) → (N) tags → (N) records_numeric
                      → (N) records_text
                      → (N) records_null
                      
sensor_catalog → enriches → tags (unit, description)
```

## Common Operations

### System Management

```bash
# Start/Stop
./scripts/start-all.sh          # Start all
./scripts/start-all.sh build    # Start with rebuild
./scripts/start-infra.sh        # Infrastructure only
./scripts/stop-all.sh           # Stop all

# Restart
./scripts/restart.sh                # Restart all
./scripts/restart.sh weather-publish  # Restart specific service

# Status
./scripts/status.sh             # System status
docker ps                       # Container list
docker stats                    # Resource usage
```

### Logs and Monitoring

```bash
# View logs
./scripts/logs.sh                    # All services
./scripts/logs.sh weather-publish    # Specific service
docker logs -f roach-kafka           # Follow logs

# Kafka UI
# Open http://localhost:8080 for web interface
```

### Database Operations

```bash
# Query database
./scripts/db/query.sh stats          # Statistics
./scripts/db/query.sh devices        # List devices
./scripts/db/query.sh tags 918290    # Tags for device
./scripts/db/query.sh recent         # Recent records
./scripts/db/query.sh orphans        # Orphaned messages
./scripts/db/query.sh psql           # Interactive psql

# Migrations
./scripts/db/migrate.sh status       # Show migration status
./scripts/db/migrate.sh up           # Apply pending migrations
./scripts/db/migrate.sh down         # Rollback last migration
./scripts/db/migrate.sh create <name>  # Create new migration

# Reprocess orphans
./scripts/db/reload-orphans.sh       # Interactive reprocessing
```

### Kafka Operations

```bash
# List topics
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# View messages
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --from-beginning \
  --max-messages 10

# With headers
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --property print.headers=true \
  --from-beginning
```

### Service Updates

```bash
# After code changes
./scripts/start-all.sh build         # Rebuild all

# Or rebuild specific service
docker compose build weather-publish
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d weather-publish
```

## Quick Troubleshooting

### System Won't Start
**Check**: Docker running, ports available
```bash
docker ps
lsof -i :9092  # Kafka
lsof -i :5432  # PostgreSQL
lsof -i :8080  # Kafka UI
```

### Connection Refused
**Cause**: Kafka not ready (takes 30-60s)
**Fix**: Wait for health check
```bash
docker ps  # Look for "(healthy)" status
./scripts/restart.sh weather-publish
```

### No Data Publishing
**Check**: API credentials, service logs
```bash
docker exec roach-weather-publish env | grep WEATHERLINK
./scripts/logs.sh weather-publish
```

### Kafka UI Not Loading
**Check**: Kafka healthy, port not in use
```bash
docker logs roach-kafka-ui
lsof -i :8080
```

### Empty Tag Units/Descriptions
**Cause**: Catalog message issues (fixed in latest version)
**Check**: Catalog size and enrichment
```bash
./scripts/db/query.sh psql
SELECT COUNT(*) FROM tags WHERE unit IS NOT NULL;
SELECT COUNT(*) FROM sensor_catalog;
```

### Clean Restart
**When**: System in bad state, need fresh start
```bash
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
rm -rf data/
./scripts/start-all.sh
```

**For more troubleshooting**: See [troubleshooting.md](troubleshooting.md)

## Code Organization

### Go Standards Summary

All Go services follow clean architecture with:
- **Minimal main.go** (<100 lines): Dependency wiring only
- **config/** package: Environment variable parsing
- **models/** package: Data structures (no logic)
- **service/** package: Business logic orchestration
- **repository/** package: Database operations (if applicable)
- **cache/** package: In-memory caching (if applicable)
- **kafka/** or **api/** packages: External integrations

### Design Principles

**Dependency Injection**: All dependencies via constructors
```go
svc := service.New(cfg, apiClient, producer, db)
```

**Context Propagation**: All long-running operations accept `context.Context`
```go
func (s *Service) Start(ctx context.Context) error
```

**Error Wrapping**: Errors wrapped with context at each layer
```go
return fmt.Errorf("failed to fetch data: %w", err)
```

**No Package-Level State**: Avoid global variables, use structs with methods

**Interface Segregation**: Small, focused interfaces where needed

**For detailed standards**: See [go-standards.md](go-standards.md)

### Package Dependencies

Both services follow this dependency flow:
```
main → config, service
service → repository, cache, models
repository → models
cache → models
```

No circular dependencies allowed.

## Extension Guide

### Adding New Services

1. **Create service directory**:
```bash
mkdir -p services/new-service
cd services/new-service
```

2. **Follow Go standards**: See [go-standards.md](go-standards.md)
   - Create `main.go`, `config/`, `models/`, `service/` packages
   - Use dependency injection
   - Keep main.go minimal

3. **Add to docker-compose.yml**:
```yaml
services:
  new-service:
    build: ./services/new-service
    container_name: roach-new-service
    environment:
      - KAFKA_BROKER=kafka:29092
      - POSTGRES_DSN=host=postgres port=5432 user=roach password=${POSTGRES_PASSWORD} dbname=roach sslmode=disable
      - LOG_LEVEL=info
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - roach-network
    restart: unless-stopped
```

4. **Use topic naming convention**: `namespace.category.subcategory`

### Topic Patterns

**Home Automation**:
- `home.hvac.temperature`
- `home.hvac.humidity`
- `home.lighting.state`

**Security**:
- `home.security.motion`
- `home.security.door`
- `home.security.camera`

**Energy**:
- `home.energy.consumption`
- `home.energy.solar`
- `home.energy.battery`

### Message Guidelines

**Include headers**:
```go
msg := kafka.Message{
    Key:   []byte("device-id"),
    Value: []byte(jsonData),
    Headers: []kafka.Header{
        {Key: "device_id", Value: []byte("sensor-01")},
        {Key: "timestamp", Value: []byte("1706140800")},
        {Key: "location", Value: []byte("living-room")},
    },
}
```

**Use JSON for body**: Structured, self-describing

**Timestamp in headers**: Unix seconds for consistency

## Additional Resources

- **[architecture.md](architecture.md)** - Detailed component specs, resource usage
- **[operations.md](operations.md)** - Advanced operations, maintenance
- **[troubleshooting.md](troubleshooting.md)** - Comprehensive problem solving
- **[go-standards.md](go-standards.md)** - Complete code organization standards
- **[kafka-standards.md](kafka-standards.md)** - Kafka best practices, storage optimization
- **[kafka-topics.md](kafka-topics.md)** - Full topic schemas and field definitions
- **[migrations.md](migrations.md)** - Database migration framework details
- **[../README.md](../README.md)** - Project README
- **[../CHANGELOG.md](../CHANGELOG.md)** - Version history
