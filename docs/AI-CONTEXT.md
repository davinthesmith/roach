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

**Current Implementation**: WeatherLink weather station integration with 7 Kafka topics, 4 Go services (2 real-time, 2 backfill), optimized for storage efficiency (70% reduction via compression and header optimization)

**Technology Stack**: Docker Compose, Kafka 7.5.0, PostgreSQL 16, Zookeeper, Go 1.21+

## Quick Start

```bash
# Configure credentials
cp .env.example .env
vim .env  # Add WEATHERLINK_API_KEY, WEATHERLINK_API_SECRET, WEATHERLINK_STATION_ID, POSTGRES_PASSWORD

# Start system
./scripts/start-all.sh          # Normal start
./scripts/start-all.sh build    # With rebuild (after code changes)
./scripts/start-all.sh clean    # Clean slate (removes all data)

# Monitor
./scripts/status.sh             # Check health
./scripts/logs.sh weatherlink-kafka  # View service logs
./scripts/db/query.sh stats     # Database statistics
docker ps                       # Check containers

# Stop
./scripts/stop-all.sh           # Keep data
./scripts/stop-all.sh clean     # Remove all data

# Access
# Kafka UI: http://localhost:8080
# PostgreSQL: localhost:5432 (user: roach, db: roach)
```

**For complete script documentation:** See [scripts/README.md](../scripts/README.md)

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

**weatherlink-kafka** (`roach-weatherlink-kafka`)
- Language: Go
- Purpose: Real-time data ingestion (API → Kafka)
- Interval: 5 minutes (configurable via `FETCH_INTERVAL`)
- Topics Published: 7 (4 data, 3 metadata)
- Deduplication: Timestamp-based cache with PostgreSQL rehydration

**weatherlink-sql** (`roach-weatherlink-sql`)
- Language: Go
- Purpose: Real-time materialization (Kafka → PostgreSQL)
- Consumers: All `weather.*` topics
- Features: Auto-tag creation, metadata enrichment, orphaned message tracking
- Performance: Batched writes with COPY protocol, worker pool processing

**weatherlink-kafka-backfill** (`roach-weatherlink-kafka-backfill`)
- Language: Go
- Purpose: Historical data backfill (API → Kafka)
- Run Mode: One-shot execution (manual)
- Features: 24-hour windows, rate limiting (8 req/s), client-side deduplication
- Use Case: Populate Kafka with historical API data

**weatherlink-sql-backfill** (`roach-weatherlink-sql-backfill`)
- Language: Go
- Purpose: Database backfill (Kafka → PostgreSQL)
- Run Mode: One-shot execution (manual)
- Features: Configurable offset ranges, worker pool, progress tracking
- Use Case: Materialize historical Kafka data when DB is behind

### Data Flow

**Real-time Pipeline**:
```
WeatherLink API (HTTPS)
    ↓ Every 5 minutes
weatherlink-kafka (Go) - Deduplication
    ↓ Publish JSON messages
Kafka Broker (Infinite retention)
    ↓ Stream to consumers
weatherlink-sql (Go) - Materialization
    ↓ Batched COPY inserts
PostgreSQL Database
    ↓ Query layer
Devices → Tags → Records
```

**Two-Stage Backfill Strategy**:
```
Stage 1: API → Kafka (weatherlink-kafka-backfill)
  - Fetch historical data from WeatherLink API
  - 24-hour windows, rate limited (8 req/s)
  - Client-side deduplication
  - Populates Kafka with missing historical data

Stage 2: Kafka → DB (weatherlink-sql-backfill)
  - Replay messages from Kafka topics
  - Configurable offset ranges (earliest to latest)
  - Separate consumer group
  - Populates PostgreSQL when DB is behind
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
    ├── weatherlink-kafka (connects to kafka:29092, postgres:5432)
    ├── weatherlink-sql (connects to kafka:29092, postgres:5432)
    ├── weatherlink-kafka-backfill (connects to kafka:29092) [manual]
    └── weatherlink-sql-backfill (connects to kafka:29092, postgres:5432) [manual]
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

**weatherlink-kafka**:
```bash
KAFKA_BROKER=kafka:29092        # Default
POSTGRES_DSN=host=postgres...   # Default provided
FETCH_INTERVAL=5m               # 5 minutes default (Go duration format)
LOG_LEVEL=info                  # debug|info|warn|error
```

**weatherlink-sql**:
```bash
KAFKA_BROKER=kafka:29092        # Default
POSTGRES_DSN=host=postgres...   # Default provided
LOG_LEVEL=info                  # debug|info|warn|error
BATCH_SIZE=100                  # Default (real-time)
WORKER_POOL_SIZE=4              # Default
BATCH_FLUSH_INTERVAL_MS=500     # Default
```

**weatherlink-kafka-backfill**:
```bash
KAFKA_BROKER=kafka:29092        # Default
LOG_LEVEL=info                  # debug|info|warn|error
# Plus CLI flags: --start, --end, --workers, --requests-per-second
```

**weatherlink-sql-backfill**:
```bash
KAFKA_BROKER=kafka:29092        # Default
POSTGRES_DSN=host=postgres...   # Default provided
LOG_LEVEL=info                  # debug|info|warn|error
BATCH_SIZE=500                  # Default (backfill-optimized)
WORKER_POOL_SIZE=8              # Default
TOPICS=weather.iss,weather.barometer,weather.indoor,weather.health
START_OFFSET=-2                 # -2=earliest, -1=latest
END_OFFSET=-1                   # -1=latest
# Plus CLI flags: --topics, --start-offset, --end-offset, --workers, --batch-size
```

### File Locations

- Credentials: `.env` (project root)
- Infrastructure config: `docker-compose.infrastructure.yml`
- Services config: `docker-compose.yml`
- Data: `./data/kafka`, `./data/zookeeper`, `./data/postgres`
- Scripts: `./scripts/*.sh`
- Documentation: `./docs/*.md`
- Services: `./services/weatherlink-kafka/`, `./services/weatherlink-sql/`, `./services/weatherlink-kafka-backfill/`, `./services/weatherlink-sql-backfill/`

### Common Customizations

**Change fetch interval**:
```yaml
# docker-compose.yml
services:
  weatherlink-kafka:
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

### Service Naming Convention

**Source-Based Pattern**: `weatherlink-{destination}`
- Services named by their destination (kafka or sql)
- Real-time services run as daemons
- Backfill services are one-shot executables

**Services**:
- **weatherlink-kafka**: Real-time API → Kafka (streaming daemon)
- **weatherlink-sql**: Real-time Kafka → PostgreSQL (streaming daemon)
- **weatherlink-kafka-backfill**: Historical API → Kafka (one-shot)
- **weatherlink-sql-backfill**: Historical Kafka → PostgreSQL (one-shot)

### weatherlink-kafka Service

**Purpose**: Fetch weather data from WeatherLink v2 API and publish to Kafka

**Package Structure**:
```
weatherlink-kafka/
├── main.go              # Entry point
├── config/              # Environment variable parsing
│   └── config.go
├── api/                 # WeatherLink API client
│   ├── client.go        # HTTP client wrapper
│   ├── auth.go          # HMAC-SHA256 authentication
│   └── weatherlink.go   # API endpoints
├── kafka/               # Kafka producer
│   ├── producer.go      # Idempotent producer
│   └── consumer.go      # Scanner utilities
├── models/              # Data models
│   └── types.go
├── util/                # Hash utilities
│   └── hash.go
├── service/             # Business logic
│   ├── service.go       # Orchestration
│   ├── metadata.go      # Metadata fetching
│   ├── conditions.go    # Current conditions
│   └── cache.go         # Timestamp cache
└── Dockerfile
```

**Key Operations**:
1. Fetch sensor metadata (on change via hash comparison)
2. Fetch sensor catalog (on change via hash comparison)
3. Fetch station info (on change via hash comparison)
4. Fetch current conditions (every 5 minutes)
5. Deduplicate via timestamp cache
6. Publish to Kafka topics

**Dependencies**: `github.com/confluentinc/confluent-kafka-go/v2`, `github.com/lib/pq`

### weatherlink-sql Service

**Purpose**: Materialize Kafka messages to PostgreSQL with automatic tag creation

**Package Structure**:
```
weatherlink-sql/
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
- Primary key: `(tag_id, ts)`
- Fields: `tag_id`, `value` (numeric), `ts`
- Index: `(tag_id, ts DESC)`

**records_text** - Text time-series data
- Primary key: `(tag_id, ts)`
- Fields: `tag_id`, `value` (text), `ts`
- Index: `(tag_id, ts DESC)`

**records_null** - Null value tracking
- Primary key: `(tag_id, ts)`
- Fields: `tag_id`, `ts`
- Index: `(tag_id, ts DESC)`

**records** (view) - Unified query interface over all record types
- Fields: `tag_id`, `value`, `value_type`, `ts`
- Note: Device ID available via JOIN with tags table

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
./scripts/restart-all.sh                # Restart all
./scripts/restart-all.sh weatherlink-kafka  # Restart specific service

# Status
./scripts/status.sh             # System status
docker ps                       # Container list
docker stats                    # Resource usage
```

### Logs and Monitoring

```bash
# View logs
./scripts/logs.sh                    # All services
./scripts/logs.sh weatherlink-kafka    # Specific service
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

# View messages (last 10)
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --from-beginning \
  --max-messages 10

# With headers and timestamps
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --property print.headers=true \
  --property print.timestamp=true \
  --from-beginning

# Consumer groups
docker exec roach-kafka kafka-consumer-groups \
  --list \
  --bootstrap-server localhost:29092

docker exec roach-kafka kafka-consumer-groups \
  --describe \
  --group weatherlink-sql-data-iss \
  --bootstrap-server localhost:29092
```

### Service Updates

```bash
# After code changes
./scripts/start-all.sh build         # Rebuild all services

# Or rebuild specific service
docker compose build weatherlink-kafka
./scripts/restart-all.sh weatherlink-kafka

# View logs after update
./scripts/logs.sh weatherlink-kafka
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

**Cause**: Kafka not ready (takes 20-60s for health check)

**Fix**: Wait for health check or restart
```bash
./scripts/status.sh  # Check health status
docker ps            # Look for "(healthy)" status
./scripts/restart-all.sh weatherlink-kafka
```

### No Data Publishing
**Check**: API credentials, service logs
```bash
docker exec roach-weatherlink-kafka env | grep WEATHERLINK
./scripts/logs.sh weatherlink-kafka
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
./scripts/stop-all.sh clean
./scripts/start-all.sh
```

**Complete reset (WARNING: deletes ALL data):**
```bash
./scripts/stop-all.sh clean
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
