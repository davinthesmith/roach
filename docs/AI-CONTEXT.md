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
- Real-time streaming and SQL storage

**Current Implementation**: WeatherLink weather station, Home Assistant Ecobee, and UniFi Protect integrations with 16 Kafka topics and 6 Go services (4 real-time daemons, 2 backfill/tool), optimized for storage efficiency via compression and header optimization

**Technology Stack**: Docker Compose, Kafka 7.5.0, PostgreSQL 16, Zookeeper, Go 1.21+

## Quick Start

```bash
# Configure credentials
cp .env.example .env
vim .env  # Add WEATHERLINK_API_KEY, WEATHERLINK_API_SECRET, WEATHERLINK_STATION_ID, HA_URL, HA_TOKEN, UNIFI_HOST, UNIFI_API_KEY, POSTGRES_PASSWORD

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
- Interval: `FETCH_INTERVAL` for data, `METADATA_FETCH_INTERVAL` for metadata
- Topics Published: 7 (4 data, 3 metadata)
- Deduplication: Kafka key cache (record keys + metadata keys)

**weatherlink-sql** (`roach-weatherlink-sql`)
- Language: Go
- Purpose: Real-time materialization (Kafka → PostgreSQL)
- Consumers: All `weather.*` topics
- Features: Auto-tag creation, metadata enrichment, orphaned message tracking
- Performance: Batched writes with COPY protocol, worker pool processing

**homeassistant-kafka** (`roach-homeassistant-kafka`)
- Language: Go
- Purpose: Real-time Home Assistant event ingestion (WebSocket → Kafka)
- Topics Published: `homeassistant.ecobee.*`
- Filtering: `ecobee` entity_id substring or explicit list (optional)

**homeassistant-command** (`roach-homeassistant-command`)
- Language: Go
- Purpose: Thermostat control via Kafka commands (Kafka → Home Assistant WebSocket)
- Topic Consumed: `homeassistant.command`
- Consumer Group: `homeassistant-command`
- Supported: `climate.set_temperature`, `set_hvac_mode`, `set_preset_mode`, `set_fan_mode`, `turn_on`, `turn_off`
- Testing: `scripts/homeassistant/send-command.sh`

**ubiquiti-kafka** (`roach-ubiquiti-kafka`)
- Language: Go
- Purpose: Real-time UniFi Protect event ingestion (WebSocket → Kafka)
- Topics Published: `ubiquiti.protect.smart`, `ubiquiti.protect.audio`, `ubiquiti.protect.motion`
- Connection: Local NVR via `UNIFI_HOST` (`/proxy/protect/integration/v1/subscribe/events`), TLS skip verify
- Events: Smart video (person, vehicle, animal, package), audio (babyCry, coAlarm, smoke, speak), motion

**weatherlink-kafka** backfill mode
- Language: Go
- Purpose: Historical data backfill (API → Kafka)
- Run Mode: Optional one-shot execution (enabled via env)
- Features: 24-hour windows, rate limiting, client-side deduplication
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
    ↓ Every `FETCH_INTERVAL`
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
Stage 1: API → Kafka (weatherlink-kafka backfill mode)
  - Fetch historical data from WeatherLink API
  - 24-hour windows, rate limited (8 req/s)
  - Client-side deduplication
  - Populates Kafka with missing historical data

Stage 2: Kafka → DB (weatherlink-sql-backfill)
  - Replay messages from Kafka topics
  - Configurable offset ranges (earliest to latest)
  - Direct partition readers (no consumer group)
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
    ├── weatherlink-kafka (connects to kafka:29092, postgres:5432 [unused])
    ├── weatherlink-sql (connects to kafka:29092, postgres:5432)
    ├── homeassistant-kafka (connects to kafka:29092, homeassistant:8123)
    ├── homeassistant-command (connects to kafka:29092, homeassistant:8123)
    ├── ubiquiti-kafka (connects to kafka:29092, UNIFI_HOST [local NVR])
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

# Home Assistant (Required for homeassistant-kafka)
HA_URL=http://homeassistant:8123
HA_TOKEN=<your_long_lived_access_token>

# UniFi Protect (Required for ubiquiti-kafka)
UNIFI_API_KEY=<your_api_key>
UNIFI_HOST=<your_nvr_url>           # e.g. https://192.168.1.1

# PostgreSQL (Required)
POSTGRES_PASSWORD=<secure_password>
```

### Optional Environment Variables

**weatherlink-kafka**:
```bash
KAFKA_BROKER=kafka:29092        # Default
POSTGRES_DSN=host=postgres...   # Optional (currently unused)
FETCH_INTERVAL=5m               # 5 minutes default (Go duration format)
METADATA_FETCH_INTERVAL=168h   # 7 days default (Go duration format)
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

**homeassistant-kafka**:
```bash
KAFKA_BROKER=kafka:29092        # Default
HA_WS_URL=ws://homeassistant:8123/api/websocket  # Optional override
WS_RECONNECT_BACKOFF=1s,5s,30s  # Reconnect delays
POLL_ENABLED=false              # REST polling fallback
POLL_INTERVAL=60s               # Poll interval
POLL_ENTITY_FILTER=             # Comma-separated entity_ids (optional)
LOG_LEVEL=info                  # debug|info|warn|error
```

**homeassistant-command**:
```bash
KAFKA_BROKER=kafka:29092        # Default
HA_WS_URL=ws://homeassistant:8123/api/websocket  # Optional override
KAFKA_TOPIC=homeassistant.command  # Topic to consume
KAFKA_CONSUMER_GROUP=homeassistant-command  # Consumer group
WS_RECONNECT_BACKOFF=1s,5s,30s  # Reconnect delays
LOG_LEVEL=info                  # debug|info|warn|error
```

**ubiquiti-kafka**:
```bash
KAFKA_BROKER=kafka:29092        # Default
RECONNECT_BACKOFF=1s,5s,30s     # Reconnect delays
LOG_LEVEL=info                  # debug|info|warn|error
```

**weatherlink-kafka** backfill (optional):
```bash
KAFKA_BACKFILL_ENABLED=true     # Enable historical backfill
BACKFILL_START_TS=0             # Unix timestamp (seconds)
BACKFILL_END_TS=0               # Unix timestamp (0 = now)
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
- Services: `./services/weatherlink-kafka/`, `./services/weatherlink-sql/`, `./services/homeassistant-kafka/`, `./services/homeassistant-command/`, `./services/ubiquiti-kafka/`, `./services/weatherlink-sql-backfill/`

### Common Customizations

**Change fetch intervals**:
```yaml
# docker-compose.yml
services:
  weatherlink-kafka:
    environment:
      - FETCH_INTERVAL=10m           # Change from 5m to 10m
      - METADATA_FETCH_INTERVAL=72h # Change from 168h to 72h
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

**Source-Based Pattern**: `{source}-{destination}`
- Services named by source + destination (e.g., weatherlink-kafka, homeassistant-kafka)
- Real-time services run as daemons
- Backfill services are one-shot executables

**Services**:
- **weatherlink-kafka**: Real-time API → Kafka (streaming daemon, optional backfill mode)
- **weatherlink-sql**: Real-time Kafka → PostgreSQL (streaming daemon)
- **homeassistant-kafka**: Real-time Home Assistant → Kafka (event stream)
- **homeassistant-command**: Kafka → Home Assistant (command execution)
- **ubiquiti-kafka**: Real-time UniFi Protect → Kafka (WebSocket event stream)
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
│   └── weatherlink.go   # API endpoints
├── kafka/               # Kafka producer
│   ├── producer.go      # Idempotent producer
│   └── consumer.go      # Scanner utilities
├── models/              # Data models
│   └── types.go
├── util/                # Shared helpers
│   ├── hash.go
│   ├── time.go
│   └── topic.go
├── service/             # Business logic
│   ├── backfill.go      # Historical backfill
│   ├── conditions.go    # Current conditions
│   ├── metadata.go      # Metadata fetching
│   ├── ratelimit.go     # Backfill rate limiter
│   ├── scanner.go       # Kafka key scan
│   └── service.go       # Orchestration
├── testdata/            # Sample API payloads
│   └── api/             # current.json, sensors.json, sensor-catalog.json
└── Dockerfile
```

**Key Operations**:
1. Scan Kafka topics to hydrate key caches (records + metadata)
2. Fetch sensor metadata → catalog → station info on startup and on `METADATA_FETCH_INTERVAL`
3. Fetch current conditions on `FETCH_INTERVAL`
4. Deduplicate by Kafka key caches (e.g., `lsid:timestamp` for records, weekly keys for sensor/station metadata)
5. Optionally backfill historical windows when enabled
6. Publish to Kafka topics

**Dependencies**: `github.com/confluentinc/confluent-kafka-go/v2`

### homeassistant-kafka Service

**Purpose**: Stream Home Assistant `state_changed` events and publish Ecobee updates to Kafka

**Package Structure**:
```
homeassistant-kafka/
├── main.go              # Entry point
├── config/              # Environment variable parsing
│   └── config.go
├── ha/                  # Home Assistant clients (WebSocket + REST)
│   └── client.go
├── kafka/               # Kafka producer
│   └── producer.go
├── models/              # Event + config models
│   ├── config.go
│   └── events.go
├── service/             # Business logic
│   └── service.go
└── Dockerfile
```

**Key Operations**:
1. Connect to Home Assistant WebSocket and authenticate
2. Subscribe to `state_changed` events
3. Filter Ecobee entities (substring match or explicit list)
4. Route to `homeassistant.ecobee.*` topics
5. Publish full event payloads with headers
6. Optional REST polling fallback when enabled

**Dependencies**: `github.com/confluentinc/confluent-kafka-go/v2`, `github.com/gorilla/websocket`

### homeassistant-command Service

**Purpose**: Consume thermostat commands from Kafka and execute via Home Assistant WebSocket `call_service` API

**Package Structure**:
```
homeassistant-command/
├── main.go              # Entry point
├── config/              # Environment variable parsing
│   └── config.go
├── ha/                  # Home Assistant WebSocket client (call_service)
│   └── client.go
├── kafka/               # Kafka consumer
│   └── consumer.go
├── models/              # Command + config models
│   ├── config.go
│   └── command.go
├── service/             # Business logic
│   └── service.go
└── Dockerfile
```

**Key Operations**:
1. Connect to Home Assistant WebSocket and authenticate
2. Consume commands from `homeassistant.command` topic
3. Translate to `call_service` WebSocket messages
4. Wait for result confirmation from HA
5. Reconnect automatically on WebSocket failure
6. Periodic ping/pong keepalive

**Dependencies**: `github.com/gorilla/websocket`, `github.com/segmentio/kafka-go`

### ubiquiti-kafka Service

**Purpose**: Stream UniFi Protect events via WebSocket and publish smart/audio/motion detections to Kafka

**Package Structure**:
```
ubiquiti-kafka/
├── main.go              # Entry point
├── config/              # Environment variable parsing
│   └── config.go
├── api/                 # UniFi Protect API client (WebSocket)
│   └── client.go
├── kafka/               # Kafka producer
│   └── producer.go
├── models/              # Event + config models
│   ├── config.go
│   └── events.go
├── service/             # Business logic
│   └── service.go
└── Dockerfile
```

**Key Operations**:
1. Fetch camera metadata from Protect API (`/cameras`) for name resolution
2. Subscribe to WebSocket event stream at `/proxy/protect/integration/v1/subscribe/events`
3. Classify events: smart video (person, vehicle, animal, package), audio (babyCry, coAlarm, smoke, speak), motion
4. Route to `ubiquiti.protect.smart`, `ubiquiti.protect.audio`, `ubiquiti.protect.motion` topics
5. Publish full event payloads with headers
6. Auto-reconnect with exponential backoff

**Dependencies**: `github.com/confluentinc/confluent-kafka-go/v2`, `github.com/gorilla/websocket`

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

**Dependencies**: `github.com/segmentio/kafka-go`, `github.com/jackc/pgx/v5`

## Kafka Topics

### Naming Convention
Format: `namespace.category[.subcategory]`

### Data Topics (Published every `FETCH_INTERVAL`)

**weather.iss** - Outdoor weather (Integrated Sensor Suite)
- Key fields: `temp`, `hum`, `wind_speed_last`, `wind_dir_last`, `rain_rate_last`, `solar_rad`, `uv_index`, `dew_point`, `heat_index`

**weather.barometer** - Barometric pressure
- Key fields: `bar_sea_level`, `bar_absolute`, `bar_trend`

**weather.indoor** - Indoor conditions
- Key fields: `temp_in`, `hum_in`, `dew_point_in`, `heat_index_in`

**weather.health** - Console health metrics
- Key fields: `battery_voltage`, `wifi_rssi`, `uptime`, `firmware_version`, `free_mem`

**weather.other** - Fallback for unknown categories

### UniFi Protect Topics (Published via WebSocket event stream)

**ubiquiti.protect.smart** - Smart video AI detections
- Detection types: `person`, `vehicle`, `animal`, `package`
- Key: `{camera_name}:{timestamp}`

**ubiquiti.protect.audio** - Smart audio AI detections
- Detection types: `babyCry`, `coAlarm`, `smoke`, `speak`
- Key: `{camera_name}:{timestamp}`

**ubiquiti.protect.motion** - Motion events
- Key: `{camera_name}:{timestamp}`

### Metadata Topics (Published every `METADATA_FETCH_INTERVAL`, deduped by key cache)

**weather.metadata.sensors** - Sensor configuration (LSIDs, sensor details)
**weather.metadata.catalog** - Sensor type catalog (field schemas, units, descriptions)
**weather.metadata.station** - Station information (name, location, timezone)

### Message Structure

**Headers** (data topics):
- `schema_version` - Schema version (e.g., "1")
- `lsid` - Logical Sensor ID
- `timestamp` - Unix timestamp (seconds)
- `sensor_type` - Sensor type code
- `data_structure_type` - Data structure type

**Note**: Removed redundant headers (`station_id`, `station_id_uuid`, `category`, `product_name`) for storage optimization. These are available via metadata lookup. See [kafka-standards.md](docs/kafka-standards.md).

**Body** (JSON data point):
```json
{
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
  --group weatherlink-sql-data \
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
