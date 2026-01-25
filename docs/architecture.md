# Architecture

## System Overview

ROACH is a Kafka-based data aggregation system for home IoT devices with infinite data persistence.

## Components

### Infrastructure Layer
**File**: `docker-compose.infrastructure.yml`

#### Zookeeper
- **Image**: `confluentinc/cp-zookeeper:7.5.0`
- **Port**: 2181
- **Purpose**: Kafka cluster coordination
- **Data**: `./data/zookeeper`
- **Health Check**: TCP check on port 2181

#### Kafka Broker
- **Image**: `confluentinc/cp-kafka:7.5.0`
- **Ports**: 
  - 9092 (external, localhost)
  - 29092 (internal, Docker network)
- **Retention**: Infinite (`KAFKA_LOG_RETENTION_MS=-1`)
- **Data**: `./data/kafka`
- **Health Check**: Broker API version check
- **Network**: `roach-network`

#### Kafka UI
- **Image**: `provectuslabs/kafka-ui:latest`
- **Port**: 8080
- **Purpose**: Web-based Kafka monitoring and management

#### PostgreSQL
- **Image**: `postgres:16-alpine`
- **Port**: 5432
- **Purpose**: Time-series data storage and materialization
- **Data**: `./data/postgres`
- **Health Check**: `pg_isready -U roach`
- **Network**: `roach-network`
- **Database**: roach (user: roach)
- **Schema**:
  - `devices` - Sensor registry with full metadata
  - `tags` - Field definitions with units/descriptions
  - `sensor_catalog` - Field metadata from API catalog
  - `records_*` - Time-series data tables
  - `schema_migrations` - Migration tracking

### Application Layer
**File**: `docker-compose.yml`

#### Weather Publisher (weather-publish)
- **Language**: Go
- **Build**: Multi-stage Dockerfile
- **Purpose**: Fetch WeatherLink data, publish to Kafka with deduplication
- **Interval**: 5 minutes (configurable)
- **Topics**: 7 topics (4 data, 3 metadata)
- **Deduplication**: Timestamp-based cache with PostgreSQL rehydration

#### Weather SQL (weather-sql)
- **Language**: Go
- **Build**: Multi-stage Dockerfile
- **Purpose**: Materialize Kafka messages to PostgreSQL
- **Consumers**: All `weather.*` data topics + metadata + catalog
- **Schema**: Device/Tag/Record hierarchy
- **Features**: 
  - Auto-tag creation with metadata enrichment
  - Sensor catalog consumption and caching
  - Orphaned message tracking
  - Units and descriptions from API catalog

## Data Flow

```
WeatherLink API (HTTPS)
    ↓ Every 5 minutes
Weather Publisher (Go)
    ↓ Deduplication check
Kafka Broker (Infinite retention)
    ↓ Real-time streaming
Weather SQL (Go)
    ↓ Materialization
PostgreSQL Database
    ↓ Persistent storage
Devices → Tags → Records
```

## Network Architecture

```
┌──────────────────────────────────────────────────┐
│  Infrastructure (docker-compose.infra)           │
│  ┌────────────┐  ┌───────┐  ┌────────┐          │
│  │ Zookeeper  │→ │ Kafka │← │ Kafka  │          │
│  │   :2181    │  │:29092 │  │UI:8080 │          │
│  └────────────┘  └───────┘  └────────┘          │
│                                                  │
│  ┌──────────────────────────────────┐           │
│  │ PostgreSQL :5432                 │           │
│  │ - roach database                 │           │
│  │ - Device/Tag/Record schema       │           │
│  └──────────────────────────────────┘           │
│           roach-network                          │
└──────────────────────────────────────────────────┘
                    ↑
                    │ kafka:29092, postgres:5432
┌───────────────────┴──────────────────────────────┐
│  Applications (docker-compose.yml)               │
│  ┌──────────────┐  ┌─────────────┐  ┌─────────┐ │
│  │Weather       │  │Weather      │  │ Future  │ │
│  │Publisher     │  │SQL          │  │Service  │ │
│  │(Fetch & Pub) │  │(Materialize)│  │         │ │
│  └──────────────┘  └─────────────┘  └─────────┘ │
└──────────────────────────────────────────────────┘
```

## Service Communication

### Internal (Docker Network)
- Services → Kafka: `kafka:29092` (plaintext)
- Services → PostgreSQL: `postgres:5432` (plaintext)
- Kafka UI → Kafka: `kafka:29092` (plaintext)
- Kafka → Zookeeper: `zookeeper:2181`

### External (Host)
- Kafka: `localhost:9092` (plaintext)
- Kafka UI: `localhost:8080` (HTTP)
- PostgreSQL: `localhost:5432` (user: roach, db: roach)

## Directory Structure

```
roach/
├── docker-compose.infrastructure.yml  # Infrastructure
├── docker-compose.yml                 # Applications
├── .env                              # Configuration
├── scripts/                          # Operations
│   ├── start-all.sh
│   ├── start-infra.sh
│   ├── stop-all.sh
│   ├── logs.sh
│   ├── restart.sh
│   ├── status.sh
│   └── db/
│       ├── init/
│       │   └── 01-schema.sql         # Database schema
│       ├── migrations/               # Schema migrations
│       │   ├── 001_*.up.sql
│       │   └── 001_*.down.sql
│       ├── migrate.sh                # Migration tool
│       ├── query.sh                  # Database queries
│       └── reload-orphans.sh         # Reprocess orphaned messages
├── docs/                             # Documentation
├── data/                             # Persistent data
│   ├── kafka/
│   ├── zookeeper/
│   └── postgres/
└── services/                         # Service implementations
    ├── weather/                      # weather-publish
    │   ├── main.go
    │   ├── go.mod
    │   ├── Dockerfile
    │   └── README.md
    └── weather-sql/                  # weather-sql
        ├── main.go
        ├── go.mod
        ├── Dockerfile
        └── README.md
```

## Code Organization

Both services follow clean architecture principles with clear separation of concerns.

> **See [go-standards.md](go-standards.md) for complete Go code organization standards, design principles, and implementation examples.**

### weather-sql Service Structure

```
weather-sql/
├── main.go              # Entry point, dependency wiring (~75 lines)
├── config/              # Configuration loading
│   └── config.go        # Environment variable parsing
├── models/              # Data structures
│   └── types.go         # Device, Tag, FieldMetadata structs
├── cache/               # In-memory caching
│   └── cache.go         # Thread-safe cache for devices, tags, catalog
├── repository/          # Database operations
│   ├── devices.go       # Device CRUD operations
│   ├── tags.go          # Tag CRUD and enrichment queries
│   ├── catalog.go       # Catalog storage and retrieval
│   ├── records.go       # Record insertion (numeric, text, null)
│   └── orphans.go       # Orphaned message tracking
├── service/             # Business logic
│   ├── materializer.go  # Service orchestration
│   ├── metadata.go      # Metadata processor (devices)
│   ├── catalog.go       # Catalog processor
│   ├── data.go          # Data processor (tags and records)
│   └── enrichment.go    # Tag enrichment with catalog
└── kafka/               # Kafka consumers
    └── consumer.go      # Reader creation utilities
```

**Package Dependencies**:
- `main` → `config`, `kafka`, `service`
- `service` → `repository`, `cache`, `models`
- `repository` → `models`
- `cache` → `models`

### weather Service Structure

```
weather/
├── main.go              # Entry point, dependency wiring (~85 lines)
├── config/              # Configuration loading
│   └── config.go        # Environment variable parsing
├── models/              # Data structures
│   └── types.go         # API response structs
├── api/                 # WeatherLink API client
│   ├── client.go        # HTTP client wrapper
│   ├── auth.go          # HMAC-SHA256 signature generation
│   └── weatherlink.go   # API endpoint methods
├── kafka/               # Kafka producer
│   └── producer.go      # Message publishing
├── service/             # Business logic
│   ├── service.go       # Service orchestration
│   ├── metadata.go      # Metadata fetching (sensors, catalog, station)
│   ├── conditions.go    # Current conditions fetching
│   └── cache.go         # Timestamp cache and rehydration
└── internal/            # Internal utilities
    └── hash.go          # SHA-256 hash calculation
```

**Package Dependencies**:
- `main` → `config`, `api`, `kafka`, `service`
- `service` → `api`, `kafka`, `internal`, `models`
- `api` → `models`
- `kafka` → none (generic)

### Design Principles

#### Dependency Injection
- Services receive dependencies via constructors
- No package-level global state
- Enables easy mocking for tests

#### Interface Usage
- Repository operations can be behind interfaces (future enhancement)
- API client operations can be behind interfaces (future enhancement)
- Kafka operations encapsulated in dedicated packages

#### Error Handling
- Errors propagated up to service layer
- Service layer decides logging strategy
- Repository layer returns descriptive errors

#### Context Propagation
- All long-running operations accept `context.Context`
- Enables cancellation and timeout
- Proper cleanup on shutdown

## Extension Points

### Adding New Services

1. Create service directory: `services/<name>/`
2. Implement Kafka producer/consumer and/or PostgreSQL integration
3. Add to `docker-compose.yml`:
```yaml
services:
  new-service:
    build: ./services/new-service
    environment:
      - KAFKA_BROKER=kafka:29092
      - POSTGRES_DSN=host=postgres port=5432 user=roach password=${POSTGRES_PASSWORD} dbname=roach sslmode=disable
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - roach-network
```

### Topic Naming Convention
- Format: `namespace.category.subcategory`
- Examples:
  - `weather.iss` (outdoor weather)
  - `home.hvac.temperature`
  - `home.security.motion`

## Database Schema

### Migration Framework

ROACH uses a lightweight migration system:
- **Location**: `scripts/db/migrations/`
- **Tracking**: `schema_migrations` table stores applied migrations
- **Commands**: `./scripts/db/migrate.sh` with `up`, `down`, `status`, `create`
- **Format**: `NNN_description.{up,down}.sql`

Migrations are applied in order and tracked with checksums for integrity.

### Schema Tables

**devices** - Sensor registry
- Core: `id`, `lsid`, `sensor_type`, `category`, `manufacturer`, `product_name`
- Extended: `product_number`, `rain_collector_type`, `active`, `tx_id`, `port_number`
- Parent device: `parent_device_type`, `parent_device_name`, `parent_device_id`
- Location: `station_id`, `station_name`, `latitude`, `longitude`, `elevation`
- Metadata: `metadata` (JSONB), `data_structure_type`

**tags** - Field definitions
- Core: `id`, `device_id`, `tag_name`, `data_type`
- Metadata: `unit`, `description`, `metadata` (JSONB)
- Unique: `(device_id, tag_name)`

**sensor_catalog** - Field metadata from WeatherLink API
- `sensor_type`, `data_structure_type`, `field_name`
- `field_type`, `units`, `description`
- Unique: `(sensor_type, data_structure_type, field_name)`

**records_numeric** - Numeric time-series data
- `tag_id`, `device_id`, `value`, `timestamp`

**records_text** - Text time-series data
- `tag_id`, `device_id`, `value`, `timestamp`

**records_null** - Null value tracking
- `tag_id`, `device_id`, `timestamp`

**records** (view) - Unified query interface over all record types

**orphaned_messages** - Messages that couldn't be processed
- Tracks topic, partition, offset, reason
- Can be reprocessed after fixing issues

**schema_migrations** - Migration tracking
- `version`, `name`, `applied_at`, `checksum`

## Resource Usage

### CPU
- Kafka: 1-5% average
- Zookeeper: <1%
- PostgreSQL: 1-3% average
- Weather Publisher: <1% (spikes during fetch)
- Weather SQL: 1-3% average
- Kafka UI: 1-2%

### Memory
- Kafka: 1-2 GB
- Zookeeper: 100-200 MB
- PostgreSQL: 100-500 MB (grows with data)
- Weather Publisher: 20-50 MB
- Weather SQL: 50-100 MB
- Kafka UI: 100-200 MB

### Disk
- Growth: ~1-5 MB/day per service (varies by frequency)
- Kafka: `./data/kafka`
- Zookeeper: `./data/zookeeper`
- PostgreSQL: `./data/postgres`

## Security Model

### Current (Development)
- All communication plaintext within Docker network
- Kafka UI accessible only on localhost
- No authentication required

### Future (Production)
- SSL/TLS for external Kafka access
- Certificate-based authentication
- Let's Encrypt certificates mounted read-only
