# Architecture

> **For basics, see [AI-CONTEXT.md](AI-CONTEXT.md)**. This document covers detailed specifications and advanced architecture topics.

## Component Specifications

### Kafka Broker

**Image**: `confluentinc/cp-kafka:7.5.0`

**Listeners**:
```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

**Retention**:
```yaml
KAFKA_LOG_RETENTION_MS: -1       # Infinite retention
KAFKA_LOG_RETENTION_BYTES: -1    # No size limit
KAFKA_LOG_DIRS: /var/lib/kafka/data  # Ensures persistence
```

**Performance**:
```yaml
KAFKA_NUM_NETWORK_THREADS: 3
KAFKA_NUM_IO_THREADS: 8
KAFKA_SOCKET_SEND_BUFFER_BYTES: 102400
KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: 102400
KAFKA_SOCKET_REQUEST_MAX_BYTES: 104857600
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

**Health Check**: `kafka-broker-api-versions --bootstrap-server localhost:29092`

### PostgreSQL

**Image**: `postgres:16-alpine`
**Database**: `roach`, **User**: `roach`
**Init Script**: `scripts/db/init/01-schema.sql` (auto-run on first start)

**Schema Tables**:
- `devices` - 20+ columns including parent device, location, metadata JSONB
- `tags` - Unique on (device_id, tag_name), includes unit/description
- `sensor_catalog` - Field metadata indexed by (sensor_type, data_structure_type, field_name)
- `records_numeric`, `records_text`, `records_null` - Typed time-series storage
- `records` (view) - Unified interface
- `orphaned_messages` - Failed processing tracking
- `schema_migrations` - Migration version tracking

**Indexes**:
- `idx_devices_lsid` on `devices(lsid)`
- `idx_devices_active` on `devices(active)`
- `idx_devices_data_structure_type` on `devices(data_structure_type)`
- `idx_tags_device_tag` on `tags(device_id, tag_name)` (unique)
- `idx_sensor_catalog_lookup` on `sensor_catalog(sensor_type, data_structure_type, field_name)` (unique)
- `idx_records_numeric_tag_timestamp` on `records_numeric(tag_id, timestamp)`
- `idx_records_text_tag_timestamp` on `records_text(tag_id, timestamp)`

### Zookeeper

**Image**: `confluentinc/cp-zookeeper:7.5.0`
**Configuration**:
```yaml
ZOOKEEPER_CLIENT_PORT: 2181
ZOOKEEPER_TICK_TIME: 2000
```
**Purpose**: Kafka cluster coordination only

### Kafka UI

**Image**: `provectuslabs/kafka-ui:latest`
**Configuration**: Connects to `kafka:29092` via Docker network
**Features**: Browse topics, messages, consumer groups, broker health

## Service Implementation Details

### weather-publish Architecture

**Goroutine Structure**:
```
main goroutine
├── Metadata update loop (background)
│   ├── Fetch sensors (every 24h or on change)
│   ├── Fetch catalog (every 24h or on change)
│   └── Fetch station (every 24h or on change)
└── Main fetch loop (foreground)
    └── Fetch current conditions (every 5m)
        ├── Check timestamp cache (deduplicate)
        ├── Publish to 4 data topics
        └── Update cache
```

**Deduplication Strategy**:
1. In-memory timestamp cache (last 5 minutes)
2. PostgreSQL rehydration on startup (last 24 hours)
3. Cache stores: `map[int64]map[int]time.Time` (LSID → data_structure_type → timestamp)

**API Authentication**: HMAC-SHA256 signature generation per WeatherLink v2 spec

**Catalog Filtering**: Dynamically discovers sensor types from `/v2/sensors` endpoint, filters catalog to only include active sensor types before publishing (reduces size from 3.4MB to ~50-100KB)

### weather-sql Architecture

**Consumer Groups**:
- `weather-sql-metadata` - Consumes `weather.metadata.sensors`
- `weather-sql-catalog` - Consumes `weather.metadata.catalog`
- `weather-sql-data-iss` - Consumes `weather.iss`
- `weather-sql-data-barometer` - Consumes `weather.barometer`
- `weather-sql-data-indoor` - Consumes `weather.indoor`
- `weather-sql-data-health` - Consumes `weather.health`

**Processing Flow**:
```
Message arrives
├── Extract headers (LSID, sensor_type, data_structure_type)
├── Lookup device in cache
│   ├── If found: Process message
│   └── If not found: Save to orphaned_messages
├── Parse JSON body (field → value map)
└── For each field:
    ├── Lookup tag in cache
    │   ├── If found: Use existing tag
    │   └── If not found: Create tag with catalog enrichment
    ├── Determine data type (numeric, text, null)
    └── Insert to appropriate records_* table
```

**Cache Management**:
- Devices cache: `map[int64]*models.Device` (LSID → Device)
- Tags cache: `map[string]*models.Tag` (key: "device_id:tag_name")
- Catalog cache: `map[string]*models.FieldMetadata` (key: "sensor_type:data_structure_type:field_name")
- Thread-safe with `sync.RWMutex`

**Tag Enrichment**:
1. On catalog update, query tags missing unit/description
2. Join tags with devices to get sensor metadata
3. Lookup field metadata in catalog cache
4. Update tags with units, descriptions, metadata JSONB
5. Update cache

## Network Details

### Service Communication Matrix

| From | To | Address | Protocol | Purpose |
|------|----|---------| ---------|---------|
| weather-publish | Kafka | kafka:29092 | PLAINTEXT | Publish messages |
| weather-publish | PostgreSQL | postgres:5432 | TCP | Cache rehydration |
| weather-sql | Kafka | kafka:29092 | PLAINTEXT | Consume messages |
| weather-sql | PostgreSQL | postgres:5432 | TCP | Materialize data |
| kafka-ui | Kafka | kafka:29092 | PLAINTEXT | Monitoring |
| Kafka | Zookeeper | zookeeper:2181 | TCP | Coordination |
| Host | Kafka | localhost:9092 | PLAINTEXT | External access |
| Host | PostgreSQL | localhost:5432 | TCP | Database access |
| Host | Kafka UI | localhost:8080 | HTTP | Web interface |

### Docker Network Configuration

**Network**: `roach-network` (bridge driver)
**DNS**: Automatic container name resolution
**Isolation**: Internal services not exposed to host except via port mappings

## Resource Usage

### Average Resource Consumption

| Component | CPU | Memory | Disk Growth |
|-----------|-----|--------|-------------|
| Kafka | 1-5% | 1-2 GB | ~1-3 MB/day |
| Zookeeper | <1% | 100-200 MB | ~1 MB/day |
| PostgreSQL | 1-3% | 100-500 MB | ~2-5 MB/day |
| weather-publish | <1% | 20-50 MB | Minimal |
| weather-sql | 1-3% | 50-100 MB | Minimal |
| Kafka UI | 1-2% | 100-200 MB | Minimal |

**Total Baseline**: ~2-5% CPU, ~1.5-3 GB RAM, ~5-10 MB/day disk

**Scaling Notes**:
- Disk growth linear with data frequency
- At 5-minute intervals: ~1-3 messages/topic × 7 topics × 288 times/day
- PostgreSQL growth depends on field count and data types

## Directory Structure

```
roach/
├── docker-compose.infrastructure.yml  # Kafka, Zookeeper, PostgreSQL, UI
├── docker-compose.yml                 # weather-publish, weather-sql
├── .env                              # Credentials (gitignored)
├── .env.example                      # Template
├── scripts/
│   ├── start-all.sh                  # Start infrastructure + services
│   ├── start-infra.sh                # Start infrastructure only
│   ├── stop-all.sh                   # Stop all
│   ├── restart.sh                    # Restart service(s)
│   ├── logs.sh                       # View logs
│   ├── status.sh                     # System status
│   └── db/
│       ├── init/
│       │   └── 01-schema.sql         # Initial PostgreSQL schema
│       ├── migrations/               # Schema migrations
│       │   ├── 001_*.up.sql
│       │   └── 001_*.down.sql
│       ├── migrate.sh                # Migration tool
│       ├── query.sh                  # Database query helper
│       └── reload-orphans.sh         # Reprocess orphaned messages
├── docs/                             # Documentation
│   ├── AI-CONTEXT.md                 # Quick start context (read first)
│   ├── README.md                     # Documentation index
│   ├── architecture.md               # This file
│   ├── operations.md                 # Operations guide
│   ├── troubleshooting.md            # Problem solving
│   ├── go-standards.md               # Code standards
│   ├── kafka-topics.md               # Topic reference
│   └── migrations.md                 # Migration details
├── data/                             # Persistent storage (gitignored)
│   ├── kafka/                        # Kafka logs and data
│   ├── zookeeper/                    # Zookeeper data
│   └── postgres/                     # PostgreSQL data
└── services/
    ├── weather-publish/              # Weather publisher
    │   ├── main.go                   # Entry point
    │   ├── Dockerfile                # Multi-stage build
    │   ├── go.mod, go.sum            # Go dependencies
    │   ├── config/, models/, api/    # Packages
    │   ├── service/, kafka/, internal/
    │   └── README.md
    └── weather-sql/                  # Weather materializer
        ├── main.go                   # Entry point
        ├── Dockerfile                # Multi-stage build
        ├── go.mod, go.sum            # Go dependencies
        ├── config/, models/, cache/  # Packages
        ├── repository/, service/, kafka/
        └── README.md
```

## Extension Points

### Adding Kafka Topics

Topics auto-created on first publish. Follow naming convention: `namespace.category.subcategory`

**Examples**:
- `home.hvac.temperature`
- `home.security.motion`
- `home.energy.consumption`

### Adding Database Tables

Use migration framework:
```bash
./scripts/db/migrate.sh create add_new_table
# Edit generated .up.sql and .down.sql files
./scripts/db/migrate.sh up
```

### Adding New Services

1. Create `services/<name>/` with Go service following [go-standards.md](go-standards.md)
2. Create Dockerfile (use existing services as template)
3. Add to `docker-compose.yml` with dependencies on `kafka` and `postgres` health checks
4. Connect to `kafka:29092` and `postgres:5432` via Docker network
5. Add to `roach-network`

## Security Model

### Current (Development)
- All communication plaintext within Docker network
- No authentication on Kafka, PostgreSQL, Kafka UI
- Services trust each other implicitly
- Suitable for private networks only

### Future (Production)
- SSL/TLS for Kafka external access
- Certificate-based authentication
- PostgreSQL password authentication (already configured)
- Let's Encrypt certificates for external services
- Network policies for container isolation

## Data Flow Details

### Weather Data Pipeline

```
1. WeatherLink API (HTTPS)
   ↓
   GET /v2/current/{station_id} (every 5m)
   GET /v2/sensors/{station_id} (on change)
   GET /v2/sensor-catalog (on change)
   
2. weather-publish
   ↓
   Deduplicate (timestamp cache)
   Parse response
   Split by sensor category
   ↓
   Publish to Kafka (JSON + headers)
   
3. Kafka Broker
   ↓
   Persist to disk (infinite retention)
   Replicate (if multi-broker)
   
4. weather-sql
   ↓
   Consume messages
   Lookup/create devices
   Lookup/create tags (with enrichment)
   Insert to typed records tables
   
5. PostgreSQL
   ↓
   Query via records view
   Analyze with SQL
```

### Metadata Flow

```
1. API: Sensors/Catalog/Station
   ↓
2. weather-publish: Hash comparison
   ↓ (only if changed)
3. Kafka: Metadata topics
   ↓
4. weather-sql: Upsert to DB + cache
   ↓
5. Tag enrichment: Backfill units/descriptions
```

## Performance Tuning

### Kafka

- `KAFKA_LOG_RETENTION_MS=-1` for infinite retention (or set limit to save disk)
- `KAFKA_NUM_IO_THREADS=8` for I/O throughput
- Increase `KAFKA_SOCKET_REQUEST_MAX_BYTES` for large messages

### PostgreSQL

- Default settings suitable for low-volume IoT data
- For higher volume: Tune `shared_buffers`, `work_mem`, `maintenance_work_mem`
- Consider partitioning `records_*` tables by timestamp for large datasets

### Services

- Increase `FETCH_INTERVAL` to reduce API calls and Kafka writes
- Adjust `BATCH_SIZE` in weather-sql for bulk inserts (future)
- Use `LOG_LEVEL=warn` or `error` in production to reduce I/O
