# WeatherLink SQL Materializer

Consumes weather data from Kafka and materializes it into PostgreSQL using a Device → Tag → Record model with concurrent processing and batched writes.

**Service Name**: `weatherlink-sql` (Docker service)
**Binary Name**: `weatherlink-sql` (Go module and executable)
**Container Name**: `roach-weatherlink-sql`

## Overview

This service:
- Subscribes to `weather.*` data topics (excluding `weather.metadata.*`)
- Listens to `weather.metadata.sensors` for device updates
- Listens to `weather.metadata.catalog` for field definitions
- Materializes sensor readings to PostgreSQL using batched COPY inserts
- Manages hierarchical data: Devices → Tags → Records
- Auto-creates tags when new fields are discovered, with catalog enrichment
- Tracks orphaned messages for missing devices or processing errors
- Logs periodic throughput metrics

## Architecture

```
Kafka Topics → Topic Readers → Worker Pool → Batch Writer → PostgreSQL
                     ↓              ↓             ↓
               Device Cache     Tag Creation   Buffered Flush
               Tag Cache        Orphans        (numeric/text/null)
               Catalog Cache
```

## Configuration

### Environment Variables

```bash
KAFKA_BROKER=kafka:29092
POSTGRES_DSN=host=postgres port=5432 user=roach password=xxx dbname=roach sslmode=disable
LOG_LEVEL=info
BATCH_SIZE=100
WORKER_POOL_SIZE=4
BATCH_FLUSH_INTERVAL_MS=500
DB_POOL_MAX_CONNS=10
```

## Database Schema (High Level)

- **devices**: Sensor metadata (LSID, category, location, station info)
- **tags**: Field definitions per device (temperature, humidity, etc.)
- **records_numeric**: Numeric values (primary key: tag_id, ts)
- **records_text**: Text values (primary key: tag_id, ts)
- **records_null**: Null values (primary key: tag_id, ts)
- **orphaned_messages**: Messages that failed processing

## Running

### With Docker Compose (Recommended)

```bash
# From project root
docker compose up weatherlink-sql
```

### Standalone with Go

```bash
cd services/weatherlink-sql

go mod download

export KAFKA_BROKER=localhost:9092
export POSTGRES_DSN="host=localhost port=5432 user=roach password=roach dbname=roach sslmode=disable"

go run main.go
```

## How It Works

### Startup Sequence

1. Connect to PostgreSQL with pgx pool
2. Load devices, tags, and catalog into caches
3. Enrich existing tags with catalog metadata
4. Start worker pool and metrics logger
5. Start catalog and metadata listeners (background)
6. Discover data topics and start consuming from the latest offsets

### Data Processing (Worker Pool)

1. Readers fetch messages from each `weather.*` data topic
2. Messages are submitted to the worker pool and offsets are committed immediately
3. Each worker:
   - Extracts `lsid`, `timestamp`, `sensor_type`, `data_structure_type` from headers
   - Looks up the device in cache; orphaned if missing
   - Parses JSON payload and iterates fields
   - Creates tags on-the-fly (with catalog enrichment if available)
   - Adds records to the batch writer (numeric/text/null)

### Batch Writing

- Records are buffered per type and flushed on size or interval
- Each flush uses a temporary table + COPY, then inserts with `ON CONFLICT DO NOTHING`
- Flush operations are serialized to avoid deadlocks

### Metadata & Catalog

- `weather.metadata.sensors`: upserts devices and refreshes the device cache
- `weather.metadata.catalog`: upserts catalog entries, refreshes cache, and re-enriches tags

### Topic Discovery

The service lists Kafka partitions on startup and subscribes to topics that match
`weather.*` (excluding `weather.metadata.*`). New topics require a restart to be picked up.

## Monitoring

### Metrics Logs

Every 30 seconds the service logs deltas and totals:

```
=== METRICS ===
Pool New: processed=123, errors=0
Pool Totals: processed=4567, errors=3
Batch New: numeric=980, text=42, null=0, flushes=6
Batch Totals: numeric=120000, text=5100, null=72, flushes=820
===============
```

### Check Service Health

```bash
# View logs
docker compose logs -f weatherlink-sql

# Check if running
docker compose ps weatherlink-sql

# Restart service
docker compose restart weatherlink-sql
```

## Troubleshooting

### Service Won't Start

Common issues:
1. PostgreSQL not ready
2. Kafka not reachable
3. Invalid `POSTGRES_DSN`

### No Data Being Written

1. Verify Kafka topics have data
2. Check device cache is populated (requires `weather.metadata.sensors`)
3. Look for errors in logs
4. Query `orphaned_messages` for failure reasons

## Dependencies

- `github.com/jackc/pgx/v5` - PostgreSQL driver and COPY
- `github.com/segmentio/kafka-go` - Kafka client
