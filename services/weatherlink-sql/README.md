# weatherlink-sql

Consumes weather data from Kafka and materializes it into PostgreSQL (Device → Tag → Record). Batched COPY writes, worker pool, catalog enrichment.

## Architecture

```
Kafka (weather.* data + metadata) → weatherlink-sql → PostgreSQL
         ↓                              ↓
  metadata.sensors              Device/Tag cache
  metadata.catalog              Batch writer (numeric/text/null)
                                Orphan store
```

**Flow**: Connect PostgreSQL → Load devices, tags, catalog into cache → Enrich tags from catalog → Start metadata/catalog readers (background) → Discover data topics (`weather.*` excluding `weather.metadata.*`) → Consume from latest offset → Worker pool parses messages, upserts devices/tags, buffers records → Flush on batch size or interval (COPY + ON CONFLICT DO NOTHING).

## Configuration

**Required**:

```bash
POSTGRES_DSN=host=postgres port=5432 user=roach password=xxx dbname=roach sslmode=disable
```

**Optional** (defaults in code):

```bash
KAFKA_BROKER=kafka:29092
LOG_LEVEL=info
BATCH_SIZE=100
WORKER_POOL_SIZE=4
BATCH_FLUSH_INTERVAL_MS=500
DB_POOL_MAX_CONNS=10
```

## Database

- **devices**: Sensor metadata (lsid, category, station, etc.)
- **tags**: Field definitions per device (from catalog when available)
- **records_numeric**, **records_text**, **records_null**: Time-series by type (PK: tag_id, ts)
- **orphaned_messages**: Failed messages (missing device, parse error, etc.)

## Run

```bash
# From repo root
docker compose up weatherlink-sql

# Standalone
cd services/weatherlink-sql
export POSTGRES_DSN="host=localhost port=5432 user=roach password=roach dbname=roach sslmode=disable"
export KAFKA_BROKER=localhost:9092
go run main.go
```

## Behavior

- **Topic discovery**: At startup, lists partitions and subscribes to topics with prefix `weather.` but not `weather.metadata.`. New topics require restart.
- **Metadata**: `weather.metadata.sensors` → upsert devices, refresh cache; `weather.metadata.catalog` → upsert catalog, re-enrich tags.
- **Data**: Headers `lsid`, `timestamp`, `sensor_type`, `data_structure_type`; body JSON. Missing device or tag-creation failure → record in `orphaned_messages`.
- **Metrics**: Logged every 30s (pool processed/errors, batch counts, flushes).

## Troubleshooting

| Issue | Action |
|-------|--------|
| Won't start | Ensure PostgreSQL and Kafka are up; valid `POSTGRES_DSN` |
| No writes | Confirm Kafka has data; devices exist (need `weather.metadata.sensors`); check logs and `orphaned_messages` |

## Dependencies

- `github.com/jackc/pgx/v5` (pool, COPY)
- `github.com/segmentio/kafka-go`
