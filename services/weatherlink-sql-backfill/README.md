# WeatherLink SQL Backfill

One-shot Kafka → PostgreSQL backfill for historical weather data. Replays Kafka topics and materializes them into the same schema used by `weatherlink-sql`.

**Data Flow**: Kafka → PostgreSQL

## When to Use

Use this service when:
- Kafka has complete historical data but the database is behind
- The real-time materializer was down and you need to catch up
- You want to rebuild the database from Kafka

## Overview

This service:
- Replays Kafka topics using explicit offsets (earliest, latest, or specific)
- Processes data topics in parallel, one goroutine per topic
- Uses a worker pool + batch writer (COPY) for high throughput
- Optionally backfills metadata topics in a first phase
- Writes orphaned messages to `orphaned_messages`
- Exits when all configured topics are processed

## Usage

### Docker (Recommended)

```bash
./scripts/weatherlink/sql-backfill.sh

# Backfill specific topics
./scripts/weatherlink/sql-backfill.sh --topics weather.iss,weather.indoor

# Include metadata phase
./scripts/weatherlink/sql-backfill.sh --metadata
```

### Standalone Binary

```bash
cd services/weatherlink-sql-backfill

go build -o weatherlink-sql-backfill

export KAFKA_BROKER=localhost:9092
export POSTGRES_DSN="host=localhost port=5432 user=roach password=roach dbname=roach sslmode=disable"

./weatherlink-sql-backfill
```

## Configuration

### Environment Variables

```bash
POSTGRES_DSN=host=postgres port=5432 user=roach password=xxx dbname=roach sslmode=disable
KAFKA_BROKER=kafka:29092

WORKER_POOL_SIZE=8
BATCH_SIZE=500
BATCH_FLUSH_INTERVAL_MS=1000
DB_POOL_MAX_CONNS=10

TOPICS=weather.iss,weather.barometer,weather.indoor,weather.health
START_OFFSET=-2   # -2=earliest, -1=latest, or specific offset
END_OFFSET=-1     # -1=latest, or specific offset
INCLUDE_METADATA=false

LOG_LEVEL=info
```

### CLI Flags

CLI flags override environment variables:

- `--topics`: Comma-separated list of topics to backfill
- `--start-offset`: Start offset (-2=earliest, -1=latest, or specific)
- `--end-offset`: End offset (-1=latest, or specific)
- `--workers`: Worker pool size
- `--batch-size`: Batch size for database writes
- `--metadata`: Include metadata topics in backfill

## How It Works

### Startup

1. Connect to PostgreSQL with pgx pool
2. Load devices, tags, and catalog into caches
3. If `--metadata` / `INCLUDE_METADATA=true`:
   - Process metadata topics sequentially: sensors → catalog → station
   - Reload devices and catalog into cache
4. Start worker pool
5. Start progress logger
6. Process data topics in parallel

### Data Topic Processing

- Each topic is read using a direct partition reader (partition 0)
- Offsets are set based on `START_OFFSET`
- Messages are submitted to the worker pool and processed asynchronously
- When `END_OFFSET` is set (>=0), processing stops at that offset
- If `END_OFFSET=-1`, the reader stops after a read timeout when no messages remain

### Metadata Phase (Optional)

- `weather.metadata.sensors`: upserts devices
- `weather.metadata.catalog`: upserts catalog entries (per-sensor-type messages)
- `weather.metadata.station`: updates station info for all devices

### Deduplication

Records are deduplicated at the database level via `ON CONFLICT (tag_id, ts) DO NOTHING`.

## Monitoring

### Progress Logs

The service logs progress every 10 seconds when an `END_OFFSET` is set:

```
Progress: weather.iss 5000/15234 (33.0%)
```

### Final Summary

```
=== BACKFILL COMPLETE ===
Messages processed: 45678
Processing errors: 2
Records inserted: numeric=145032, text=8234, null=1043
Total batch flushes: 428
Orphaned messages: 12 (check orphaned_messages table)
========================
```

## Troubleshooting

### Service Won't Start

Common issues:
1. PostgreSQL not ready
2. Kafka not reachable
3. Invalid `POSTGRES_DSN`

### No Data Being Written

1. Verify devices exist in the database
2. Backfill metadata first if needed: `./scripts/weatherlink/sql-backfill.sh --metadata`
3. Check `orphaned_messages` for failure reasons
