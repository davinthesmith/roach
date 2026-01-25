# WeatherLink Kafka Backfill Service

Kafka to PostgreSQL backfill service for materializing historical weather data from Kafka topics to the database.

**Data Flow**: Kafka → PostgreSQL

**Use Case**: Populate PostgreSQL with historical data from Kafka when the database is behind but Kafka topics contain complete data.

**Note**: This service backfills from Kafka to PostgreSQL. For API→Kafka backfill, see `weatherlink-api-backfill`.

## Overview

This service replays messages from Kafka topics and materializes them to PostgreSQL using the same processing pipeline as `weatherlink-materializer`, but with:

- **Configurable offset ranges**: Start from earliest, latest, or specific offsets
- **One-shot execution**: Runs to completion then exits (not a long-running daemon)
- **Separate consumer group**: Uses `weatherlink-kafka-backfill` to avoid interfering with real-time materializer
- **Progress tracking**: Logs backfill progress for each topic
- **Parallel processing**: Worker pool for concurrent message processing
- **Batched writes**: Bulk inserts using PostgreSQL COPY protocol
- **Deduplication**: `ON CONFLICT DO NOTHING` prevents duplicate records

## When to Use

Use this service when:
- Kafka has complete historical data but database is missing records
- Real-time materializer was down and needs to catch up
- You need to rebuild database from Kafka (e.g., after schema migration)
- Specific topics need to be re-materialized

## Usage

### Command Line

```bash
# Backfill all topics from earliest to latest
./weatherlink-kafka-backfill

# Backfill specific topics
./weatherlink-kafka-backfill --topics weather.iss,weather.barometer

# Backfill from specific offset range
./weatherlink-kafka-backfill --start-offset 0 --end-offset 10000

# Custom performance settings
./weatherlink-kafka-backfill --workers 16 --batch-size 1000

# Backfill from latest (only new messages)
./weatherlink-kafka-backfill --start-offset -1
```

### Docker

```bash
# Using helper script (recommended)
./scripts/kafka-backfill.sh

# With custom arguments
./scripts/kafka-backfill.sh --topics weather.iss --workers 16

# Full docker compose command
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml run --rm weatherlink-kafka-backfill --topics weather.iss

# Override environment variables
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml run --rm \
  -e WORKER_POOL_SIZE=16 \
  -e BATCH_SIZE=1000 \
  weatherlink-kafka-backfill
```

### Metadata Backfill

The `--metadata` flag enables backfilling of metadata topics (devices, catalog, station) in addition to data topics.

**When to use**: When starting from an empty database or when device/catalog metadata is missing.

```bash
# Backfill metadata topics only
./scripts/kafka-backfill.sh --metadata --topics ""

# Backfill metadata AND data topics (recommended for fresh database)
./scripts/kafka-backfill.sh --metadata

# Docker command
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml run --rm \
  weatherlink-kafka-backfill --metadata
```

**Processing order** (when `--metadata` is enabled):

1. **Phase 1: Metadata** (sequential, in foreign-key dependency order)
   - `weather.metadata.sensors` → `devices` table (creates/updates devices)
   - `weather.metadata.catalog` → `sensor_catalog` table (creates/updates field metadata)
   - `weather.metadata.station` → `devices` table (updates station info for all devices)

2. **Phase 2: Data** (parallel)
   - `weather.iss`, `weather.barometer`, `weather.indoor`, `weather.health`
   - Tags and records automatically created from catalog metadata

**Why separate phases**: Devices must exist before processing data messages, otherwise all data messages become orphaned with "missing_device" errors.

**Station metadata handling**: The `weather.metadata.station` topic contains station-level information (station_id, station_name, station_id_uuid) that applies to all devices at that station. Unlike sensor metadata which has an `lsid` field for a specific device, station metadata has a `stations` array and updates all devices belonging to that station.


## Configuration

### Environment Variables

```bash
# Database connection (required)
POSTGRES_DSN=host=postgres port=5432 user=roach password=xxx dbname=roach sslmode=disable

# Kafka connection
KAFKA_BROKER=kafka:29092                    # Kafka broker address

# Performance tuning
WORKER_POOL_SIZE=8                          # Concurrent message processors (default: 8)
BATCH_SIZE=500                              # Records per batch flush (default: 500)
BATCH_FLUSH_INTERVAL_MS=1000                # Max ms between flushes (default: 1000)
DB_POOL_MAX_CONNS=10                        # Max database connections (default: 10)

# Backfill control
TOPICS=weather.iss,weather.barometer,weather.indoor,weather.health  # Topics to backfill
START_OFFSET=-2                             # -2=earliest, -1=latest, or specific offset
END_OFFSET=-1                               # -1=latest, or specific offset
INCLUDE_METADATA=false                      # Include metadata topics (default: false)

# Logging
LOG_LEVEL=info                              # Log level (debug, info, warn, error)
```

### CLI Flags

CLI flags override environment variables:

- `--topics`: Comma-separated list of topics to backfill
- `--start-offset`: Start offset (-2=earliest, -1=latest, or specific)
- `--end-offset`: End offset (-1=latest, or specific)
- `--workers`: Number of worker threads
- `--batch-size`: Batch size for database writes
- `--metadata`: Include metadata topics in backfill (default: false)

## How It Works

### Startup Sequence

1. Connect to PostgreSQL with connection pool
2. Load devices into memory cache
3. Load tags into memory cache
4. Load catalog into memory cache
5. Initialize batch writer with COPY protocol support
6. Start worker pool (8 workers by default)
7. Create Kafka readers for each topic with configured offsets
8. Start progress logger (10-second interval)

### Backfill Process

1. **Topic Discovery**: For each configured topic:
   - Create Kafka reader with consumer group `weatherlink-kafka-backfill`
   - Determine actual start/end offsets from configuration
   - Initialize progress tracking

2. **Message Processing**: For each message:
   - Fetch message from Kafka
   - Submit to worker pool (non-blocking)
   - Worker extracts headers (lsid, timestamp, sensor_type, etc.)
   - Lookup device in cache (orphan if missing)
   - Parse JSON body
   - For each field:
     - Lookup/create tag with catalog enrichment
     - Add record to batch buffer
   - Commit offset after submission

3. **Batch Writing**: Background process monitors batches:
   - Flush triggers on size (500 records) or time (1000ms)
   - Uses PostgreSQL COPY protocol for bulk insert
   - Deduplicates via `ON CONFLICT (tag_id, ts) DO NOTHING`

4. **Completion**: When all topics reach end offset:
   - Shutdown worker pool (drain pending messages)
   - Flush all remaining batches
   - Report final statistics
   - Exit cleanly

### Deduplication

Records are automatically deduplicated at the database level:
- Composite primary key: `(tag_id, ts)` on all record tables
- Insert uses `ON CONFLICT DO NOTHING`
- Safe to re-run backfill multiple times
- No duplicate records even if overlapping with real-time materializer

## Database Schema

Uses the same schema as `weatherlink-materializer`:

- **devices**: Sensor metadata (LSID, category, location)
- **tags**: Field definitions (temperature, humidity, etc.)
- **records_numeric**: Numeric values with composite PK (tag_id, ts)
- **records_text**: Text values with composite PK (tag_id, ts)
- **records_null**: Null value tracking with composite PK (tag_id, ts)
- **orphaned_messages**: Messages that couldn't be processed

## Monitoring

### Progress Logs

The service logs progress every 10 seconds:

```
Progress: weather.iss 5000/15234 (33%)
Progress: weather.barometer 2500/8192 (31%)
Progress: weather.indoor 1200/3456 (35%)
Progress: weather.health 800/2100 (38%)
```

### Orphaned Messages

All messages that fail processing are automatically saved to the `orphaned_messages` table with:
- Complete message content (headers + body) for reprocessing
- Kafka topic, partition, and offset for tracking
- Failure reason (missing_device, failed_to_parse, etc.)
- Timestamp for when the orphan was created

**Types of orphaned messages:**

1. **Data messages** (from `weather.*` topics):
   - `missing_device`: Device (LSID) not found in database
   - `failed_to_parse`: JSON parsing error
   - `failed_to_create_tag`: Unable to create tag in database

2. **Metadata messages** (from `weather.metadata.*` topics):
   - `missing or invalid lsid in metadata`: Device metadata missing LSID (not applicable to station metadata)
   - `failed to parse metadata`: JSON parsing error
   - `failed to upsert device`: Database error during device upsert
   - `missing sensor_type in catalog message`: Catalog metadata missing sensor_type
   - `missing data_structures in catalog message`: Catalog missing data structures

**Note**: Station metadata messages (from `weather.metadata.station`) have a different structure with a `stations` array instead of an `lsid` field. These are processed separately to update station information for all devices at that station.

The final statistics report includes orphan count if any messages failed processing.

### Final Statistics

```
=== BACKFILL COMPLETE ===
Messages processed: 45678
Processing errors: 2
Records inserted: numeric=145032, text=8234, null=1043
Total batch flushes: 428
Orphaned messages: 12 (check orphaned_messages table)
========================
```

### Check Backfill Status

```bash
# View logs in real-time
docker compose logs -f weatherlink-kafka-backfill

# Check database record counts
psql -c "SELECT 
  (SELECT COUNT(*) FROM records_numeric) as numeric_records,
  (SELECT COUNT(*) FROM records_text) as text_records,
  (SELECT COUNT(*) FROM records_null) as null_records;"

# Check orphaned messages
psql -c "SELECT reason, COUNT(*) FROM orphaned_messages GROUP BY reason;"
```

## Performance

### Benchmarks

**Typical throughput**:
- 500-1000 messages/second (depending on message size and field count)
- 5000-10000 records/second inserted into database
- 90% lower database load vs individual INSERTs

**Performance vs weatherlink-materializer**:
- Larger batch size (500 vs 100) = fewer database round trips
- More workers (8 vs 4) = higher parallelism
- Longer flush interval (1000ms vs 500ms) = larger batches

### Performance Tuning

**For maximum throughput**:
```bash
--workers 16 --batch-size 1000
```

**For lower latency** (catch up quickly):
```bash
--batch-size 200 --workers 8
```

**For memory-constrained environments**:
```bash
--workers 4 --batch-size 250
```

## Examples

### Complete Database Rebuild

```bash
# 1. Stop real-time materializer
docker compose stop weatherlink-materializer

# 2. Truncate all tables (optional - for clean slate)
psql -c "TRUNCATE devices, tags, sensor_catalog, records_numeric, records_text, records_null CASCADE;"

# 3. Run backfill with metadata first
./scripts/kafka-backfill.sh --metadata

# 4. Restart materializer
docker compose start weatherlink-materializer
```

**Note**: The `--metadata` flag ensures devices and catalog are populated before processing data messages, preventing orphaned messages.

### Backfill Specific Topics

```bash
# Only backfill ISS sensor data
./scripts/kafka-backfill.sh --topics weather.iss

# Backfill indoor and barometer data
./scripts/kafka-backfill.sh --topics weather.indoor,weather.barometer
```

### Backfill Recent Data Only

```bash
# Get current offset for a topic
docker exec roach-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 --topic weather.iss --time -1

# Backfill from offset 10000 to latest
./scripts/kafka-backfill.sh --topics weather.iss --start-offset 10000
```

### High-Performance Backfill

```bash
# Maximum parallelism for large backfills
./scripts/kafka-backfill.sh --workers 16 --batch-size 1000
```

## Troubleshooting

### Service Won't Start

```bash
# Check logs
docker compose logs weatherlink-kafka-backfill

# Common issues:
# 1. PostgreSQL not ready (wait for health check)
# 2. Kafka not reachable
# 3. Invalid POSTGRES_DSN
```

### No Data Being Written

1. **Check if devices exist in database**:
   ```bash
   psql -c "SELECT COUNT(*) FROM devices;"
   ```
   
   If 0, you need to backfill metadata first:
   ```bash
   ./scripts/kafka-backfill.sh --metadata
   ```

2. Verify Kafka topics exist and have data:
   ```bash
   docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092
   ```

3. Look for errors in logs

4. Query orphaned_messages table:
   ```sql
   SELECT reason, COUNT(*) FROM orphaned_messages WHERE NOT reprocessed GROUP BY reason;
   ```

**Common issue**: If you see "missing_device" orphans, devices table is empty. Run backfill with `--metadata` flag to populate devices from Kafka.

### Slow Backfill Performance

1. Increase worker pool size: `--workers 16`
2. Increase batch size: `--batch-size 1000`
3. Increase database connection pool: `-e DB_POOL_MAX_CONNS=20`
4. Check database performance (indexes, disk I/O)

### Orphaned Messages Accumulating

Check reasons:
```sql
SELECT reason, COUNT(*) FROM orphaned_messages WHERE NOT reprocessed GROUP BY reason;
```

**Common reasons and solutions:**

1. **`missing_device`**: Device (LSID) not found in database
   - **Solution**: Run backfill with `--metadata` flag to populate devices from Kafka:
     ```bash
     ./scripts/kafka-backfill.sh --metadata
     ```
   - Verify `weather.metadata.sensors` topic has data:
     ```bash
     docker exec roach-kafka kafka-console-consumer --bootstrap-server localhost:29092 \
       --topic weather.metadata.sensors --from-beginning --max-messages 1
     ```

2. **`failed_to_parse`**: Malformed JSON in message
   - **Solution**: Review message content in `orphaned_messages.message_body`
   - May indicate upstream data quality issue

3. **`failed_to_create_tag`**: Database error creating tag
   - **Solution**: Check database logs for constraint violations or connection issues
   - Verify `tags` table is healthy

4. **`missing or invalid lsid in metadata`**: Station metadata message missing LSID
   - **Solution**: This is now handled automatically - station metadata uses a different structure
   - Station metadata messages have a `stations` array instead of `lsid`
   - The system automatically detects and processes both formats
   - If you see this error, it indicates an unexpected metadata format

**View orphaned message details:**
```sql
SELECT topic, partition, "offset", lsid, reason, created_at 
FROM orphaned_messages 
WHERE NOT reprocessed 
ORDER BY created_at DESC 
LIMIT 10;
```

**Reprocess orphaned messages** (after fixing root cause):
```sql
-- Mark as reprocessed after manual fix
UPDATE orphaned_messages SET reprocessed = true WHERE id = <id>;
```

### Consumer Group Conflicts

If backfill interferes with real-time materializer:
- Backfill uses separate consumer group: `weatherlink-kafka-backfill`
- Materializer uses: `weather-sql-materializer`
- No conflicts should occur

To reset backfill consumer group:
```bash
docker exec roach-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group weatherlink-kafka-backfill --reset-offsets --to-earliest --all-topics --execute
```

## Comparison with Real-time Materializer

| Feature | weatherlink-materializer | weatherlink-kafka-backfill |
|---------|-------------------------|---------------------------|
| **Mode** | Continuous daemon | One-shot execution |
| **Start Offset** | LastOffset (new messages only) | Configurable (earliest/latest/specific) |
| **Consumer Group** | weather-sql-materializer | weatherlink-kafka-backfill |
| **Worker Pool Size** | 4 (default) | 8 (default) |
| **Batch Size** | 100 (default) | 500 (default) |
| **Flush Interval** | 500ms | 1000ms |
| **Use Case** | Real-time streaming | Historical backfill |

## Dependencies

- `github.com/jackc/pgx/v5` - High-performance PostgreSQL driver with COPY protocol
- `github.com/segmentio/kafka-go` - Kafka client

## Related Services

- **weatherlink-materializer**: Real-time Kafka→DB streaming
- **weatherlink-api-backfill**: API→Kafka historical backfill
- **weatherlink-ingest**: Real-time API→Kafka streaming
