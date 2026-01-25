# Weather SQL Materializer

Go-based service that consumes weather data from Kafka topics and materializes it to PostgreSQL using a Device/Tag/Record hierarchy.

## Overview

This service:
- Subscribes to all `weather.*` topics (excluding metadata)
- Listens to `weather.metadata.sensors` for device updates
- Materializes sensor readings to PostgreSQL
- Manages hierarchical data: Devices → Tags → Records
- Auto-creates tags when new fields are discovered
- Tracks orphaned messages for fields without valid devices/tags

## Architecture

```
Kafka Topics → Consumer → PostgreSQL
                ↓
          Device Cache
          Tag Cache
                ↓
     Type-specific Tables
     (numeric, text, null)
```

## Configuration

### Environment Variables

```bash
KAFKA_BROKER=kafka:29092           # Kafka broker address
POSTGRES_DSN=host=postgres port=5432 user=roach password=xxx dbname=roach sslmode=disable
LOG_LEVEL=info                     # Logging level
BATCH_SIZE=100                     # Batch processing size
```

## Database Schema

### Tables
- **devices**: Sensor metadata (LSID, category, location)
- **tags**: Field definitions (temperature, humidity, etc.)
- **records_numeric**: Numeric values - optimized with composite primary key (tag_id, ts)
- **records_text**: Text values - optimized with composite primary key (tag_id, ts)
- **records_null**: Null value tracking - optimized with composite primary key (tag_id, ts)
- **orphaned_messages**: Messages that couldn't be processed

**Note**: Records tables use `ts` (timestamp) instead of `timestamp` to avoid SQL reserved keywords. Device ID is accessible via JOIN with tags table.

### Views
- **records**: Union view of all record types (fields: tag_id, value, value_type, ts)

## Running

### With Docker Compose (Recommended)

```bash
# From project root
docker compose up weather-sql
```

### Standalone with Go

```bash
cd services/weather-sql

# Install dependencies
go mod download

# Set environment variables
export KAFKA_BROKER=localhost:9092
export POSTGRES_DSN="host=localhost port=5432 user=roach password=roach dbname=roach sslmode=disable"

# Run
go run main.go
```

## How It Works

### Startup Sequence

1. Connect to PostgreSQL
2. Load devices into memory cache
3. Load tags into memory cache
4. Start metadata listener (background goroutine)
5. Subscribe to weather data topics
6. Process messages continuously

### Message Processing

1. Extract LSID and timestamp from headers
2. Lookup device in cache (orphan if missing)
3. Parse JSON body
4. For each field:
   - Lookup tag in cache
   - Create tag if missing
   - Determine data type
   - Insert into appropriate records table
5. Commit offset

### Metadata Updates

Separate goroutine listens to `weather.metadata.sensors`:
- Upserts device information
- Refreshes device cache
- Ensures devices are always current

### Tag Auto-Creation

When a new field is encountered:
1. Determine data type from value
2. Create tag in database
3. Add to cache
4. Continue processing

### Orphaned Messages

If processing fails (missing device, database error):
- Save to `orphaned_messages` table
- Include full context (headers, body, reason)
- Can be reprocessed later

## Monitoring

### Check Service Health

```bash
# View logs
docker compose logs -f weather-sql

# Check if running
docker compose ps weather-sql

# Restart service
docker compose restart weather-sql
```

### Database Queries

```sql
-- Check device count
SELECT COUNT(*) FROM devices;

-- Check tag count per device
SELECT d.category, d.product_name, COUNT(t.id) as tag_count
FROM devices d
LEFT JOIN tags t ON d.id = t.device_id
GROUP BY d.id, d.category, d.product_name;

-- Check record counts
SELECT 
  (SELECT COUNT(*) FROM records_numeric) as numeric_records,
  (SELECT COUNT(*) FROM records_text) as text_records,
  (SELECT COUNT(*) FROM records_null) as null_records;

-- Check orphaned messages
SELECT reason, COUNT(*) as count
FROM orphaned_messages
WHERE NOT reprocessed
GROUP BY reason;

-- Query recent records for a device
SELECT r.*, t.tag_name, d.category 
FROM records r
JOIN tags t ON r.tag_id = t.id
JOIN devices d ON t.device_id = d.id
WHERE d.lsid = 918290 
ORDER BY r.ts DESC 
LIMIT 100;
```

## Performance

- **CPU**: 1-3% average
- **Memory**: 50-100 MB
- **Database**: Optimized indexes for fast queries
- **Processing**: ~100-500 messages/second

## Troubleshooting

### Service Won't Start

```bash
# Check logs
docker compose logs weather-sql

# Common issues:
# 1. PostgreSQL not ready (wait for health check)
# 2. Kafka not reachable
# 3. Invalid POSTGRES_DSN
```

### No Data Being Written

1. Verify Kafka topics have data
2. Check device cache is populated
3. Look for errors in logs
4. Query orphaned_messages table

### Orphaned Messages Accumulating

Check reasons:
```sql
SELECT reason, COUNT(*) FROM orphaned_messages GROUP BY reason;
```

If "missing_device":
- Ensure weather-publish service is running
- Verify metadata is being published

## Dependencies

- `github.com/lib/pq` - PostgreSQL driver
- `github.com/segmentio/kafka-go` - Kafka client
