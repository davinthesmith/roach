# WeatherLink API Backfill Service

Historical data backfill service for WeatherLink weather stations that fetches data from the WeatherLink API and publishes to Kafka topics.

**Data Flow**: WeatherLink API → Kafka

**Use Case**: Populate Kafka with historical data from the API when Kafka topics are missing historical data.

**Note**: This service backfills from WeatherLink API to Kafka. For Kafka→DB backfill, see `weatherlink-kafka-backfill`.

## Overview

This service fetches historical weather data from the WeatherLink API and publishes it to Kafka topics. It uses:

- **24-hour windows**: API limits historic requests to 24-hour windows
- **Conservative rate limiting**: 8 requests/second (80% of 10/s limit)
- **Idempotent producer**: Kafka automatically prevents duplicate messages
- **Automatic window splitting**: Breaks large date ranges into 24-hour chunks

## Usage

### Command Line

```bash
# Backfill from start timestamp to end timestamp (Unix timestamps)
./weatherlink-api-backfill --start 1768780863 --end 1768865863

# Backfill using datetime strings
./weatherlink-api-backfill --start "2026-01-11 18:20:47" --end "2026-01-12 18:20:47"

# Backfill from start to now
./weatherlink-api-backfill --start 1768780863

# Backfill from datetime to now
./weatherlink-api-backfill --start "2026-01-11 18:20:47"

# Custom rate limiting and parallelism
./weatherlink-api-backfill --start 1768780863 --requests-per-second 5 --workers 8
```

**Supported timestamp formats:**
- Unix timestamps: `1768780863`
- Full datetime: `2026-01-11 18:20:47`
- ISO 8601: `2026-01-11T18:20:47`
- Date and time (no seconds): `2026-01-11 18:20`
- Date only: `2026-01-11` (assumes 00:00:00)

### Docker

```bash
# Using helper script (recommended) - Unix timestamp
./scripts/api-backfill.sh --start $(date -v-24H +%s)

# Using datetime strings
./scripts/api-backfill.sh --start "2026-01-11 18:20:47" --end "2026-01-12 18:20:47"

# Or with full command
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml run --rm weatherlink-api-backfill --start $(date -v-24H +%s)

# With custom args
./scripts/api-backfill.sh --start $(date -v-24H +%s) --end $(date +%s) --requests-per-second 5 --workers 8

# Using datetime format with date command
./scripts/api-backfill.sh --start "$(date -v-24H '+%Y-%m-%d %H:%M:%S')"
```

## Configuration

Environment variables (use `.env` file):

- `WEATHERLINK_API_KEY` - WeatherLink API key (required)
- `WEATHERLINK_API_SECRET` - WeatherLink API secret (required)
- `WEATHERLINK_STATION_ID` - Weather station ID (required)
- `KAFKA_BROKER` - Kafka broker address (default: kafka:29092)
- `LOG_LEVEL` - Log level: debug, info, warn, error (default: info)

## How It Works

1. Fetches sensor metadata to determine topic routing
2. Scans existing Kafka topics to build deduplication cache
3. Splits time range into 24-hour windows (API limit)
4. Processes windows in parallel (4 workers):
   - Applies rate limiting (8 req/s shared across workers)
   - Fetches historic data from API
   - Publishes to Kafka asynchronously with unique keys (lsid:timestamp)
   - Uses client-side deduplication cache to skip existing messages
5. Flushes all pending Kafka messages before completion
6. Logs progress and statistics

**Performance optimizations:**
- **Parallel window processing**: 4 workers process different 24-hour windows simultaneously
- **Async Kafka publishing**: Messages are batched and sent without waiting for individual confirmations
- **Shared rate limiter**: All workers respect the same API rate limit

## Duplicate Prevention

The service uses **client-side deduplication** by scanning existing Kafka topics:

1. Before backfilling, scans all weather topics to find existing message keys
2. Builds in-memory cache of existing keys (lsid:timestamp format)
3. During backfill, checks each message key against cache before publishing
4. Skips messages that already exist in Kafka
5. Adds new keys to cache after successful publish

This approach ensures:
- No duplicate messages even when running backfill multiple times
- Works regardless of Kafka's idempotent producer settings
- Transparent reporting of new vs skipped messages

**Note**: Kafka's idempotent producer prevents duplicate messages only from network retries (same producer request), not from re-running backfill operations (different producer requests). Application-level deduplication is required.

### Testing Idempotency

To verify that duplicates are prevented:

```bash
# Automated test script
./scripts/test-backfill.sh
```

This script:
1. Counts messages in Kafka topic before backfill
2. Runs backfill with fixed timestamps
3. Counts messages after first run (should increase)
4. Runs SAME backfill again with same timestamps
5. Counts messages after second run (should NOT increase - duplicates skipped)
6. Verifies the skip count from logs matches expected duplicates

The service will log:
```
Published X new messages, skipped Y duplicates
```

**Manual testing**:

```bash
# Get topic message count BEFORE backfill
BEFORE=$(docker exec roach-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 --topic weather.iss --time -1 | \
  awk -F: '{sum += $3} END {print sum}')

# Run backfill with specific timestamps
./scripts/backfill.sh --start 1769359875 --end 1769363477

# Get count AFTER first run
AFTER1=$(docker exec roach-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 --topic weather.iss --time -1 | \
  awk -F: '{sum += $3} END {print sum}')

# Run SAME backfill again (same timestamps!)
./scripts/backfill.sh --start 1769359875 --end 1769363477

# Get count AFTER second run
AFTER2=$(docker exec roach-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 --topic weather.iss --time -1 | \
  awk -F: '{sum += $3} END {print sum}')

echo "Before: $BEFORE, After 1st: $AFTER1, After 2nd: $AFTER2"
# AFTER1 should be > BEFORE (new messages added)
# AFTER2 should equal AFTER1 (duplicates skipped by client)
```

## Rate Limiting

Conservative rate limiting to stay well under API limits:

- 8 requests/second (80% of 10/s limit)
- Burst capacity: 16 requests
- Exponential backoff on 429 errors: 1s → 2s → 4s → 8s
- Tracks hourly requests (1000/hour limit)

## Examples

```bash
# Backfill last 7 days (Unix timestamp)
./scripts/api-backfill.sh --start $(date -v-7d +%s)

# Backfill last 24 hours (Unix timestamp)
./scripts/api-backfill.sh --start $(date -v-24H +%s)

# Backfill last 24 hours (datetime format)
./scripts/api-backfill.sh --start "$(date -v-24H '+%Y-%m-%d %H:%M:%S')"

# Backfill specific date range (using Unix timestamps)
./scripts/api-backfill.sh --start 1768780863 --end 1768865863

# Backfill specific date range (using datetime strings)
./scripts/api-backfill.sh --start "2026-01-11 18:20:47" --end "2026-01-12 18:20:47"

# Backfill with date only (assumes 00:00:00)
./scripts/api-backfill.sh --start "2026-01-11" --end "2026-01-12"

# Backfill with slower rate (if hitting limits)
./scripts/api-backfill.sh --start "2026-01-11 18:20:47" --requests-per-second 5

# Backfill with more workers for faster processing
./scripts/api-backfill.sh --start "2026-01-11" --workers 8
```

## Monitoring

The service logs:

- Window progress (1/N, 2/N, etc.)
- Messages published per window
- Rate limit status (requests used)
- Duplicate detection events
- API errors and retries

## Error Handling

- **API Errors**: Retry with exponential backoff (up to 3 attempts)
- **Kafka Errors**: Log and continue (don't fail entire backfill)
- **Rate Limit (429)**: Wait with exponential backoff, then retry
- **Invalid Window**: Fail fast with clear error message
