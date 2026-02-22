# ROACH Scripts

Management scripts for the ROACH system. All scripts should be run from the project root directory.

## Quick Start

```bash
# Start everything
./scripts/start-all.sh

# Check system status
./scripts/status.sh

# View logs
./scripts/logs.sh

# Stop everything
./scripts/stop-all.sh
```

---

## System Management

### start-all.sh

Start all services (infrastructure + application services).

**Usage:**
```bash
./scripts/start-all.sh [build] [clean]
```

**Options:**
- `build` - Rebuild containers before starting (use after code changes)
- `clean` - Remove volumes before starting (fixes Kafka cluster ID mismatches)

**Examples:**
```bash
# Normal start
./scripts/start-all.sh

# Rebuild and start (after code changes)
./scripts/start-all.sh build

# Clean slate (removes all data)
./scripts/start-all.sh clean

# Rebuild with clean slate
./scripts/start-all.sh build clean
```

**Note:** Automatically retries startup if Kafka health check fails during initial Zookeeper session recovery.

---

### start-infra.sh

Start only infrastructure services (Zookeeper, Kafka, PostgreSQL, Kafka UI).

**Usage:**
```bash
./scripts/start-infra.sh
```

**Use Case:** Local service development - start infrastructure in Docker, run application services locally outside Docker.

**Access Points:**
- Kafka UI: http://localhost:8080
- Kafka Broker: localhost:9092
- PostgreSQL: localhost:5432 (user: roach, db: roach)

---

### stop-all.sh

Stop all ROACH services.

**Usage:**
```bash
./scripts/stop-all.sh [clean]
```

**Options:**
- `clean` - Remove volumes to ensure fresh start (fixes cluster ID mismatches)

**Examples:**
```bash
# Stop services (keeps data)
./scripts/stop-all.sh

# Stop and remove all data
./scripts/stop-all.sh clean
```

---

### restart-all.sh

Restart services without rebuilding.

**Usage:**
```bash
./scripts/restart-all.sh [service_name]
```

**Options:**
- No arguments - Restart all services
- `service_name` - Restart specific service only

**Service Names:**
- `zookeeper`
- `kafka`
- `kafka-ui`
- `postgres`
- `weatherlink-kafka`
- `weatherlink-sql`
- `homeassistant-kafka`
- `homeassistant-command`

**Examples:**
```bash
# Restart all services
./scripts/restart-all.sh

# Restart specific service
./scripts/restart-all.sh weatherlink-kafka
./scripts/restart-all.sh postgres
```

---

### status.sh

Check health and status of all ROACH services.

**Usage:**
```bash
./scripts/status.sh
```

**Output Includes:**
- Docker daemon status
- Container health checks (healthy, unhealthy, no healthcheck)
- Access points (URLs, ports)
- Kafka topics list
- Database statistics (device count, tag count, record counts)
- Disk usage by component
- Quick command reference

**Health Check States:**
- ✅ Healthy
- ⚠️ Running (no healthcheck) or starting
- ❌ Not running or failed

---

### logs.sh

View logs for services.

**Usage:**
```bash
./scripts/logs.sh [service_name]
```

**Options:**
- No arguments - View all service logs (follows)
- `service_name` - View specific service logs (follows)

**Service Names:**
- `zookeeper`
- `kafka`
- `kafka-ui`
- `postgres`
- `weatherlink-kafka`
- `weatherlink-sql`
- `homeassistant-kafka`
- `homeassistant-command`

**Examples:**
```bash
# All logs
./scripts/logs.sh

# Specific service
./scripts/logs.sh weatherlink-kafka
./scripts/logs.sh homeassistant-command
```

**Note:** Logs follow by default (Ctrl+C to exit).

---

## Database Operations

### db/query.sh

Interactive database query tool for common operations.

**Usage:**
```bash
./scripts/db/query.sh [command] [arguments]
```

**Commands:**

#### stats
Show database statistics (device count, tag count, record counts, orphaned messages).

```bash
./scripts/db/query.sh stats
```

**Output:** Total counts for devices, tags, numeric/text/null records, and pending orphaned messages.

---

#### devices
List all devices with category, product name, and tag count.

```bash
./scripts/db/query.sh devices
```

**Output:** Table with LSID, category, product_name, and tag count per device.

---

#### tags
List tags for all devices or a specific device.

```bash
./scripts/db/query.sh tags [lsid]
```

**Options:**
- No arguments - Summary of tags per device
- `lsid` - Detailed tags for specific device (tag_name, data_type, record_count)

**Examples:**
```bash
# All devices tag summary
./scripts/db/query.sh tags

# Tags for device 918290
./scripts/db/query.sh tags 918290
```

---

#### recent
Show recent records (last 20).

```bash
./scripts/db/query.sh recent [lsid]
```

**Options:**
- No arguments - Recent records across all devices
- `lsid` - Recent records for specific device only

**Examples:**
```bash
# Recent records (all devices)
./scripts/db/query.sh recent

# Recent records for device 918290
./scripts/db/query.sh recent 918290
```

**Output:** Timestamp, device LSID, category, tag name, value, and value type.

---

#### orphans
Show orphaned messages grouped by reason.

```bash
./scripts/db/query.sh orphans
```

**Output:** Topic, LSID, tag name, reason, and count of orphaned messages.

**Common Reasons:**
- `missing_device` - Device not found in database (need metadata backfill)
- `failed_to_parse` - JSON parsing error
- `failed_to_create_tag` - Database error creating tag

---

#### psql
Open interactive PostgreSQL session.

```bash
./scripts/db/query.sh psql
```

**Use Case:** Run custom SQL queries, inspect schema, manual data operations.

**Commands in psql:**
- `\dt` - List tables
- `\d table_name` - Describe table schema
- `\q` - Exit psql

---

### db/migrate.sh

Database migration framework for schema changes.

**Usage:**
```bash
./scripts/db/migrate.sh <command> [arguments]
```

**Commands:**

#### status
Show migration status (applied vs pending).

```bash
./scripts/db/migrate.sh status
```

**Output:** 
- ✓ Applied migrations (with timestamp)
- ○ Pending migrations
- Summary: Total, applied, pending counts

---

#### up
Apply all pending migrations.

```bash
./scripts/db/migrate.sh up
```

**Behavior:**
- Applies migrations in version order (001, 002, 003, etc.)
- Skips already-applied migrations
- Stops on first error
- Each migration runs in a transaction (atomic)
- Updates `schema_migrations` table

**Safety:** Safe to run multiple times (idempotent).

---

#### down
Rollback the last applied migration.

```bash
./scripts/db/migrate.sh down
```

**Behavior:**
- Prompts for confirmation (`yes` required)
- Rolls back most recent migration
- Removes entry from `schema_migrations` table
- Runs in transaction (atomic)

**Warning:** Only rolls back ONE migration at a time.

---

#### create
Create a new migration file pair (.up.sql and .down.sql).

```bash
./scripts/db/migrate.sh create <migration_name>
```

**Arguments:**
- `migration_name` - Descriptive name (use snake_case)

**Examples:**
```bash
./scripts/db/migrate.sh create add_user_table
./scripts/db/migrate.sh create add_temperature_index
./scripts/db/migrate.sh create modify_device_schema
```

**Output:**
- `NNN_migration_name.up.sql` - Forward migration
- `NNN_migration_name.down.sql` - Rollback migration

**Workflow:**
1. Create migration: `./scripts/db/migrate.sh create add_column`
2. Edit generated files: `scripts/db/migrations/00N_add_column.up.sql` and `.down.sql`
3. Review migration: `./scripts/db/migrate.sh status`
4. Apply migration: `./scripts/db/migrate.sh up`
5. Test rollback: `./scripts/db/migrate.sh down` (optional)

**Best Practices:**
- Always create both up and down migrations
- Test rollback before production
- Use `IF EXISTS`/`IF NOT EXISTS` for idempotency
- Backup data before migrations
- Descriptive migration names

---

### db/reload-orphans.sh

Interactive tool to reprocess orphaned messages.

**Usage:**
```bash
./scripts/db/reload-orphans.sh
```

**Behavior:**
1. Shows orphaned message count grouped by reason
2. Prompts for confirmation
3. Restarts `weatherlink-sql` service to trigger reprocessing

**When to Use:**
- After fixing root cause of orphaned messages
- After running metadata backfill (fixes `missing_device` orphans)
- After schema fixes (fixes `failed_to_create_tag` orphans)

**Process:**
1. Identify orphan reason: `./scripts/db/query.sh orphans`
2. Fix root cause (e.g., run metadata backfill)
3. Reprocess orphans: `./scripts/db/reload-orphans.sh`
4. Monitor: `./scripts/logs.sh weatherlink-sql`

---

## Home Assistant Operations

### homeassistant/send-command.sh

Send thermostat commands to the `homeassistant.command` Kafka topic for the `homeassistant-command` service to execute.

**Usage:**
```bash
./scripts/homeassistant/send-command.sh <service> [value] [--entity <entity_id>]
```

**Services:**

| Service | Value | Example |
|---|---|---|
| `set_temperature` | Temperature (number) | `set_temperature 72` |
| `set_hvac_mode` | Mode (off, heat, cool, heat_cool, auto) | `set_hvac_mode heat` |
| `set_preset_mode` | Preset (away, home, sleep) | `set_preset_mode away` |
| `set_fan_mode` | Fan mode (auto, on) | `set_fan_mode auto` |
| `turn_on` | _(none)_ | `turn_on` |
| `turn_off` | _(none)_ | `turn_off` |

**Options:**
- `--entity <entity_id>` - Target entity (default: `$HA_THERMOSTAT_ENTITY` or `climate.sneaux`)

**Examples:**
```bash
# Set temperature to 72
./scripts/homeassistant/send-command.sh set_temperature 72

# Set HVAC mode to heat
./scripts/homeassistant/send-command.sh set_hvac_mode heat

# Set preset mode to away
./scripts/homeassistant/send-command.sh set_preset_mode away

# Target a specific entity
./scripts/homeassistant/send-command.sh set_temperature 68 --entity climate.sneaux

# Turn off the thermostat
./scripts/homeassistant/send-command.sh turn_off
```

**Environment Variables:**
- `HA_THERMOSTAT_ENTITY` - Default entity ID (default: `climate.sneaux`)
- `KAFKA_CONTAINER` - Docker container name (default: `roach-kafka`)
- `KAFKA_TOPIC` - Kafka topic (default: `homeassistant.command`)

**Prerequisites:** Kafka infrastructure must be running (`./scripts/start-infra.sh`).

---

## CoreML services (coreml-smart-crop, coreml-face-crop, coreml-vehicle-detect)

Native macOS Swift services. Run on the host (not Docker). All scripts run from project root.

**Model installation**
- **coreml-smart-crop**: run `./scripts/models/download-yolo.sh` (or follow its instructions) to obtain `./data/models/yolo.mlpackage`.
- **coreml-vehicle-detect**: run `./scripts/models/download-car-model.sh` to download `./data/models/CarRecognition.mlmodel` (and `.mlmodelc`).
- **coreml-face-crop**: no external model.

### coreml-smart-crop

Watches `data/streams/unifi/protect/smart` (person/package/animal/vehicle), YOLO Core ML detection, crops to best bbox per type, writes to `data/streams/coreml/{person|package|animal|vehicle}/`. See [docs/coreml-smart-crop.md](../docs/coreml-smart-crop.md).

| Script | Description |
|--------|-------------|
| `scripts/coreml-smart-crop/build/build.sh [release\|clean]` | Build Swift package (release = release build; clean = full rebuild) |
| `scripts/coreml-smart-crop/run/detect.sh` | Run in foreground |
| `scripts/coreml-smart-crop/run/start.sh` | Start daemon |
| `scripts/coreml-smart-crop/run/stop.sh` | Stop daemon (also kills any lingering CoreMLSmartCrop processes from this project) |
| `scripts/coreml-smart-crop/run/status.sh` | Diagnostics |
| `scripts/coreml-smart-crop/run/logs.sh [lines]` | Tail log file |

Logs: `data/logs/coreml-smart-crop.log`. PID: `data/logs/coreml-smart-crop.pid`.

### coreml-face-crop

Watches `data/streams/coreml/person`, detects faces with Vision, crops each face to `data/streams/coreml/faces/`. See [docs/coreml-face-crop.md](../docs/coreml-face-crop.md).

| Script | Description |
|--------|-------------|
| `scripts/coreml-face-crop/build/build.sh [release\|clean]` | Build Swift package (release = release build; clean = full rebuild) |
| `scripts/coreml-face-crop/run/detect.sh` | Run in foreground |
| `scripts/coreml-face-crop/run/start.sh` | Start daemon |
| `scripts/coreml-face-crop/run/stop.sh` | Stop daemon (also kills any lingering CoreMLFaceCrop processes) |
| `scripts/coreml-face-crop/run/status.sh` | Diagnostics |
| `scripts/coreml-face-crop/run/logs.sh [lines]` | Tail log file |

Logs: `data/logs/coreml-face-crop.log`. PID: `data/logs/coreml-face-crop.pid`.

### coreml-vehicle-detect

Watches `data/streams/coreml/vehicle`, runs CompCars-based Core ML make/model classifier, publishes to Kafka `detect.vehicle`. See [docs/coreml-vehicle-detect.md](../docs/coreml-vehicle-detect.md).

| Script | Description |
|--------|-------------|
| `scripts/coreml-vehicle-detect/build/build.sh [release\|clean]` | Build Swift package (release = release build; clean = full rebuild) |
| `scripts/coreml-vehicle-detect/run/detect.sh` | Run in foreground |
| `scripts/coreml-vehicle-detect/run/start.sh` | Start daemon |
| `scripts/coreml-vehicle-detect/run/stop.sh` | Stop daemon (also kills any lingering CoreMLVehicleDetect processes) |
| `scripts/coreml-vehicle-detect/run/status.sh` | Diagnostics |
| `scripts/coreml-vehicle-detect/run/logs.sh [lines]` | Tail log file |

Logs: `data/logs/coreml-vehicle-detect.log`. PID: `data/logs/coreml-vehicle-detect.pid`.

---

## Backfill Operations

### weatherlink/kafka-backfill.sh

Backfill historical data from WeatherLink API to Kafka topics.

**Data Flow:** WeatherLink API → Kafka

**Usage:**
```bash
./scripts/weatherlink/kafka-backfill.sh [options]
```

**Options:**
- `--start` - Start timestamp (required)
- `--end` - End timestamp (optional, defaults to now)
- `--requests-per-second` - Rate limit (default: 8, max: 10)
- `--workers` - Parallel workers (default: 4)

**Timestamp Formats:**
- Unix timestamp: `1768780863`
- Full datetime: `2026-01-11 18:20:47`
- ISO 8601: `2026-01-11T18:20:47`
- Date only: `2026-01-11` (assumes 00:00:00)

**Examples:**
```bash
# Backfill last 24 hours (Unix timestamp)
./scripts/weatherlink/kafka-backfill.sh --start $(date -v-24H +%s)

# Backfill last 7 days
./scripts/weatherlink/kafka-backfill.sh --start $(date -v-7d +%s)

# Backfill specific date range (datetime strings)
./scripts/weatherlink/kafka-backfill.sh --start "2026-01-11 18:20:47" --end "2026-01-12 18:20:47"

# Backfill with date only
./scripts/weatherlink/kafka-backfill.sh --start "2026-01-11" --end "2026-01-12"

# Slower rate (if hitting API limits)
./scripts/weatherlink/kafka-backfill.sh --start "2026-01-11" --requests-per-second 5

# More workers for faster processing
./scripts/weatherlink/kafka-backfill.sh --start "2026-01-11" --workers 8
```

**How It Works:**
1. Fetches sensor metadata to determine topic routing
2. Scans existing Kafka topics to build deduplication cache
3. Splits time range into 24-hour windows (API limit)
4. Processes windows in parallel with rate limiting
5. Publishes to Kafka with unique keys (lsid:timestamp)
6. Client-side deduplication prevents duplicates

**Performance:**
- Rate limit: 8 req/s (80% of 10/s API limit)
- Parallel workers: 4 (configurable)
- Burst capacity: 16 requests
- Exponential backoff on 429 errors

**Safety:**
- Idempotent - safe to run multiple times
- Client-side deduplication skips existing messages
- Respects API rate limits with exponential backoff
- Logs progress and statistics

**Monitoring:**
- Window progress (1/N, 2/N, etc.)
- Messages published vs skipped
- Rate limit usage
- API errors and retries

**When to Use:**
- Kafka topics are missing historical data
- Need to populate Kafka with API history
- After Kafka data loss or new topics

---

### weatherlink/sql-backfill.sh

Backfill historical data from Kafka topics to PostgreSQL.

**Data Flow:** Kafka → PostgreSQL

**Usage:**
```bash
./scripts/weatherlink/sql-backfill.sh [options]
```

**Options:**
- `--topics` - Comma-separated topic list (default: all data topics)
- `--start-offset` - Start offset (-2=earliest, -1=latest, or specific)
- `--end-offset` - End offset (-1=latest, or specific)
- `--workers` - Worker pool size (default: 8)
- `--batch-size` - Batch size for writes (default: 500)
- `--metadata` - Include metadata topics (default: false)

**Examples:**
```bash
# Backfill all data topics from beginning
./scripts/weatherlink/sql-backfill.sh

# Backfill with metadata (fresh database)
./scripts/weatherlink/sql-backfill.sh --metadata

# Backfill specific topics
./scripts/weatherlink/sql-backfill.sh --topics weather.iss,weather.barometer

# Backfill from specific offset
./scripts/weatherlink/sql-backfill.sh --start-offset 10000

# High-performance backfill
./scripts/weatherlink/sql-backfill.sh --workers 16 --batch-size 1000

# Backfill metadata topics only
./scripts/weatherlink/sql-backfill.sh --metadata --topics ""

# Backfill latest messages only
./scripts/weatherlink/sql-backfill.sh --start-offset -1
```

**How It Works:**
1. Connects to PostgreSQL and loads caches (devices, tags, catalog)
2. Starts worker pool (8 workers by default)
3. Creates Kafka readers with configured offsets
4. Processes messages in parallel:
   - Looks up device in cache (orphans if missing)
   - Creates/enriches tags with catalog metadata
   - Batches records for bulk insert
5. Flushes batches on size (500) or time (1000ms)
6. Uses PostgreSQL COPY protocol for bulk inserts
7. Deduplicates via `ON CONFLICT DO NOTHING`

**Metadata Processing:**

When `--metadata` flag is enabled, processes in two phases:

**Phase 1: Metadata (sequential)**
1. `weather.metadata.sensors` → Creates devices
2. `weather.metadata.catalog` → Creates field metadata
3. `weather.metadata.station` → Updates station info

**Phase 2: Data (parallel)**
- `weather.iss`, `weather.barometer`, `weather.indoor`, `weather.health`

**Why Phases:** Devices must exist before processing data messages, otherwise data becomes orphaned with "missing_device" errors.

**Performance:**
- Typical: 500-1000 messages/second
- Database: 5000-10000 records/second
- 90% lower database load vs individual INSERTs

**Tuning:**
```bash
# Maximum throughput
./scripts/weatherlink/sql-backfill.sh --workers 16 --batch-size 1000

# Lower latency (catch up quickly)
./scripts/weatherlink/sql-backfill.sh --batch-size 200 --workers 8

# Memory-constrained
./scripts/weatherlink/sql-backfill.sh --workers 4 --batch-size 250
```

**Safety:**
- Separate consumer group: `weatherlink-sql-backfill`
- Doesn't interfere with real-time `weatherlink-sql`
- Idempotent - safe to run multiple times
- Automatic deduplication at database level

**Monitoring:**
```bash
# View progress
docker compose logs -f weatherlink-sql-backfill

# Check database counts
./scripts/db/query.sh stats

# Check orphaned messages
./scripts/db/query.sh orphans
```

**When to Use:**
- Database is missing records but Kafka has complete data
- Real-time materializer was down and needs to catch up
- Rebuilding database from Kafka (e.g., after schema migration)
- Need to re-materialize specific topics

**Common Workflows:**

**Fresh Database Setup:**
```bash
# 1. Backfill metadata first
./scripts/weatherlink/sql-backfill.sh --metadata

# 2. Backfill data
./scripts/weatherlink/sql-backfill.sh
```

**Database Rebuild:**
```bash
# 1. Stop real-time materializer
docker compose stop weatherlink-sql

# 2. Truncate tables (optional)
./scripts/db/query.sh psql
# TRUNCATE devices, tags, sensor_catalog, records_numeric, records_text, records_null CASCADE;

# 3. Backfill with metadata
./scripts/weatherlink/sql-backfill.sh --metadata

# 4. Restart materializer
docker compose start weatherlink-sql
```

**Troubleshooting:**

**No data being written:**
1. Check if devices exist: `./scripts/db/query.sh stats`
2. If 0 devices, run: `./scripts/weatherlink/sql-backfill.sh --metadata`
3. Check orphaned messages: `./scripts/db/query.sh orphans`
4. View logs: `docker compose logs weatherlink-sql-backfill`

**Orphaned messages accumulating:**
1. Check reasons: `./scripts/db/query.sh orphans`
2. If "missing_device": Run `./scripts/weatherlink/sql-backfill.sh --metadata`
3. Reprocess orphans: `./scripts/db/reload-orphans.sh`

---

## Advanced Operations

### Direct Docker Compose Commands

All scripts are wrappers around Docker Compose commands. For advanced control:

```bash
# Start with compose files
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d

# Stop and remove volumes
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v

# Rebuild specific service
docker compose build weatherlink-kafka

# View resolved configuration
docker compose config

# Execute command in container
docker exec -it roach-postgres psql -U roach -d roach
```

### Service Development

```bash
# Start infrastructure only
./scripts/start-infra.sh

# Run service locally (outside Docker)
cd services/weatherlink-kafka
export KAFKA_BROKER=localhost:9092
export POSTGRES_DSN="host=localhost port=5432 user=roach password=xxx dbname=roach sslmode=disable"
export WEATHERLINK_API_KEY=your_key
export WEATHERLINK_API_SECRET=your_secret
export WEATHERLINK_STATION_ID=your_station
go run main.go
```

### Kafka Operations

```bash
# List topics
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# Consume messages
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --from-beginning \
  --max-messages 10

# Consumer groups
docker exec roach-kafka kafka-consumer-groups \
  --list \
  --bootstrap-server localhost:29092

# Topic details
docker exec roach-kafka kafka-topics \
  --describe \
  --topic weather.iss \
  --bootstrap-server localhost:29092
```

### Database Backup and Restore

```bash
# Backup database
docker exec roach-postgres pg_dump -U roach roach > backup-$(date +%Y%m%d).sql

# Restore database
cat backup.sql | docker exec -i roach-postgres psql -U roach -d roach

# Full data backup (Kafka + Zookeeper + PostgreSQL)
./scripts/stop-all.sh
tar -czf roach-backup-$(date +%Y%m%d).tar.gz data/
./scripts/start-all.sh

# Full data restore
./scripts/stop-all.sh
rm -rf data/
tar -xzf roach-backup-YYYYMMDD.tar.gz
./scripts/start-all.sh
```

---

## Troubleshooting

### Services won't start
```bash
# Check logs
./scripts/logs.sh

# Check Docker daemon
docker info

# Clean volumes and restart
./scripts/stop-all.sh clean
./scripts/start-all.sh clean
```

### Kafka health check fails
```bash
# Usually resolves with automatic retry
# If persistent:
./scripts/stop-all.sh clean
./scripts/start-all.sh clean
```

### Database connection errors
```bash
# Check PostgreSQL is running
./scripts/status.sh

# Check logs
./scripts/logs.sh postgres

# Restart PostgreSQL
./scripts/restart-all.sh postgres
```

### Orphaned messages accumulating
```bash
# Check reasons
./scripts/db/query.sh orphans

# If "missing_device", run metadata backfill
./scripts/weatherlink/sql-backfill.sh --metadata

# Reprocess orphans
./scripts/db/reload-orphans.sh
```

### Disk space issues
```bash
# Check disk usage
./scripts/status.sh

# View detailed usage
du -sh data/*

# Clean up (WARNING: deletes all data)
./scripts/stop-all.sh clean
```

---

## Common Workflows

### Fresh System Setup
```bash
cp .env.example .env
vim .env  # Configure credentials
./scripts/start-all.sh build
./scripts/status.sh
```

### After Code Changes
```bash
./scripts/start-all.sh build
./scripts/logs.sh weatherlink-kafka  # Check for errors
```

### Daily Monitoring
```bash
./scripts/status.sh
./scripts/db/query.sh stats
./scripts/db/query.sh orphans
```

### Backfill Historical Data
```bash
# 1. API → Kafka
./scripts/weatherlink/kafka-backfill.sh --start "2026-01-01" --end "2026-01-15"

# 2. Kafka → DB (with metadata)
./scripts/weatherlink/sql-backfill.sh --metadata

# 3. Verify
./scripts/db/query.sh stats
```

### Database Migration
```bash
# 1. Create migration
./scripts/db/migrate.sh create add_new_column

# 2. Edit migration files
vim scripts/db/migrations/00N_add_new_column.up.sql
vim scripts/db/migrations/00N_add_new_column.down.sql

# 3. Check status
./scripts/db/migrate.sh status

# 4. Apply migration
./scripts/db/migrate.sh up

# 5. Verify
./scripts/db/query.sh psql
# \d table_name
```

---

## Environment Variables

All scripts respect environment variables from `.env` file. Key variables:

**WeatherLink API:**
- `WEATHERLINK_API_KEY` - API key (required)
- `WEATHERLINK_API_SECRET` - API secret (required)
- `WEATHERLINK_STATION_ID` - Station ID (required)

**Database:**
- `POSTGRES_PASSWORD` - PostgreSQL password (required)
- `POSTGRES_DSN` - Full connection string (optional, auto-generated)

**Kafka:**
- `KAFKA_BROKER` - Broker address (default: kafka:29092)

**Application:**
- `FETCH_INTERVAL` - API fetch interval (default: 5m)
- `LOG_LEVEL` - Log verbosity: debug, info, warn, error (default: info)
- `WORKER_POOL_SIZE` - Worker pool size (default: varies by service)
- `BATCH_SIZE` - Batch size for writes (default: varies by service)

---

## Related Documentation

- **[CLAUDE.md](../CLAUDE.md)** - Single-file overview (start here)
- **[operations.md](../docs/operations.md)** - Advanced operations and maintenance
- **[troubleshooting.md](../docs/troubleshooting.md)** - Problem solving guide
- **[architecture.md](../docs/architecture.md)** - System design details
- **[migrations.md](../docs/migrations.md)** - Migration framework details
