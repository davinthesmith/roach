# Operations

> **For basics, see [AI-CONTEXT.md](AI-CONTEXT.md)**. For complete script documentation, see [scripts/README.md](../scripts/README.md).
>
> This document covers advanced operations, maintenance procedures, and deep technical details beyond script usage.

## Quick Reference

**Common Operations:**
- Start system: `./scripts/start-all.sh`
- Check status: `./scripts/status.sh`
- View logs: `./scripts/logs.sh [service]`
- Database queries: `./scripts/db/query.sh [command]`
- Backfill API→Kafka: `./scripts/weatherlink/kafka-backfill.sh --start "date"`
- Backfill Kafka→DB: `./scripts/weatherlink/sql-backfill.sh --metadata`
- Stop system: `./scripts/stop-all.sh`

**See [scripts/README.md](../scripts/README.md) for complete script documentation with all options.**

## System Management

### Service Lifecycle

**Starting Services:**
```bash
# Standard startup
./scripts/start-all.sh

# After code changes (rebuild containers)
./scripts/start-all.sh build

# Clean slate (remove all volumes/data)
./scripts/start-all.sh clean

# Both rebuild and clean
./scripts/start-all.sh build clean

# Infrastructure only (for local service development)
./scripts/start-infra.sh
```

**Stopping Services:**
```bash
# Stop all (preserve data)
./scripts/stop-all.sh

# Stop and remove volumes (clean slate)
./scripts/stop-all.sh clean
```

**Restarting Services:**
```bash
# Restart all services
./scripts/restart-all.sh

# Restart specific service
./scripts/restart-all.sh weatherlink-kafka
./scripts/restart-all.sh postgres
```

## Monitoring

### System Status

```bash
# Comprehensive status check
./scripts/status.sh
```

**Status output includes:**
- Docker daemon health
- Container health checks (✅ healthy, ⚠️ starting, ❌ failed)
- Access points (URLs, ports)
- Kafka topics list
- Database statistics (devices, tags, records)
- Disk usage by component
- Quick command reference

**Additional monitoring:**
```bash
# Container list with health
docker ps

# Real-time resource usage
docker stats

# Specific container stats
docker stats roach-kafka
docker stats roach-postgres
```

### Logs

```bash
# All service logs (follows)
./scripts/logs.sh

# Specific service logs
./scripts/logs.sh weatherlink-kafka
./scripts/logs.sh weatherlink-sql
./scripts/logs.sh postgres

# Direct container logs
docker logs -f roach-weatherlink-kafka
docker logs --tail=100 roach-kafka
```

### Health Checks

**Expected startup sequence:** Zookeeper (5-10s) → Kafka (20-30s) → PostgreSQL (5-10s) → Application Services (depends on health)

**Health states:**
- `healthy` - Service is fully operational
- `starting` - Health check in progress (normal during startup)
- `unhealthy` - Health check failing (check logs)
- No healthcheck - Service running without health monitoring (normal for some services)

**Manual health verification:**
```bash
# Container health status
docker ps

# Detailed health info
docker inspect roach-kafka | grep -A 10 Health
docker inspect roach-postgres | grep -A 10 Health
```

### Kafka UI

**Access:** http://localhost:8080

**Features:**
- Browse topics and messages
- Monitor consumer groups and lag
- Check broker health and configuration
- View topic retention and size
- Inspect message headers and payloads

## Database Operations

### Query Database

**Quick queries** (see [scripts/README.md](../scripts/README.md) for complete documentation):

```bash
# Database statistics
./scripts/db/query.sh stats

# List all devices
./scripts/db/query.sh devices

# Tags for specific device
./scripts/db/query.sh tags 918290

# Recent records (all devices)
./scripts/db/query.sh recent

# Recent records (specific device)
./scripts/db/query.sh recent 918290

# Check orphaned messages
./scripts/db/query.sh orphans

# Interactive SQL session
./scripts/db/query.sh psql
```

### Database Migrations

**Migration workflow:**
```bash
# 1. Check migration status
./scripts/db/migrate.sh status

# 2. Create new migration
./scripts/db/migrate.sh create add_new_column

# 3. Edit generated files
#    - scripts/db/migrations/NNN_add_new_column.up.sql
#    - scripts/db/migrations/NNN_add_new_column.down.sql

# 4. Apply migration
./scripts/db/migrate.sh up

# 5. Verify
./scripts/db/query.sh psql
# \d table_name  # Describe table

# 6. Rollback if needed (prompts for confirmation)
./scripts/db/migrate.sh down
```

**Migration best practices:**
1. Always create both up and down migrations
2. Test rollback before production
3. Use `IF EXISTS`/`IF NOT EXISTS` for idempotency
4. Backup data before migrations
5. Descriptive migration names (use snake_case)
6. Review migration status after applying

**Migration table:**
- Migrations tracked in `schema_migrations` table
- Includes version, name, applied timestamp, checksum
- Checksum validation ensures file integrity

### Orphaned Messages

**Check orphaned messages:**
```bash
./scripts/db/query.sh orphans
```

**Common orphan reasons:**
- `missing_device` - Device not in database (need metadata backfill)
- `failed_to_parse` - JSON parsing error
- `failed_to_create_tag` - Database error creating tag
- `missing or invalid lsid in metadata` - Metadata message format issue

**Reprocess orphaned messages:**
```bash
# After fixing root cause, reprocess
./scripts/db/reload-orphans.sh
```

**Typical workflow:**
1. Check orphans: `./scripts/db/query.sh orphans`
2. If "missing_device", run: `./scripts/weatherlink/sql-backfill.sh --metadata`
3. Reprocess: `./scripts/db/reload-orphans.sh`
4. Monitor: `./scripts/logs.sh weatherlink-sql`

## Backup and Restore

### Database Backup

**SQL dump (recommended for portability):**
```bash
# Dump database to SQL file
docker exec roach-postgres pg_dump -U roach roach > backup-$(date +%Y%m%d).sql

# Compressed SQL dump
docker exec roach-postgres pg_dump -U roach roach | gzip > backup-$(date +%Y%m%d).sql.gz
```

**Data directory backup (faster, same PostgreSQL version required):**
```bash
# Stop services
./scripts/stop-all.sh

# Backup PostgreSQL data directory
tar -czf postgres-backup-$(date +%Y%m%d).tar.gz data/postgres/

# Restart services
./scripts/start-all.sh
```

### Database Restore

**From SQL dump:**
```bash
# Uncompressed SQL
cat backup-20260125.sql | docker exec -i roach-postgres psql -U roach -d roach

# Compressed SQL
gunzip -c backup-20260125.sql.gz | docker exec -i roach-postgres psql -U roach -d roach
```

**From data directory:**
```bash
./scripts/stop-all.sh
rm -rf data/postgres/
tar -xzf postgres-backup-20260125.tar.gz
./scripts/start-all.sh
```

### Full System Backup

**Complete backup (Kafka + Zookeeper + PostgreSQL):**
```bash
# Stop all services
./scripts/stop-all.sh

# Backup all data
tar -czf roach-backup-$(date +%Y%m%d).tar.gz data/

# Restart services
./scripts/start-all.sh
```

**Backup size estimation:**
- Kafka: Variable (depends on retention and message rate)
- PostgreSQL: ~2-5 MB per day of weather data
- Zookeeper: ~1-2 MB
- Compression: Typically 50-70% reduction

### Full System Restore

```bash
# Stop all services
./scripts/stop-all.sh

# Remove existing data
rm -rf data/

# Extract backup
tar -xzf roach-backup-20260125.tar.gz

# Start services
./scripts/start-all.sh

# Verify restoration
./scripts/status.sh
./scripts/db/query.sh stats
```

### Backup Best Practices

1. **Regular backups**: Schedule daily backups via cron
2. **Test restores**: Verify backups can be restored
3. **Off-site storage**: Copy backups to remote location
4. **Retention policy**: Keep 7 daily, 4 weekly, 12 monthly
5. **Pre-migration backups**: Always backup before schema changes
6. **Verify integrity**: Check backup file sizes and test extraction

## Kafka Operations

### Topics Management

**List and describe topics:**
```bash
# List all topics
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# View topic details (partitions, retention, size)
docker exec roach-kafka kafka-topics \
  --describe \
  --topic weather.iss \
  --bootstrap-server localhost:29092

# Get topic message count
docker exec roach-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 \
  --topic weather.iss \
  --time -1
```

**Create and delete topics:**
```bash
# Create topic (usually auto-created by producers)
docker exec roach-kafka kafka-topics \
  --create \
  --topic test.topic \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:29092

# Delete topic
docker exec roach-kafka kafka-topics \
  --delete \
  --topic test.topic \
  --bootstrap-server localhost:29092
```

**Note:** ROACH system auto-creates topics with default settings when producers first publish.

### Consuming Messages

**Basic consumption:**
```bash
# Last 10 messages from beginning
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --from-beginning \
  --max-messages 10

# Follow latest messages (Ctrl+C to exit)
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss
```

**Advanced consumption:**
```bash
# With headers and timestamps
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --property print.headers=true \
  --property print.timestamp=true \
  --property print.key=true \
  --from-beginning

# From specific offset
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --partition 0 \
  --offset 100 \
  --max-messages 10
```

### Consumer Groups

**Monitor consumer groups:**
```bash
# List all consumer groups
docker exec roach-kafka kafka-consumer-groups \
  --list \
  --bootstrap-server localhost:29092

# Describe consumer group (shows lag, offsets)
docker exec roach-kafka kafka-consumer-groups \
  --describe \
  --group weatherlink-sql-data-iss \
  --bootstrap-server localhost:29092

# All groups details
docker exec roach-kafka kafka-consumer-groups \
  --describe \
  --all-groups \
  --bootstrap-server localhost:29092
```

**Reset consumer group offsets:**
```bash
# Reset to earliest (reprocess all messages)
docker exec roach-kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group weatherlink-sql-backfill \
  --reset-offsets \
  --to-earliest \
  --all-topics \
  --execute

# Reset to latest (skip to current)
docker exec roach-kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group weatherlink-sql-data-iss \
  --reset-offsets \
  --to-latest \
  --all-topics \
  --execute

# Reset to specific offset
docker exec roach-kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group weatherlink-sql-data-iss \
  --topic weather.iss:0 \
  --reset-offsets \
  --to-offset 1000 \
  --execute
```

**Note:** Consumer group must be stopped before resetting offsets.

### Broker Information

```bash
# Check broker API versions
docker exec roach-kafka kafka-broker-api-versions \
  --bootstrap-server localhost:29092

# Cluster metadata
docker exec roach-kafka kafka-metadata \
  --bootstrap-server localhost:29092

# Broker configuration
docker exec roach-kafka kafka-configs \
  --bootstrap-server localhost:29092 \
  --entity-type brokers \
  --entity-name 1 \
  --describe
```

## System Maintenance

### Routine Maintenance

**Daily tasks:**
```bash
# Check system status
./scripts/status.sh

# Check database statistics
./scripts/db/query.sh stats

# Check for orphaned messages
./scripts/db/query.sh orphans

# Monitor disk usage
du -sh data/*
```

**Weekly tasks:**
```bash
# Review logs for errors
./scripts/logs.sh weatherlink-kafka | grep -i error
./scripts/logs.sh weatherlink-sql | grep -i error

# Check Kafka topic sizes
docker exec roach-kafka kafka-topics --describe --bootstrap-server localhost:29092

# Verify backups exist and are recent
ls -lh *backup*.tar.gz *backup*.sql
```

**Monthly tasks:**
```bash
# Database backup
docker exec roach-postgres pg_dump -U roach roach | gzip > backup-$(date +%Y%m%d).sql.gz

# Full system backup
./scripts/stop-all.sh
tar -czf roach-backup-$(date +%Y%m%d).tar.gz data/
./scripts/start-all.sh

# Review disk usage trends
df -h
du -sh data/* | sort -h
```

### Service Updates

**After code changes:**
```bash
# Rebuild all services
./scripts/start-all.sh build

# Rebuild specific service only
docker compose build weatherlink-kafka
./scripts/restart-all.sh weatherlink-kafka

# Verify service health
./scripts/logs.sh weatherlink-kafka
./scripts/status.sh
```

**Update base images:**
```bash
# Pull latest images
docker pull confluentinc/cp-kafka:7.5.0
docker pull postgres:16-alpine

# Rebuild and restart
./scripts/stop-all.sh
./scripts/start-all.sh build
```

### Credential Rotation

**Update WeatherLink API credentials:**
```bash
# 1. Edit .env file
vi .env
# Update WEATHERLINK_API_KEY and WEATHERLINK_API_SECRET

# 2. Restart affected services
./scripts/restart-all.sh weatherlink-kafka

# 3. Verify in logs
./scripts/logs.sh weatherlink-kafka | grep -i auth
```

**Update PostgreSQL password:**
```bash
# 1. Stop all services
./scripts/stop-all.sh

# 2. Edit .env file
vi .env
# Update POSTGRES_PASSWORD

# 3. Remove PostgreSQL volume (password stored in volume)
docker volume rm roach_postgres-data

# 4. Restart system (will reinitialize with new password)
./scripts/start-all.sh

# 5. Verify connection
./scripts/db/query.sh stats
```

**Note:** Changing PostgreSQL password requires volume recreation and database reinitialization. Backup data first.

### Log Rotation

**Docker manages log rotation automatically**, but you can configure limits:

**docker-compose.yml:**
```yaml
services:
  weatherlink-kafka:
    logging:
      driver: "json-file"
      options:
        max-size: "10m"    # Max log file size
        max-file: "3"      # Number of log files to keep
```

**Manual log cleanup:**
```bash
# Truncate all container logs
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down
find /var/lib/docker/containers/ -name "*-json.log" -exec truncate -s 0 {} \;
./scripts/start-all.sh
```

## Backfill Operations

### API to Kafka Backfill

**Backfill historical data from WeatherLink API to Kafka:**

```bash
# Last 24 hours
./scripts/weatherlink/kafka-backfill.sh --start $(date -v-24H +%s)

# Last 7 days
./scripts/weatherlink/kafka-backfill.sh --start $(date -v-7d +%s)

# Specific date range (datetime strings)
./scripts/weatherlink/kafka-backfill.sh --start "2026-01-11 18:20:47" --end "2026-01-12 18:20:47"

# Custom rate limiting and parallelism
./scripts/weatherlink/kafka-backfill.sh --start "2026-01-11" --requests-per-second 5 --workers 8
```

**See [scripts/README.md](../scripts/README.md#weatherlinkkafka-backfillsh) for complete options and examples.**

### Kafka to PostgreSQL Backfill

**Backfill from Kafka topics to PostgreSQL:**

```bash
# Fresh database (with metadata)
./scripts/weatherlink/sql-backfill.sh --metadata

# All data topics from beginning
./scripts/weatherlink/sql-backfill.sh

# Specific topics
./scripts/weatherlink/sql-backfill.sh --topics weather.iss,weather.barometer

# High-performance backfill
./scripts/weatherlink/sql-backfill.sh --workers 16 --batch-size 1000
```

**See [scripts/README.md](../scripts/README.md#weatherlinksql-backfillsh) for complete options and examples.**

**Common workflows:**

**Fresh database setup:**
```bash
# 1. Backfill metadata first (creates devices)
./scripts/weatherlink/sql-backfill.sh --metadata

# 2. Backfill data
./scripts/weatherlink/sql-backfill.sh

# 3. Verify
./scripts/db/query.sh stats
```

**Database rebuild:**
```bash
# 1. Stop materializer
docker compose stop weatherlink-sql

# 2. Truncate tables
./scripts/db/query.sh psql
# TRUNCATE devices, tags, sensor_catalog, records_numeric, records_text, records_null CASCADE;

# 3. Backfill with metadata
./scripts/weatherlink/sql-backfill.sh --metadata

# 4. Restart materializer
docker compose start weatherlink-sql
```

## Development Workflow

### Local Service Development

**Run services locally outside Docker:**

```bash
# 1. Start infrastructure only
./scripts/start-infra.sh

# 2. Set environment variables
export KAFKA_BROKER=localhost:9092
export POSTGRES_DSN="host=localhost port=5432 user=roach password=yourpass dbname=roach sslmode=disable"
export WEATHERLINK_API_KEY=your_key
export WEATHERLINK_API_SECRET=your_secret
export WEATHERLINK_STATION_ID=your_station
export LOG_LEVEL=debug

# 3. Run service locally
cd services/weatherlink-kafka
go run main.go
```

**Test in Docker after code changes:**
```bash
# Build and start specific service
docker compose build weatherlink-kafka
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d weatherlink-kafka

# View logs
./scripts/logs.sh weatherlink-kafka
```

### Debugging Containers

**Container inspection:**
```bash
# Enter running container (interactive shell)
docker exec -it roach-weatherlink-kafka sh

# Check environment variables
docker exec roach-weatherlink-kafka env | grep WEATHERLINK

# View files in container
docker exec roach-weatherlink-kafka ls -la

# Check process list
docker exec roach-weatherlink-kafka ps aux
```

**Network connectivity testing:**
```bash
# Test Kafka connection
docker exec roach-weatherlink-kafka nc -zv kafka 29092

# Test PostgreSQL connection
docker exec roach-weatherlink-sql nc -zv postgres 5432

# Test from host
nc -zv localhost 9092  # Kafka external
nc -zv localhost 5432  # PostgreSQL
```

**Container logs and metrics:**
```bash
# Detailed logs with timestamps
docker logs --timestamps roach-weatherlink-kafka

# Follow logs with grep filter
docker logs -f roach-weatherlink-kafka | grep ERROR

# Container resource usage
docker stats roach-weatherlink-kafka

# Container inspection
docker inspect roach-weatherlink-kafka | jq '.State'
docker inspect roach-weatherlink-kafka | jq '.Config.Env'
```

## Performance Monitoring

### Resource Usage

**Real-time monitoring:**
```bash
# All containers resource usage
docker stats

# Specific container
docker stats roach-kafka
docker stats roach-postgres

# One-time snapshot (no-stream)
docker stats --no-stream
```

**Expected resource usage:**
- Kafka: 1-5% CPU, 1-2 GB RAM
- PostgreSQL: 1-3% CPU, 100-500 MB RAM
- Zookeeper: <1% CPU, 100-200 MB RAM
- weatherlink-kafka: <1% CPU, 20-50 MB RAM
- weatherlink-sql: 1-3% CPU, 50-100 MB RAM
- Kafka UI: 1-2% CPU, 100-200 MB RAM

### Disk Usage Monitoring

```bash
# Overall disk usage (via status.sh)
./scripts/status.sh

# Detailed breakdown
du -sh data/*
du -sh data/kafka data/zookeeper data/postgres

# Find large files
find data/ -type f -size +100M -exec du -h {} \;
```

**Expected disk growth:**
- Kafka: ~0.3 MB/day (with compression)
- PostgreSQL: ~2-5 MB/day
- Zookeeper: ~1 MB/day
- Total: ~3-6 MB/day

### Performance Tuning

**API fetch interval (docker-compose.yml):**
```yaml
services:
  weatherlink-kafka:
    environment:
      - FETCH_INTERVAL=10m  # Increase from 5m (reduce API calls)
```

**Logging levels:**
```yaml
services:
  weatherlink-kafka:
    environment:
      - LOG_LEVEL=debug  # For debugging (verbose)
      - LOG_LEVEL=info   # Default (balanced)
      - LOG_LEVEL=warn   # Production (minimal)
      - LOG_LEVEL=error  # Critical only
```

**Database performance:**
```yaml
services:
  weatherlink-sql:
    environment:
      - WORKER_POOL_SIZE=16    # Increase parallelism
      - BATCH_SIZE=1000        # Larger batch writes
      - BATCH_FLUSH_INTERVAL_MS=2000  # Longer flush interval
```

**Kafka performance:**
```yaml
# docker-compose.infrastructure.yml
services:
  kafka:
    environment:
      - KAFKA_NUM_IO_THREADS=16           # Increase I/O threads
      - KAFKA_LOG_FLUSH_INTERVAL_MS=10000 # Batch disk writes
      - KAFKA_COMPRESSION_TYPE=lz4        # Enable compression
```

## Docker Management

### Container Management

**List and inspect:**
```bash
# List running containers
docker ps

# List all containers (including stopped)
docker ps -a

# Inspect container details
docker inspect roach-kafka
docker inspect roach-kafka | jq '.State'
docker inspect roach-kafka | jq '.NetworkSettings.IPAddress'
```

**Container logs:**
```bash
# View logs
docker logs roach-kafka

# Follow logs (Ctrl+C to exit)
docker logs -f roach-kafka

# Last N lines
docker logs --tail=100 roach-kafka

# With timestamps
docker logs --timestamps roach-kafka

# Since specific time
docker logs --since 2026-01-25T10:00:00 roach-kafka
```

**Container control:**
```bash
# Stop container
docker stop roach-weatherlink-kafka

# Start stopped container
docker start roach-weatherlink-kafka

# Restart container
docker restart roach-weatherlink-kafka

# Remove stopped container
docker rm roach-weatherlink-kafka
```

### Image Management

```bash
# List images
docker images

# Remove unused images
docker image prune

# Remove specific image
docker rmi roach-weatherlink-kafka

# Pull latest base images
docker pull confluentinc/cp-kafka:7.5.0
docker pull postgres:16-alpine

# View image history
docker history roach-weatherlink-kafka
```

### Network Management

**Inspect network:**
```bash
# List all networks
docker network ls

# Inspect roach network details
docker network inspect roach-network

# Show connected containers
docker network inspect roach-network | jq '.[0].Containers'
```

**Troubleshoot network issues:**
```bash
# Recreate network (if connectivity issues)
./scripts/stop-all.sh
docker network rm roach-network
./scripts/start-all.sh

# Test network connectivity
docker run --rm --network roach-network alpine ping -c 3 kafka
docker run --rm --network roach-network alpine nc -zv postgres 5432
```

### Volume Management

**Inspect volumes:**
```bash
# List volumes
docker volume ls

# Inspect volume details
docker volume inspect roach_kafka-data
docker volume inspect roach_postgres-data

# Check volume size
docker system df -v
```

**Clean up volumes:**
```bash
# Remove unused volumes (safe)
docker volume prune

# Remove all roach volumes (WARNING: deletes all data)
./scripts/stop-all.sh clean

# Or manually
docker volume rm roach_kafka-data roach_zookeeper-data roach_postgres-data
```

## Testing and Validation

### Configuration Validation

**Verify configuration without starting services:**
```bash
# View resolved Docker Compose configuration
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml config

# Check environment variables are loaded
docker compose config | grep WEATHERLINK
docker compose config | grep POSTGRES

# Validate compose file syntax
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml config --quiet
```

### Connectivity Testing

**Infrastructure connectivity:**
```bash
# Test Kafka broker
docker exec roach-kafka kafka-broker-api-versions --bootstrap-server localhost:29092

# Test PostgreSQL ready
docker exec roach-postgres pg_isready -U roach

# Interactive PostgreSQL session
docker exec -it roach-postgres psql -U roach -d roach
```

**Service connectivity:**
```bash
# Test from weatherlink-kafka to Kafka
docker exec roach-weatherlink-kafka nc -zv kafka 29092

# Test from weatherlink-sql to PostgreSQL
docker exec roach-weatherlink-sql nc -zv postgres 5432

# Test from host to external ports
nc -zv localhost 9092   # Kafka
nc -zv localhost 5432   # PostgreSQL
curl -s http://localhost:8080 > /dev/null && echo "Kafka UI accessible"
```

### Service Health Verification

**Container health:**
```bash
# Check all containers running with health status
./scripts/status.sh

# Or manually
docker ps
```

**Data flow verification:**
```bash
# Verify weatherlink-kafka is producing
./scripts/logs.sh weatherlink-kafka | tail -50

# Verify weatherlink-sql is consuming
./scripts/logs.sh weatherlink-sql | tail -50

# Check Kafka topics exist
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# Check topics have messages
docker exec roach-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 \
  --topic weather.iss \
  --time -1

# Check database has data
./scripts/db/query.sh stats
./scripts/db/query.sh recent
```

### End-to-End Testing

**Complete data flow test:**
```bash
# 1. Check baseline
./scripts/db/query.sh stats

# 2. Wait for next fetch cycle (5 minutes)
date && ./scripts/logs.sh weatherlink-kafka | grep "Publishing"

# 3. Wait for materialization
sleep 10

# 4. Verify new records
./scripts/db/query.sh recent

# 5. Check for errors
./scripts/db/query.sh orphans
```

**Backfill testing:**
```bash
# 1. Test API→Kafka backfill
./scripts/weatherlink/kafka-backfill.sh --start "$(date -v-1H '+%Y-%m-%d %H:%M:%S')"

# 2. Test Kafka→DB backfill
./scripts/weatherlink/sql-backfill.sh --topics weather.iss --start-offset -100

# 3. Verify results
./scripts/db/query.sh stats
```
