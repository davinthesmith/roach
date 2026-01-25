# Operations

> **For basics, see [AI-CONTEXT.md](AI-CONTEXT.md)**. This document covers advanced operations and maintenance procedures.

## System Management

### Starting Services

```bash
# Complete startup
./scripts/start-all.sh

# With rebuild (after code changes)
./scripts/start-all.sh build

# Infrastructure only
./scripts/start-infra.sh

# Manual control with compose files
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d
```

### Stopping Services

```bash
# Stop all
./scripts/stop-all.sh

# Stop and remove volumes (clean slate)
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
```

### Restarting Services

```bash
# Restart all
./scripts/restart-all.sh

# Restart specific service
./scripts/restart-all.sh weatherlink-kafka
./scripts/restart-all.sh weatherlink-sql
./scripts/restart-all.sh postgres

# Rebuild specific service after code changes
docker compose build weatherlink-kafka
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d weatherlink-kafka
```

## Monitoring

### System Status

```bash
# Overall status
./scripts/status.sh

# Container list with health
docker ps

# Resource usage (real-time)
docker stats

# Specific container
docker stats roach-kafka
```

### Logs

```bash
# All services
./scripts/logs.sh

# Specific service
./scripts/logs.sh weatherlink-kafka
./scripts/logs.sh weatherlink-sql
./scripts/logs.sh kafka
./scripts/logs.sh postgres

# Follow logs
docker logs -f roach-weatherlink-kafka

# Last N lines
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml logs --tail=100
```

### Health Checks

**Expected startup sequence**: Zookeeper (5-10s) → Kafka (20-30s) → PostgreSQL (5-10s) → Services

```bash
# Check health status
docker ps  # Look for "(healthy)" on kafka, postgres, zookeeper

# Detailed health info
docker inspect roach-kafka | grep -A 10 Health
```

### Kafka UI

Access: http://localhost:8080

**Features**: Browse topics/messages, monitor consumer groups, check broker health, inspect configs

## Database Operations

### Query Database

```bash
# Statistics
./scripts/db/query.sh stats

# List devices
./scripts/db/query.sh devices

# Show tags for device
./scripts/db/query.sh tags 918290

# Recent records
./scripts/db/query.sh recent
./scripts/db/query.sh recent 918290  # For specific device

# Check orphaned messages
./scripts/db/query.sh orphans

# Interactive psql session
./scripts/db/query.sh psql
```

### Database Migrations

```bash
# View migration status
./scripts/db/migrate.sh status

# Apply all pending migrations
./scripts/db/migrate.sh up

# Rollback last migration (prompts for confirmation)
./scripts/db/migrate.sh down

# Create new migration
./scripts/db/migrate.sh create add_new_column
# Edit generated files:
# - scripts/db/migrations/NNN_add_new_column.up.sql
# - scripts/db/migrations/NNN_add_new_column.down.sql
```

**Migration best practices**:
1. Always create both up and down migrations
2. Test rollback before applying in production
3. Use IF EXISTS/IF NOT EXISTS for idempotency
4. Backup data before migrations
5. Review migration status after applying

### Reprocess Orphaned Messages

```bash
# Interactive tool to reprocess messages that failed
./scripts/db/reload-orphans.sh
```

### Backup and Restore

**Database backup**:
```bash
# Dump to SQL
docker exec roach-postgres pg_dump -U roach roach > backup-$(date +%Y%m%d).sql

# Or backup data directory (requires services stopped)
./scripts/stop-all.sh
tar -czf postgres-backup-$(date +%Y%m%d).tar.gz data/postgres/
./scripts/start-all.sh
```

**Database restore**:
```bash
# From SQL dump
cat backup.sql | docker exec -i roach-postgres psql -U roach -d roach
```

**Full data backup** (Kafka + Zookeeper + PostgreSQL):
```bash
./scripts/stop-all.sh
tar -czf roach-backup-$(date +%Y%m%d).tar.gz data/
./scripts/start-all.sh
```

**Full data restore**:
```bash
./scripts/stop-all.sh
rm -rf data/
tar -xzf roach-backup-YYYYMMDD.tar.gz
./scripts/start-all.sh
```

## Kafka Operations

### Topics Management

```bash
# List all topics
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# View topic details
docker exec roach-kafka kafka-topics \
  --describe \
  --topic weather.iss \
  --bootstrap-server localhost:29092

# Create topic manually (usually auto-created)
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

### Consuming Messages

```bash
# Last 10 messages from beginning
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --from-beginning \
  --max-messages 10

# Latest messages (follow)
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss

# With headers
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --property print.headers=true \
  --property print.timestamp=true \
  --from-beginning
```

### Consumer Groups

```bash
# List consumer groups
docker exec roach-kafka kafka-consumer-groups \
  --list \
  --bootstrap-server localhost:29092

# Describe consumer group
docker exec roach-kafka kafka-consumer-groups \
  --describe \
  --group weatherlink-sql-data-iss \
  --bootstrap-server localhost:29092
```

### Broker Information

```bash
# Check broker API versions
docker exec roach-kafka kafka-broker-api-versions \
  --bootstrap-server localhost:29092

# Cluster metadata
docker exec roach-kafka kafka-metadata \
  --bootstrap-server localhost:29092
```

## Maintenance

### Disk Usage Monitoring

```bash
# Check data directory size
du -sh data/

# Breakdown by component
du -sh data/kafka data/zookeeper data/postgres

# System disk usage
df -h
```

### Service Updates

**After code changes**:
```bash
# Method 1: Rebuild all services
./scripts/start-all.sh build

# Method 2: Rebuild specific service
cd services/weatherlink-kafka
# ... make changes ...
docker compose build weatherlink-kafka
docker compose -f ../../docker-compose.infrastructure.yml -f ../../docker-compose.yml up -d weatherlink-kafka
./scripts/logs.sh weatherlink-kafka  # Check logs
```

### Credential Rotation

**Update API credentials**:
```bash
# 1. Edit .env file
nano .env

# 2. Restart affected service
./scripts/restart-all.sh weatherlink-kafka
```

**Update PostgreSQL password**:
```bash
# 1. Edit .env file
nano .env

# 2. Restart infrastructure and all services
./scripts/stop-all.sh
./scripts/start-all.sh
```

## Development Workflow

### Local Service Development

```bash
# 1. Start infrastructure only
./scripts/start-infra.sh

# 2. Develop service locally (outside Docker)
cd services/weatherlink-kafka
export KAFKA_BROKER=localhost:9092
export POSTGRES_DSN="host=localhost port=5432 user=roach password=yourpass dbname=roach sslmode=disable"
export WEATHERLINK_API_KEY=your_key
export WEATHERLINK_API_SECRET=your_secret
export WEATHERLINK_STATION_ID=your_station
go run main.go

# 3. Or test in Docker
docker compose build weatherlink-kafka
docker compose -f ../../docker-compose.infrastructure.yml -f ../../docker-compose.yml up weatherlink-kafka
```

### Debugging Containers

```bash
# Enter running container
docker exec -it roach-weatherlink-kafka sh

# Check environment variables
docker exec roach-weatherlink-kafka env

# View files
docker exec roach-weatherlink-kafka ls -la

# Check connectivity
docker exec roach-weatherlink-kafka nc -zv kafka 29092
docker exec roach-weatherlink-kafka nc -zv postgres 5432
```

## Performance Monitoring

### Resource Usage

```bash
# Real-time stats for all containers
docker stats

# Specific container
docker stats roach-kafka

# Log sizes
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml logs --no-log-prefix weatherlink-kafka | wc -l
```

### Performance Tuning

**Reduce API call frequency**:
```yaml
# docker-compose.yml
services:
  weatherlink-kafka:
    environment:
      - FETCH_INTERVAL=10m  # Increase from 5m
```

**Increase logging verbosity for debugging**:
```yaml
services:
  weatherlink-kafka:
    environment:
      - LOG_LEVEL=debug
```

**Decrease logging for production**:
```yaml
services:
  weatherlink-kafka:
    environment:
      - LOG_LEVEL=warn
```

## Docker Management

### Container Management

```bash
# List containers
docker ps
docker ps -a  # Include stopped

# Inspect container
docker inspect roach-kafka
docker inspect roach-kafka | grep IPAddress

# Container logs
docker logs roach-kafka
docker logs -f roach-kafka  # Follow
docker logs --tail=100 roach-kafka  # Last 100 lines
```

### Image Management

```bash
# List images
docker images

# Remove unused images
docker image prune

# Pull latest image
docker pull confluentinc/cp-kafka:7.5.0
```

### Network Management

```bash
# List networks
docker network ls

# Inspect roach network
docker network inspect roach-network

# Recreate network (if issues)
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down
docker network rm roach-network
./scripts/start-all.sh
```

### Volume Management

```bash
# List volumes
docker volume ls

# Inspect volume
docker volume inspect roach_kafka-data

# Clean up unused volumes
docker volume prune
```

## Validation and Testing

### Configuration Validation

```bash
# View resolved configuration
docker compose config

# Verify environment variables loaded
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml config | grep WEATHERLINK
```

### Connectivity Testing

```bash
# Test Kafka
docker exec roach-kafka kafka-broker-api-versions --bootstrap-server localhost:29092

# Test PostgreSQL
docker exec roach-postgres pg_isready -U roach
docker exec -it roach-postgres psql -U roach -d roach

# Test from services
docker exec roach-weatherlink-kafka nc -zv kafka 29092
docker exec roach-weatherlink-sql nc -zv postgres 5432
```

### Service Health Verification

```bash
# Check all containers are running
docker ps

# Verify services processing data
./scripts/logs.sh weatherlink-kafka | tail -20
./scripts/logs.sh weatherlink-sql | tail -20

# Check Kafka topics have messages
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# Check database has data
./scripts/db/query.sh stats
```
