# Operations

## Starting the System

### Start Everything
```bash
./scripts/start-all.sh
```

### Start Infrastructure Only
```bash
./scripts/start-infra.sh
```

### Start with Manual Control
```bash
# Infrastructure + Services
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d

# Watch logs while starting
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up
```

## Stopping the System

### Stop All Services
```bash
./scripts/stop-all.sh
```

### Stop and Remove Data (Clean Slate)
```bash
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
```

## Monitoring

### System Status
```bash
./scripts/status.sh
```

### View Logs

```bash
# All services
./scripts/logs.sh

# Specific service
./scripts/logs.sh weather-publish
./scripts/logs.sh weather-sql
./scripts/logs.sh kafka
./scripts/logs.sh postgres
./scripts/logs.sh zookeeper

# Last N lines
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml logs --tail=100

# Follow logs
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml logs -f weather-publish
```

### Check Service Health
```bash
# List running containers with health status
docker ps

# Detailed health info
docker inspect roach-kafka | grep -A 10 Health
```

### Kafka UI
Access at: http://localhost:8080

Features:
- View topics and messages
- Monitor consumer groups
- Check broker health
- Inspect message headers and payloads

## Restarting Services

### Restart Specific Service
```bash
./scripts/restart.sh weather-publish
./scripts/restart.sh weather-sql
./scripts/restart.sh postgres
```

### Restart All Services
```bash
./scripts/restart.sh
```

### Restart After Code Changes
```bash
# Rebuild and restart
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d --build weather-publish
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d --build weather-sql
```

## Database Operations

### Query Database
```bash
# Show statistics
./scripts/db/query.sh stats

# List devices
./scripts/db/query.sh devices

# Show tags for device
./scripts/db/query.sh tags 918290

# Recent records
./scripts/db/query.sh recent

# Recent records for specific device
./scripts/db/query.sh recent 918290

# Check orphaned messages
./scripts/db/query.sh orphans

# Interactive psql
./scripts/db/query.sh psql
```

### Backup Database
```bash
# Dump database
docker exec roach-postgres pg_dump -U roach roach > backup-$(date +%Y%m%d).sql

# Or backup entire data directory
./scripts/stop-all.sh
tar -czf postgres-backup-$(date +%Y%m%d).tar.gz data/postgres/
./scripts/start-all.sh
```

### Restore Database
```bash
# From SQL dump
cat backup.sql | docker exec -i roach-postgres psql -U roach -d roach
```

### Reprocess Orphaned Messages
```bash
# Interactive tool
./scripts/db/reload-orphans.sh
```

## Kafka Operations

### List Topics
```bash
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092
```

### View Topic Details
```bash
docker exec roach-kafka kafka-topics --describe --topic weather.iss --bootstrap-server localhost:29092
```

### Consume Messages
```bash
# Last 10 messages
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --from-beginning \
  --max-messages 10

# With headers
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --property print.headers=true \
  --from-beginning \
  --max-messages 5
```

### Delete Topic (if needed)
```bash
docker exec roach-kafka kafka-topics \
  --delete \
  --topic topic-name \
  --bootstrap-server localhost:29092
```

## Maintenance

### Disk Usage Monitoring
```bash
# Check data directory size
du -sh data/

# Breakdown by component
du -sh data/kafka
du -sh data/zookeeper
du -sh data/postgres

# System disk usage
df -h
```

### Backup Data
```bash
# Stop services first
./scripts/stop-all.sh

# Backup
tar -czf kafka-backup-$(date +%Y%m%d).tar.gz data/

# Restart
./scripts/start-all.sh
```

### Restore Data
```bash
# Stop services
./scripts/stop-all.sh

# Remove current data
rm -rf data/

# Restore from backup
tar -xzf kafka-backup-YYYYMMDD.tar.gz

# Restart
./scripts/start-all.sh
```

### Update Service Code

```bash
# 1. Edit code
nano services/weather/main.go

# 2. Rebuild and restart
docker compose build weather-publish
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d weather-publish

# 3. Check logs
./scripts/logs.sh weather-publish
```

## Development Workflow

### Work on Service Locally

```bash
# 1. Start infrastructure
./scripts/start-infra.sh

# 2. Develop service
cd services/weather
# ... make changes ...

# 3. Test locally (without Docker)
export KAFKA_BROKER=localhost:9092
export WEATHERLINK_API_KEY=your_key
export WEATHERLINK_API_SECRET=your_secret
export WEATHERLINK_STATION_ID=your_station
go run main.go

# 4. Or test in Docker
docker compose build weather
docker compose up weather
```

### Debug Container

```bash
# Enter running container
docker exec -it roach-weather-publish sh

# Check environment
docker exec roach-weather-publish env

# View files
docker exec roach-weather-publish ls -la
```

## Health Checks

### Expected Startup Sequence

1. Zookeeper starts (5-10 seconds)
2. Zookeeper becomes healthy
3. Kafka starts (20-30 seconds)
4. Kafka becomes healthy
5. PostgreSQL starts (5-10 seconds)
6. PostgreSQL becomes healthy
7. Weather Publisher starts
8. Weather SQL starts
9. Kafka UI starts

### Verify Health

```bash
# Check all services
docker ps

# Look for "(healthy)" status on:
# - roach-zookeeper
# - roach-kafka
# - roach-postgres

# Services should show "Up":
# - roach-weather-publish
# - roach-weather-sql
# - roach-kafka-ui
```

## Performance Monitoring

### Resource Usage
```bash
# Real-time stats
docker stats

# Specific container
docker stats roach-kafka
```

### Log File Sizes
```bash
# Check log sizes
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml logs --no-log-prefix weather | wc -l
```

## Security Operations

### Rotate API Credentials

```bash
# 1. Update .env file
nano .env

# 2. Restart services
./scripts/restart.sh weather-publish
```

### View Service Credentials (debugging)
```bash
# Never log these in production
docker exec roach-weather-publish env | grep WEATHERLINK
docker exec roach-weather-publish env | grep POSTGRES
```
