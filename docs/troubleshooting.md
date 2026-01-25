# Troubleshooting

> **For common issues, see [AI-CONTEXT.md](AI-CONTEXT.md)**. This document covers comprehensive problem solving.

## System Issues

### Services Won't Start

**Symptom**: Services fail to start or exit immediately

**Check**:
```bash
docker compose -f docker-compose.infrastructure.yml logs
docker ps
```

**Common causes**:
1. Port conflicts (9092, 2181, 8080, 5432 already in use)
2. Insufficient disk space
3. Docker not running
4. Invalid .env file

**Solutions**:
```bash
# Check ports
lsof -i :9092 :2181 :8080 :5432

# Check disk space
df -h

# Restart Docker (macOS: Docker Desktop, Linux: sudo systemctl restart docker)

# Verify .env file
cat .env | grep WEATHERLINK
```

### Services Fail Health Check

**Symptom**: Containers restart repeatedly or stay "unhealthy"

**Check**:
```bash
docker ps
docker inspect roach-kafka | grep -A 10 Health
docker logs roach-kafka
docker logs roach-zookeeper
docker logs roach-postgres
```

**Solutions**:
```bash
# Wait longer (Kafka takes 30-60s)
sleep 60 && docker ps

# Clean restart
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
./scripts/start-all.sh
```

### Port Conflicts

**Symptom**: "address already in use" errors

**Identify conflicting process**:
```bash
lsof -i :9092  # Kafka
lsof -i :5432  # PostgreSQL
lsof -i :8080  # Kafka UI
```

**Solutions**:
- Stop conflicting process
- Change ports in docker-compose files
- Use different external ports

## Weather Service Issues

### Connection Refused

**Symptom**: `dial tcp: connect: connection refused`

**Cause**: Kafka or PostgreSQL not ready when service started

**Solution**:
```bash
# Wait for health checks
docker ps  # Check for "(healthy)"

# Restart service
./scripts/restart.sh weather-publish
./scripts/restart.sh weather-sql
```

### API Authentication Errors

**Symptom**: 401 Unauthorized, invalid credentials

**Check**:
```bash
docker exec roach-weather-publish env | grep WEATHERLINK
```

**Solutions**:
```bash
# Update .env file
nano .env

# Restart service
./scripts/restart.sh weather-publish
```

### No Data Publishing

**Symptom**: Service runs but no topics created or no messages

**Check**:
```bash
./scripts/logs.sh weather-publish
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092
```

**Common causes**:
1. Invalid API credentials
2. Incorrect station ID
3. Network connectivity
4. API rate limiting

**Debug**:
```bash
# Test API directly
curl -v "https://api.weatherlink.com/v2/current/[station-id]?api-key=[key]"

# Check service connectivity
docker exec roach-weather-publish nc -zv kafka 29092
docker exec roach-weather-publish nc -zv postgres 5432
```

### No Data in PostgreSQL

**Symptom**: Kafka has messages but PostgreSQL has no records

**Check**:
```bash
./scripts/logs.sh weather-sql
./scripts/db/query.sh stats
./scripts/db/query.sh orphans
```

**Common causes**:
1. Device metadata not received yet
2. Messages orphaned due to missing devices
3. PostgreSQL connection issues

**Solutions**:
```bash
# Check for orphaned messages
./scripts/db/query.sh orphans

# Reprocess after devices loaded
./scripts/db/reload-orphans.sh

# Restart materializer
./scripts/restart.sh weather-sql
```

## Kafka Issues

### Broker Not Responding

**Symptom**: Kafka doesn't respond to requests

**Check**:
```bash
docker logs roach-kafka
docker exec roach-kafka kafka-broker-api-versions --bootstrap-server localhost:29092
```

**Solutions**:
```bash
# Restart Kafka
docker restart roach-kafka

# Clean restart
docker compose -f docker-compose.infrastructure.yml down
rm -rf data/kafka  # WARNING: Deletes all data
./scripts/start-infra.sh
```

### Cluster ID Mismatch

**Symptom**: `InconsistentClusterIdException: The Cluster ID X doesn't match stored clusterId Some(Y)`

**Cause**: Kafka's cluster ID regenerated while Zookeeper retains old ID

**Check**:
```bash
docker logs roach-kafka 2>&1 | grep -i "InconsistentClusterIdException"
find ./data/kafka -name "meta.properties"
```

**Solution**:
```bash
./scripts/stop-all.sh
rm -rf data/kafka/* data/zookeeper/*
./scripts/start-all.sh
```

**Prevention**: Ensure `KAFKA_LOG_DIRS=/var/lib/kafka/data` is set (already configured)

### Topics Not Auto-Created

**Symptom**: Topics don't appear when service publishes

**Check**:
```bash
docker exec roach-kafka kafka-configs \
  --bootstrap-server localhost:29092 \
  --entity-type brokers \
  --entity-default \
  --describe | grep auto.create
```

**Should show**: `auto.create.topics.enable=true`

**Manual topic creation**:
```bash
docker exec roach-kafka kafka-topics \
  --create \
  --topic weather.iss \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:29092
```

### Out of Disk Space

**Symptom**: Kafka fails to write, disk space errors

**Check**:
```bash
df -h
du -sh data/kafka data/zookeeper data/postgres
```

**Solutions**:
```bash
# Temporary: Delete old data
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down
rm -rf data/kafka/* data/zookeeper/* data/postgres/*
./scripts/start-all.sh

# Permanent: Configure retention limits
# Edit docker-compose.infrastructure.yml:
# KAFKA_LOG_RETENTION_MS: 2592000000  # 30 days
```

## Kafka UI Issues

### UI Won't Load

**Symptom**: http://localhost:8080 doesn't respond

**Check**:
```bash
docker logs roach-kafka-ui
docker ps | grep kafka-ui
lsof -i :8080
```

**Solutions**:
```bash
# Wait for Kafka to be healthy
sleep 30

# Restart UI
docker restart roach-kafka-ui
```

### Topics Page Loading Forever

**Symptom**: Topics page spins with "Retrying to fetch metadata"

**Cause**: Kafka listener misconfiguration

**Check**:
```bash
docker logs roach-kafka-ui | grep -i "retry"
```

**Solution** (if configuration changed):
```bash
# Clean restart required
docker compose -f docker-compose.infrastructure.yml down -v
rm -rf data/zookeeper/* data/kafka/*
./scripts/start-all.sh
```

**Verify listener configuration** in docker-compose.infrastructure.yml:
```yaml
KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
```

### UI Shows No Topics

**Symptom**: UI loads but topics list is empty

**Check**:
```bash
# Verify topics exist
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# Check UI connectivity
docker exec roach-kafka-ui nc -zv kafka 29092
```

**Solution**:
```bash
docker restart roach-kafka-ui
```

## PostgreSQL Issues

### Connection Refused

**Symptom**: Services can't connect to PostgreSQL

**Check**:
```bash
docker logs roach-postgres
docker exec roach-postgres pg_isready -U roach
```

**Solutions**:
```bash
# Restart PostgreSQL
docker restart roach-postgres

# Check DSN in service
docker exec roach-weather-sql env | grep POSTGRES_DSN
```

### Migration Failures

**Symptom**: Migration won't apply or rolls back incorrectly

**Check**:
```bash
./scripts/db/migrate.sh status
./scripts/db/query.sh psql
SELECT * FROM schema_migrations;
```

**Solutions**:
- Review migration SQL for errors
- Check PostgreSQL logs: `docker logs roach-postgres`
- Manually fix schema if needed
- Update migration checksum if file was corrected

### Empty Tag Units/Descriptions

**Symptom**: Tags have NULL values for `unit` and `description`

**Cause**: Fixed in latest version (catalog filtering implemented)

**Check**:
```bash
./scripts/db/query.sh psql
SELECT COUNT(*) FROM tags WHERE unit IS NOT NULL;
SELECT COUNT(*) FROM sensor_catalog;

# Check catalog size
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.metadata.catalog \
  --from-beginning --max-messages 1 2>/dev/null | wc -c
```

**Solution**: Ensure running latest version with catalog filtering

### Data Not Persisting

**Symptom**: Data lost after restart

**Check**:
```bash
docker inspect roach-postgres | grep -A 10 Mounts
```

**Solution**: Ensure volume mounted in docker-compose.infrastructure.yml:
```yaml
postgres:
  volumes:
    - ./data/postgres:/var/lib/postgresql/data
```

### Non-Interactive Database Queries

**Symptom**: `./scripts/db/query.sh` fails with "the input device is not a TTY"

**Cause**: Script uses `docker exec -it` (requires interactive terminal)

**Solution**: Use `-c` flag without `-it`:
```bash
docker exec roach-postgres psql -U roach -d roach -c "SELECT COUNT(*) FROM devices;"
docker exec roach-postgres psql -U roach -d roach -c "SELECT COUNT(*) FROM devices; SELECT COUNT(*) FROM tags;"
```

**Capture output** (add `-t -A` flags):
```bash
COUNT=$(docker exec roach-postgres psql -U roach -d roach -t -A -c "SELECT COUNT(*) FROM devices;")
```

**Flags**: `-t` (no headers), `-A` (unaligned), `-c` (execute and exit), no `-it` (non-interactive)

## Network Issues

### Services Can't Communicate

**Symptom**: Services can't reach each other

**Check**:
```bash
docker network ls | grep roach
docker network inspect roach-network
```

**Solutions**:
```bash
# Recreate network
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down
docker network rm roach-network
./scripts/start-all.sh
```

### External Access Not Working

**Symptom**: Can't connect to Kafka/PostgreSQL from host

**Check**:
```bash
nc -zv localhost 9092  # Kafka
nc -zv localhost 5432  # PostgreSQL

# Check port mappings
docker ps | grep 9092
docker ps | grep 5432
```

**Solution**: Verify port mappings in docker-compose files

## Performance Issues

### High CPU Usage

**Check**:
```bash
docker stats
```

**Common causes**:
1. Kafka compaction running
2. Too many consumers
3. Fetch interval too short

**Solutions**:
```bash
# Increase fetch interval
# Edit docker-compose.yml: FETCH_INTERVAL=10m

# Restart service
./scripts/restart.sh weather-publish
```

### High Memory Usage

**Check**:
```bash
docker stats
```

**Solutions**:
```yaml
# Limit Kafka memory (docker-compose.infrastructure.yml)
kafka:
  deploy:
    resources:
      limits:
        memory: 1G
```

## Debug Mode

### Enable Verbose Logging

**Weather services**:
```yaml
# docker-compose.yml
services:
  weather-publish:
    environment:
      - LOG_LEVEL=debug
  weather-sql:
    environment:
      - LOG_LEVEL=debug
```

### Interactive Debugging

```bash
# Enter container
docker exec -it roach-weather-publish sh

# Check environment
env

# Test connectivity
nc -zv kafka 29092
nc -zv postgres 5432

# View service files
ls -la
```

### Collect Debug Information

```bash
# System status
./scripts/status.sh > debug.txt

# Service logs
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml logs >> debug.txt

# Configuration
docker compose config >> debug.txt

# Container info
docker ps -a >> debug.txt

# Network info
docker network inspect roach-network >> debug.txt
```

## Recovery Procedures

### Clean State Reset

**When**: System in corrupted state, need fresh start

```bash
# Complete reset
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
docker network prune -f
docker volume prune -f
rm -rf data/
./scripts/start-all.sh
```

**Note**: This deletes all data. Backup first if needed.

### Partial Reset

**Reset Kafka only**:
```bash
./scripts/stop-all.sh
rm -rf data/kafka/* data/zookeeper/*
./scripts/start-all.sh
```

**Reset PostgreSQL only**:
```bash
./scripts/stop-all.sh
rm -rf data/postgres/*
./scripts/start-all.sh
# Re-run migrations: ./scripts/db/migrate.sh up
```
