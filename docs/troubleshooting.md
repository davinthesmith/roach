# Troubleshooting

## System Won't Start

### Infrastructure Not Starting

**Symptom**: Services fail to start or exit immediately

**Check**:
```bash
docker compose -f docker-compose.infrastructure.yml logs
```

**Common Causes**:
1. Port conflict (9092, 2181, 8080 already in use)
2. Insufficient disk space
3. Docker not running

**Solutions**:
```bash
# Check ports
lsof -i :9092
lsof -i :2181
lsof -i :8080

# Check disk space
df -h

# Restart Docker
# On macOS: Restart Docker Desktop
# On Linux: sudo systemctl restart docker
```

### Services Fail Health Check

**Symptom**: Containers restart repeatedly or stay "unhealthy"

**Check**:
```bash
docker ps
docker inspect roach-kafka | grep -A 10 Health
```

**Solutions**:
```bash
# Wait longer (Kafka takes 30-60s)
sleep 60
docker ps

# Check logs for errors
docker logs roach-kafka
docker logs roach-zookeeper

# Clean restart
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
./scripts/start-all.sh
```

## Weather Service Issues

### Connection Refused

**Symptom**: `dial tcp: connect: connection refused`

**Cause**: Kafka not ready when service started

**Solution**:
```bash
# Wait for Kafka to be healthy
docker ps  # Check for "(healthy)" status

# Restart weather service
./scripts/restart.sh weather
```

### API Authentication Errors

**Symptom**: 401 Unauthorized or invalid credentials

**Check**:
```bash
# Verify credentials loaded
docker exec roach-weather env | grep WEATHERLINK
```

**Solution**:
```bash
# Update .env file
nano .env

# Restart service
./scripts/restart.sh weather
```

### No Data Publishing

**Symptom**: Service runs but no topics created

**Check**:
```bash
# View service logs
./scripts/logs.sh weather

# Check if topics exist
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092
```

**Common Causes**:
1. Invalid API credentials
2. Station ID incorrect
3. Network connectivity issues

**Debug**:
```bash
# Test WeatherLink API directly
curl -v "https://api.weatherlink.com/v2/current/[station-id]?api-key=[key]"

# Check service can reach Kafka
docker exec roach-weather nc -zv kafka 29092
```

## Kafka Issues

### Broker Not Responding

**Symptom**: Kafka doesn't respond to requests

**Check**:
```bash
docker logs roach-kafka
docker exec roach-kafka kafka-broker-api-versions --bootstrap-server localhost:29092
```

**Solution**:
```bash
# Restart Kafka
docker restart roach-kafka

# If persists, clean restart
docker compose -f docker-compose.infrastructure.yml down
rm -rf data/kafka  # WARNING: Deletes all data
./scripts/start-infra.sh
```

### Topics Not Auto-Created

**Symptom**: Topics don't appear when service publishes

**Check**:
```bash
docker exec roach-kafka kafka-configs --bootstrap-server localhost:29092 \
  --entity-type brokers --entity-default --describe | grep auto.create
```

**Verify**: Should show `auto.create.topics.enable=true`

**Solution**: Manually create topic:
```bash
docker exec roach-kafka kafka-topics \
  --create \
  --topic weather.iss \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:29092
```

### Out of Disk Space

**Symptom**: Kafka fails to write, errors about disk

**Check**:
```bash
df -h
du -sh data/kafka
```

**Solution**:
```bash
# Temporary: Delete old data
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down
rm -rf data/kafka/*
./scripts/start-all.sh

# Permanent: Configure retention limits (see configuration.md)
```

## Kafka UI Issues

### UI Won't Load

**Symptom**: http://localhost:8080 doesn't respond

**Check**:
```bash
docker logs roach-kafka-ui
docker ps | grep kafka-ui
```

**Common Causes**:
1. Kafka not healthy yet (UI waits for Kafka)
2. Port 8080 in use

**Solution**:
```bash
# Wait for Kafka
sleep 30

# Check port
lsof -i :8080

# Restart UI
docker restart roach-kafka-ui
```

### Topics Page Spinning / "Retrying to fetch metadata"

**Symptom**: Topics page loads but spins forever, logs show "Retrying to fetch metadata"

**Cause**: Kafka listener configuration - kafka-ui getting wrong advertised listener

**Check**:
```bash
docker logs roach-kafka-ui | grep -i "retry"
```

**Solution**:
```bash
# Verify listener configuration in docker-compose.infrastructure.yml
# Must have INTERNAL and EXTERNAL listeners properly separated:
# KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
# KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092

# If configuration changed, clean restart required:
docker compose -f docker-compose.infrastructure.yml down -v
rm -rf data/zookeeper/* data/kafka/*
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d
```

### UI Shows No Topics

**Symptom**: UI loads but shows empty topics list

**Check**:
```bash
# Verify topics exist
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# Check UI can reach Kafka
docker exec roach-kafka-ui nc -zv kafka 29092
```

**Solution**:
```bash
# Restart UI
./scripts/restart.sh
```

## Network Issues

### Services Can't Communicate

**Symptom**: Services can't reach each other

**Check**:
```bash
docker network ls | grep roach
docker network inspect roach-network
```

**Solution**:
```bash
# Recreate network
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down
docker network rm roach-network
./scripts/start-all.sh
```

### External Access Not Working

**Symptom**: Can't connect to Kafka from host

**Check**:
```bash
# Test connection
nc -zv localhost 9092

# Check if port exposed
docker ps | grep 9092
```

**Solution**: Verify port mapping in `docker-compose.infrastructure.yml`:
```yaml
kafka:
  ports:
    - "9092:9092"
```

## Performance Issues

### High CPU Usage

**Check**:
```bash
docker stats
```

**Common Causes**:
1. Kafka compaction running
2. Too many consumers
3. Weather service fetch interval too short

**Solution**:
```bash
# Increase fetch interval
# Edit docker-compose.yml: FETCH_INTERVAL=10m

# Restart service
./scripts/restart.sh weather
```

### High Memory Usage

**Symptom**: System slowing down, Docker using too much RAM

**Check**:
```bash
docker stats
```

**Solution**:
```bash
# Limit Kafka memory (docker-compose.infrastructure.yml)
kafka:
  deploy:
    resources:
      limits:
        memory: 1G
```

## Data Issues

### Data Not Persisting

**Symptom**: Data lost after restart

**Check**: Verify volumes mounted:
```bash
docker inspect roach-kafka | grep -A 10 Mounts
```

**Solution**: Ensure volumes in `docker-compose.infrastructure.yml`:
```yaml
kafka:
  volumes:
    - ./data/kafka:/var/lib/kafka/data
```

### Corrupt Data Files

**Symptom**: Kafka won't start, log errors about corrupt data

**Solution**:
```bash
# Last resort: Delete and restart
docker compose -f docker-compose.infrastructure.yml down
rm -rf data/kafka
./scripts/start-infra.sh
```

## Debug Mode

### Enable Verbose Logging

**Weather Service**:
```yaml
# docker-compose.yml
services:
  weather:
    environment:
      - LOG_LEVEL=debug
```

**Kafka Broker**:
```bash
# View detailed logs
docker logs roach-kafka 2>&1 | grep -i error
```

### Interactive Debugging

```bash
# Enter container
docker exec -it roach-weather sh

# Check environment
env

# Test Kafka connection
nc -zv kafka 29092

# View service files
ls -la
cat main.go
```

## Getting Help

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
```

### Clean State Reset

```bash
# Complete reset
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
docker network prune -f
docker volume prune -f
rm -rf data/
./scripts/start-all.sh
```
