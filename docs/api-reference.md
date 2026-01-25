# API Reference

## Helper Scripts

### start-all.sh
Start infrastructure and all services

```bash
./scripts/start-all.sh
```

### start-infra.sh
Start only infrastructure (Kafka, Zookeeper, Kafka UI)

```bash
./scripts/start-infra.sh
```

### stop-all.sh
Stop all services

```bash
./scripts/stop-all.sh
```

### logs.sh
View logs for all or specific service

```bash
# All services
./scripts/logs.sh

# Specific service
./scripts/logs.sh weather
./scripts/logs.sh kafka
```

### restart.sh
Restart service(s)

```bash
# Restart specific service
./scripts/restart.sh weather

# Restart all
./scripts/restart.sh
```

### status.sh
Check system health and status

```bash
./scripts/status.sh
```

## Docker Compose Commands

### Start Services
```bash
# Infrastructure only
docker compose -f docker-compose.infrastructure.yml up -d

# Everything
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d

# With logs visible
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up
```

### Stop Services
```bash
# Stop all
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down

# Stop and remove volumes
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
```

### View Status
```bash
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml ps
```

### Rebuild Service
```bash
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d --build weather
```

## Kafka CLI Commands

### Topics

#### List Topics
```bash
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092
```

#### Describe Topic
```bash
docker exec roach-kafka kafka-topics \
  --describe \
  --topic weather.iss \
  --bootstrap-server localhost:29092
```

#### Create Topic (Manual)
```bash
docker exec roach-kafka kafka-topics \
  --create \
  --topic test.topic \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:29092
```

#### Delete Topic
```bash
docker exec roach-kafka kafka-topics \
  --delete \
  --topic test.topic \
  --bootstrap-server localhost:29092
```

### Consumers

#### Read Messages
```bash
# From beginning
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --from-beginning \
  --max-messages 10

# From end (latest)
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss

# With headers
docker exec roach-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather.iss \
  --property print.headers=true \
  --from-beginning
```

#### List Consumer Groups
```bash
docker exec roach-kafka kafka-consumer-groups \
  --list \
  --bootstrap-server localhost:29092
```

#### Describe Consumer Group
```bash
docker exec roach-kafka kafka-consumer-groups \
  --describe \
  --group my-group \
  --bootstrap-server localhost:29092
```

### Producers

#### Produce Message (Interactive)
```bash
docker exec -it roach-kafka kafka-console-producer \
  --bootstrap-server localhost:29092 \
  --topic test.topic
# Type messages, press Enter, Ctrl+C to exit
```

#### Produce with Key
```bash
docker exec -it roach-kafka kafka-console-producer \
  --bootstrap-server localhost:29092 \
  --topic test.topic \
  --property "parse.key=true" \
  --property "key.separator=:"
# Format: key:value
```

### Broker Info

#### Broker API Versions
```bash
docker exec roach-kafka kafka-broker-api-versions \
  --bootstrap-server localhost:29092
```

#### Cluster Info
```bash
docker exec roach-kafka kafka-metadata \
  --bootstrap-server localhost:29092
```

## Docker Commands

### Container Management

#### List Containers
```bash
docker ps
docker ps -a  # Include stopped
```

#### View Logs
```bash
# All logs
docker logs roach-kafka

# Follow logs
docker logs -f roach-weather

# Last N lines
docker logs --tail=100 roach-kafka
```

#### Execute Command in Container
```bash
docker exec roach-kafka <command>
docker exec -it roach-kafka bash  # Interactive shell
```

#### Inspect Container
```bash
docker inspect roach-kafka
docker inspect roach-kafka | grep IPAddress
```

### Image Management

#### List Images
```bash
docker images
```

#### Remove Image
```bash
docker rmi confluentinc/cp-kafka:7.5.0
```

#### Pull Image
```bash
docker pull confluentinc/cp-kafka:7.5.0
```

### Network Management

#### List Networks
```bash
docker network ls
```

#### Inspect Network
```bash
docker network inspect roach-network
```

### Volume Management

#### List Volumes
```bash
docker volume ls
```

#### Inspect Volume
```bash
docker volume inspect roach_kafka-data
```

## System Monitoring

### Resource Usage
```bash
# All containers
docker stats

# Specific container
docker stats roach-kafka roach-weather
```

### Disk Usage
```bash
# Docker system usage
docker system df

# Detailed
docker system df -v

# Project data
du -sh data/
```

### Health Checks
```bash
# Check health status
docker inspect roach-kafka | grep -A 10 Health

# Container status
docker ps --format "table {{.Names}}\t{{.Status}}"
```

## WeatherLink API (Postman)

### Collection
**Location**: `services/weather/postman/WeatherLink v2 API.postman_collection.json`

### Environment
**Location**: `services/weather/postman/WeatherLink v2 API.postman_environment.json`

### Import to Postman
1. Open Postman
2. Import → Upload Files
3. Select both JSON files
4. Update environment variables with your credentials

## Kafka UI Web Interface

**URL**: http://localhost:8080

### Features
- Browse topics and messages
- View consumer groups
- Monitor broker health
- Inspect configurations
- Search messages
- View message headers and payloads
