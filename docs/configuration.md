# Configuration

## Environment Variables

### Required (All Services)

```bash
# WeatherLink API
WEATHERLINK_API_KEY=<your_api_key>
WEATHERLINK_API_SECRET=<your_api_secret>
WEATHERLINK_STATION_ID=<your_station_id>

# PostgreSQL
POSTGRES_PASSWORD=<secure_password>
```

### Weather Publisher (weather-publish)

#### Optional
```bash
KAFKA_BROKER=kafka:29092            # Kafka broker address
POSTGRES_DSN=host=postgres port=5432 user=roach password=${POSTGRES_PASSWORD} dbname=roach sslmode=disable
FETCH_INTERVAL=5m                   # Data fetch interval (Go duration format)
LOG_LEVEL=info                      # Logging level: debug, info, warn, error
```

### Weather SQL (weather-sql)

#### Optional
```bash
KAFKA_BROKER=kafka:29092            # Kafka broker address
POSTGRES_DSN=host=postgres port=5432 user=roach password=${POSTGRES_PASSWORD} dbname=roach sslmode=disable
LOG_LEVEL=info                      # Logging level
BATCH_SIZE=100                      # Processing batch size
```

### PostgreSQL Configuration

**File**: `docker-compose.infrastructure.yml`

```yaml
postgres:
  environment:
    POSTGRES_DB: roach              # Database name
    POSTGRES_USER: roach            # Database user
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}  # From .env file
```

**Schema**: Auto-initialized from `scripts/db/init/01-schema.sql`

### Kafka Configuration

**File**: `docker-compose.infrastructure.yml`

#### Broker Settings
```yaml
KAFKA_BROKER_ID: 1
KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```

#### Listeners
```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

#### Retention (Infinite)
```yaml
KAFKA_LOG_RETENTION_MS: -1       # Keep forever
KAFKA_LOG_RETENTION_BYTES: -1    # No size limit
```

#### Performance
```yaml
KAFKA_NUM_NETWORK_THREADS: 3
KAFKA_NUM_IO_THREADS: 8
KAFKA_SOCKET_SEND_BUFFER_BYTES: 102400
KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: 102400
KAFKA_SOCKET_REQUEST_MAX_BYTES: 104857600
```

#### Auto-Create Topics
```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

### Zookeeper Configuration

```yaml
ZOOKEEPER_CLIENT_PORT: 2181
ZOOKEEPER_TICK_TIME: 2000
```

## Configuration Files

### .env
**Location**: Project root
**Purpose**: Environment variables for Docker Compose
**Template**: `.env.example`

```bash
# Copy template and edit
cp .env.example .env
nano .env
```

### docker-compose.infrastructure.yml
**Purpose**: Infrastructure configuration (Kafka, Zookeeper, UI)
**Edit**: For Kafka broker settings, retention policies, ports

### docker-compose.yml
**Purpose**: Application services configuration
**Edit**: For adding/modifying services

## Customization

### Change Data Fetch Interval

In `docker-compose.yml`:
```yaml
services:
  weather-publish:
    environment:
      - FETCH_INTERVAL=10m  # Change from 5m to 10m
```

### Configure PostgreSQL Password

In `.env` file:
```bash
POSTGRES_PASSWORD=your_secure_password_here
```

**Note**: Required for PostgreSQL to start. Choose a strong password.

### Limit Data Retention (Optional)

To limit disk growth, edit `docker-compose.infrastructure.yml`:
```yaml
kafka:
  environment:
    KAFKA_LOG_RETENTION_MS: 2592000000  # 30 days in milliseconds
    KAFKA_LOG_RETENTION_BYTES: 1073741824  # 1GB
```

### Change Kafka Ports

Edit `docker-compose.infrastructure.yml`:
```yaml
kafka:
  ports:
    - "9093:9092"  # Change external port from 9092 to 9093
```

### Add Logging Configuration

For services in `docker-compose.yml`:
```yaml
services:
  weather-publish:
    environment:
      - LOG_LEVEL=debug  # More verbose logging
  
  weather-sql:
    environment:
      - LOG_LEVEL=debug
```

### Database Query Tool

Query weather data from PostgreSQL:
```bash
# Show statistics
./scripts/db/query.sh stats

# List devices
./scripts/db/query.sh devices

# Show recent records
./scripts/db/query.sh recent

# Interactive psql session
./scripts/db/query.sh psql
```

## WeatherLink API Credentials

### Obtaining Credentials

1. Visit https://www.weatherlink.com/account
2. Navigate to API Tokens section
3. Generate v2 API token
4. Copy API Key and API Secret
5. Find Station ID in Devices section

### Station ID Lookup

If you don't know your station ID:
1. Log into WeatherLink account
2. Go to Devices
3. Select your weather station
4. Station ID is in the URL or device details

## Validation

### Check Configuration

```bash
# View current environment variables
docker compose config

# Verify .env file loaded
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml config | grep WEATHERLINK
```

### Test Kafka Connection

```bash
# Check Kafka is listening
docker exec roach-kafka kafka-broker-api-versions --bootstrap-server localhost:29092
```

### Test PostgreSQL Connection

```bash
# Check PostgreSQL is ready
docker exec roach-postgres pg_isready -U roach

# Connect to database
docker exec -it roach-postgres psql -U roach -d roach
```

### Verify Service Configuration

```bash
# Check weather-publish environment
docker exec roach-weather-publish env | grep WEATHERLINK
docker exec roach-weather-publish env | grep KAFKA
docker exec roach-weather-publish env | grep POSTGRES

# Check weather-sql environment
docker exec roach-weather-sql env | grep KAFKA
docker exec roach-weather-sql env | grep POSTGRES
```
