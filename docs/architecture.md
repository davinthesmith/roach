# Architecture

## System Overview

ROACH is a Kafka-based data aggregation system for home IoT devices with infinite data persistence.

## Components

### Infrastructure Layer
**File**: `docker-compose.infrastructure.yml`

#### Zookeeper
- **Image**: `confluentinc/cp-zookeeper:7.5.0`
- **Port**: 2181
- **Purpose**: Kafka cluster coordination
- **Data**: `./data/zookeeper`
- **Health Check**: TCP check on port 2181

#### Kafka Broker
- **Image**: `confluentinc/cp-kafka:7.5.0`
- **Ports**: 
  - 9092 (external, localhost)
  - 29092 (internal, Docker network)
- **Retention**: Infinite (`KAFKA_LOG_RETENTION_MS=-1`)
- **Data**: `./data/kafka`
- **Health Check**: Broker API version check
- **Network**: `roach-network`

#### Kafka UI
- **Image**: `provectuslabs/kafka-ui:latest`
- **Port**: 8080
- **Purpose**: Web-based Kafka monitoring and management

#### PostgreSQL
- **Image**: `postgres:16-alpine`
- **Port**: 5432
- **Purpose**: Time-series data storage and materialization
- **Data**: `./data/postgres`
- **Health Check**: `pg_isready -U roach`
- **Network**: `roach-network`
- **Database**: roach (user: roach)

### Application Layer
**File**: `docker-compose.yml`

#### Weather Publisher (weather-publish)
- **Language**: Go
- **Build**: Multi-stage Dockerfile
- **Purpose**: Fetch WeatherLink data, publish to Kafka with deduplication
- **Interval**: 5 minutes (configurable)
- **Topics**: 7 topics (4 data, 3 metadata)
- **Deduplication**: Timestamp-based cache with PostgreSQL rehydration

#### Weather SQL (weather-sql)
- **Language**: Go
- **Build**: Multi-stage Dockerfile
- **Purpose**: Materialize Kafka messages to PostgreSQL
- **Consumers**: All `weather.*` data topics + metadata
- **Schema**: Device/Tag/Record hierarchy
- **Features**: Auto-tag creation, orphaned message tracking

## Data Flow

```
WeatherLink API (HTTPS)
    ↓ Every 5 minutes
Weather Publisher (Go)
    ↓ Deduplication check
Kafka Broker (Infinite retention)
    ↓ Real-time streaming
Weather SQL (Go)
    ↓ Materialization
PostgreSQL Database
    ↓ Persistent storage
Devices → Tags → Records
```

## Network Architecture

```
┌──────────────────────────────────────────────────┐
│  Infrastructure (docker-compose.infra)           │
│  ┌────────────┐  ┌───────┐  ┌────────┐          │
│  │ Zookeeper  │→ │ Kafka │← │ Kafka  │          │
│  │   :2181    │  │:29092 │  │UI:8080 │          │
│  └────────────┘  └───────┘  └────────┘          │
│                                                  │
│  ┌──────────────────────────────────┐           │
│  │ PostgreSQL :5432                 │           │
│  │ - roach database                 │           │
│  │ - Device/Tag/Record schema       │           │
│  └──────────────────────────────────┘           │
│           roach-network                          │
└──────────────────────────────────────────────────┘
                    ↑
                    │ kafka:29092, postgres:5432
┌───────────────────┴──────────────────────────────┐
│  Applications (docker-compose.yml)               │
│  ┌──────────────┐  ┌─────────────┐  ┌─────────┐ │
│  │Weather       │  │Weather      │  │ Future  │ │
│  │Publisher     │  │SQL          │  │Service  │ │
│  │(Fetch & Pub) │  │(Materialize)│  │         │ │
│  └──────────────┘  └─────────────┘  └─────────┘ │
└──────────────────────────────────────────────────┘
```

## Service Communication

### Internal (Docker Network)
- Services → Kafka: `kafka:29092` (plaintext)
- Services → PostgreSQL: `postgres:5432` (plaintext)
- Kafka UI → Kafka: `kafka:29092` (plaintext)
- Kafka → Zookeeper: `zookeeper:2181`

### External (Host)
- Kafka: `localhost:9092` (plaintext)
- Kafka UI: `localhost:8080` (HTTP)
- PostgreSQL: `localhost:5432` (user: roach, db: roach)

## Directory Structure

```
roach/
├── docker-compose.infrastructure.yml  # Infrastructure
├── docker-compose.yml                 # Applications
├── .env                              # Configuration
├── scripts/                          # Operations
│   ├── start-all.sh
│   ├── start-infra.sh
│   ├── stop-all.sh
│   ├── logs.sh
│   ├── restart.sh
│   ├── status.sh
│   └── db/
│       ├── init/
│       │   └── 01-schema.sql         # Database schema
│       └── reload-orphans.sh          # Reprocess orphaned messages
├── docs/                             # Documentation
├── data/                             # Persistent data
│   ├── kafka/
│   ├── zookeeper/
│   └── postgres/
└── services/                         # Service implementations
    ├── weather/                      # weather-publish
    │   ├── main.go
    │   ├── go.mod
    │   ├── Dockerfile
    │   └── README.md
    └── weather-sql/                  # weather-sql
        ├── main.go
        ├── go.mod
        ├── Dockerfile
        └── README.md
```

## Extension Points

### Adding New Services

1. Create service directory: `services/<name>/`
2. Implement Kafka producer/consumer and/or PostgreSQL integration
3. Add to `docker-compose.yml`:
```yaml
services:
  new-service:
    build: ./services/new-service
    environment:
      - KAFKA_BROKER=kafka:29092
      - POSTGRES_DSN=host=postgres port=5432 user=roach password=${POSTGRES_PASSWORD} dbname=roach sslmode=disable
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - roach-network
```

### Topic Naming Convention
- Format: `namespace.category.subcategory`
- Examples:
  - `weather.iss` (outdoor weather)
  - `home.hvac.temperature`
  - `home.security.motion`

## Resource Usage

### CPU
- Kafka: 1-5% average
- Zookeeper: <1%
- PostgreSQL: 1-3% average
- Weather Publisher: <1% (spikes during fetch)
- Weather SQL: 1-3% average
- Kafka UI: 1-2%

### Memory
- Kafka: 1-2 GB
- Zookeeper: 100-200 MB
- PostgreSQL: 100-500 MB (grows with data)
- Weather Publisher: 20-50 MB
- Weather SQL: 50-100 MB
- Kafka UI: 100-200 MB

### Disk
- Growth: ~1-5 MB/day per service (varies by frequency)
- Kafka: `./data/kafka`
- Zookeeper: `./data/zookeeper`
- PostgreSQL: `./data/postgres`

## Security Model

### Current (Development)
- All communication plaintext within Docker network
- Kafka UI accessible only on localhost
- No authentication required

### Future (Production)
- SSL/TLS for external Kafka access
- Certificate-based authentication
- Let's Encrypt certificates mounted read-only
