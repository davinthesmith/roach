# ROACH — Real-time Observability Aggregation Conduit for the Home

Kafka-based data aggregation system for home IoT and automation with infinite data persistence.

## Overview

ROACH is a scalable Kafka system designed to collect, persist, and stream data from home IoT devices. Currently includes WeatherLink weather station integration.

## Quick Start

```bash
# Configure
cp .env.example .env
vim .env  # Add your WeatherLink credentials

# Start
./scripts/start-all.sh

# Or start with rebuild (after code changes)
./scripts/start-all.sh build

# Monitor
./scripts/status.sh
```

Access Kafka UI: http://localhost:8080

## Features

- Infinite data retention in Kafka
- PostgreSQL materialization with Device/Tag/Record hierarchy
- Rich metadata capture with units and descriptions from API
- Database migration framework with version tracking
- Timestamp-based deduplication
- Auto-restart on system boot
- Web-based monitoring UI
- Topic-based data organization
- Change detection for metadata
- Docker Compose infrastructure
- Real-time data streaming and SQL storage

## Documentation

**Start here**: [AI-CONTEXT.md](docs/AI-CONTEXT.md) - Consolidated context covering 80% of what you need

### Core Documentation
- **[AI-CONTEXT.md](docs/AI-CONTEXT.md)** - **Start here** - Complete overview, architecture, operations, services, troubleshooting
- **[Architecture](docs/architecture.md)** - Detailed system design and specifications
- **[Operations](docs/operations.md)** - Advanced operations and maintenance
- **[Troubleshooting](docs/troubleshooting.md)** - Comprehensive problem solving
- **[Go Standards](docs/go-standards.md)** - Complete code organization standards
- **[Kafka Topics](docs/kafka-topics.md)** - Full topic schemas and message formats
- **[Migrations](docs/migrations.md)** - Database migration framework
- **[Changelog](CHANGELOG.md)** - Version history and changes

## Project Structure

```
roach/
├── docker-compose.infrastructure.yml  # Kafka, Zookeeper, UI
├── docker-compose.yml                 # Application services
├── scripts/                           # Helper scripts
├── docs/                              # Documentation
├── services/                          # Service implementations
│   ├── weather-publish/              # Weather publisher service
│   └── weather-sql/                  # Weather SQL materializer
└── data/                             # Persistent data
```

## Prerequisites

- Docker & Docker Compose
- WeatherLink account with API credentials
- (Optional) SSL certificates for external access

## Configuration

Required environment variables in `.env`:

```bash
# WeatherLink API
WEATHERLINK_API_KEY=your_api_key
WEATHERLINK_API_SECRET=your_api_secret
WEATHERLINK_STATION_ID=your_station_id

# PostgreSQL
POSTGRES_PASSWORD=your_secure_password
```

See [AI-CONTEXT.md](docs/AI-CONTEXT.md) for all configuration options.

## Common Commands

```bash
# Start everything
./scripts/start-all.sh

# Start with rebuild (after code changes)
./scripts/start-all.sh build

# View status
./scripts/status.sh

# View logs
./scripts/logs.sh weather-publish
./scripts/logs.sh weather-sql

# Query database
./scripts/db/query.sh stats

# Database migrations
./scripts/db/migrate.sh status  # Show migration status
./scripts/db/migrate.sh up      # Apply pending migrations
./scripts/db/migrate.sh down    # Rollback last migration

# Restart service
./scripts/restart.sh weather

# Stop all
./scripts/stop-all.sh
```

See [Operations](docs/operations.md) for complete command reference.

## Topics

Current Kafka topics:
- `weather.iss` - Outdoor weather data
- `weather.barometer` - Barometric pressure
- `weather.indoor` - Indoor conditions
- `weather.health` - Console health
- `weather.metadata.*` - Sensor metadata

PostgreSQL tables:
- `devices` - Sensor registry with full device metadata
- `tags` - Field definitions with units and descriptions
- `sensor_catalog` - Field metadata from WeatherLink API
- `records_numeric`, `records_text`, `records_null` - Time-series data
- `records` view - Unified query interface
- `schema_migrations` - Migration tracking

See [Kafka Topics](docs/kafka-topics.md) for schemas and message formats.

## Adding Services

1. Create service in `services/<name>/`
2. Add to `docker-compose.yml`:
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

3. Use topic naming: `namespace.category.subcategory`

See [Architecture](docs/architecture.md) for extension details.

## Monitoring

- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: localhost:5432 (user: roach, db: roach)
- **Status Script**: `./scripts/status.sh`
- **Database Query**: `./scripts/db/query.sh`
- **Logs**: `./scripts/logs.sh`
- **Docker Stats**: `docker stats`

## Troubleshooting

Common issues:
- Services won't start → Check Docker running, ports available
- Connection refused → Wait for Kafka health check (30-60s)
- No data → Verify API credentials in `.env`

See [Troubleshooting](docs/troubleshooting.md) for detailed solutions.

## License

Personal project - use as you wish.
