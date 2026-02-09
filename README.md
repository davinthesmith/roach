## ROACH — Real-time Observability Aggregation Conduit for the Home

Kafka-based observability pipeline for home IoT. Collects from WeatherLink, Home Assistant (Ecobee), and UniFi Protect; stores everything in Kafka with effectively infinite retention and materializes into PostgreSQL for querying.

### Features
- **Infinite retention** in Kafka, optimized with compression and lean schemas
- **PostgreSQL materialization** with `devices` / `tags` / `records_*` hierarchy
- **WeatherLink integration** for outdoor/indoor weather data and rich metadata
- **Home Assistant (Ecobee)** streaming and thermostat command handling
- **UniFi Protect** smart video/audio/motion event ingestion
- **Two-stage backfill** (API→Kafka, Kafka→DB) for recovery and reconstruction

### Architecture

- **Stack**: Docker Compose, Kafka 7.5, PostgreSQL 16, Zookeeper, Go 1.21+
- **Data flow**: IoT → service → Kafka → PostgreSQL view (`records`) → SQL
- **Services**:
  - `weatherlink-kafka`: WeatherLink API → Kafka (real-time + optional backfill)
  - `weatherlink-sql`: Kafka → PostgreSQL materializer (COPY, worker pool)
  - `weatherlink-sql-backfill`: One-shot Kafka → PostgreSQL backfill
  - `homeassistant-kafka`: Home Assistant WebSocket → Kafka (Ecobee events)
  - `homeassistant-command`: Kafka → Home Assistant (`call_service` commands)
  - `ubiquiti-kafka`: UniFi Protect WebSocket → Kafka (smart/audio/motion)

### Project layout

```text
roach/
├── docker-compose.infrastructure.yml   # Kafka, Zookeeper, Postgres, Kafka UI
├── docker-compose.yml                  # Application services
├── CLAUDE.md                           # Single-file context for AI/maintainers
├── scripts/                            # Ops scripts (start, status, logs, db, backfill)
├── docs/                               # Architecture, operations, topics, standards
├── services/                           # Go services (each with its own README)
└── data/                               # Kafka / Zookeeper / Postgres volumes
```

### Quick start

```bash
# 1. Configure credentials (WeatherLink, Home Assistant, UniFi, Postgres)
cp .env.example .env
vim .env

# 2. Start everything
./scripts/start-all.sh          # Normal
./scripts/start-all.sh build    # Rebuild containers after code changes

# 3. Check health and logs
./scripts/status.sh
./scripts/logs.sh weatherlink-kafka

# 4. Stop
./scripts/stop-all.sh           # Preserve data
./scripts/stop-all.sh clean     # Remove volumes (fresh start)
```

Access:
- **Kafka UI**: `http://localhost:8080`
- **PostgreSQL**: `localhost:5432` (`roach` / `roach` by default)

### Configuration

Set these in `.env`:

```bash
# WeatherLink
WEATHERLINK_API_KEY=...
WEATHERLINK_API_SECRET=...
WEATHERLINK_STATION_ID=...

# Home Assistant
HA_URL=http://homeassistant:8123
HA_TOKEN=...

# UniFi Protect
UNIFI_HOST=https://192.168.1.1
UNIFI_API_KEY=...

# PostgreSQL
POSTGRES_PASSWORD=...
```

Each service also supports additional env vars (intervals, backfill ranges, log levels); see the READMEs under `services/<name>/`.

### Operations

Common commands:

```bash
./scripts/start-all.sh | ./scripts/start-all.sh build | ./scripts/start-all.sh clean
./scripts/stop-all.sh  | ./scripts/stop-all.sh clean
./scripts/start-infra.sh                # Infra only (for local dev)
./scripts/status.sh                     # System overview
./scripts/logs.sh <service>             # Tail logs

# Database
./scripts/db/query.sh stats | devices | tags <lsid> | recent | orphans | psql
./scripts/db/migrate.sh status | up | down | create <name>

# Backfill
BACKFILL_START_TS=... BACKFILL_END_TS=... ./scripts/weatherlink/kafka-backfill.sh
./scripts/weatherlink/sql-backfill.sh [--metadata] [--topics ...]
```

More detail:
- Operations & maintenance: `docs/operations.md`
- Troubleshooting: `docs/troubleshooting.md`

### Documentation

- High-level context: `CLAUDE.md`
- Architecture: `docs/architecture.md`
- Operations: `docs/operations.md`
- Troubleshooting: `docs/troubleshooting.md`
- Go structure: `docs/go-standards.md`
- Kafka standards & topics: `docs/kafka-standards.md`, `docs/kafka-topics.md`
- Migrations: `docs/migrations.md`

Each service also has a focused README under `services/<name>/README.md` with its specific config, behavior, and examples.

### Status & roadmap

This is a personal project focused on reliable home observability with long-term storage. See `CHANGELOG.md` for history and evolution; new services and integrations follow the standards in `CLAUDE.md` and `docs/go-standards.md`.

