# Architecture

> **Basics**: [CLAUDE.md](../CLAUDE.md). This doc: component specs, network, resources, extension.

## Components

### Kafka Broker
**Image**: `confluentinc/cp-kafka:7.5.0`

| Area | Settings |
|------|----------|
| Listeners | PLAINTEXT 0.0.0.0:29092 (internal), 0.0.0.0:9092 (host); advertised `kafka:29092`, `localhost:9092` |
| Retention | `KAFKA_LOG_RETENTION_MS=-1`, `KAFKA_LOG_RETENTION_BYTES=-1`, `KAFKA_LOG_DIRS=/var/lib/kafka/data` |
| Performance | `KAFKA_NUM_NETWORK_THREADS=3`, `KAFKA_NUM_IO_THREADS=8`, socket buffers 102400, request max 104857600, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` |

**Health**: `kafka-broker-api-versions --bootstrap-server localhost:29092`

### PostgreSQL
**Image**: `postgres:16-alpine`. DB: `roach`, user: `roach`. Init: `scripts/db/init/01-schema.sql`.

**Tables**: `devices` (lsid, metadata JSONB), `tags` (device_id, tag_name, unit, description), `sensor_catalog` (sensor_type, data_structure_type, field_name), `records_numeric`/`records_text`/`records_null` (tag_id, ts), `records` view, `orphaned_messages`, `schema_migrations`.

**Indexes**: Unique on `devices(lsid)`, `tags(device_id, tag_name)`, `sensor_catalog(sensor_type, data_structure_type, field_name)`; (tag_id, ts DESC) on each records_* table.

### Zookeeper
**Image**: `confluentinc/cp-zookeeper:7.5.0`. Port 2181. Kafka coordination only.

### Kafka UI
**Image**: `provectuslabs/kafka-ui:latest`. Connects to `kafka:29092`. Browse topics, messages, consumer groups.

## Service Details

### weatherlink-kafka
- **Flow**: Startup key scan → metadata fetch → current conditions → optional backfill; then metadata loop (interval) + fetch loop (foreground).
- **Dedup**: Kafka key scan at startup (record keys `lsid:timestamp`, metadata keys); in-memory cache updated after publish.
- **API**: `X-Api-Secret` + `api-key` query. Catalog filtered to active sensor types from `/v2/sensors`; one message per sensor type.

### weatherlink-sql
- **Consumer groups**: `weatherlink-sql-metadata` (sensors), `weatherlink-sql-catalog` (catalog), `weatherlink-sql-data` (weather.* data, discovered at startup).
- **Flow**: Headers → device lookup (or orphan) → parse JSON → per field: tag lookup/create (catalog enrichment) → batch writer (numeric/text/null).
- **Caches**: Device (LSID→Device), Tag (device_id:tag_name→Tag), Catalog (sensor_type:data_structure_type:field_name→FieldMetadata); thread-safe.

### unifi-video-kafka
- **Flow**: Fetch cameras → per-camera goroutine: POST rtsps-stream → ffmpeg (1 fps MJPEG) → parse JPEG frames → Kafka topic per camera.
- **Topics**: `unifi.protect.video.{camera_name}` with 30-min retention. Service creates topics via Kafka Admin API.
- **Reconnect**: On ffmpeg exit or RTSPS token expiry, backoff and re-fetch stream URL. Per-camera isolation (one failing camera does not block others).

### unifi-video-jpg
- **Flow**: Same as unifi-video-kafka but writes JPEG frames to filesystem: `{JPG_OUTPUT_DIR}/{camera_name}/{timestamp}.jpg`.
- **Retention**: Configurable `RETENTION` (default 30m); per-camera cleanup every 2 minutes deletes expired files.
- **Reconnect**: Same backoff and offline polling as unifi-video-kafka. No Kafka dependency. Only one of unifi-video-kafka or unifi-video-jpg should run at a time.

## Network

| From | To | Address | Purpose |
|------|----|---------|---------|
| weatherlink-kafka | Kafka | kafka:29092 | Publish |
| weatherlink-sql | Kafka | kafka:29092 | Consume |
| weatherlink-sql | PostgreSQL | postgres:5432 | Materialize |
| kafka-ui | Kafka | kafka:29092 | Monitor |
| Kafka | Zookeeper | zookeeper:2181 | Coordination |
| Host | Kafka / PG / UI | localhost:9092, 5432, 8080 | External access |

**Network**: `roach-network` (bridge). DNS: container names.

## Resources

| Component | CPU | Memory | Disk/day |
|-----------|-----|--------|----------|
| Kafka | 1–5% | 1–2 GB | ~0.3 MB (with LZ4) |
| Zookeeper | <1% | 100–200 MB | ~1 MB |
| PostgreSQL | 1–3% | 100–500 MB | 2–5 MB |
| weatherlink-kafka | <1% | 20–50 MB | — |
| weatherlink-sql | 1–3% | 50–100 MB | — |
| Kafka UI | 1–2% | 100–200 MB | — |

**Total**: ~2–5% CPU, ~1.5–3 GB RAM, ~3–6 MB/day. Kafka: LZ4 + header optimization → ~110 MB/year. See [kafka-standards.md](kafka-standards.md).

## Directory (summary)

Root: `docker-compose*.yml`, `.env`, `CLAUDE.md`, `scripts/`, `docs/`, `data/`, `services/<name>/`. Migrations: `scripts/db/migrations/`. Full tree in [CLAUDE.md](../CLAUDE.md).

## Extension

- **Topics**: Auto-created. Name: `namespace.category.subcategory` (e.g. `home.hvac.temperature`).
- **DB**: `./scripts/db/migrate.sh create <name>` → edit `.up.sql`/`.down.sql` → `migrate.sh up`. See [migrations.md](migrations.md).
- **New service**: Create `services/<name>/` per [go-standards.md](go-standards.md), Dockerfile, add to `docker-compose.yml` with `kafka:29092`, `postgres:5432`, health deps, `roach-network`.

## Security

**Current**: Plaintext in Docker; no auth on Kafka/UI; PG password only. Private network only.

**Production**: TLS for Kafka external; cert auth; PG auth; network policies.

## Data Flow (summary)

Weather: API → weatherlink-kafka (dedup, route by category) → Kafka → weatherlink-sql (devices/tags/records, batch COPY) → PostgreSQL. Metadata: API → weatherlink-kafka (key dedup) → metadata topics → weatherlink-sql (upsert + tag enrichment).

## Tuning

- **Kafka**: Retention -1 or set limit; increase `KAFKA_NUM_IO_THREADS`, `KAFKA_SOCKET_REQUEST_MAX_BYTES` for large messages.
- **PostgreSQL**: Defaults OK for low volume; for scale: `shared_buffers`, `work_mem`; consider partitioning records_* by `ts`.
- **Services**: Increase `FETCH_INTERVAL`; tune `BATCH_SIZE` (weatherlink-sql); `LOG_LEVEL=warn` in production.
