# ROACH — Real-time Observability Aggregation Conduit for the Home

Kafka-based data aggregation for home IoT with infinite retention. WeatherLink, Home Assistant (Ecobee), UniFi Protect. PostgreSQL materialization with Device/Tag/Record hierarchy. Eight Go services (6 daemons, 2 backfill/tools) + four native macOS Swift services (detect-person-stream, coreml-smart-crop, coreml-face-crop, coreml-vehicle-detect).

## Architecture

**Stack**: Docker Compose, Kafka 7.5, PostgreSQL 16, Zookeeper, Go 1.21+, Swift 5.9+ (Swift services)

**Data flow**: IoT → Service → Kafka (infinite) → PostgreSQL. Two-stage backfill: (1) API→Kafka via weatherlink-kafka backfill mode; (2) Kafka→DB via weatherlink-sql-backfill.

**Services**:
- `weatherlink-kafka` — WeatherLink API → Kafka (real-time + optional backfill)
- `weatherlink-sql` — Kafka → PostgreSQL materialization (batched COPY, worker pool)
- `weatherlink-sql-backfill` — One-shot Kafka → PostgreSQL replay
- `homeassistant-kafka` — Home Assistant WebSocket → Kafka (Ecobee events)
- `homeassistant-command` — Kafka → Home Assistant (thermostat `call_service`)
- `unifi-kafka` — UniFi Protect WebSocket → Kafka (smart/audio/motion)
- `unifi-video-kafka` — UniFi Protect RTSPS → ffmpeg → Kafka (1 frame/sec per camera, 30-min retention). Commented out in docker-compose; only one video stream at a time.
- `unifi-video-jpg` — UniFi Protect RTSPS → ffmpeg → filesystem (1 frame/sec per camera to `./data/streams/unifi/jpg`, configurable RETENTION)
- `unifi-smart-archive` — Consumes `unifi.protect.smart`; streams event time-window JPEGs from unifi-video-jpg output to `./data/streams/unifi/protect` in real time (archive dir created on detection start, frames copied as they appear via fsnotify); 30-day retention; stops waiting for event end if no follow-up within EVENT_END_TIMEOUT (removes partial archive); exits on Kafka consumer/commit errors
- `detect-person-stream` — Native macOS Swift service (not Docker). Connects to UniFi Protect RTSPS streams (like unifi-video-jpg), samples frames with AVFoundation, runs person classifier, publishes to `detect.person`. Runs on host via `scripts/detect-person-stream/`
- `coreml-smart-crop` — Native macOS Swift service (not Docker). Watches `data/streams/unifi/protect/smart` (person/package/animal/vehicle), YOLO Core ML detection, crops to best bbox per type, writes to `data/streams/coreml/{person|package|animal|vehicle}/`. Runs on host via `scripts/coreml-smart-crop/`
- `coreml-face-crop` — Native macOS Swift service (not Docker). Watches `data/streams/coreml/person`, detects faces with Vision (VNDetectFaceRectanglesRequest), crops each face to `data/streams/coreml/faces/`. Runs on host via `scripts/coreml-face-crop/`
- `coreml-vehicle-detect` — Native macOS Swift service (not Docker). Watches `data/streams/coreml/vehicle`, runs CompCars-based Core ML make/model classifier, publishes to Kafka `detect.vehicle`. Runs on host via `scripts/coreml-vehicle-detect/`

**Infrastructure** (from `docker-compose.infrastructure.yml`):
- Zookeeper: 2181. Kafka: 9092 (external), 29092 (internal). PostgreSQL: 5432. Kafka UI: 8080.
- Data: `./data/kafka`, `./data/zookeeper`, `./data/postgres`
- Topic auto-create enabled; retention -1 (infinite)

**Network**: `roach-network`. Apps use `kafka:29092`, `postgres:5432`. HA/UniFi use HA_URL / UNIFI_HOST.

**Directory structure**:
```
roach/
├── services/<name>/         # main.go, Dockerfile, README.md, config/, service/, ...
├── scripts/                 # start-all.sh, logs.sh, db/, weatherlink/, homeassistant/, detect-person-stream/, coreml-smart-crop/, coreml-face-crop/, coreml-vehicle-detect/
├── docs/                    # architecture, operations, troubleshooting, go-standards, kafka-topics, migrations
├── docker-compose.yml       # Application services
├── docker-compose.infrastructure.yml
├── .env                     # Credentials (from .env.example)
└── data/                    # Ephemeral/downloaded/personal data (see Data below)
```

**Data** (`./data/`): All ephemeral, downloaded, and personal information lives under `./data/`. Not committed (gitignored). Includes:
- **Infrastructure**: `data/kafka`, `data/zookeeper`, `data/postgres` (Docker volumes).
- **Streams**: `data/streams/` — UniFi video/jpg, protect/smart archives, coreml crops (person, vehicle, faces, etc.).
- **Models**: `data/models/` — ML models consumed by services. Download scripts and trained outputs go here. Examples: `data/models/CarRecognition.mlmodel` (+ `.mlmodelc`), `data/models/yolo.mlpackage` (coreml-smart-crop). For new models, use `data/models/<service-or-name>/` or a single file under `data/models/` as appropriate; document in the service README and in this file.
- **Logs**: `data/logs/` — e.g. coreml-vehicle-detect, detect-person-stream daemon logs.

Per-service detail: each has a README in `services/<name>/README.md` (e.g. [weatherlink-kafka](services/weatherlink-kafka/README.md)).

## Quick Start

```bash
cp .env.example .env && vim .env   # WEATHERLINK_*, HA_URL, HA_TOKEN, UNIFI_*, POSTGRES_PASSWORD
./scripts/start-all.sh             # Start all
./scripts/start-all.sh build       # Rebuild + start (after code changes)
./scripts/start-all.sh clean       # Clean slate (removes all data)
./scripts/status.sh
./scripts/logs.sh weatherlink-kafka
./scripts/stop-all.sh              # Keep data. stop-all.sh clean = remove data
```

**Access**: Kafka UI http://localhost:8080 | PostgreSQL localhost:5432 (user `roach`, db `roach`).

## Configuration

**Required** (in `.env`):
```bash
WEATHERLINK_API_KEY=...   WEATHERLINK_API_SECRET=...   WEATHERLINK_STATION_ID=...
HA_URL=http://homeassistant:8123   HA_TOKEN=...
UNIFI_HOST=https://192.168.1.1     UNIFI_API_KEY=...
POSTGRES_PASSWORD=...
```

**Optional (defaults)** — key vars only; see service READMEs for full lists:
- weatherlink-kafka: `KAFKA_BROKER=kafka:29092`, `FETCH_INTERVAL=5m`, `METADATA_FETCH_INTERVAL=168h`, `KAFKA_BACKFILL_ENABLED`, `BACKFILL_START_TS`/`BACKFILL_END_TS`
- weatherlink-sql: `BATCH_SIZE=100`, `WORKER_POOL_SIZE=4`, `BATCH_FLUSH_INTERVAL_MS=500`, `DB_POOL_MAX_CONNS=10`
- homeassistant-kafka: `HA_WS_URL` (derived from HA_URL), `POLL_ENABLED=false`, `POLL_ENTITY_FILTER`
- homeassistant-command: `KAFKA_TOPIC=homeassistant.command`, `KAFKA_CONSUMER_GROUP=homeassistant-command`
- unifi-kafka: `RECONNECT_BACKOFF=1s,5s,30s`
- unifi-video-kafka: `RECONNECT_BACKOFF=1s,5s,30s` (topics: `unifi.protect.video.*`, 30-min retention)
- unifi-video-jpg: `JPG_OUTPUT_DIR`, `RETENTION=30m`, `RECONNECT_BACKOFF=1s,5s,30s`
- unifi-smart-archive: `SOURCE_DIR`, `ARCHIVE_DIR`, `EVENT_END_TIMEOUT=1m` (stop waiting for event end), `ARCHIVE_RETENTION_DAYS=10`, `LEAD_SECONDS`, `TRAIL_SECONDS`
- weatherlink-sql-backfill: `TOPICS=weather.iss,...`, `START_OFFSET=-2`, `END_OFFSET=-1`, `INCLUDE_METADATA`, CLI: `--topics`, `--metadata`, `--workers`, etc.
- detect-person-stream: `UNIFI_HOST`, `UNIFI_API_KEY` (required), `KAFKA_BROKER=localhost:9092`, `KAFKA_TOPIC=detect.person`, `MODEL_DIR`, `CONFIDENCE_THRESHOLD=0.7`, `FRAME_INTERVAL=1.0`, `RECONNECT_BACKOFF=1s,5s,30s`
- coreml-smart-crop: `WATCH_ROOT=./data/streams/unifi/protect/smart`, `COREML_OUTPUT_DIR=./data/streams/coreml`, `YOLO_MODEL_PATH=./data/models/yolo.mlpackage`, `DEBOUNCE_SECONDS=1.0`, `LOG_LEVEL=info`
- coreml-face-crop: `WATCH_DIR=./data/streams/coreml/person`, `FACES_DIR=./data/streams/coreml/faces`, `DEBOUNCE_SECONDS=1.0`, `LOG_LEVEL=info`
- coreml-vehicle-detect: `WATCH_DIR=./data/streams/coreml/vehicle`, `CAR_MODEL_PATH=./data/models/CarRecognition.mlmodel`, `KAFKA_BROKER=localhost:9092`, `KAFKA_TOPIC=detect.vehicle`, `DEBOUNCE_SECONDS=1.0`, `LOG_LEVEL=info`

**File locations**: Credentials `.env` (root). Infra `docker-compose.infrastructure.yml`. Services `docker-compose.yml`. Scripts `./scripts/`. Docs `./docs/`.

## Commands

```bash
# Start/Stop
./scripts/start-all.sh | start-all.sh build | start-all.sh clean
./scripts/stop-all.sh | stop-all.sh clean
./scripts/start-infra.sh              # Infrastructure only
./scripts/restart-all.sh [service]    # Restart one or all

# Status & logs
./scripts/status.sh
./scripts/logs.sh [service]
docker stats

# Database
./scripts/db/query.sh stats | devices | tags <lsid> | recent | orphans | psql
./scripts/db/migrate.sh status | up | down | create <name>
./scripts/db/reload-orphans.sh        # Reprocess orphans

# Backfill
BACKFILL_START_TS=... BACKFILL_END_TS=... ./scripts/weatherlink/kafka-backfill.sh
./scripts/weatherlink/sql-backfill.sh [--topics ...] [--metadata]

# coreml-smart-crop / coreml-face-crop / coreml-vehicle-detect (native macOS, not Docker)
./scripts/coreml-smart-crop/build/build.sh [release|clean]
./scripts/coreml-smart-crop/run/detect.sh | start.sh | stop.sh | status.sh | logs.sh [lines]
./scripts/coreml-face-crop/build/build.sh [release|clean]
./scripts/coreml-face-crop/run/detect.sh | start.sh | stop.sh | status.sh | logs.sh [lines]
./scripts/coreml-vehicle-detect/build/build.sh [release|clean]
./scripts/coreml-vehicle-detect/run/detect.sh | start.sh | stop.sh | status.sh | logs.sh [lines]
Models: `./scripts/models/download-yolo.sh` (smart-crop → `data/models/yolo.mlpackage`), `./scripts/models/download-car-model.sh` (vehicle-detect → `data/models/CarRecognition.mlmodel` + `.mlmodelc`).
```

**Kafka**: List topics / consumer groups / consume: use `docker exec roach-kafka kafka-topics ...` and `kafka-console-consumer` / `kafka-consumer-groups` (see [docs/operations.md](docs/operations.md)).

Full script reference: [scripts/README.md](scripts/README.md).

## Kafka Topics

**Naming**: `namespace.category[.subcategory]`

**Weather** (weatherlink-kafka): `weather.iss`, `weather.barometer`, `weather.indoor`, `weather.health`, `weather.other`; metadata: `weather.metadata.sensors`, `weather.metadata.catalog`, `weather.metadata.station`. Key `lsid:timestamp`. Headers: `schema_version`, `lsid`, `timestamp`, `sensor_type`, `data_structure_type`. Body: JSON data point.

**Home Assistant**: `homeassistant.ecobee.*` (thermostat, weather, sensor.*, other). Key `friendly_name:timestamp`. Consumed: `homeassistant.command`.

**UniFi Protect**: `unifi.protect.smart` (person, vehicle, animal, package); consumed by unifi-smart-archive for image archiving. `unifi.protect.audio` (babyCry, coAlarm, smoke, speak), `unifi.protect.motion`. Key `camera_name:timestamp`. **Video**: `unifi.protect.video.{camera_name}` (1 JPEG frame/sec, 30-min retention). Key `camera_id:timestamp`.

**Person Detection** (detect-person-stream): `detect.person`. Key `person_name:image_timestamp`. Headers: `schema_version`, `camera_name`, `event_start`, `timestamp`, `source`. Body: JSON with `person`, `confidence`, `alternatives`, `image_path`, `camera_name`, `event_start`, `image_timestamp`.

Full schemas: [docs/kafka-topics.md](docs/kafka-topics.md). Storage/optimization: [docs/kafka-standards.md](docs/kafka-standards.md).

## Database Schema

**Core**: `devices` (sensors, PK id, unique lsid) → `tags` (field defs, unit/description from catalog) → `records_numeric` | `records_text` | `records_null` (PK `tag_id`, `ts`). `records` view unifies record types. `sensor_catalog` enriches tags. `orphaned_messages` for failed processing. `schema_migrations` for migration tracking.

**Relationships**: devices 1→N tags → N records_*; sensor_catalog → tag enrichment.

Migrations: [docs/migrations.md](docs/migrations.md). Init: `scripts/db/init/01-schema.sql`.

## Conventions

### Code (Go)
- [docs/go-standards.md](docs/go-standards.md): minimal main, config/, models/, service/, repository/ or api/ or kafka/; DI; context propagation; no package-level state.
- Config via env only; graceful shutdown on SIGTERM.

### Documentation (MUST FOLLOW)
1. **NO new .md in project root** — all docs in `docs/`.
2. **Update docs when adding features** — terse, AI-optimized.
3. **NO historical info except CHANGELOG.md** — all changes/summaries in CHANGELOG only.
4. **Update CLAUDE.md for significant changes** — keep this file current.
5. **NEVER edit past CHANGELOG entries** — append to top only.

### Adding a service
1. Create `services/<name>/` (main.go, Dockerfile, config/, service/, README).
2. Add to `docker-compose.yml` with `KAFKA_BROKER=kafka:29092`, health deps, `roach-network`.
3. Topic naming: `namespace.category.subcategory`. Message: headers + JSON body, timestamp in headers.

### Migrations
- Path: `scripts/db/migrations/`. Naming: `NNN_description.up.sql` / `.down.sql`. Include up + down; test rollback. See [docs/migrations.md](docs/migrations.md).

## Monitoring

- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: localhost:5432, user `roach`, db `roach`
- **Logs**: `./scripts/logs.sh <service>`
- **Status**: `./scripts/status.sh`

## Troubleshooting

| Issue | Action |
|-------|--------|
| Won't start | Docker running; ports 5432, 8080, 9092, 29092 free; `docker ps`, `lsof -i :5432` |
| Connection refused | Kafka health 20–60s; `./scripts/status.sh`; then `./scripts/restart-all.sh <service>` |
| No data in Kafka | Check API credentials in `.env`; `./scripts/logs.sh weatherlink-kafka` |
| DB empty | Run `./scripts/weatherlink/sql-backfill.sh`; optionally `--metadata` first |
| Kafka UI not loading | Kafka healthy; `docker logs roach-kafka-ui`; `lsof -i :8080` |
| Empty tag units | Catalog/enrichment; `SELECT COUNT(*) FROM tags WHERE unit IS NOT NULL;` |
| Clean restart | `./scripts/stop-all.sh clean` then `./scripts/start-all.sh`. Full reset: also `rm -rf data/` |

Full guide: [docs/troubleshooting.md](docs/troubleshooting.md).

## Extension & resources

**Add service**: See Conventions above. Example compose block and topic patterns in [docs/architecture.md](docs/architecture.md).

**Deep-dive docs**:
- [docs/architecture.md](docs/architecture.md) — Component specs, network, resources
- [docs/operations.md](docs/operations.md) — Operations, maintenance
- [docs/troubleshooting.md](docs/troubleshooting.md) — Problem solving
- [docs/go-standards.md](docs/go-standards.md) — Go organization
- [docs/kafka-standards.md](docs/kafka-standards.md) — Kafka best practices
- [docs/kafka-topics.md](docs/kafka-topics.md) — Topic schemas
- [docs/migrations.md](docs/migrations.md) — Migrations
- [scripts/README.md](scripts/README.md) — Scripts
- [CHANGELOG.md](CHANGELOG.md) — Version history
