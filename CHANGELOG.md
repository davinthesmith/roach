# Changelog

## 2026-02-21

### detect-person Service

#### Added
- **detect-person**: New native macOS Swift service for person classification using CoreML and CreateML. Not Docker — runs on host with Apple Silicon.
- **Train mode**: `MLImageClassifier` trained from labeled directories (`data/train/{person_name}/*.jpg`). Saves compiled CoreML model to `data/models/detect-person/PersonClassifier.mlmodelc`.
- **Detect mode**: FSEvents watcher on `data/streams/unifi/protect/smart/person/` classifies new images via `VNCoreMLRequest` and publishes results to Kafka topic `detect.person` when confidence exceeds threshold.
- **Kafka message**: Topic `detect.person`, key `{person}:{image_timestamp}`, body includes `person`, `confidence`, `alternatives` (with scores), `image_path`, `camera_name`, `event_start`, `image_timestamp`.
- **Scripts**: `scripts/detect-person/` with `build/build.sh`, `train/train.sh`, `run/detect.sh`, `run/start.sh`, `run/stop.sh`, `run/status.sh`, `run/logs.sh`, `run/run-daemon.sh`, `launchd/install-launchd.sh`, `launchd/uninstall-launchd.sh`.
- **Auto-start on reboot**: LaunchAgent support so detect-person starts at login and restarts after reboot or crash. Install with `./scripts/detect-person/launchd/install-launchd.sh`; uses `run/run-daemon.sh` and `.env` for config.
- **Dependencies**: swift-argument-parser, swift-kafka-client, swift-log, swift-service-lifecycle.
- **Documentation**: Service README, updated kafka-topics.md, scripts/README.md, CLAUDE.md.

#### Changed
- **Script layout**: detect-person scripts reorganized into subdirectories: `build/`, `train/`, `run/`, `launchd/` (was flat in `scripts/detect-person/`).

### unifi-smart-archive

#### Changed
- **Retention**: Default archive retention increased from 10 days to 30 days (`ARCHIVE_RETENTION_DAYS` default and docker-compose default).
- **Lead and Lag**: Remove lead and lag time (set to 0) for captures

## 2026-02-09

### unifi-smart-archive Service

#### Added
- **unifi-smart-archive**: New service that consumes `unifi.protect.smart` and copies event time-window JPEGs from unifi-video-jpg output to a long-term archive.
- **Logic**: Only events with `end` are archived. Window: 1 min before event start, 1 min after event end; copy runs after end+trail+delay so trailing frames exist. Path: `{ARCHIVE_DIR}/smart/{detection_type}/{camera_name}/{start_sec}/{timestamp}.jpg`.
- **Event end timeout**: Multiple messages per event (middle ones without `end`, final with `end`). If no message for a given event within `EVENT_END_TIMEOUT` (default 1m), stop waiting for end and do not archive that event; process keeps running.
- **Retention**: Archive content older than 10 days (configurable `ARCHIVE_RETENTION_DAYS`) is deleted periodically.
- **Failure behavior**: Exits on Kafka consumer or commit error; no indefinite retry. Restart via Docker/orchestrator.
- **Docker Compose**: Added `unifi-smart-archive` with read-only mount of `./data/streams/unifi/jpg`, read-write mount of `./data/streams/unifi/protect`.
- **Documentation**: Service README, CLAUDE.md, docs/architecture.md, docs/kafka-topics.md.

### Rename: ubiquiti → unifi

#### Changed
- **Service and topic naming**: Renamed all "ubiquiti" to "unifi" (UniFi is the product line; Ubiquiti is the company). Services: `unifi-kafka`, `unifi-video-kafka`, `unifi-video-jpg`. Kafka topics: `unifi.protect.smart`, `unifi.protect.audio`, `unifi.protect.motion`, `unifi.protect.video.*`. Directories: `services/unifi-kafka`, `services/unifi-video-kafka`, `services/unifi-video-jpg`. Docker service and container names updated accordingly.

### unifi-video-jpg Service

#### Added
- **unifi-video-jpg**: New service that captures 1 frame/sec per UniFi Protect camera via RTSPS → ffmpeg and writes JPEGs to `./data/streams/unifi/jpg` (configurable `JPG_OUTPUT_DIR`).
- **Retention**: Configurable `RETENTION` (default 30m); per-camera cleanup every 2 minutes removes files older than retention.
- **Docker Compose**: Added `unifi-video-jpg` with volume `./data/streams/unifi/jpg`; commented out `unifi-video-kafka` so only one video stream runs at a time.
- **Documentation**: Service README, CLAUDE.md, docs/architecture.md.

### unifi-kafka Service

#### Added
- **unifi-kafka**: New service that subscribes to UniFi Protect events via WebSocket (`/proxy/protect/integration/v1/subscribe/events`) and publishes to Kafka.
- **API client** (`api/client.go`): Connects to local NVR Protect Integration API with API key auth, TLS skip verify for self-signed certs, and WebSocket event subscription.
- **Event classification**: Routes events to `unifi.protect.smart` (person, vehicle, animal, package), `unifi.protect.audio` (babyCry, coAlarm, smoke, speak), and `unifi.protect.motion`.
- **Camera name resolution**: Fetches camera metadata on startup via `/cameras` endpoint for friendly key derivation (e.g., `courtyard:1706140800`).
- **Kafka producer**: Idempotent `confluent-kafka-go` producer with LZ4 compression, batching, and delivery reports.
- **Reconnect loop**: Exponential backoff with configurable delays (`RECONNECT_BACKOFF`), resets after 60s of stable connection.
- **Docker Compose**: Added `unifi-kafka` service definition.
- **Configuration**: Added `UNIFI_HOST`, `UNIFI_API_KEY` to `.env.example`.
- **Documentation**: Service README, updated AI-CONTEXT.md, kafka-topics.md with service entry, topics, and config.

## 2026-02-08

### homeassistant-command Service

#### Added
- **homeassistant-command**: New service that consumes thermostat commands from `homeassistant.command` Kafka topic and executes them against Home Assistant via WebSocket `call_service` API.
- **WebSocket client**: Persistent connection with authentication, auto-reconnect with backoff, ping/pong keepalive, and `call_service` execution with result correlation.
- **Kafka consumer**: Uses `segmentio/kafka-go` with consumer group `homeassistant-command` for exactly-once processing.
- **Supported services**: `climate.set_temperature`, `climate.set_hvac_mode`, `climate.set_preset_mode`, `climate.set_fan_mode`, `climate.turn_on`, `climate.turn_off`.
- **Testing script**: `scripts/homeassistant/send-command.sh` for producing test commands to Kafka with friendly CLI syntax.
- **Docker Compose**: Added `homeassistant-command` service definition.
- **Documentation**: Service README, updated kafka-topics.md, AI-CONTEXT.md, scripts/README.md, .env.example.

## 2026-02-01

### homeassistant-kafka Service

#### Added
- **homeassistant-kafka**: WebSocket `state_changed` ingestion from Home Assistant with Ecobee routing to `homeassistant.ecobee.*` topics.
- **homeassistant-kafka**: Optional REST polling fallback for state changes.
- **Kafka topics**: Added `homeassistant.ecobee.*` topic schemas and headers.
- **Configuration**: Added Home Assistant environment variables and service wiring.

## 2026-01-26

### weatherlink-kafka Metadata Scheduling and Key Dedup

#### Added
- **weatherlink-kafka**: `METADATA_FETCH_INTERVAL` configuration (default `168h`) for periodic metadata refresh.

#### Changed
- **weatherlink-kafka**: Metadata refresh runs on a configurable interval and publishes weekly-keyed sensor/station metadata.
- **weatherlink-kafka**: Startup flow now fetches metadata and current conditions immediately, matching loop logging.
- **weatherlink-kafka**: Deduplication now relies on Kafka key caches for records and metadata topics.
- **weatherlink-kafka**: Topic selection helper moved to `util.GetTopicForCategory()`.

#### Removed
- **weatherlink-kafka**: Timestamp cache and PostgreSQL cache rehydration logic.

### weatherlink-kafka Metadata Hashing

#### Changed
- **weatherlink-kafka**: Exclude `generated_at` from sensor and station metadata hash inputs to avoid republishing unchanged metadata.

#### Added
- **weatherlink-kafka**: Sample WeatherLink API payloads for reference:
  - `services/weatherlink-kafka/testdata/api/current.json`
  - `services/weatherlink-kafka/testdata/api/sensors.json`
  - `services/weatherlink-kafka/testdata/api/sensor-catalog.json`

## 2026-01-25

### Script Reorganization - WeatherLink Scripts

#### Changed
- **Script organization**: Reorganized weatherlink-prefixed scripts into a dedicated directory
  - Created `scripts/weatherlink/` directory for weatherlink-related scripts
  - Moved and renamed scripts:
    - `scripts/weatherlink-kafka-backfill.sh` → `scripts/weatherlink/kafka-backfill.sh`
    - `scripts/weatherlink-sql-backfill.sh` → `scripts/weatherlink/sql-backfill.sh`
  - Updated all documentation references to use new paths
  - Files updated: README.md, service READMEs (weatherlink-kafka-backfill, weatherlink-sql-backfill)
  - Removed reference to non-existent `test-backfill.sh` from README.md

#### Benefits
- Cleaner scripts directory structure with related scripts grouped together
- Shorter script names (removed redundant "weatherlink-" prefix)
- Better organization for future weatherlink-related scripts
- Maintains backward compatibility (scripts still work the same way, just in new location)

#### Migration
If you have any custom scripts or documentation referencing the old paths:
- Old: `./scripts/weatherlink-kafka-backfill.sh`
- New: `./scripts/weatherlink/kafka-backfill.sh`
- Old: `./scripts/weatherlink-sql-backfill.sh`
- New: `./scripts/weatherlink/sql-backfill.sh`

### Kafka Backfill Service - Station Metadata Processing Fix

#### Fixed
- **weatherlink-kafka-backfill service**: Station metadata messages are now processed correctly
- **weatherlink-materializer service**: Station metadata messages are now processed correctly
  - **Root Cause**: Station metadata has different structure than device metadata
    - Device metadata: Has `lsid` field for specific device
    - Station metadata: Has `stations` array with station-level information
  - **Solution**: Enhanced `MetadataProcessor` (in both services) to detect and handle both message formats
    - Automatically detects station metadata by checking for `stations` array
    - Updates all devices at a station with station_id, station_name, station_id_uuid
    - No more orphaned messages from `weather.metadata.station` topic
  
#### Added
- **DeviceRepository.UpdateStationInfo()**: New method to update station info for all devices
  - Updates station_id, station_name, station_id_uuid for all devices at a station
  - Logs number of devices updated
  - Thread-safe database operation

#### Changed
- **MetadataProcessor.ProcessMessage()**: Enhanced to handle two metadata formats
  - Checks for `stations` array (station metadata) vs `lsid` field (device metadata)
  - Routes to appropriate handler: `processStationMetadata()` or device upsert
  - Both formats are now processed without errors
- **MetadataProcessor.processStationMetadata()**: New method for station-level updates
  - Iterates through stations array (typically one station)
  - Extracts station_id, station_name, station_id_uuid
  - Updates all devices at that station via `UpdateStationInfo()`

#### Technical Details
- **Message format detection**:
  ```go
  if stations, ok := metadata["stations"].([]interface{}); ok {
      // Station metadata format
      return p.processStationMetadata(ctx, msg, stations)
  }
  // Otherwise device metadata format (has lsid)
  ```
- **Station metadata structure**:
  ```json
  {
    "stations": [{
      "station_id": 228773,
      "station_name": "Bellevue",
      "station_id_uuid": "a7a3248e-d78f-4ab4-9785-a96abd084493"
    }],
    "generated_at": 1769374906
  }
  ```
- **Device updates**: Updates all devices with matching station_id or NULL station_id

#### Impact
- **Before**: 4 orphaned messages per backfill run from station metadata
- **After**: 0 orphaned messages - all station metadata processed successfully
- Station information now properly populated across all devices

#### Documentation
- **README.md**: Updated metadata processing section
  - Clarified station metadata vs device metadata formats
  - Removed incorrect advice about ignoring station metadata orphans
  - Added explanation of station metadata handling

### Kafka Backfill Service - Comprehensive Orphaned Message Tracking

#### Enhanced
- **weatherlink-kafka-backfill service**: Now saves ALL failed messages to `orphaned_messages` table
  - **Metadata processing failures**: Station, sensor, and catalog metadata errors are now captured
    - Missing LSID in metadata messages
    - Failed JSON parsing in metadata
    - Database upsert failures for devices
    - Missing sensor_type or data_structures in catalog messages
  - **Data processing failures**: Existing orphan tracking for data messages improved
    - Missing device (LSID not found in database)
    - Failed JSON parsing in message body
    - Failed tag creation in database
  - **Complete message storage**: All orphaned messages stored with full headers and body for reprocessing
  - **Final statistics**: Backfill completion report now includes orphan count if any failures occurred

#### Changed
- **MetadataProcessor**: Updated to accept `OrphanRepository` and save failed messages
  - Saves orphans for parse errors, missing LSID, and upsert failures
  - All metadata processing errors now preserved for investigation
- **CatalogProcessor**: Updated to accept `OrphanRepository` and save failed messages
  - Saves orphans for parse errors, missing sensor_type, and missing data_structures
  - Catalog processing errors now preserved for investigation
- **WorkerPool**: Improved orphan saving with detailed error messages
  - Enhanced error logging when saving orphans fails
  - More descriptive reason strings in orphaned_messages table
- **Final statistics**: Enhanced backfill completion report
  - Shows orphan count if any messages failed
  - Directs users to check `orphaned_messages` table for details

#### Technical Details
- **Orphan reasons tracked**:
  - Data messages: `missing_device`, `failed to parse message body`, `failed_to_create_tag`
  - Metadata messages: `missing or invalid lsid in metadata`, `failed to parse metadata`, `failed to upsert device`
  - Catalog messages: `missing sensor_type in catalog message`, `missing data_structures in catalog message`
- **Database schema**: Existing `orphaned_messages` table (from migration 001)
  - Stores topic, partition, offset for Kafka tracking
  - Stores lsid, timestamp, tag_name for debugging
  - Stores complete message_headers and message_body as JSONB for reprocessing
  - Tracks reprocessed status for workflow management

#### Documentation
- **README.md**: Updated with comprehensive orphaned message documentation
  - Added "Orphaned Messages" section under "Monitoring"
  - Lists all orphan types with descriptions
  - Provides troubleshooting guide for common orphan reasons
  - Includes SQL queries for investigating orphaned messages
  - Shows how to mark messages as reprocessed after manual fixes

#### Usage
```bash
# Run backfill with metadata
./scripts/kafka-backfill.sh --start-offset 0 --metadata

# Check completion stats (now includes orphan count)
# === BACKFILL COMPLETE ===
# Messages processed: 5892
# Processing errors: 0
# Records inserted: numeric=173619, text=8802, null=47119
# Total batch flushes: 463
# Orphaned messages: 4 (check orphaned_messages table)
# ========================

# Query orphaned messages by reason
docker exec roach-postgres psql -U roach -d roach -c \
  "SELECT reason, COUNT(*) FROM orphaned_messages WHERE NOT reprocessed GROUP BY reason;"

# View details of orphaned messages
docker exec roach-postgres psql -U roach -d roach -c \
  "SELECT id, topic, partition, offset, lsid, reason, created_at FROM orphaned_messages ORDER BY created_at DESC LIMIT 10;"
```

### Backfill Service Performance Optimization

#### Changed
- **weatherlink-backfill service**: Dramatic performance improvements (~100x faster)
  - **Async Kafka publishing**: Messages no longer wait for individual delivery confirmations
    - Added `PublishAsync()` method to `weatherlink-lib/kafka/producer.go`
    - Messages are batched automatically by Kafka (50ms linger, 100KB batches)
    - Flush happens once at end of all windows instead of per-message
    - Impact: ~100x faster publishing (1ms vs 100ms per message)
  - **Parallel window processing**: Multiple 24-hour windows processed concurrently
    - Worker pool with configurable parallelism (default: 4 workers)
    - All workers share single rate limiter for API calls
    - Thread-safe deduplication cache with RWMutex
    - Impact: 4x throughput by default
  - **New CLI flag**: `--workers` to control parallelism
    - Default: 4 parallel workers
    - Configurable for different workloads (more windows = more workers beneficial)
    - Example: `--workers 8` for backfilling multiple weeks

#### Added
- **weatherlink-lib/kafka/producer.go**:
  - `PublishAsync()`: Fire-and-forget publishing for bulk operations
  - `Flush()`: Exposed method to wait for all pending messages
  - Kept `Publish()` for backward compatibility (synchronous)
- **Config option**: `ParallelWorkers` in backfill config (default: 4)
- **Logging improvements**:
  - Worker IDs in logs for parallel operation visibility
  - Flush status reporting at completion
  - Error count tracking across workers

#### Technical Details
- **Thread safety**:
  - `existingKeys` map protected by `sync.RWMutex` for concurrent worker access
  - Rate limiter protected by `sync.Mutex` for shared API limit enforcement
  - All workers coordinate through channels and wait groups
- **Error handling**:
  - Failed Kafka deliveries logged asynchronously via event handler
  - Failed API calls don't block other workers
  - Flush timeout (30 seconds) with remaining message count
- **Performance numbers** (estimated for 1 week backfill with 10K messages):
  - Before: Hours (synchronous blocking on each message)
  - After: Minutes (async batching + 4x parallelism)
  - Improvement: ~100x faster overall

#### Usage
```bash
# Default: 4 parallel workers
./scripts/backfill.sh --start "2026-01-11" --end "2026-01-12"

# More workers for many windows (e.g., backfilling weeks/months)
./scripts/backfill.sh --start "2026-01-01" --end "2026-01-20" --workers 8

# Slower API rate if hitting limits
./scripts/backfill.sh --start "2026-01-11" --requests-per-second 5 --workers 4
```

#### Breaking Changes
None - backward compatible. Old `Publish()` method still works.

### Kafka Duplicate Messages Fix - Application-Level Deduplication

#### Fixed
- **weatherlink-backfill service**: Fixed duplicate messages being stored in Kafka when running backfill multiple times
  - **Root Cause**: Kafka's idempotent producer only prevents duplicates from network retries (same producer request ID), not from re-running backfill operations (different producer requests)
  - **Solution**: Implemented application-level deduplication by scanning existing Kafka topics before backfilling
  
#### Added
- **Kafka topic scanner**: New `scanExistingKeys()` function that reads all existing messages from Kafka topics
  - Scans topics: weather.iss, weather.barometer, weather.indoor, weather.health
  - Builds in-memory cache of existing message keys (lsid:timestamp format)
  - Progress logging every 10,000 messages
  - Efficient memory usage (~1MB per 10 years of data)
- **Deduplication cache**: Added `existingKeys` map and `keysMutex` to Service struct
  - Thread-safe access with RWMutex
  - Checks cache before publishing each message
  - Updates cache after successful publish
- **Consumer helper**: New `kafka/consumer.go` in weatherlink-lib for creating scanners
  - Configures consumer to read from beginning (auto.offset.reset=earliest)
  - Temporary group ID for one-time scanning
  - No offset commits (enable.auto.commit=false)

#### Changed
- **weatherlink-backfill service**: Updated backfill workflow
  - Step 1: Fetch sensor metadata (existing)
  - Step 2: **NEW** - Scan Kafka topics to build deduplication cache
  - Step 3: Split time range into 24-hour windows (existing)
  - Step 4: Process windows with duplicate checking (updated)
- **processHistoricData()**: Now returns (published, skipped) counts instead of just published
  - Checks cache before publishing each message
  - Skips messages that already exist
  - Adds new keys to cache after successful publish
- **Logging improvements**: 
  - Removed misleading "broker deduplicates" messages
  - Now logs: "Published X new messages, skipped Y duplicates"
  - Shows count of existing keys found during scan
- **README.md**: Updated duplicate prevention section
  - Explains client-side deduplication approach
  - Clarifies Kafka idempotent producer limitations
  - Updated testing instructions to check skip counts

#### Technical Details
- **Memory overhead**: Negligible (~1MB for 10 years of 5-minute data)
- **Scanning time**: Few seconds for thousands of messages
- **Performance**: O(1) lookup with map-based cache
- **Thread safety**: RWMutex for concurrent access during processing

#### Testing
Run backfill twice with same timestamp range to verify:
```bash
./scripts/backfill.sh --start 1769359875 --end 1769363477
# Check logs: "Published X new messages, skipped 0 duplicates"

./scripts/backfill.sh --start 1769359875 --end 1769363477  # Same range
# Check logs: "Published 0 new messages, skipped X duplicates"
```

### Historical Data Backfill Feature

#### Added
- **weatherlink-lib shared library**: New module at `services/weatherlink-lib/` for code reuse across WeatherLink services
  - `api/` package: WeatherLink API client with authentication and all endpoint methods
  - `models/` package: Common data types and structures
  - `kafka/` package: Idempotent Kafka producer with exactly-once semantics
  - `util/` package: Hash calculation utilities (formerly internal)
  - New `FetchHistoricData()` method in API client with 24-hour window validation
- **weatherlink-backfill service**: New standalone tool for historical data backfill
  - CLI interface with `--start`, `--end`, `--requests-per-second` flags
  - Conservative rate limiter: 8 req/s with burst capacity, exponential backoff on errors
  - Automatic 24-hour window splitting for large date ranges
  - Progress tracking and detailed logging
  - Retry logic with exponential backoff (up to 3 attempts)
  - Docker support with profiles for manual execution
  - Documentation: README.md with usage examples
- Test script: `scripts/test-backfill.sh` for verifying backfill with 1-hour window

#### Changed
- **Service renaming for clarity** (self-documenting, action-oriented names):
  - `weather-publish` → `weatherlink-ingest` (real-time data ingestion from WeatherLink API)
  - `weather-sql` → `weatherlink-materializer` (Kafka to PostgreSQL materialization)
- **weatherlink-ingest** (formerly weather-publish): Refactored to use shared library
  - Removed duplicate code: api/, models/, kafka/, internal/ moved to weatherlink-lib
  - Updated all imports to use `github.com/roach/weatherlink-lib/*`
  - Added Go module replace directive for local development
  - No functional changes, only code organization
- **docker-compose.yml**: Updated service names and added weatherlink-backfill
  - Container names: `roach-weatherlink-ingest`, `roach-weatherlink-materializer`, `roach-weatherlink-backfill`
  - Backfill uses `profiles: [tools]` for manual execution only
- **Documentation updates**:
  - Updated README.md with new service structure and backfill usage
  - Updated `scripts/status.sh` with new container names
  - Created `docs/IMPLEMENTATION_SUMMARY.md` with complete implementation details
  - Created `docs/DEPLOYMENT_CHECKLIST.md` with pre-deployment verification steps

#### Technical Details
- **Duplicate Prevention**: Leverages existing Kafka idempotent producer
  - Messages use unique keys: `lsid:timestamp`
  - Producer configured with `enable.idempotence=true`
  - Kafka automatically deduplicates messages with same key
  - Safe to run backfill multiple times on same time range
- **Rate Limiting**: Token bucket algorithm with conservative limits
  - 8 requests/second (80% of API's 10/s limit)
  - Burst capacity: 16 requests for short bursts
  - Exponential backoff on 429 errors: 1s → 2s → 4s → 8s
  - Hourly tracking: stops at 90% of 1000/hour limit
- **Window Management**: Automatic splitting into 24-hour chunks
  - API limit: historic requests must be ≤ 24 hours
  - Service automatically calculates windows from start to end timestamp
  - Each window: fetch → publish → log progress
- **Shared Library Pattern**: Go modules with replace directive
  - Standard monorepo pattern for microservices
  - Each service maintains own config and business logic
  - Shared code: API client, models, Kafka producer, utilities

#### Breaking Changes
- Container names changed (affects direct docker commands):
  - Old: `roach-weather-publish`, `roach-weather-sql`
  - New: `roach-weatherlink-ingest`, `roach-weatherlink-materializer`
- Service names in docker-compose.yml changed:
  - Commands like `docker compose logs weather-publish` must use `weatherlink-ingest`
- Directory structure changed:
  - `services/weather-publish/` → `services/weatherlink-ingest/`
  - `services/weather-sql/` → `services/weatherlink-materializer/`
  - New: `services/weatherlink-lib/` (shared library)
  - New: `services/weatherlink-backfill/` (backfill tool)

#### Non-Breaking
- Kafka topics unchanged: `weather.iss`, `weather.barometer`, `weather.indoor`, `weather.health`, `weather.metadata.*`
- Database schema unchanged
- Message format and structure unchanged
- API integration unchanged

#### Usage
```bash
# Docker (recommended)
docker compose run --rm weatherlink-backfill --start 1768780863
docker compose run --rm weatherlink-backfill --start 1768780863 --end 1768865863

# Local development
cd services/weatherlink-backfill
go build .
./weatherlink-backfill --start 1768780863 --requests-per-second 5

# Test with 1-hour window
./scripts/test-backfill.sh
```

## 2026-01-25 (earlier)

### Kafka Message Optimization - Metadata Header Cleanup

#### Changed
- **weather-publish service**: Removed redundant headers from metadata messages
  - Sensors metadata: Removed `lsid`, `sensor_type`, `category`, `station_id` headers (already in message body)
  - Catalog metadata: Removed `sensor_type` header (already in message body)
  - Metadata messages now only include `schema_version` header plus catalog-specific headers (`catalog_hash`, `generated_at`)
  - Storage savings: ~1 KB/year (minimal impact, primary benefit is code clarity)
  - No breaking changes: Consumers only read from message body, not headers

#### Removed
- **Documentation cleanup**: Deleted `DEDUPLICATION_VERIFICATION.md`
  - Document referenced legacy `segmentio/kafka-go` library
  - Current implementation uses `confluent-kafka-go/v2` with broker-level idempotency
  - Idempotency documentation consolidated in `docs/kafka-standards.md`

#### Rationale
- Metadata message headers duplicated fields already present in message body
- weather-sql consumers only parse message body, headers were unused
- Reduces message header overhead and improves code clarity
- Aligns metadata messages with data message optimization principles

### Kafka Standards and Storage Optimization

#### Added
- Created `docs/kafka-standards.md` - Comprehensive Kafka best practices guide (900+ lines)
  - Producer/consumer configuration standards
  - Message structure optimization
  - Storage analysis and projections
  - Compression and batching guidelines
  - Monitoring and testing recommendations
  - Troubleshooting guide
  - Future enhancement roadmap

#### Changed
- **weather-publish service**: Implemented Kafka storage optimizations (72% reduction)
  - Enabled LZ4 compression in Kafka producer (60-70% savings)
  - Configured message batching (100KB/50ms) for better compression ratios
  - Optimized message headers: removed 4 redundant headers (station_id, station_id_uuid, category, product_name)
  - Added schema_version header to all messages for future evolution
  - Split large catalog message (3.5 MB) into per-sensor-type messages
- **Cache implementation improvements**:
  - Fixed cache rehydration bug (device_id vs LSID mismatch) that caused duplicates after restart
  - Enhanced cache structure to include data_structure_type dimension for correct deduplication
  - Updated deduplication logic: checkDuplicate() and updateCache() now account for data_structure_type
- **Documentation updates**:
  - Updated `docs/kafka-topics.md` with new optimized header structure
  - Updated `docs/architecture.md` with storage metrics (0.3 MB/day down from 1-3 MB/day)
  - Updated `docs/AI-CONTEXT.md` with optimization notes and references
  - Updated `docs/README.md` to include kafka-standards.md in index

#### Storage Impact
- Daily: 1.1 MB → 0.3 MB (73% reduction)
- Annual: 400 MB → 110 MB (72% reduction)
- 100 years: 40 GB → 11 GB (72% reduction)

#### Technical Details
- **Idempotent Producer** (MAJOR): Migrated from `segmentio/kafka-go` to `confluent-kafka-go/v2`
  - Enables true exactly-once semantics with `enable.idempotence=true`
  - Eliminates duplicate messages on network failures with retries
  - Producer ID (PID) and sequence numbers prevent duplicates at broker level
  - Requires CGO and librdkafka (Alpine packages: gcc, musl-dev, librdkafka-dev)
  - Upgraded Go version: 1.21 → 1.23 (required by confluent-kafka-go/v2)
  - Updated Dockerfile: CGO_ENABLED=1 with dynamic linking to system librdkafka
- Compression: LZ4 codec (2-5% CPU overhead for 60-70% storage savings)
- Batching: 100KB batches with 50ms timeout (minimal latency impact)
- Header optimization: 115 bytes saved per message (12 MB/year across all messages)
- Catalog messages: Split into multiple messages with key format `sensor_type:{id}`
- Schema versioning: All messages now include `schema_version: "1"` header

#### Breaking Changes
- **Consumer impact**: Removed headers require metadata lookups
  - station_id → Lookup via devices table using LSID
  - station_id_uuid → Lookup via devices table
  - category → Derive from sensor_type or lookup
  - product_name → Lookup via devices table
- weather-sql service may require updates if it reads removed headers
- **Build requirements**: Now requires librdkafka at runtime (added to Alpine image)
- **Local development**: Requires Go 1.22+ (Docker uses Go 1.23)

### Documentation Optimization for AI Agents

#### Changed
- Reorganized documentation structure for optimal AI agent consumption
- Created `docs/AI-CONTEXT.md` - single consolidated context file (740 lines) covering 80% of essential information
- Condensed specialized docs while preserving all information:
  - `architecture.md`: 376 → 244 lines (detailed specs)
  - `operations.md`: 410 → 298 lines (advanced operations)
  - `troubleshooting.md`: 519 → 333 lines (comprehensive problem solving)
  - `go-standards.md`: 824 → 394 lines (complete standards, reduced examples)
- Updated `docs/README.md` as ultra-concise entry point
- Removed redundant files by consolidating into AI-CONTEXT.md:
  - `quick-reference.md` (106 lines)
  - `configuration.md` (247 lines)
  - `api-reference.md` (337 lines)
  - `weather-service.md` (549 lines)
- Updated all cross-references in root README.md

#### Benefits
- Reduced "must-read" files from 5-7 to 1-2 for basic understanding
- Reduced total documentation from ~3,500 to ~2,500 lines
- Eliminated redundancy and scattered information
- AI agents can now get 80% context from single file read
- Clear tiered structure: AI-CONTEXT.md for quick start, specialized docs for deep dives
- No information loss - all content preserved, just reorganized

### Service Refactoring

#### Changed
- Refactored weather and weather-sql services from monolithic main.go to modular package structure
- Improved separation of concerns and testability
- Main.go files now serve only as entry points with dependency wiring

#### Technical Details
- **weather-sql service**: Organized into 7 packages
  - `config/` - Environment variable configuration loading
  - `models/` - Data structures and types
  - `cache/` - Thread-safe in-memory caching for devices, tags, and catalog
  - `repository/` - Database operations (devices, tags, catalog, records, orphans)
  - `service/` - Business logic (materializer, metadata processor, catalog processor, data processor, enricher)
  - `kafka/` - Kafka consumer creation
  - `main.go` - Entry point (~75 lines)
- **weather service**: Organized into 6 packages
  - `config/` - Environment variable configuration loading
  - `models/` - API response structures and types
  - `api/` - WeatherLink API client (client, auth, endpoint methods)
  - `kafka/` - Kafka producer
  - `service/` - Business logic (metadata fetching, conditions fetching, cache management)
  - `internal/` - Internal utilities (hash calculation)
  - `main.go` - Entry point (~85 lines)
- All existing functionality preserved
- No breaking changes to external interfaces
- Dockerfiles updated to copy entire package directories

### Fixed

#### Metadata Enrichment - Empty Tag Units and Descriptions
- **Problem**: Tags in the database were missing `unit`, `description`, and `metadata` fields
- **Root Cause**: Full sensor catalog (280 sensor types, 3.4MB) exceeded Kafka's 1MB message limit, causing truncation of the `data_structures` array
- **Solution**: Implemented dynamic catalog filtering to only include sensor types from actual sensors
  - Weather service now dynamically discovers sensor types from `/v2/sensors` API response
  - Catalog filtered before publishing to Kafka (reduces from 3.4MB to ~50-100KB for 4 sensor types)
  - Complete `data_structures` preserved in filtered catalog
  - Tags now automatically enriched with units and descriptions from sensor catalog
- **Changes**: 
  - Added `sensorTypes map[int]bool` field to Service struct to track sensor types dynamically
  - Modified `fetchSensorMetadata()` to extract and track sensor types from API response
  - Modified `fetchSensorCatalog()` to filter catalog entries before publishing
  - Added `getKeysFromMap()` helper function for logging
  - Updated startup order to ensure sensors are fetched before catalog (filtering dependency)
- **Benefits**: No infrastructure changes needed, efficient storage, automatically adapts if sensors are added/removed

#### Kafka-Zookeeper Cluster ID Sync Issue
- **Problem**: Kafka crashed on restart with `InconsistentClusterIdException` due to cluster ID mismatch between Kafka and Zookeeper
- **Root Cause**: Kafka was not persisting its cluster ID because logs were stored in `/tmp/kafka-logs` (ephemeral) while volume was mounted at `/var/lib/kafka/data`
- **Solution**: Added `KAFKA_LOG_DIRS: /var/lib/kafka/data` environment variable to Kafka service in `docker-compose.infrastructure.yml`
- **Result**: Cluster ID now persists across restarts; system is stable
- Updated `docs/troubleshooting.md` with documentation for this issue

### Tag Metadata Enhancement

#### Added
- Database migration framework (`scripts/db/migrate.sh`)
  - Commands: `up`, `down`, `status`, `create`
  - Version tracking in `schema_migrations` table
  - Checksum validation for integrity
- `sensor_catalog` table storing field metadata from WeatherLink API
  - Columns: sensor_type, data_structure_type, field_name, field_type, units, description
- 10 new device metadata columns: product_number, rain_collector_type, active, tx_id, port_number, parent_device_type, parent_device_name, parent_device_id, parent_device_id_hex, data_structure_type
- Catalog consumption in weather-sql service
  - Listens to `weather.metadata.catalog` topic
  - Caches field metadata in memory
  - Auto-enriches tags with units and descriptions
- Migration 001: Enhanced device and tag metadata schema

#### Changed
- Tags now capture `unit` and `description` from sensor catalog
- Devices store complete metadata from sensors API
- Weather service structs expanded to capture all API fields
- Weather-SQL processes data_structure_type from message headers

### PostgreSQL Integration & SQL Materialization

#### Added
- PostgreSQL database added to infrastructure for data materialization
- Database schema with hierarchical design: Devices → Tags → Records
  - `devices` table for sensor metadata
  - `tags` table for field definitions
  - `records_numeric`, `records_text`, `records_null` typed record tables for optimized storage
  - `records` view for unified querying across all record types
  - `orphaned_messages` table for tracking unprocessable messages
- New `weather-sql` service for materializing Kafka messages to PostgreSQL
  - Auto-creates tags when new fields are discovered
  - Listens to metadata updates to keep devices current
  - Handles missing devices/tags gracefully with orphaned message tracking
- Timestamp-based deduplication in weather publisher
  - In-memory cache prevents duplicate messages from being published to Kafka
  - Cache rehydrates from PostgreSQL on restart (last 24 hours)
- PostgreSQL password configuration via `.env` file
- Database initialization scripts that run automatically on first start

#### Changed
- `weather` service renamed to `weather-publish` for clarity
- Weather publisher now connects to PostgreSQL for timestamp cache rehydration
- Added `POSTGRES_DSN` and `POSTGRES_PASSWORD` environment variables
- Docker Compose files updated with new service names and PostgreSQL dependency

#### Technical Details
- Weather publisher prevents duplicates by checking timestamps before publishing to Kafka
- Materialization service uses in-memory caching for devices and tags for performance
- Database initialization scripts run automatically via PostgreSQL init system
- All record tables use UNIQUE constraints to prevent duplicate entries

### Documentation Reorganization
- Created `docs/` directory with organized structure
- Added: architecture.md, configuration.md, operations.md, kafka-topics.md, api-reference.md, troubleshooting.md, weather-service.md, quick-reference.md
- Removed: IMPLEMENTATION.md, RESTRUCTURE_COMPLETE.md, MIGRATION.md, DOCKER_SETUP.md, COMMANDS.md
- Updated root README.md with links to docs

### Docker Compose Restructure

### Changed
- Migrated from Bitnami images to Confluent Platform images
  - `bitnami/zookeeper:latest` → `confluentinc/cp-zookeeper:7.5.0`
  - `bitnami/kafka:latest` → `confluentinc/cp-kafka:7.5.0`
- Separated Docker Compose into two files:
  - `docker-compose.infrastructure.yml` for Kafka infrastructure
  - `docker-compose.yml` for application services
- Converted environment variables from Bitnami format to Confluent format

### Added
- Health checks for Zookeeper and Kafka
- Helper scripts in `scripts/` directory for common operations
- Status monitoring script (`scripts/status.sh`)

### Fixed
- Service startup order (services now wait for Kafka health check)
- Docker image availability issues
- Dockerfile `go.sum` optional copy

## 2026-01-24 - Initial Implementation

### Added
- Kafka broker with infinite data retention
- Zookeeper for Kafka coordination
- Kafka UI web interface
- Weather service (Go) for WeatherLink API integration
- SSL/TLS support for external Kafka access
- Auto-restart configuration for all services
- Comprehensive documentation
- Postman API collection and environment
- Setup verification script

### Features
- Multiple Kafka topics by sensor type
- Metadata tracking with change detection
- Hash-based comparison to avoid duplicate metadata
- 5-minute data fetch interval (configurable)
