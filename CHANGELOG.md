# Changelog

## 2026-01-25

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
