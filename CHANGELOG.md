# Changelog

## 2026-01-25

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
