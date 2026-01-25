# Changelog

## 2026-01-25

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
