# Database Migrations

## Framework

**Location**: `scripts/db/migrate.sh`
**Tracking**: `schema_migrations` table

## Commands

```bash
./scripts/db/migrate.sh status  # Show applied/pending migrations
./scripts/db/migrate.sh up      # Apply pending migrations
./scripts/db/migrate.sh down    # Rollback last migration (prompts confirmation)
./scripts/db/migrate.sh create <name>  # Generate new migration files
```

## Migration Files

**Location**: `scripts/db/migrations/`
**Naming**: `NNN_description.{up,down}.sql`

Example:
- `001_enhance_tag_and_device_metadata.up.sql` - Forward migration
- `001_enhance_tag_and_device_metadata.down.sql` - Rollback migration

## Creating Migrations

```bash
./scripts/db/migrate.sh create add_feature
# Creates:
# - scripts/db/migrations/002_add_feature.up.sql
# - scripts/db/migrations/002_add_feature.down.sql
```

Edit files with SQL:
- `.up.sql` - Apply changes
- `.down.sql` - Revert changes

Use `IF EXISTS`/`IF NOT EXISTS` for idempotency.

## Tracking

`schema_migrations` table stores:
- `version` - Migration filename
- `name` - Human-readable name
- `applied_at` - Timestamp
- `checksum` - MD5 of migration file

## Applied Migrations

**001_enhance_tag_and_device_metadata**
- Added 10 columns to `devices`: product_number, rain_collector_type, active, tx_id, port_number, parent_device_type, parent_device_name, parent_device_id, parent_device_id_hex, data_structure_type
- Created `sensor_catalog` table for field metadata
- Added indexes: `idx_devices_active`, `idx_devices_data_structure_type`, `idx_sensor_catalog_lookup`, `idx_sensor_catalog_field`

**002_optimize_device_schema**
- Added 2 columns to `devices`: `created_date` (BIGINT), `modified_date` (BIGINT) - timestamps from WeatherLink API sensors metadata
- Renamed column: `data_structure_type` → `rt_data_structure_type` to indicate field is populated from real-time current data messages (not sensors metadata)
- Added indexes: `idx_devices_created_date`, `idx_devices_modified_date`, `idx_devices_rt_data_structure_type`
- Updated index: Dropped `idx_devices_data_structure_type`, replaced with `idx_devices_rt_data_structure_type`
- Added column comments documenting data sources (API metadata vs real-time data)
- **Purpose**: Complete API coverage and clear data provenance with `rt_` prefix for real-time fields

## Verification

```sql
-- Check migration status
SELECT * FROM schema_migrations ORDER BY applied_at;

-- Verify schema
\d devices
\d sensor_catalog
\d tags

-- Check catalog population
SELECT COUNT(*) FROM sensor_catalog;

-- View enriched tags
SELECT tag_name, unit, description FROM tags WHERE unit IS NOT NULL LIMIT 5;

-- Check new device timestamp fields (migration 002)
SELECT lsid, category, created_date, modified_date, rt_data_structure_type 
FROM devices 
ORDER BY lsid;

-- Verify indexes for new columns
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'devices' 
  AND (indexname LIKE '%created_date%' OR indexname LIKE '%modified_date%' OR indexname LIKE '%rt_data%');
```
