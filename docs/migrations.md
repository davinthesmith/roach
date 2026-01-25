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
```
