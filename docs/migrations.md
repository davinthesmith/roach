# Database Migrations

> **Overview**: [CLAUDE.md](../CLAUDE.md). **Ops**: [operations.md](operations.md). This doc: migration framework and usage.

**Tool**: `scripts/db/migrate.sh`  
**Tracking**: `schema_migrations` table (version, name, applied_at, checksum)

## Commands

```bash
./scripts/db/migrate.sh status           # Applied vs pending
./scripts/db/migrate.sh up               # Apply pending
./scripts/db/migrate.sh down             # Rollback last (prompts)
./scripts/db/migrate.sh create <name>    # Create NNN_<name>.up.sql and .down.sql
```

## Files

**Location**: `scripts/db/migrations/`  
**Naming**: `NNN_description.up.sql`, `NNN_description.down.sql` (e.g. `001_add_orphaned_messages.up.sql`).

**Rules**: `.up.sql` = apply; `.down.sql` = revert. Use `IF EXISTS`/`IF NOT EXISTS` for idempotency. Test rollback before committing.

## Create workflow

```bash
./scripts/db/migrate.sh create add_feature
# Edits: scripts/db/migrations/NNN_add_feature.up.sql and .down.sql
./scripts/db/migrate.sh up
```

## Verification

```sql
SELECT * FROM schema_migrations ORDER BY applied_at;
\d devices
\d tags
SELECT COUNT(*) FROM sensor_catalog;
SELECT tag_name, unit, description FROM tags WHERE unit IS NOT NULL LIMIT 5;
```
