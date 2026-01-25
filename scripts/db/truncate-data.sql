-- Truncate data tables for clean backfill
-- Preserves schema_migrations table

TRUNCATE TABLE records_numeric CASCADE;
TRUNCATE TABLE records_text CASCADE;
TRUNCATE TABLE records_null CASCADE;
TRUNCATE TABLE tags CASCADE;
TRUNCATE TABLE devices CASCADE;
TRUNCATE TABLE sensor_catalog CASCADE;

-- Verify truncation
SELECT 
    'devices' as table_name, COUNT(*) as count FROM devices
UNION ALL
SELECT 'tags', COUNT(*) FROM tags
UNION ALL
SELECT 'sensor_catalog', COUNT(*) FROM sensor_catalog
UNION ALL
SELECT 'records_numeric', COUNT(*) FROM records_numeric
UNION ALL
SELECT 'records_text', COUNT(*) FROM records_text
UNION ALL
SELECT 'records_null', COUNT(*) FROM records_null;
