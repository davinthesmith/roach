-- Migration: Optimize records tables storage efficiency
-- Created: 2026-01-25
-- Description: Remove redundant columns (id, device_id, recorded_at), rename timestamp to ts,
--              use composite primary key (tag_id, ts), and reduce index overhead

-- ====================
-- records_numeric
-- ====================

-- Drop existing indexes
DROP INDEX IF EXISTS idx_records_numeric_tag_ts;
DROP INDEX IF EXISTS idx_records_numeric_device_ts;
DROP INDEX IF EXISTS idx_records_numeric_timestamp;

-- Drop the records view (depends on table columns)
DROP VIEW IF EXISTS records;

-- Drop constraints
ALTER TABLE records_numeric 
    DROP CONSTRAINT IF EXISTS records_numeric_pkey,
    DROP CONSTRAINT IF EXISTS records_numeric_tag_id_timestamp_key;

-- Drop redundant columns
ALTER TABLE records_numeric 
    DROP COLUMN IF EXISTS id,
    DROP COLUMN IF EXISTS device_id,
    DROP COLUMN IF EXISTS recorded_at;

-- Rename timestamp to ts
ALTER TABLE records_numeric 
    RENAME COLUMN timestamp TO ts;

-- Add composite primary key
ALTER TABLE records_numeric
    ADD PRIMARY KEY (tag_id, ts);

-- Create optimized index
CREATE INDEX idx_records_numeric_tag_ts ON records_numeric(tag_id, ts DESC);

-- ====================
-- records_text
-- ====================

-- Drop existing indexes
DROP INDEX IF EXISTS idx_records_text_tag_ts;
DROP INDEX IF EXISTS idx_records_text_device_ts;

-- Drop constraints
ALTER TABLE records_text 
    DROP CONSTRAINT IF EXISTS records_text_pkey,
    DROP CONSTRAINT IF EXISTS records_text_tag_id_timestamp_key;

-- Drop redundant columns
ALTER TABLE records_text 
    DROP COLUMN IF EXISTS id,
    DROP COLUMN IF EXISTS device_id,
    DROP COLUMN IF EXISTS recorded_at;

-- Rename timestamp to ts
ALTER TABLE records_text 
    RENAME COLUMN timestamp TO ts;

-- Add composite primary key
ALTER TABLE records_text
    ADD PRIMARY KEY (tag_id, ts);

-- Create optimized index
CREATE INDEX idx_records_text_tag_ts ON records_text(tag_id, ts DESC);

-- ====================
-- records_null
-- ====================

-- Drop existing indexes
DROP INDEX IF EXISTS idx_records_null_tag_ts;

-- Drop constraints
ALTER TABLE records_null 
    DROP CONSTRAINT IF EXISTS records_null_pkey,
    DROP CONSTRAINT IF EXISTS records_null_tag_id_timestamp_key;

-- Drop redundant columns
ALTER TABLE records_null 
    DROP COLUMN IF EXISTS id,
    DROP COLUMN IF EXISTS device_id,
    DROP COLUMN IF EXISTS recorded_at;

-- Rename timestamp to ts
ALTER TABLE records_null 
    RENAME COLUMN timestamp TO ts;

-- Add composite primary key
ALTER TABLE records_null
    ADD PRIMARY KEY (tag_id, ts);

-- Create optimized index
CREATE INDEX idx_records_null_tag_ts ON records_null(tag_id, ts DESC);

-- ====================
-- Recreate records view
-- ====================

CREATE VIEW records AS
SELECT 
    tag_id,
    value::TEXT as value,
    'numeric' as value_type,
    ts
FROM records_numeric
UNION ALL
SELECT 
    tag_id,
    value,
    'text' as value_type,
    ts
FROM records_text
UNION ALL
SELECT 
    tag_id,
    NULL as value,
    'null' as value_type,
    ts
FROM records_null;

-- Add comments for documentation
COMMENT ON COLUMN records_numeric.ts IS 'Unix timestamp of the record (event time, not insertion time)';
COMMENT ON COLUMN records_text.ts IS 'Unix timestamp of the record (event time, not insertion time)';
COMMENT ON COLUMN records_null.ts IS 'Unix timestamp of the record (event time, not insertion time)';
