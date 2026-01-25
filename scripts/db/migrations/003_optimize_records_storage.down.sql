-- Migration Rollback: Restore original records tables schema
-- Created: 2026-01-25
-- Description: Restore id, device_id, recorded_at columns, rename ts back to timestamp,
--              restore original indexes and constraints

-- Drop the records view (depends on table columns)
DROP VIEW IF EXISTS records;

-- ====================
-- records_numeric
-- ====================

-- Drop optimized index and primary key
DROP INDEX IF EXISTS idx_records_numeric_tag_ts;
ALTER TABLE records_numeric
    DROP CONSTRAINT IF EXISTS records_numeric_pkey;

-- Rename ts back to timestamp and restore columns
ALTER TABLE records_numeric
    RENAME COLUMN ts TO timestamp;

ALTER TABLE records_numeric
    ADD COLUMN id BIGSERIAL,
    ADD COLUMN device_id INTEGER,
    ADD COLUMN recorded_at TIMESTAMP DEFAULT NOW();

-- Populate device_id from tags table
UPDATE records_numeric rn
SET device_id = t.device_id
FROM tags t
WHERE rn.tag_id = t.id;

-- Add NOT NULL constraint after populating
ALTER TABLE records_numeric
    ALTER COLUMN device_id SET NOT NULL;

-- Restore original primary key and constraints
ALTER TABLE records_numeric
    ADD PRIMARY KEY (id),
    ADD CONSTRAINT records_numeric_tag_id_timestamp_key UNIQUE (tag_id, timestamp);

-- Restore original indexes
CREATE INDEX idx_records_numeric_tag_ts ON records_numeric(tag_id, timestamp DESC);
CREATE INDEX idx_records_numeric_device_ts ON records_numeric(device_id, timestamp DESC);
CREATE INDEX idx_records_numeric_timestamp ON records_numeric(timestamp DESC);

-- ====================
-- records_text
-- ====================

-- Drop optimized index and primary key
DROP INDEX IF EXISTS idx_records_text_tag_ts;
ALTER TABLE records_text
    DROP CONSTRAINT IF EXISTS records_text_pkey;

-- Rename ts back to timestamp and restore columns
ALTER TABLE records_text
    RENAME COLUMN ts TO timestamp;

ALTER TABLE records_text
    ADD COLUMN id BIGSERIAL,
    ADD COLUMN device_id INTEGER,
    ADD COLUMN recorded_at TIMESTAMP DEFAULT NOW();

-- Populate device_id from tags table
UPDATE records_text rt
SET device_id = t.device_id
FROM tags t
WHERE rt.tag_id = t.id;

-- Add NOT NULL constraint after populating
ALTER TABLE records_text
    ALTER COLUMN device_id SET NOT NULL;

-- Restore original primary key and constraints
ALTER TABLE records_text
    ADD PRIMARY KEY (id),
    ADD CONSTRAINT records_text_tag_id_timestamp_key UNIQUE (tag_id, timestamp);

-- Restore original indexes
CREATE INDEX idx_records_text_tag_ts ON records_text(tag_id, timestamp DESC);
CREATE INDEX idx_records_text_device_ts ON records_text(device_id, timestamp DESC);

-- ====================
-- records_null
-- ====================

-- Drop optimized index and primary key
DROP INDEX IF EXISTS idx_records_null_tag_ts;
ALTER TABLE records_null
    DROP CONSTRAINT IF EXISTS records_null_pkey;

-- Rename ts back to timestamp and restore columns
ALTER TABLE records_null
    RENAME COLUMN ts TO timestamp;

ALTER TABLE records_null
    ADD COLUMN id BIGSERIAL,
    ADD COLUMN device_id INTEGER,
    ADD COLUMN recorded_at TIMESTAMP DEFAULT NOW();

-- Populate device_id from tags table
UPDATE records_null rn
SET device_id = t.device_id
FROM tags t
WHERE rn.tag_id = t.id;

-- Add NOT NULL constraint after populating
ALTER TABLE records_null
    ALTER COLUMN device_id SET NOT NULL;

-- Restore original primary key and constraints
ALTER TABLE records_null
    ADD PRIMARY KEY (id),
    ADD CONSTRAINT records_null_tag_id_timestamp_key UNIQUE (tag_id, timestamp);

-- Restore original indexes
CREATE INDEX idx_records_null_tag_ts ON records_null(tag_id, timestamp DESC);

-- ====================
-- Recreate records view with original schema
-- ====================

CREATE VIEW records AS
SELECT 
    id,
    tag_id,
    device_id,
    value::TEXT as value,
    'numeric' as value_type,
    timestamp,
    recorded_at
FROM records_numeric
UNION ALL
SELECT 
    id,
    tag_id,
    device_id,
    value,
    'text' as value_type,
    timestamp,
    recorded_at
FROM records_text
UNION ALL
SELECT 
    id,
    tag_id,
    device_id,
    NULL as value,
    'null' as value_type,
    timestamp,
    recorded_at
FROM records_null;
