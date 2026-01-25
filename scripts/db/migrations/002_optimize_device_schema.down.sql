-- Migration rollback: Optimize device schema for API alignment
-- Created: 2026-01-25
-- Description: Revert schema changes from migration 002

-- Rename back to original
ALTER TABLE devices 
    RENAME COLUMN rt_data_structure_type TO data_structure_type;

-- Remove added columns
ALTER TABLE devices 
    DROP COLUMN IF EXISTS modified_date,
    DROP COLUMN IF EXISTS created_date;

-- Restore original index
DROP INDEX IF EXISTS idx_devices_rt_data_structure_type;
CREATE INDEX IF NOT EXISTS idx_devices_data_structure_type 
    ON devices(data_structure_type);

-- Remove indexes
DROP INDEX IF EXISTS idx_devices_modified_date;
DROP INDEX IF EXISTS idx_devices_created_date;
