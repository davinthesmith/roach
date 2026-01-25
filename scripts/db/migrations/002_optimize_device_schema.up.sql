-- Migration: Optimize device schema for API alignment
-- Created: 2026-01-25
-- Description: Add missing API metadata fields (created_date, modified_date) and rename data_structure_type to rt_data_structure_type to indicate real-time data source

-- Add missing API metadata fields
ALTER TABLE devices 
    ADD COLUMN IF NOT EXISTS created_date BIGINT,
    ADD COLUMN IF NOT EXISTS modified_date BIGINT;

-- Rename real-time field to indicate data source
ALTER TABLE devices 
    RENAME COLUMN data_structure_type TO rt_data_structure_type;

-- Add indexes for new timestamp fields
CREATE INDEX IF NOT EXISTS idx_devices_created_date 
    ON devices(created_date);

CREATE INDEX IF NOT EXISTS idx_devices_modified_date 
    ON devices(modified_date);

-- Update existing index name for consistency
DROP INDEX IF EXISTS idx_devices_data_structure_type;
CREATE INDEX IF NOT EXISTS idx_devices_rt_data_structure_type 
    ON devices(rt_data_structure_type);

-- Add comments for documentation
COMMENT ON COLUMN devices.created_date IS 'Unix timestamp when device was created in WeatherLink (from sensors metadata API)';
COMMENT ON COLUMN devices.modified_date IS 'Unix timestamp when device was last modified in WeatherLink (from sensors metadata API)';
COMMENT ON COLUMN devices.rt_data_structure_type IS 'Data structure type ID (from real-time current data messages, not available in sensors metadata)';
