-- Rollback: Enhance tag and device metadata
-- Created: 2026-01-24
-- Description: Remove added columns and sensor_catalog table

-- Drop indexes first
DROP INDEX IF EXISTS idx_devices_active;
DROP INDEX IF EXISTS idx_devices_data_structure_type;
DROP INDEX IF EXISTS idx_sensor_catalog_field;
DROP INDEX IF EXISTS idx_sensor_catalog_lookup;

-- Drop sensor_catalog table
DROP TABLE IF EXISTS sensor_catalog;

-- Remove added columns from devices table
ALTER TABLE devices 
    DROP COLUMN IF EXISTS data_structure_type,
    DROP COLUMN IF EXISTS parent_device_id_hex,
    DROP COLUMN IF EXISTS parent_device_id,
    DROP COLUMN IF EXISTS parent_device_name,
    DROP COLUMN IF EXISTS parent_device_type,
    DROP COLUMN IF EXISTS port_number,
    DROP COLUMN IF EXISTS tx_id,
    DROP COLUMN IF EXISTS active,
    DROP COLUMN IF EXISTS rain_collector_type,
    DROP COLUMN IF EXISTS product_number;
