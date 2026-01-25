-- Migration: Enhance tag and device metadata
-- Created: 2026-01-24
-- Description: Add missing device fields and create sensor_catalog table for field metadata

-- Add missing columns to devices table
ALTER TABLE devices 
    ADD COLUMN IF NOT EXISTS product_number VARCHAR(100),
    ADD COLUMN IF NOT EXISTS rain_collector_type INTEGER,
    ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT true,
    ADD COLUMN IF NOT EXISTS tx_id INTEGER,
    ADD COLUMN IF NOT EXISTS port_number INTEGER,
    ADD COLUMN IF NOT EXISTS parent_device_type VARCHAR(100),
    ADD COLUMN IF NOT EXISTS parent_device_name VARCHAR(200),
    ADD COLUMN IF NOT EXISTS parent_device_id BIGINT,
    ADD COLUMN IF NOT EXISTS parent_device_id_hex VARCHAR(50),
    ADD COLUMN IF NOT EXISTS data_structure_type INTEGER;

-- Create sensor_catalog table to store field metadata from API catalog
CREATE TABLE IF NOT EXISTS sensor_catalog (
    id SERIAL PRIMARY KEY,
    sensor_type INTEGER NOT NULL,
    data_structure_type VARCHAR(10) NOT NULL,
    field_name VARCHAR(200) NOT NULL,
    field_type VARCHAR(50),
    units VARCHAR(100),
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(sensor_type, data_structure_type, field_name)
);

-- Create indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_sensor_catalog_lookup 
    ON sensor_catalog(sensor_type, data_structure_type);

CREATE INDEX IF NOT EXISTS idx_sensor_catalog_field 
    ON sensor_catalog(field_name);

-- Add indexes on new device columns
CREATE INDEX IF NOT EXISTS idx_devices_data_structure_type 
    ON devices(data_structure_type);

CREATE INDEX IF NOT EXISTS idx_devices_active 
    ON devices(active);

-- Add comment for documentation
COMMENT ON TABLE sensor_catalog IS 'Stores field metadata from WeatherLink sensor catalog API';
COMMENT ON COLUMN sensor_catalog.sensor_type IS 'Sensor type ID from WeatherLink API';
COMMENT ON COLUMN sensor_catalog.data_structure_type IS 'Data structure type ID for this sensor';
COMMENT ON COLUMN sensor_catalog.field_name IS 'Name of the data field (e.g., temp, hum)';
COMMENT ON COLUMN sensor_catalog.field_type IS 'Data type (float, integer, string)';
COMMENT ON COLUMN sensor_catalog.units IS 'Measurement units (e.g., degrees Fahrenheit)';
COMMENT ON COLUMN sensor_catalog.description IS 'Human-readable description of the field';
