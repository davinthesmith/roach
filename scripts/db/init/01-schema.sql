-- ROACH Database Schema
-- Device/Tag/Record hierarchy for weather data materialization
--
-- Note: This is the initial schema. See migrations/ directory for subsequent changes:
--   - 001_enhance_tag_and_device_metadata: Added product_number, rain_collector_type, parent_device_*, sensor_catalog table
--   - 002_optimize_device_schema: Added created_date, modified_date, renamed data_structure_type to rt_data_structure_type
--   - 003_optimize_records_storage: Removed id, device_id, recorded_at; renamed timestamp to ts; optimized indexes

-- Devices Table: Maps to sensors from WeatherLink
CREATE TABLE devices (
    id SERIAL PRIMARY KEY,
    lsid INTEGER UNIQUE NOT NULL,
    sensor_type INTEGER NOT NULL,
    category VARCHAR(100),
    manufacturer VARCHAR(200),
    product_name VARCHAR(200),
    station_id INTEGER,
    station_id_uuid UUID,
    station_name VARCHAR(200),
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6),
    elevation NUMERIC(10, 3),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_devices_lsid ON devices(lsid);
CREATE INDEX idx_devices_category ON devices(category);

-- Tags Table: Maps to data structure fields/properties for each device
CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    tag_name VARCHAR(200) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    unit VARCHAR(50),
    description TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(device_id, tag_name)
);

CREATE INDEX idx_tags_device_id ON tags(device_id);
CREATE INDEX idx_tags_name ON tags(tag_name);

-- Records Tables: Typed for performance

-- Numeric Records (integers, floats)
CREATE TABLE records_numeric (
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    value NUMERIC,
    ts BIGINT NOT NULL,
    PRIMARY KEY (tag_id, ts)
);

CREATE INDEX idx_records_numeric_tag_ts ON records_numeric(tag_id, ts DESC);

-- Text Records (strings)
CREATE TABLE records_text (
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    value TEXT,
    ts BIGINT NOT NULL,
    PRIMARY KEY (tag_id, ts)
);

CREATE INDEX idx_records_text_tag_ts ON records_text(tag_id, ts DESC);

-- Null Records (track when fields are null)
CREATE TABLE records_null (
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    ts BIGINT NOT NULL,
    PRIMARY KEY (tag_id, ts)
);

CREATE INDEX idx_records_null_tag_ts ON records_null(tag_id, ts DESC);

-- Records View: Union of all record types
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

-- Orphaned Messages Table: Tracks messages that couldn't be processed
CREATE TABLE orphaned_messages (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(200) NOT NULL,
    partition INTEGER NOT NULL,
    offset BIGINT NOT NULL,
    lsid INTEGER,
    timestamp BIGINT,
    tag_name VARCHAR(200),
    reason VARCHAR(500),
    message_headers JSONB,
    message_body JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    reprocessed BOOLEAN DEFAULT FALSE,
    UNIQUE(topic, partition, offset)
);

CREATE INDEX idx_orphaned_reprocessed ON orphaned_messages(reprocessed, created_at);
