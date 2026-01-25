-- Migration: add_orphaned_messages
-- Created: 2026-01-25
-- Description: Add orphaned_messages table for tracking messages that couldn't be processed

-- Create orphaned_messages table
CREATE TABLE orphaned_messages (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(200) NOT NULL,
    partition INTEGER NOT NULL,
    "offset" BIGINT NOT NULL,
    lsid INTEGER,
    timestamp BIGINT,
    tag_name VARCHAR(200),
    reason VARCHAR(500),
    message_headers JSONB,
    message_body JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    reprocessed BOOLEAN DEFAULT FALSE,
    UNIQUE(topic, partition, "offset")
);

-- Create index for efficient querying of unprocessed orphaned messages
CREATE INDEX idx_orphaned_reprocessed ON orphaned_messages(reprocessed, created_at);

-- Add comments for documentation
COMMENT ON TABLE orphaned_messages IS 'Tracks Kafka messages that could not be processed due to missing dependencies';
COMMENT ON COLUMN orphaned_messages.topic IS 'Kafka topic name';
COMMENT ON COLUMN orphaned_messages.partition IS 'Kafka partition number';
COMMENT ON COLUMN orphaned_messages."offset" IS 'Kafka message offset';
COMMENT ON COLUMN orphaned_messages.lsid IS 'Logical Sensor ID from the message';
COMMENT ON COLUMN orphaned_messages.timestamp IS 'Timestamp from the message';
COMMENT ON COLUMN orphaned_messages.tag_name IS 'Tag name that failed to process';
COMMENT ON COLUMN orphaned_messages.reason IS 'Reason for failure (e.g., device not found)';
COMMENT ON COLUMN orphaned_messages.message_headers IS 'Kafka message headers';
COMMENT ON COLUMN orphaned_messages.message_body IS 'Full message body for reprocessing';
COMMENT ON COLUMN orphaned_messages.reprocessed IS 'Whether this message has been successfully reprocessed';
