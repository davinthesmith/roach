package models

import "time"

// Config holds environment configuration for unifi-smart-archive.
type Config struct {
	KafkaBroker          string
	KafkaTopic           string
	KafkaConsumerGroup   string
	EventEndTimeout      time.Duration // if no message for this event within this duration, stop waiting for end (don't archive)
	SourceDir            string        // unifi-video-jpg output (read-only)
	ArchiveDir           string        // long-term archive base (read-write)
	LeadSeconds          int           // seconds before event start to include
	TrailSeconds         int           // seconds after event end to include
	CopyDelaySeconds     int           // extra delay after end+trail before copy
	ArchiveRetentionDays int           // delete archive content older than this
	WorkerInterval       time.Duration // copy worker and retention tick interval
	LogLevel             string
}
