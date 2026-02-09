package models

import "time"

// Config holds all configuration for the ubiquiti-kafka service.
type Config struct {
	// UniFi API
	UnifiAPIKey string
	UnifiHost   string // NVR URL (e.g. "https://192.168.1.1")

	// Kafka
	KafkaBroker string

	// Service
	LogLevel         string
	ReconnectBackoff []time.Duration
}
