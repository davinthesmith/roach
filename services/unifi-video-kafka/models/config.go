package models

import "time"

// Config holds all configuration for the unifi-video-kafka service.
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

// CameraInfo holds basic camera metadata from the Protect API.
type CameraInfo struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"` // "CONNECTED", "DISCONNECTED", etc.
}

// IsConnected returns true if the camera is online and available for streaming.
func (c CameraInfo) IsConnected() bool {
	return c.State == "CONNECTED"
}
