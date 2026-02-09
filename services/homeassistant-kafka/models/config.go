package models

import "time"

// Config holds environment configuration for the Home Assistant Kafka service.
type Config struct {
	HAURL              string
	HAWSURL            string
	HAToken            string
	KafkaBroker        string
	LogLevel           string
	WSReconnectBackoff []time.Duration
	PollEnabled        bool
	PollInterval       time.Duration
	EntityFilter       []string
}
