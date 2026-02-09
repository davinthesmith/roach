package models

import "time"

// Config holds environment configuration for the Home Assistant Command service.
type Config struct {
	HAURL              string
	HAWSURL            string
	HAToken            string
	KafkaBroker        string
	KafkaTopic         string
	KafkaConsumerGroup string
	LogLevel           string
	WSReconnectBackoff []time.Duration
}
