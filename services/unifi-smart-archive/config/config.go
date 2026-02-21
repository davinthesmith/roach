package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"unifi-smart-archive/models"
)

// Load reads configuration from environment variables.
func Load() models.Config {
	eventEndTimeout, err := time.ParseDuration(getEnvOrDefault("EVENT_END_TIMEOUT", "1m"))
	if err != nil {
		log.Fatalf("Invalid EVENT_END_TIMEOUT: %v", err)
	}
	workerInterval, err := time.ParseDuration(getEnvOrDefault("WORKER_INTERVAL", "10s"))
	if err != nil {
		log.Fatalf("Invalid WORKER_INTERVAL: %v", err)
	}
	lead := parseIntEnv("LEAD_SECONDS", 0)
	trail := parseIntEnv("TRAIL_SECONDS", 0)
	copyDelay := parseIntEnv("COPY_DELAY_SECONDS", 5)
	retentionDays := parseIntEnv("ARCHIVE_RETENTION_DAYS", 10)
	if retentionDays < 1 {
		retentionDays = 10
	}

	return models.Config{
		KafkaBroker:          getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		KafkaTopic:           getEnvOrDefault("KAFKA_TOPIC", "unifi.protect.smart"),
		KafkaConsumerGroup:   getEnvOrDefault("KAFKA_CONSUMER_GROUP", "unifi-smart-archive"),
		EventEndTimeout:      eventEndTimeout,
		SourceDir:            getEnvOrDefault("SOURCE_DIR", "/data/streams/unifi/jpg"),
		ArchiveDir:           getEnvOrDefault("ARCHIVE_DIR", "/data/streams/unifi/protect"),
		LeadSeconds:          lead,
		TrailSeconds:         trail,
		CopyDelaySeconds:     copyDelay,
		ArchiveRetentionDays: retentionDays,
		WorkerInterval:       workerInterval,
		LogLevel:             getEnvOrDefault("LOG_LEVEL", "info"),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseIntEnv(key string, defaultVal int) int {
	if value := os.Getenv(key); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil {
			log.Fatalf("Invalid %s: %v", key, err)
		}
		return n
	}
	return defaultVal
}
