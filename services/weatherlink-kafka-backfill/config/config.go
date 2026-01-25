package config

import (
	"os"
	"strconv"
	"strings"
)

// Config holds the kafka-backfill service configuration
type Config struct {
	KafkaBroker          string
	PostgresDSN          string
	LogLevel             string
	BatchSize            int
	WorkerPoolSize       int
	BatchFlushIntervalMs int
	DBPoolMaxConns       int
	Topics               []string
	StartOffset          int64 // -2 = earliest, -1 = latest, or specific offset
	EndOffset            int64  // -1 = latest, or specific offset
	IncludeMetadata      bool   // Enable metadata topic backfill
}

// Load reads configuration from environment variables
func Load() Config {
	batchSize := 500
	if bs := os.Getenv("BATCH_SIZE"); bs != "" {
		if val, err := strconv.Atoi(bs); err == nil {
			batchSize = val
		}
	}

	workerPoolSize := 8
	if wps := os.Getenv("WORKER_POOL_SIZE"); wps != "" {
		if val, err := strconv.Atoi(wps); err == nil {
			workerPoolSize = val
		}
	}

	batchFlushIntervalMs := 1000
	if bfi := os.Getenv("BATCH_FLUSH_INTERVAL_MS"); bfi != "" {
		if val, err := strconv.Atoi(bfi); err == nil {
			batchFlushIntervalMs = val
		}
	}

	dbPoolMaxConns := 10
	if dbc := os.Getenv("DB_POOL_MAX_CONNS"); dbc != "" {
		if val, err := strconv.Atoi(dbc); err == nil {
			dbPoolMaxConns = val
		}
	}

	// Parse topics (comma-separated)
	topicsStr := getEnvOrDefault("TOPICS", "weather.iss,weather.barometer,weather.indoor,weather.health")
	topics := strings.Split(topicsStr, ",")
	for i := range topics {
		topics[i] = strings.TrimSpace(topics[i])
	}

	startOffset := int64(-2) // Default: earliest
	if so := os.Getenv("START_OFFSET"); so != "" {
		if val, err := strconv.ParseInt(so, 10, 64); err == nil {
			startOffset = val
		}
	}

	endOffset := int64(-1) // Default: latest
	if eo := os.Getenv("END_OFFSET"); eo != "" {
		if val, err := strconv.ParseInt(eo, 10, 64); err == nil {
			endOffset = val
		}
	}

	includeMetadata := false
	if im := os.Getenv("INCLUDE_METADATA"); im != "" {
		if val, err := strconv.ParseBool(im); err == nil {
			includeMetadata = val
		}
	}

	return Config{
		KafkaBroker:          getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		PostgresDSN:          os.Getenv("POSTGRES_DSN"),
		LogLevel:             getEnvOrDefault("LOG_LEVEL", "info"),
		BatchSize:            batchSize,
		WorkerPoolSize:       workerPoolSize,
		BatchFlushIntervalMs: batchFlushIntervalMs,
		DBPoolMaxConns:       dbPoolMaxConns,
		Topics:               topics,
		StartOffset:          startOffset,
		EndOffset:            endOffset,
		IncludeMetadata:      includeMetadata,
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Validate checks if required configuration is present
func (c *Config) Validate() error {
	if c.PostgresDSN == "" {
		return nil // Will be handled by main
	}
	if c.KafkaBroker == "" {
		return nil // Will be handled by main
	}
	return nil
}
