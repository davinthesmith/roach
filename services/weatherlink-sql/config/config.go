package config

import (
	"os"
	"strconv"

	"weatherlink-sql/models"
)

// Load reads configuration from environment variables
func Load() models.Config {
	batchSize := 100
	if bs := os.Getenv("BATCH_SIZE"); bs != "" {
		if val, err := strconv.Atoi(bs); err == nil {
			batchSize = val
		}
	}

	workerPoolSize := 4
	if wps := os.Getenv("WORKER_POOL_SIZE"); wps != "" {
		if val, err := strconv.Atoi(wps); err == nil {
			workerPoolSize = val
		}
	}

	batchFlushIntervalMs := 500
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

	return models.Config{
		KafkaBroker:          getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		PostgresDSN:          os.Getenv("POSTGRES_DSN"),
		LogLevel:             getEnvOrDefault("LOG_LEVEL", "info"),
		BatchSize:            batchSize,
		WorkerPoolSize:       workerPoolSize,
		BatchFlushIntervalMs: batchFlushIntervalMs,
		DBPoolMaxConns:       dbPoolMaxConns,
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
