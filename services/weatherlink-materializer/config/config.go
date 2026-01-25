package config

import (
	"os"
	"strconv"

	"weather-sql/models"
)

// Load reads configuration from environment variables
func Load() models.Config {
	batchSize := 100
	if bs := os.Getenv("BATCH_SIZE"); bs != "" {
		if val, err := strconv.Atoi(bs); err == nil {
			batchSize = val
		}
	}

	return models.Config{
		KafkaBroker: getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		PostgresDSN: os.Getenv("POSTGRES_DSN"),
		LogLevel:    getEnvOrDefault("LOG_LEVEL", "info"),
		BatchSize:   batchSize,
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
