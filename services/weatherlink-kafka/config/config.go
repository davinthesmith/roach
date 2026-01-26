package config

import (
	"log"
	"os"
	"time"

	"weatherlink-kafka/models"
)

// Load reads configuration from environment variables
func Load() models.Config {
	fetchInterval := os.Getenv("FETCH_INTERVAL")
	if fetchInterval == "" {
		fetchInterval = "5m"
	}
	duration, err := time.ParseDuration(fetchInterval)
	if err != nil {
		log.Fatalf("Invalid FETCH_INTERVAL: %v", err)
	}
	metadataFetchInterval := os.Getenv("METADATA_FETCH_INTERVAL")
	if metadataFetchInterval == "" {
		metadataFetchInterval = "168h"
	}
	metadataDuration, err := time.ParseDuration(metadataFetchInterval)
	if err != nil {
		log.Fatalf("Invalid METADATA_FETCH_INTERVAL: %v", err)
	}

	return models.Config{
		WeatherLinkAPIKey:     os.Getenv("WEATHERLINK_API_KEY"),
		WeatherLinkAPISecret:  os.Getenv("WEATHERLINK_API_SECRET"),
		WeatherLinkStationID:  os.Getenv("WEATHERLINK_STATION_ID"),
		KafkaBroker:           getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		PostgresDSN:           os.Getenv("POSTGRES_DSN"),
		FetchInterval:         duration,
		MetadataFetchInterval: metadataDuration,
		LogLevel:              getEnvOrDefault("LOG_LEVEL", "info"),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
