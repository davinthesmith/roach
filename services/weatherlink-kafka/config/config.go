package config

import (
	"log"
	"os"
	"strconv"
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
	backfillEnabled, err := strconv.ParseBool(getEnvOrDefault("KAFKA_BACKFILL_ENABLED", "true"))
	if err != nil {
		log.Fatalf("Invalid KAFKA_BACKFILL_ENABLED: %v", err)
	}
	backfillStartTs, err := strconv.ParseInt(getEnvOrDefault("BACKFILL_START_TS", "0"), 10, 64)
	if err != nil {
		log.Fatalf("Invalid BACKFILL_START_TS: %v", err)
	}
	backfillEndTs, err := strconv.ParseInt(getEnvOrDefault("BACKFILL_END_TS", "0"), 10, 64)
	if err != nil {
		log.Fatalf("Invalid BACKFILL_END_TS: %v", err)
	}
	if backfillEndTs == 0 {
		backfillEndTs = time.Now().Unix()
	}
	if backfillStartTs >= backfillEndTs {
		log.Fatalf("BACKFILL_START_TS must be less than BACKFILL_END_TS")
	}

	cfg := models.Config{
		WeatherLinkAPIKey:     os.Getenv("WEATHERLINK_API_KEY"),
		WeatherLinkAPISecret:  os.Getenv("WEATHERLINK_API_SECRET"),
		WeatherLinkStationID:  os.Getenv("WEATHERLINK_STATION_ID"),
		KafkaBroker:           getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		PostgresDSN:           os.Getenv("POSTGRES_DSN"),
		FetchInterval:         duration,
		MetadataFetchInterval: metadataDuration,
		LogLevel:              getEnvOrDefault("LOG_LEVEL", "info"),

		// Backfill settings
		BackfillEnabled:          backfillEnabled,
		BackfillRequestPerSecond: 8,
		BackfillParallelWorkers:  4,
		BackfillStartTs:          backfillStartTs,
		BackfillEndTs:            backfillEndTs,
	}

	validateRequiredConfig(cfg)

	return cfg
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func validateRequiredConfig(cfg models.Config) {
	required := []struct {
		key   string
		value string
	}{
		{"WEATHERLINK_API_KEY", cfg.WeatherLinkAPIKey},
		{"WEATHERLINK_API_SECRET", cfg.WeatherLinkAPISecret},
		{"WEATHERLINK_STATION_ID", cfg.WeatherLinkStationID},
		{"KAFKA_BROKER", cfg.KafkaBroker},
	}

	for _, item := range required {
		if item.value == "" {
			log.Fatalf("%s is required", item.key)
		}
	}
}
