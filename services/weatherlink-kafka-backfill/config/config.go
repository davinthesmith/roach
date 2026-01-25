package config

import (
	"log"
	"os"
)

// Config holds the backfill service configuration
type Config struct {
	WeatherLinkAPIKey    string
	WeatherLinkAPISecret string
	WeatherLinkStationID string
	KafkaBroker          string
	PostgresDSN          string
	LogLevel             string
	RequestsPerSecond    int // Rate limit (default: 8)
	ParallelWorkers      int // Number of parallel workers (default: 4)
}

// Load reads configuration from environment variables
func Load() Config {
	reqPerSec := 8 // Default conservative rate
	
	return Config{
		WeatherLinkAPIKey:    os.Getenv("WEATHERLINK_API_KEY"),
		WeatherLinkAPISecret: os.Getenv("WEATHERLINK_API_SECRET"),
		WeatherLinkStationID: os.Getenv("WEATHERLINK_STATION_ID"),
		KafkaBroker:          getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		PostgresDSN:          os.Getenv("POSTGRES_DSN"),
		LogLevel:             getEnvOrDefault("LOG_LEVEL", "info"),
		RequestsPerSecond:    reqPerSec,
		ParallelWorkers:      4, // Default: 4 parallel workers
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
	if c.WeatherLinkAPIKey == "" {
		log.Fatal("WEATHERLINK_API_KEY is required")
	}
	if c.WeatherLinkAPISecret == "" {
		log.Fatal("WEATHERLINK_API_SECRET is required")
	}
	if c.WeatherLinkStationID == "" {
		log.Fatal("WEATHERLINK_STATION_ID is required")
	}
	if c.KafkaBroker == "" {
		log.Fatal("KAFKA_BROKER is required")
	}
	return nil
}
