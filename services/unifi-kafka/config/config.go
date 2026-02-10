package config

import (
	"log"
	"os"
	"strings"
	"time"

	"unifi-kafka/models"
)

// Load reads configuration from environment variables and returns a Config.
// Fatals on missing required values.
func Load() models.Config {
	cfg := models.Config{
		UnifiAPIKey: requireEnv("UNIFI_API_KEY"),
		UnifiHost:   requireEnv("UNIFI_HOST"),
		KafkaBroker: getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		LogLevel:    getEnvOrDefault("LOG_LEVEL", "info"),
	}

	// Parse reconnect backoff durations
	backoffStr := getEnvOrDefault("RECONNECT_BACKOFF", "1s,5s,30s")
	for _, s := range strings.Split(backoffStr, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil {
			log.Fatalf("Invalid RECONNECT_BACKOFF value %q: %v", s, err)
		}
		cfg.ReconnectBackoff = append(cfg.ReconnectBackoff, d)
	}

	return cfg
}

func requireEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Required environment variable %s is not set", key)
	}
	return val
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
