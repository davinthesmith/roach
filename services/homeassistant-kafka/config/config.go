package config

import (
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"homeassistant-kafka/models"
)

// Load reads configuration from environment variables.
func Load() models.Config {
	pollEnabled, err := strconv.ParseBool(getEnvOrDefault("POLL_ENABLED", "false"))
	if err != nil {
		log.Fatalf("Invalid POLL_ENABLED: %v", err)
	}
	pollIntervalStr := getEnvOrDefault("POLL_INTERVAL", "60s")
	pollInterval, err := time.ParseDuration(pollIntervalStr)
	if err != nil {
		log.Fatalf("Invalid POLL_INTERVAL: %v", err)
	}

	backoffStr := getEnvOrDefault("WS_RECONNECT_BACKOFF", "1s,5s,30s")
	wsBackoff, err := parseDurationList(backoffStr)
	if err != nil {
		log.Fatalf("Invalid WS_RECONNECT_BACKOFF: %v", err)
	}

	haURL := os.Getenv("HA_URL")
	haWSURL := os.Getenv("HA_WS_URL")
	if haWSURL == "" && haURL != "" {
		haWSURL = deriveWSURL(haURL)
	}

	cfg := models.Config{
		HAURL:              haURL,
		HAWSURL:            haWSURL,
		HAToken:            os.Getenv("HA_TOKEN"),
		KafkaBroker:        getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		LogLevel:           getEnvOrDefault("LOG_LEVEL", "info"),
		WSReconnectBackoff: wsBackoff,
		PollEnabled:        pollEnabled,
		PollInterval:       pollInterval,
		EntityFilter:       parseCSV(os.Getenv("POLL_ENTITY_FILTER")),
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

func parseDurationList(value string) ([]time.Duration, error) {
	parts := parseCSV(value)
	if len(parts) == 0 {
		return []time.Duration{time.Second}, nil
	}
	durations := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		dur, err := time.ParseDuration(part)
		if err != nil {
			return nil, err
		}
		durations = append(durations, dur)
	}
	return durations, nil
}

func parseCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func deriveWSURL(base string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = "/api/websocket"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func validateRequiredConfig(cfg models.Config) {
	required := []struct {
		key   string
		value string
	}{
		{"HA_URL", cfg.HAURL},
		{"HA_TOKEN", cfg.HAToken},
		{"KAFKA_BROKER", cfg.KafkaBroker},
		{"HA_WS_URL", cfg.HAWSURL},
	}

	for _, item := range required {
		if item.value == "" {
			log.Fatalf("%s is required", item.key)
		}
	}
}
