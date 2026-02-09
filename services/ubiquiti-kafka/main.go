package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ubiquiti-kafka/api"
	"ubiquiti-kafka/config"
	"ubiquiti-kafka/kafka"
	"ubiquiti-kafka/service"
)

func main() {
	cfg := config.Load()
	log.Printf("Starting ubiquiti-kafka service (log_level=%s)", cfg.LogLevel)
	log.Printf("Config: host=%s api_key=%s kafka=%s",
		cfg.UnifiHost, maskSecret(cfg.UnifiAPIKey), cfg.KafkaBroker)

	// Initialize UniFi API client
	apiClient := api.NewClient(cfg.UnifiAPIKey, cfg.UnifiHost, cfg.LogLevel)

	// Initialize Kafka producer
	producer, err := kafka.NewProducer(cfg.KafkaBroker)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	// Create service
	svc := service.New(cfg, apiClient, producer)

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	// Reconnect loop with backoff
	backoffIdx := 0
	for {
		startTime := time.Now()
		err := svc.Start(ctx)
		if ctx.Err() != nil {
			break
		}

		// Reset backoff if connection lasted more than 60 seconds
		if time.Since(startTime) > 60*time.Second {
			backoffIdx = 0
		}

		if backoffIdx >= len(cfg.ReconnectBackoff) {
			backoffIdx = len(cfg.ReconnectBackoff) - 1
		}
		delay := cfg.ReconnectBackoff[backoffIdx]
		backoffIdx++

		log.Printf("Stream disconnected: %v. Reconnecting in %v...", err, delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}

		if ctx.Err() != nil {
			break
		}
	}

	// Flush and close
	if remaining := producer.Flush(10000); remaining > 0 {
		log.Printf("Warning: %d messages not delivered", remaining)
	}
	if err := producer.Close(); err != nil {
		log.Printf("Warning: producer close error: %v", err)
	}
	log.Println("Shutdown complete")
}

// maskSecret masks a secret string, showing first 4 and last 4 characters.
func maskSecret(s string) string {
	if len(s) <= 8 {
		return "***"
	}
	return s[:4] + "..." + s[len(s)-4:]
}
