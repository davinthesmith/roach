package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"unifi-video-kafka/api"
	"unifi-video-kafka/config"
	"unifi-video-kafka/kafka"
	"unifi-video-kafka/service"
)

func main() {
	cfg := config.Load()
	log.Printf("Starting unifi-video-kafka service (log_level=%s)", cfg.LogLevel)
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

	// Start the service (blocks until context cancelled or fatal error)
	if err := svc.Start(ctx); err != nil && err != context.Canceled {
		log.Printf("Service error: %v", err)
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
