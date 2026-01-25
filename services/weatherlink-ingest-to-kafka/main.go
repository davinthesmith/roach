package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/roach/weatherlink-lib/api"
	"github.com/roach/weatherlink-lib/kafka"
	"weatherlink-ingest-to-kafka/config"
	"weatherlink-ingest-to-kafka/service"
)

func main() {
	log.Println("Starting ROACH Weather Service...")

	// Load configuration
	cfg := config.Load()

	// Validate configuration
	if cfg.WeatherLinkAPIKey == "" {
		log.Fatal("WEATHERLINK_API_KEY is required")
	}
	if cfg.WeatherLinkAPISecret == "" {
		log.Fatal("WEATHERLINK_API_SECRET is required")
	}
	if cfg.WeatherLinkStationID == "" {
		log.Fatal("WEATHERLINK_STATION_ID is required")
	}
	if cfg.KafkaBroker == "" {
		log.Fatal("KAFKA_BROKER is required")
	}

	log.Printf("Configuration loaded:")
	log.Printf("  - Station ID: %s", cfg.WeatherLinkStationID)
	log.Printf("  - Kafka Broker: %s", cfg.KafkaBroker)
	log.Printf("  - Fetch Interval: %s", cfg.FetchInterval)

	// Create API client
	apiClient := api.NewClient(cfg.WeatherLinkAPIKey, cfg.WeatherLinkAPISecret, cfg.WeatherLinkStationID)

	// Create Kafka producer
	producer, err := kafka.NewProducer(cfg.KafkaBroker)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Connect to PostgreSQL (optional)
	var db *sql.DB
	if cfg.PostgresDSN != "" {
		var err error
		db, err = sql.Open("postgres", cfg.PostgresDSN)
		if err != nil {
			log.Printf("Warning: Failed to connect to PostgreSQL: %v", err)
		} else {
			defer db.Close()
			if err := db.Ping(); err != nil {
				log.Printf("Warning: Failed to ping PostgreSQL: %v", err)
				db = nil
			} else {
				log.Println("Connected to PostgreSQL")
			}
		}
	}

	// Create service
	svc := service.New(cfg, apiClient, producer, db)

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Start service
	log.Println("Starting weather service...")
	if err := svc.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Service error: %v", err)
	}

	log.Println("Weather service stopped")
}
