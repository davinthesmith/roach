package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"weatherlink-kafka/api"
	"weatherlink-kafka/config"
	"weatherlink-kafka/kafka"
	"weatherlink-kafka/service"
)

func main() {
	log.Println("Starting ROACH Weather Service...")

	cfg := config.Load()

	log.Printf("Configuration loaded:")
	log.Printf("  - Station ID: %s", cfg.WeatherLinkStationID)
	log.Printf("  - Kafka Broker: %s", cfg.KafkaBroker)
	log.Printf("  - Fetch Interval: %s", cfg.FetchInterval)
	log.Printf("  - Metadata Fetch Interval: %s", cfg.MetadataFetchInterval)
	log.Printf("  - Backfill: %t", cfg.BackfillEnabled)

	// log backfill time range if backfill is enabled
	if cfg.BackfillEnabled {
		log.Printf("  - Backfill Time Range: %s to %s",
			time.Unix(cfg.BackfillStartTs, 0).Format("2006-01-02 15:04:05"),
			time.Unix(cfg.BackfillEndTs, 0).Format("2006-01-02 15:04:05"))
	}

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
