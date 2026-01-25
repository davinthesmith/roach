package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"weather-sql/config"
	"weather-sql/kafka"
	"weather-sql/service"
)

func main() {
	log.Println("Starting ROACH Weather SQL Materializer...")

	// Load configuration
	cfg := config.Load()

	if cfg.PostgresDSN == "" {
		log.Fatal("POSTGRES_DSN is required")
	}

	log.Printf("Configuration loaded:")
	log.Printf("  - Kafka Broker: %s", cfg.KafkaBroker)
	log.Printf("  - Batch Size: %d", cfg.BatchSize)

	// Connect to database
	db, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}

	log.Println("Connected to PostgreSQL")

	// Create Kafka readers
	metadataReader := kafka.NewMetadataReader(cfg)
	catalogReader := kafka.NewCatalogReader(cfg)

	// Create materializer
	materializer, err := service.New(cfg, db)
	if err != nil {
		log.Fatalf("Failed to create materializer: %v", err)
	}
	defer materializer.Close()

	materializer.SetReaders(metadataReader, catalogReader)

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

	// Start materializer
	log.Println("Starting materializer...")
	if err := materializer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Materializer error: %v", err)
	}

	log.Println("Materializer stopped")
}
