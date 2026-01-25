package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

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
	log.Printf("  - Worker Pool Size: %d", cfg.WorkerPoolSize)
	log.Printf("  - Batch Flush Interval: %dms", cfg.BatchFlushIntervalMs)
	log.Printf("  - DB Pool Max Conns: %d", cfg.DBPoolMaxConns)

	// Connect to database with pgxpool
	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Failed to parse PostgreSQL DSN: %v", err)
	}
	
	poolConfig.MaxConns = int32(cfg.DBPoolMaxConns)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}

	log.Println("Connected to PostgreSQL with connection pool")

	// Create Kafka readers
	metadataReader := kafka.NewMetadataReader(cfg)
	catalogReader := kafka.NewCatalogReader(cfg)

	// Create materializer
	materializer, err := service.New(cfg, pool)
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
